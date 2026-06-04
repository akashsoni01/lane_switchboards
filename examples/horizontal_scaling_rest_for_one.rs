//! Horizontal scaling + RestForOne: each cluster node runs **processor** (order=0)
//! and **reporter** (order=1) under one supervisor. Scale out by joining nodes;
//! dispatch to one node (`send_by_key`), many nodes (`broadcast` / `send_all`), or
//! a named subset (`send_to`).
//!
//! Run: `cargo run --example horizontal_scaling_rest_for_one`
//! See: `examples/horizontal_scaling_rest_for_one.md`

use lane_switchboards::actor::{Actor, ActorProcessingErr, ActorRef};
use lane_switchboards::distributed::{serve_actor, Cluster};
use lane_switchboards::supervisor::{
    spawn_child_spec, ChildRegistry, RestartStrategy, Supervisor, SupervisorConfig,
};
use lane_switchboards::prost::Message;
use std::sync::Arc;
use std::time::Duration;

// --- Remote wire message (one type per cluster) ---

#[derive(Clone, PartialEq, Message)]
struct WorkMsg {
    #[prost(oneof = "work_msg::Kind", tags = "1, 2, 3")]
    kind: Option<work_msg::Kind>,
}

mod work_msg {
    use super::{WorkFail, WorkPing, WorkProcess};
    use lane_switchboards::prost::Oneof;

    #[derive(Clone, PartialEq, Oneof)]
    pub enum Kind {
        #[prost(message, tag = "1")]
        Process(WorkProcess),
        #[prost(message, tag = "2")]
        Ping(WorkPing),
        #[prost(message, tag = "3")]
        FailProcessor(WorkFail),
    }
}

#[derive(Clone, PartialEq, Message)]
struct WorkPing {}

#[derive(Clone, PartialEq, Message)]
struct WorkFail {}

#[derive(Clone, PartialEq, Message)]
struct WorkProcess {
    #[prost(uint64, tag = "1")]
    job_id: u64,
    #[prost(double, tag = "2")]
    value: f64,
}

impl WorkMsg {
    fn process(job_id: u64, value: f64) -> Self {
        Self {
            kind: Some(work_msg::Kind::Process(WorkProcess { job_id, value })),
        }
    }

    fn ping() -> Self {
        Self {
            kind: Some(work_msg::Kind::Ping(WorkPing {})),
        }
    }

    fn fail_processor() -> Self {
        Self {
            kind: Some(work_msg::Kind::FailProcessor(WorkFail {})),
        }
    }
}

// --- Local roles (RestForOne order + registry key) ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalRole {
    Processor,
    Reporter,
}

impl LocalRole {
    const ALL: [Self; 2] = [Self::Processor, Self::Reporter];

    const fn order(self) -> usize {
        match self {
            Self::Processor => 0,
            Self::Reporter => 1,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Processor => "processor",
            Self::Reporter => "reporter",
        }
    }
}

// --- Local supervised messages (tagged by role) ---

#[derive(Debug, Clone)]
enum LocalMsg {
    Processor(ProcessorMsg),
    Reporter(ReporterMsg),
}

#[derive(Debug, Clone)]
enum ProcessorMsg {
    Compute { job_id: u64, value: f64 },
    Fail,
}

#[derive(Debug, Clone)]
enum ReporterMsg {
    Report { job_id: u64, line: String },
}

async fn role_ref(
    registry: &ChildRegistry<LocalMsg>,
    role: LocalRole,
) -> Option<ActorRef<LocalMsg>> {
    registry.get(role.name()).await
}

struct LocalWorker {
    role: LocalRole,
    site: String,
    registry: Arc<ChildRegistry<LocalMsg>>,
}

#[async_trait::async_trait]
impl Actor<LocalMsg> for LocalWorker {
    async fn pre_start(&mut self) -> Result<(), ActorProcessingErr> {
        self.registry.bump_generation(self.role.name()).await;
        println!(
            "[{}] {} generation {}",
            self.site,
            self.role.name(),
            self.registry.generation(self.role.name()).await
        );
        Ok(())
    }

    async fn handle(&mut self, msg: LocalMsg) -> Result<(), ActorProcessingErr> {
        match (self.role, msg) {
            (LocalRole::Processor, LocalMsg::Processor(ProcessorMsg::Compute { job_id, value })) => {
                let result = value * 2.0;
                println!(
                    "[{}] processor job {job_id}: {value} -> {result}",
                    self.site
                );
                if let Some(reporter) = role_ref(&self.registry, LocalRole::Reporter).await {
                    let _ = reporter
                        .send(LocalMsg::Reporter(ReporterMsg::Report {
                            job_id,
                            line: format!("computed {result}"),
                        }))
                        .await;
                }
            }
            (LocalRole::Processor, LocalMsg::Processor(ProcessorMsg::Fail)) => {
                panic!("processor fault (RestForOne will restart reporter too)")
            }
            (LocalRole::Reporter, LocalMsg::Reporter(ReporterMsg::Report { job_id, line })) => {
                println!("[{}] reporter job {job_id}: {line}", self.site);
            }
            _ => {}
        }
        Ok(())
    }
}

/// RestForOne pair on one machine: processor (0) + reporter (1).
struct SupervisedPair {
    registry: Arc<ChildRegistry<LocalMsg>>,
    _supervisor: lane_switchboards::supervisor::SupervisorHandle<LocalMsg>,
}

impl SupervisedPair {
    async fn start(site: &str) -> Result<Self, ActorProcessingErr> {
        let registry = Arc::new(ChildRegistry::new());
        let site = site.to_string();

        let children: Vec<_> = LocalRole::ALL
            .into_iter()
            .map(|role| {
                let site = site.clone();
                let registry = registry.clone();
                spawn_child_spec(role.order(), role.name(), registry.clone(), move || LocalWorker {
                    role,
                    site: site.clone(),
                    registry: registry.clone(),
                })
            })
            .collect();

        let handle = Supervisor::new(
            SupervisorConfig {
                strategy: RestartStrategy::RestForOne,
                max_restarts: 10,
                within_secs: 60,
                ..Default::default()
            },
            children,
        )
        .start_settled(Duration::from_millis(30))
        .await?;

        Ok(Self {
            registry,
            _supervisor: handle,
        })
    }

    async fn generation(&self, role: LocalRole) -> u64 {
        self.registry.generation(role.name()).await
    }
}

/// Gateway: remote entry point; fans out to local processor + reporter.
struct Gateway {
    site: String,
    registry: Arc<ChildRegistry<LocalMsg>>,
}

#[async_trait::async_trait]
impl Actor<WorkMsg> for Gateway {
    async fn handle(&mut self, msg: WorkMsg) -> Result<(), ActorProcessingErr> {
        match msg.kind {
            Some(work_msg::Kind::Process(WorkProcess { job_id, value })) => {
                if let Some(processor) = role_ref(&self.registry, LocalRole::Processor).await {
                    processor
                        .send(LocalMsg::Processor(ProcessorMsg::Compute { job_id, value }))
                        .await?;
                }
            }
            Some(work_msg::Kind::Ping(_)) => {
                println!("[{}] gateway ping", self.site);
            }
            Some(work_msg::Kind::FailProcessor(_)) => {
                if let Some(processor) = role_ref(&self.registry, LocalRole::Processor).await {
                    let _ = processor
                        .send(LocalMsg::Processor(ProcessorMsg::Fail))
                        .await;
                }
            }
            None => {}
        }
        Ok(())
    }
}

struct WorkerSite {
    pair: SupervisedPair,
    _gateway: lane_switchboards::distributed::NodeHandle<WorkMsg>,
}

impl WorkerSite {
    async fn launch(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let pair = SupervisedPair::start(name)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let gateway = Gateway {
            site: name.to_string(),
            registry: pair.registry.clone(),
        };
        let handle = serve_actor(name, "127.0.0.1:0", "gateway", gateway).await?;
        println!(
            "[cluster] site {name} online at {} (RestForOne: {}=0, {}=1)",
            handle.address(),
            LocalRole::Processor.name(),
            LocalRole::Reporter.name(),
        );
        Ok(Self {
            pair,
            _gateway: handle,
        })
    }

    fn member(&self) -> lane_switchboards::distributed::ClusterMember {
        self._gateway.member.clone()
    }
}

fn join(cluster: &mut Cluster<WorkMsg>, site: &WorkerSite) {
    println!(
        "[cluster] joining {} ({} nodes in ring)",
        site._gateway.name(),
        cluster.len() + 1
    );
    cluster.join(site.member());
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("=== Phase 1: two sites, each with RestForOne processor + reporter ===\n");

    let site_a = WorkerSite::launch("site-a").await?;
    let site_b = WorkerSite::launch("site-b").await?;

    let mut cluster = Cluster::new();
    join(&mut cluster, &site_a);
    join(&mut cluster, &site_b);

    cluster
        .send_by_key(&42u64, WorkMsg::process(42, 10.0))
        .await?;

    tokio::time::sleep(Duration::from_millis(80)).await;

    println!("\n=== Send to multiple nodes at once ===\n");

    cluster.broadcast(WorkMsg::ping()).await?;
    println!("[coordinator] broadcast Ping → all nodes\n");

    let results = cluster
        .send_all(WorkMsg::process(100, 3.0))
        .await;
    for (name, result) in &results {
        println!("[coordinator] send_all → {name}: {result:?}");
    }

    let subset = cluster
        .send_to(
            &["site-a"],
            WorkMsg::process(101, 5.0),
        )
        .await;
    println!("[coordinator] send_to [site-a]: {:?}\n", subset);

    let replicas = cluster
        .send_replicas(&7u64, 2, WorkMsg::process(7, 1.0))
        .await;
    println!("[coordinator] send_replicas(key=7, n=2): {:?}\n", replicas);

    tokio::time::sleep(Duration::from_millis(120)).await;

    println!("=== Phase 2: scale out (+2 sites) ===\n");

    let site_c = WorkerSite::launch("site-c").await?;
    let site_d = WorkerSite::launch("site-d").await?;
    join(&mut cluster, &site_c);
    join(&mut cluster, &site_d);

    cluster
        .send_all(WorkMsg::ping())
        .await
        .into_iter()
        .for_each(|(name, _)| println!("[coordinator] ping after scale-out → {name}"));

    println!("\n=== RestForOne on one site (processor fail → reporter restarts) ===\n");
    let before_p = site_a.pair.generation(LocalRole::Processor).await;
    let before_r = site_a.pair.generation(LocalRole::Reporter).await;

    cluster.send_to(&["site-a"], WorkMsg::fail_processor()).await;
    tokio::time::sleep(Duration::from_millis(150)).await;

    let after_p = site_a.pair.generation(LocalRole::Processor).await;
    let after_r = site_a.pair.generation(LocalRole::Reporter).await;
    println!(
        "[site-a] {} gen {before_p} -> {after_p}, {} gen {before_r} -> {after_r}",
        LocalRole::Processor.name(),
        LocalRole::Reporter.name(),
    );

    cluster
        .send_by_key(&99u64, WorkMsg::process(99, 2.0))
        .await?;

    tokio::time::sleep(Duration::from_millis(100)).await;
    println!(
        "\nDone — {} sites in cluster, each running RestForOne processor + reporter.",
        cluster.len()
    );
    Ok(())
}
