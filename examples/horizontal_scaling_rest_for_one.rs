//! Horizontal scaling + RestForOne: each cluster node runs **processor** (order=0)
//! and **reporter** (order=1) under one supervisor. Scale out by joining nodes;
//! dispatch to one node (`send_by_key`), many nodes (`broadcast` / `send_all`), or
//! a named subset (`send_to`).
//!
//! Run: `cargo run --example horizontal_scaling_rest_for_one`
//! See: `examples/horizontal_scaling_rest_for_one.md`

use lane_switchboards::actor::{Actor, ActorProcessingErr};
use lane_switchboards::distributed::{serve_actor, Cluster};
use lane_switchboards::supervisor::{
    spawn_child_spec, ChildRegistry, RestartStrategy, Supervisor, SupervisorConfig,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

// --- Remote wire message (one type per cluster) ---

#[derive(Debug, Clone, Serialize, Deserialize)]
enum WorkMsg {
    Process { job_id: u64, value: f64 },
    Ping,
    /// Trigger processor failure → RestForOne restarts processor + reporter on that node.
    FailProcessor,
}

// --- Local supervised actors (share LocalMsg under RestForOne) ---

#[derive(Debug, Clone)]
enum LocalMsg {
    Compute { job_id: u64, value: f64 },
    Report { job_id: u64, line: String },
    Fail,
}

struct Processor {
    site: String,
    registry: Arc<ChildRegistry<LocalMsg>>,
}

#[async_trait::async_trait]
impl Actor<LocalMsg> for Processor {
    async fn pre_start(&mut self) -> Result<(), ActorProcessingErr> {
        self.registry.bump_generation("processor").await;
        println!(
            "[{}] processor generation {}",
            self.site,
            self.registry.generation("processor").await
        );
        Ok(())
    }

    async fn handle(&mut self, msg: LocalMsg) -> Result<(), ActorProcessingErr> {
        match msg {
            LocalMsg::Compute { job_id, value } => {
                let result = value * 2.0;
                println!("[{}] processor job {job_id}: {value} -> {result}", self.site);
                if let Some(reporter) = self.registry.get("reporter").await {
                    let _ = reporter
                        .send(LocalMsg::Report {
                            job_id,
                            line: format!("computed {result}"),
                        })
                        .await;
                }
            }
            LocalMsg::Fail => panic!("processor fault (RestForOne will restart reporter too)"),
            LocalMsg::Report { .. } => {}
        }
        Ok(())
    }
}

struct Reporter {
    site: String,
    registry: Arc<ChildRegistry<LocalMsg>>,
}

#[async_trait::async_trait]
impl Actor<LocalMsg> for Reporter {
    async fn pre_start(&mut self) -> Result<(), ActorProcessingErr> {
        self.registry.bump_generation("reporter").await;
        println!(
            "[{}] reporter generation {}",
            self.site,
            self.registry.generation("reporter").await
        );
        Ok(())
    }

    async fn handle(&mut self, msg: LocalMsg) -> Result<(), ActorProcessingErr> {
        if let LocalMsg::Report { job_id, line } = msg {
            println!("[{}] reporter job {job_id}: {line}", self.site);
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
        let proc_reg = registry.clone();
        let rep_reg = registry.clone();
        let site = site.to_string();

        let handle = Supervisor::new(
            SupervisorConfig {
                strategy: RestartStrategy::RestForOne,
                max_restarts: 10,
                within_secs: 60,
                ..Default::default()
            },
            vec![
                spawn_child_spec(0, "processor", registry.clone(), {
                    let site = site.clone();
                    let registry = proc_reg.clone();
                    move || Processor {
                        site: site.clone(),
                        registry: registry.clone(),
                    }
                }),
                spawn_child_spec(1, "reporter", registry.clone(), {
                    let site = site.clone();
                    let registry = rep_reg.clone();
                    move || Reporter {
                        site: site.clone(),
                        registry: registry.clone(),
                    }
                }),
            ],
        )
        .start_settled(Duration::from_millis(30))
        .await?;

        Ok(Self {
            registry,
            _supervisor: handle,
        })
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
        match msg {
            WorkMsg::Process { job_id, value } => {
                // One remote message → two local actors (processor then reporter via processor)
                if let Some(processor) = self.registry.get("processor").await {
                    processor
                        .send(LocalMsg::Compute { job_id, value })
                        .await?;
                }
            }
            WorkMsg::Ping => {
                println!("[{}] gateway ping", self.site);
            }
            WorkMsg::FailProcessor => {
                if let Some(processor) = self.registry.get("processor").await {
                    let _ = processor.send(LocalMsg::Fail).await;
                }
            }
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
            "[cluster] site {name} online at {} (RestForOne: processor=0, reporter=1)",
            handle.address()
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

    // One node: hash ring picks owner for job_id
    cluster
        .send_by_key(&42u64, WorkMsg::Process { job_id: 42, value: 10.0 })
        .await?;

    tokio::time::sleep(Duration::from_millis(80)).await;

    println!("\n=== Send to multiple nodes at once ===\n");

    // All nodes in one go (stops on first error)
    cluster.broadcast(WorkMsg::Ping).await?;
    println!("[coordinator] broadcast Ping → all nodes\n");

    // All nodes; collect per-node results (continues on failure)
    let results = cluster
        .send_all(WorkMsg::Process {
            job_id: 100,
            value: 3.0,
        })
        .await;
    for (name, result) in &results {
        println!("[coordinator] send_all → {name}: {result:?}");
    }

    // Named subset only
    let subset = cluster
        .send_to(
            &["site-a"],
            WorkMsg::Process {
                job_id: 101,
                value: 5.0,
            },
        )
        .await;
    println!("[coordinator] send_to [site-a]: {:?}\n", subset);

    // Hash-ring replicas (primary + next node on ring)
    let replicas = cluster
        .send_replicas(&7u64, 2, WorkMsg::Process { job_id: 7, value: 1.0 })
        .await;
    println!("[coordinator] send_replicas(key=7, n=2): {:?}\n", replicas);

    tokio::time::sleep(Duration::from_millis(120)).await;

    println!("=== Phase 2: scale out (+2 sites) ===\n");

    let site_c = WorkerSite::launch("site-c").await?;
    let site_d = WorkerSite::launch("site-d").await?;
    join(&mut cluster, &site_c);
    join(&mut cluster, &site_d);

    cluster
        .send_all(WorkMsg::Ping)
        .await
        .into_iter()
        .for_each(|(name, _)| println!("[coordinator] ping after scale-out → {name}"));

    println!("\n=== RestForOne on one site (processor fail → reporter restarts) ===\n");
    let before_p = site_a.pair.registry.generation("processor").await;
    let before_r = site_a.pair.registry.generation("reporter").await;

    cluster
        .send_to(&["site-a"], WorkMsg::FailProcessor)
        .await;
    tokio::time::sleep(Duration::from_millis(150)).await;

    let after_p = site_a.pair.registry.generation("processor").await;
    let after_r = site_a.pair.registry.generation("reporter").await;
    println!(
        "[site-a] processor gen {before_p} -> {after_p}, reporter gen {before_r} -> {after_r}"
    );

    cluster
        .send_by_key(&99u64, WorkMsg::Process { job_id: 99, value: 2.0 })
        .await?;

    tokio::time::sleep(Duration::from_millis(100)).await;
    println!(
        "\nDone — {} sites in cluster, each running RestForOne processor + reporter.",
        cluster.len()
    );
    Ok(())
}
