//! Production multi-DC heartbeat deployment skeleton.
//!
//! Models the layout in [`READMEv0.0.8.md`](../READMEv0.0.8.md) section **B** with
//! real-world hostnames, load balancers, supervisors, and `send_with_ack` probes.
//! Hosts are fictional (`*.prod.example.com`) — they do not resolve today.
//!
//! ## Roles (one process per deployment)
//!
//! | `DEPLOY_ROLE` | Binds | Purpose |
//! |---------------|-------|---------|
//! | `worker` | `WORKER_BIND` (e.g. `0.0.0.0:19103`) | Supervised heartbeat worker |
//! | `regional-gateway` | `GATEWAY_BIND` (e.g. `0.0.0.0:19200`) | Fan-out to in-region workers |
//! | `coordinator` | *(client only)* | 8-member roster, health probe loop |
//!
//! ## Dry run (default)
//!
//! `DRY_RUN=1` (default) prints the deployment plan and exits without binding or
//! dialing production URLs. Use this in CI to verify the example compiles.
//!
//! ```bash
//! cargo run --example multi_dc_heartbeat_prod
//! DEPLOY_ROLE=coordinator cargo run --example multi_dc_heartbeat_prod
//! DEPLOY_ROLE=regional-gateway LOCAL_DC=eu-west cargo run --example multi_dc_heartbeat_prod
//! DEPLOY_ROLE=worker LOCAL_DC=us-east WORKER_NAME=us-east-hb-3 cargo run --example multi_dc_heartbeat_prod
//! ```
//!
//! Set `DRY_RUN=0` on real hardware when infrastructure exists.
//!
//! See also: [`multi_dc_heartbeat_topology`](./multi_dc_heartbeat_topology.rs) (localhost simulation).

use lane_switchboards::actor::{Actor, ActorProcessingErr, ActorRef};
use lane_switchboards::config::ActorConfig;
use lane_switchboards::distributed::{
    serve_actor_on_current_runtime, Cluster, ClusterMember, RemoteActorRef,
};
use lane_switchboards::prost::Message;
use lane_switchboards::supervisor::{
    ChildSlot, IntensityAction, RestartStrategy, Supervisor, SupervisorConfig,
    SupervisorHandle,
};
use std::collections::HashSet;
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

// ── Production topology (fictional DNS) ───────────────────────────────────

const LOCAL_DC_DEFAULT: &str = "us-east";
const WORKERS_PER_DC: usize = 6;

const US_EAST_GATEWAY_PUBLIC: &str = "us-east-lb.prod.example.com:19100";
const EU_WEST_GATEWAY_PUBLIC: &str = "eu-west-lb.prod.example.com:19200";
const AP_SOUTH_GATEWAY_PUBLIC: &str = "ap-south-lb.prod.example.com:19300";

/// `(worker_name, dial_addr)` — internal DNS behind each region's VPC.
const US_EAST_WORKERS: [(&str, &str); 6] = [
    ("us-east-hb-1", "us-east-hb-1.internal.prod.example.com:19101"),
    ("us-east-hb-2", "us-east-hb-2.internal.prod.example.com:19102"),
    ("us-east-hb-3", "us-east-hb-3.internal.prod.example.com:19103"),
    ("us-east-hb-4", "us-east-hb-4.internal.prod.example.com:19104"),
    ("us-east-hb-5", "us-east-hb-5.internal.prod.example.com:19105"),
    ("us-east-hb-6", "us-east-hb-6.internal.prod.example.com:19106"),
];

const EU_WEST_WORKERS: [(&str, &str); 6] = [
    ("eu-west-hb-1", "eu-west-hb-1.internal.eu-west.prod.example.com:19201"),
    ("eu-west-hb-2", "eu-west-hb-2.internal.eu-west.prod.example.com:19202"),
    ("eu-west-hb-3", "eu-west-hb-3.internal.eu-west.prod.example.com:19203"),
    ("eu-west-hb-4", "eu-west-hb-4.internal.eu-west.prod.example.com:19204"),
    ("eu-west-hb-5", "eu-west-hb-5.internal.eu-west.prod.example.com:19205"),
    ("eu-west-hb-6", "eu-west-hb-6.internal.eu-west.prod.example.com:19206"),
];

const AP_SOUTH_WORKERS: [(&str, &str); 6] = [
    ("ap-south-hb-1", "ap-south-hb-1.internal.ap-south.prod.example.com:19301"),
    ("ap-south-hb-2", "ap-south-hb-2.internal.ap-south.prod.example.com:19302"),
    ("ap-south-hb-3", "ap-south-hb-3.internal.ap-south.prod.example.com:19303"),
    ("ap-south-hb-4", "ap-south-hb-4.internal.ap-south.prod.example.com:19304"),
    ("ap-south-hb-5", "ap-south-hb-5.internal.ap-south.prod.example.com:19305"),
    ("ap-south-hb-6", "ap-south-hb-6.internal.ap-south.prod.example.com:19306"),
];

const REMOTE_GATEWAYS_DEFAULT: &str =
    "eu-west=eu-west-lb.prod.example.com:19200,ap-south=ap-south-lb.prod.example.com:19300";

const PROBE_INTERVAL: Duration = Duration::from_secs(30);
const LOCAL_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const REMOTE_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

// ── Wire types ────────────────────────────────────────────────────────────

#[derive(Clone, PartialEq, Message)]
pub struct HbMsg {
    #[prost(oneof = "hb_msg::Kind", tags = "1")]
    pub kind: Option<hb_msg::Kind>,
}

pub mod hb_msg {
    use super::Heartbeat;
    use lane_switchboards::prost::Oneof;

    #[derive(Clone, PartialEq, Oneof)]
    pub enum Kind {
        #[prost(message, tag = "1")]
        Beat(Heartbeat),
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct Heartbeat {
    #[prost(string, tag = "1")]
    pub from_node: String,
    #[prost(string, tag = "2")]
    pub from_dc: String,
    #[prost(uint64, tag = "3")]
    pub seq: u64,
}

impl HbMsg {
    fn beat(from_node: impl Into<String>, from_dc: impl Into<String>, seq: u64) -> Self {
        Self {
            kind: Some(hb_msg::Kind::Beat(Heartbeat {
                from_node: from_node.into(),
                from_dc: from_dc.into(),
                seq,
            })),
        }
    }
}

// ── Deployment config ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeployRole {
    Worker,
    RegionalGateway,
    Coordinator,
}

impl DeployRole {
    fn parse(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "worker" | "heartbeat-worker" => Self::Worker,
            "regional-gateway" | "gateway" => Self::RegionalGateway,
            "coordinator" | "coord" => Self::Coordinator,
            other => panic!("unknown DEPLOY_ROLE={other:?} — use worker | regional-gateway | coordinator"),
        }
    }
}

#[derive(Debug, Clone)]
struct ProdConfig {
    role: DeployRole,
    local_dc: String,
    dry_run: bool,
    worker_name: String,
    worker_bind: String,
    gateway_bind: String,
    worker_endpoints: Vec<(String, String)>,
    local_worker_endpoints: Vec<(String, String)>,
    remote_gateways: Vec<(String, String)>,
}

impl ProdConfig {
    fn from_env() -> Self {
        let role = DeployRole::parse(
            &env::var("DEPLOY_ROLE").unwrap_or_else(|_| "coordinator".into()),
        );
        let local_dc = env::var("LOCAL_DC").unwrap_or_else(|_| LOCAL_DC_DEFAULT.into());
        let dry_run = env_bool("DRY_RUN", true);

        let worker_name = env::var("WORKER_NAME").unwrap_or_else(|_| format!("{local_dc}-hb-1"));
        let worker_bind = env::var("WORKER_BIND").unwrap_or_else(|_| "0.0.0.0:19101".into());
        let gateway_bind = env::var("GATEWAY_BIND").unwrap_or_else(|_| default_gateway_bind(&local_dc));

        let worker_endpoints = parse_name_addr_list(
            &env::var("WORKER_ENDPOINTS").unwrap_or_else(|_| default_worker_endpoints(&local_dc)),
        );
        let local_worker_endpoints = parse_name_addr_list(
            &env::var("LOCAL_WORKER_ENDPOINTS")
                .unwrap_or_else(|_| default_worker_endpoints(LOCAL_DC_DEFAULT)),
        );
        let remote_gateways = parse_kv_list(
            &env::var("REMOTE_GATEWAYS").unwrap_or_else(|_| REMOTE_GATEWAYS_DEFAULT.into()),
        );

        Self {
            role,
            local_dc,
            dry_run,
            worker_name,
            worker_bind,
            gateway_bind,
            worker_endpoints,
            local_worker_endpoints,
            remote_gateways,
        }
    }
}

fn env_bool(key: &str, default: bool) -> bool {
    env::var(key)
        .map(|v| !matches!(v.as_str(), "0" | "false" | "no"))
        .unwrap_or(default)
}

fn default_gateway_bind(dc: &str) -> String {
    match dc {
        "us-east" => "0.0.0.0:19100".into(),
        "eu-west" => "0.0.0.0:19200".into(),
        "ap-south" => "0.0.0.0:19300".into(),
        _ => "0.0.0.0:9100".into(),
    }
}

fn default_worker_endpoints(dc: &str) -> String {
    let workers = workers_for_dc(dc);
    workers
        .iter()
        .map(|(n, a)| format!("{n}={a}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn workers_for_dc(dc: &str) -> &'static [(&'static str, &'static str)] {
    match dc {
        "us-east" => &US_EAST_WORKERS,
        "eu-west" => &EU_WEST_WORKERS,
        "ap-south" => &AP_SOUTH_WORKERS,
        _ => &US_EAST_WORKERS,
    }
}

fn parse_name_addr_list(s: &str) -> Vec<(String, String)> {
    s.split(',')
        .filter(|p| !p.trim().is_empty())
        .map(|pair| {
            let (name, addr) = pair
                .split_once('=')
                .unwrap_or_else(|| panic!("expected name=host:port, got {pair:?}"));
            (name.trim().into(), addr.trim().into())
        })
        .collect()
}

fn parse_kv_list(s: &str) -> Vec<(String, String)> {
    parse_name_addr_list(s)
}

fn production_actor_config() -> ActorConfig {
    ActorConfig {
        mailbox_capacity: 256,
        handle_timeout: Some(Duration::from_secs(10)),
        slow_handle_threshold: Some(Duration::from_secs(2)),
    }
}

fn production_supervisor_config() -> SupervisorConfig {
    SupervisorConfig {
        strategy: RestartStrategy::OneForOne,
        max_restarts: 10,
        within_secs: 60,
        intensity_action: IntensityAction::ShutdownSupervisor,
        ..Default::default()
    }
}

// ── Supervised heartbeat worker ─────────────────────────────────────────────

#[derive(Default)]
struct HeartbeatWorker {
    node_name: String,
    dc: String,
    beats_seen: u64,
}

impl HeartbeatWorker {
    fn new(node_name: String, dc: String) -> Self {
        Self {
            node_name,
            dc,
            beats_seen: 0,
        }
    }
}

#[async_trait::async_trait]
impl Actor<HbMsg> for HeartbeatWorker {
    async fn handle(&mut self, msg: HbMsg) -> Result<(), ActorProcessingErr> {
        if let Some(hb_msg::Kind::Beat(hb)) = msg.kind {
            self.beats_seen += 1;
            tracing::debug!(
                node = %self.node_name,
                dc = %self.dc,
                from = %hb.from_node,
                from_dc = %hb.from_dc,
                seq = hb.seq,
                total = self.beats_seen,
                "heartbeat received"
            );
        }
        Ok(())
    }
}

/// gRPC-facing service: supervises [`HeartbeatWorker`] with `OneForOne` restarts.
struct SupervisedHeartbeatService {
    node_name: String,
    dc: String,
    slot: Arc<ChildSlot<HbMsg>>,
    _supervisor: Option<SupervisorHandle<HbMsg>>,
}

impl SupervisedHeartbeatService {
    fn new(node_name: String, dc: String) -> Self {
        Self {
            node_name,
            dc,
            slot: Arc::new(ChildSlot::new()),
            _supervisor: None,
        }
    }
}

#[async_trait::async_trait]
impl Actor<HbMsg> for SupervisedHeartbeatService {
    async fn pre_start(&mut self) -> Result<(), ActorProcessingErr> {
        let slot = self.slot.clone();
        let node = self.node_name.clone();
        let dc = self.dc.clone();
        let spec = ChildSlot::child_spec(0, slot, move || {
            HeartbeatWorker::new(node.clone(), dc.clone())
        });
        let sup = Supervisor::with_actor_config(
            production_actor_config(),
            production_supervisor_config(),
            vec![spec],
        )
        .start_settled(Duration::from_millis(50))
        .await?;
        self._supervisor = Some(sup);
        self.slot.require().await?;
        tracing::info!(
            node = %self.node_name,
            dc = %self.dc,
            "supervised heartbeat worker online"
        );
        Ok(())
    }

    async fn handle(&mut self, msg: HbMsg) -> Result<(), ActorProcessingErr> {
        let child = self
            .slot
            .get()
            .await
            .ok_or_else(|| -> ActorProcessingErr { "heartbeat worker not running".into() })?;
        child.send(msg).await.map_err(|e| e.to_string())?;
        Ok(())
    }
}

// ── Regional gateway (supervised fan-out + stats) ───────────────────────────

#[derive(Default)]
struct GatewayStats {
    fan_outs: u64,
    messages_forwarded: u64,
}

struct RegionalGatewayService {
    dc: String,
    workers: Vec<RemoteActorRef<HbMsg>>,
    stats: Arc<Mutex<GatewayStats>>,
    stats_slot: Arc<ChildSlot<GatewayStatsMsg>>,
    _stats_supervisor: Option<SupervisorHandle<GatewayStatsMsg>>,
}

enum GatewayStatsMsg {
    RecordFanOut { worker_count: usize },
}

struct GatewayStatsActor {
    stats: Arc<Mutex<GatewayStats>>,
}

#[async_trait::async_trait]
impl Actor<GatewayStatsMsg> for GatewayStatsActor {
    async fn handle(&mut self, msg: GatewayStatsMsg) -> Result<(), ActorProcessingErr> {
        match msg {
            GatewayStatsMsg::RecordFanOut { worker_count } => {
                let mut s = self.stats.lock().await;
                s.fan_outs += 1;
                s.messages_forwarded += worker_count as u64;
            }
        }
        Ok(())
    }
}

impl RegionalGatewayService {
    fn new(dc: String, workers: Vec<RemoteActorRef<HbMsg>>) -> Self {
        Self {
            dc,
            workers,
            stats: Arc::new(Mutex::new(GatewayStats::default())),
            stats_slot: Arc::new(ChildSlot::new()),
            _stats_supervisor: None,
        }
    }

    async fn start_stats_supervisor(&mut self) -> Result<(), ActorProcessingErr> {
        let slot = self.stats_slot.clone();
        let stats = self.stats.clone();
        let spec = ChildSlot::child_spec(0, slot, move || GatewayStatsActor {
            stats: stats.clone(),
        });
        let sup = Supervisor::new(production_supervisor_config(), vec![spec])
            .start()
            .await?;
        self._stats_supervisor = Some(sup);
        self.stats_slot.require().await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Actor<HbMsg> for RegionalGatewayService {
    async fn pre_start(&mut self) -> Result<(), ActorProcessingErr> {
        self.start_stats_supervisor().await?;
        tracing::info!(
            dc = %self.dc,
            workers = self.workers.len(),
            "regional gateway online"
        );
        Ok(())
    }

    async fn handle(&mut self, msg: HbMsg) -> Result<(), ActorProcessingErr> {
        for worker in &self.workers {
            worker.send(msg.clone()).await?;
        }
        if let Some(actor) = self.stats_slot.get().await {
            let _ = actor
                .send(GatewayStatsMsg::RecordFanOut {
                    worker_count: self.workers.len(),
                })
                .await;
        }
        Ok(())
    }
}

// ── Health coordinator (supervised probe loop) ──────────────────────────────

enum CoordMsg {
    ProbeRound,
    Shutdown,
}

struct HealthCoordinator {
    local_dc: String,
    cluster: Arc<Cluster<HbMsg>>,
    seq: u64,
    unreachable_dcs: HashSet<String>,
}

impl HealthCoordinator {
    async fn probe_local(&self, seq: u64) -> (usize, usize) {
        let msg = HbMsg::beat(format!("{}-coordinator", self.local_dc), &self.local_dc, seq);
        let mut ok = 0;
        let members = self.cluster.dc_members(&self.local_dc);
        let total = members.len();
        for worker in members {
            if worker
                .send_with_ack(msg.clone(), LOCAL_PROBE_TIMEOUT)
                .await
                .is_ok()
            {
                ok += 1;
            }
        }
        (ok, total)
    }

    async fn probe_remote_dc(&self, dc: &str, seq: u64) -> (usize, usize) {
        if self.unreachable_dcs.contains(dc) {
            tracing::warn!(dc, "skipping probe — region marked unreachable");
            return (0, 0);
        }
        let msg = HbMsg::beat(format!("{}-coordinator", self.local_dc), &self.local_dc, seq);
        let mut ok = 0;
        let gateways = self.cluster.dc_members(dc);
        let total = gateways.len();
        for gw in gateways {
            match gw.send_with_ack(msg.clone(), REMOTE_PROBE_TIMEOUT).await {
                Ok(()) => ok += 1,
                Err(e) => {
                    tracing::error!(dc, error = %e, "gateway probe failed");
                }
            }
        }
        (ok, total)
    }

    /// Production partition policy — call when remote gateway probes fail repeatedly.
    #[allow(dead_code)]
    fn mark_unreachable(&mut self, dc: &str) {
        self.unreachable_dcs.insert(dc.to_string());
        tracing::warn!(dc, "region marked unreachable — coordinator stops routing");
    }
}

#[async_trait::async_trait]
impl Actor<CoordMsg> for HealthCoordinator {
    async fn handle(&mut self, msg: CoordMsg) -> Result<(), ActorProcessingErr> {
        match msg {
            CoordMsg::ProbeRound => {
                self.seq += 1;
                let seq = self.seq;
                let (local_ok, local_total) = self.probe_local(seq).await;
                tracing::info!(
                    dc = %self.local_dc,
                    ok = local_ok,
                    total = local_total,
                    seq,
                    "local probe round"
                );

                for dc in self.cluster.datacenters(&self.local_dc) {
                    if dc == self.local_dc {
                        continue;
                    }
                    let (ok, total) = self.probe_remote_dc(&dc, seq).await;
                    tracing::info!(remote_dc = %dc, ok, total, seq, "remote probe via gateway");
                }
            }
            CoordMsg::Shutdown => {
                tracing::info!("coordinator shutting down");
            }
        }
        Ok(())
    }
}

struct SupervisedCoordinator {
    _slot: Arc<ChildSlot<CoordMsg>>,
    _supervisor: SupervisorHandle<CoordMsg>,
}

impl SupervisedCoordinator {
    async fn start(
        local_dc: String,
        cluster: Arc<Cluster<HbMsg>>,
    ) -> Result<(Self, ActorRef<CoordMsg>), ActorProcessingErr> {
        let slot = Arc::new(ChildSlot::new());
        let slot_for_spec = slot.clone();
        let spec = ChildSlot::child_spec(0, slot_for_spec, move || HealthCoordinator {
            local_dc: local_dc.clone(),
            cluster: cluster.clone(),
            seq: 0,
            unreachable_dcs: HashSet::new(),
        });
        let sup = Supervisor::with_actor_config(
            production_actor_config(),
            production_supervisor_config(),
            vec![spec],
        )
        .start_settled(Duration::from_millis(50))
        .await?;
        slot.require().await?;
        let actor = slot.get().await.expect("coordinator child running");
        Ok((
            Self {
                _slot: slot.clone(),
                _supervisor: sup,
            },
            actor,
        ))
    }
}

// ── Cluster roster (coordinator) ────────────────────────────────────────────

fn build_coordinator_cluster(config: &ProdConfig) -> Arc<Cluster<HbMsg>> {
    let mut cluster = Cluster::new();

    for (name, addr) in &config.local_worker_endpoints {
        cluster.join(
            ClusterMember::new(name, addr, "heartbeat").with_dc(LOCAL_DC_DEFAULT.to_string()),
        );
    }

    for (dc, addr) in &config.remote_gateways {
        cluster.join(
            ClusterMember::new(format!("{dc}-gateway"), addr, "gateway").with_dc(dc.clone()),
        );
    }

    Arc::new(cluster)
}

fn print_deployment_banner(config: &ProdConfig) {
    println!("=== multi_dc_heartbeat_prod — production deployment skeleton ===\n");
    println!("  DEPLOY_ROLE = {:?}", config.role);
    println!("  LOCAL_DC    = {}", config.local_dc);
    println!("  DRY_RUN     = {} (set DRY_RUN=0 for live bind/dial)\n", config.dry_run);
}

fn print_coordinator_plan(config: &ProdConfig, cluster: &Cluster<HbMsg>) {
    println!("Coordinator home: {LOCAL_DC_DEFAULT}");
    println!("Public gateways (reference):");
    println!("  us-east  {US_EAST_GATEWAY_PUBLIC}");
    println!("  eu-west  {EU_WEST_GATEWAY_PUBLIC}");
    println!("  ap-south {AP_SOUTH_GATEWAY_PUBLIC}\n");

    println!("Cluster roster ({} members):", cluster.len());
    println!(
        "  local  dc_members({LOCAL_DC_DEFAULT}) → {} worker refs ({WORKERS_PER_DC} expected)",
        cluster.dc_members(LOCAL_DC_DEFAULT).len()
    );
    for (dc, _) in &config.remote_gateways {
        println!(
            "  remote dc_members({dc}) → {} gateway ref(s)",
            cluster.dc_members(dc).len()
        );
    }
    println!(
        "\nProbe policy: local {LOCAL_PROBE_TIMEOUT:?} ack | remote {REMOTE_PROBE_TIMEOUT:?} ack | interval {PROBE_INTERVAL:?}"
    );
    println!("Supervision: OneForOne on coordinator + workers, handle_timeout 10s\n");
}

// ── Role runners ────────────────────────────────────────────────────────────

async fn run_worker(config: &ProdConfig) -> anyhow::Result<()> {
    println!("Worker VM:");
    println!("  name  = {}", config.worker_name);
    println!("  dc    = {}", config.local_dc);
    println!("  bind  = {}", config.worker_bind);
    println!("  target = heartbeat\n");

    if config.dry_run {
        println!("DRY_RUN: would serve supervised heartbeat worker (OneForOne + handle_timeout)");
        return Ok(());
    }

    let service = SupervisedHeartbeatService::new(config.worker_name.clone(), config.local_dc.clone());
    let distributed = lane_switchboards::DistributedConfig::default();
    let handle = serve_actor_on_current_runtime(
        &config.worker_name,
        &config.worker_bind,
        "heartbeat",
        service,
        &distributed,
        &production_actor_config(),
    )
    .await?;

    println!("Worker listening on {}", handle.address());
    tokio::signal::ctrl_c().await?;
    Ok(())
}

async fn run_regional_gateway(config: &ProdConfig) -> anyhow::Result<()> {
    println!("Regional gateway:");
    println!("  dc    = {}", config.local_dc);
    println!("  bind  = {}", config.gateway_bind);
    println!("  workers (in-region, private DNS):");
    for (name, addr) in &config.worker_endpoints {
        println!("    {name:<16} → {addr}");
    }
    println!();

    if config.dry_run {
        println!("DRY_RUN: would serve RegionalGatewayService + stats supervisor");
        println!("DRY_RUN: publish gateway to Consul / k8s Service after bind");
        return Ok(());
    }

    let worker_refs: Vec<RemoteActorRef<HbMsg>> = config
        .worker_endpoints
        .iter()
        .map(|(_, addr)| RemoteActorRef::new(addr, "heartbeat"))
        .collect();

    let gateway_name = format!("{}-gateway", config.local_dc);
    let service = RegionalGatewayService::new(config.local_dc.clone(), worker_refs);
    let distributed = lane_switchboards::DistributedConfig::default();
    let handle = serve_actor_on_current_runtime(
        &gateway_name,
        &config.gateway_bind,
        "gateway",
        service,
        &distributed,
        &production_actor_config(),
    )
    .await?;

    println!("Gateway listening on {} (publish to service discovery)", handle.address());
    tokio::signal::ctrl_c().await?;
    Ok(())
}

async fn run_coordinator(config: &ProdConfig) -> anyhow::Result<()> {
    let cluster = build_coordinator_cluster(config);
    print_coordinator_plan(config, &cluster);

    if config.dry_run {
        println!("DRY_RUN: skipping gRPC probes — production hosts do not exist yet.");
        println!("DRY_RUN: would run supervised probe loop every {PROBE_INTERVAL:?}");
        for (name, addr) in &config.local_worker_endpoints {
            println!("  would probe local  {name} @ {addr}");
        }
        for (dc, addr) in &config.remote_gateways {
            println!("  would probe remote {dc}-gateway @ {addr}");
        }
        return Ok(());
    }

    let (_supervised, coord_ref) = SupervisedCoordinator::start(config.local_dc.clone(), cluster)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let coord_loop = coord_ref.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(PROBE_INTERVAL);
        loop {
            interval.tick().await;
            if coord_loop.send(CoordMsg::ProbeRound).await.is_err() {
                break;
            }
        }
    });

    println!("Coordinator probe loop running (Ctrl+C to stop)");
    tokio::signal::ctrl_c().await?;
    let _ = coord_ref.send(CoordMsg::Shutdown).await;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let config = ProdConfig::from_env();
    print_deployment_banner(&config);

    match config.role {
        DeployRole::Worker => run_worker(&config).await,
        DeployRole::RegionalGateway => run_regional_gateway(&config).await,
        DeployRole::Coordinator => run_coordinator(&config).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn default_coordinator_roster_has_eight_members() {
        let config = ProdConfig {
            role: DeployRole::Coordinator,
            local_dc: LOCAL_DC_DEFAULT.into(),
            dry_run: true,
            worker_name: "us-east-hb-1".into(),
            worker_bind: "0.0.0.0:19101".into(),
            gateway_bind: "0.0.0.0:19100".into(),
            worker_endpoints: US_EAST_WORKERS
                .iter()
                .map(|(n, a)| (n.to_string(), a.to_string()))
                .collect(),
            local_worker_endpoints: US_EAST_WORKERS
                .iter()
                .map(|(n, a)| (n.to_string(), a.to_string()))
                .collect(),
            remote_gateways: parse_kv_list(REMOTE_GATEWAYS_DEFAULT),
        };
        let cluster = build_coordinator_cluster(&config);
        assert_eq!(cluster.len(), 8);
        assert_eq!(cluster.dc_members(LOCAL_DC_DEFAULT).len(), WORKERS_PER_DC);
        assert_eq!(cluster.dc_members("eu-west").len(), 1);
        assert_eq!(cluster.dc_members("ap-south").len(), 1);
    }

    #[test]
    fn parse_remote_gateways() {
        let parsed = parse_kv_list(REMOTE_GATEWAYS_DEFAULT);
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].0, "eu-west");
        assert_eq!(parsed[1].0, "ap-south");
    }
}
