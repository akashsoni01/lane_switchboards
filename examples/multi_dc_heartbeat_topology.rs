//! Production-style multi-DC heartbeat using [`DcTopology`] / [`DcWorkers`].
//!
//! Unlike the all-localhost demo in [`multi_dc_heartbeat`](./multi_dc_heartbeat.rs),
//! this models how heartbeat monitoring works in a real deployment:
//!
//! - **Regional worker pools** — 6 nodes per DC on distinct port blocks (simulating
//!   separate hosts: `19101-19106`, `19201-19206`, `19301-19306`).
//! - **One gateway per DC** — cross-region traffic hits `eu-west-gateway @ :19200`,
//!   not 6 individual worker addresses. The gateway fans out locally.
//! - **Coordinator in `LOCAL_DC`** — health checks run from `us-east`; the roster
//!   contains local workers + remote gateways only (8 members, not 18).
//! - **`local_dc` is explicit** — routing uses `cluster.datacenters("us-east")` and
//!   `local_replicas_for_key(..., "us-east")`, not the meaningless `"local"` sentinel.
//!
//! Run: `cargo run --example multi_dc_heartbeat_topology`
//! See: `examples/multi_dc_heartbeat.md`

use lane_switchboards::actor::{Actor, ActorProcessingErr};
use lane_switchboards::distributed::{serve_actor, Cluster, ClusterMember, RemoteActorRef};
use lane_switchboards::prost::Message;
use lane_switchboards::topology::{DcTopology, DcWorkers, NodeInfo};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

// ── Where the health coordinator runs (env: LOCAL_DC in production) ───────

const LOCAL_DC: &str = "us-east";
const WORKERS_PER_DC: usize = 6;

/// Gateway port + workers at `port_base + 1 .. port_base + WORKERS_PER_DC`.
const REGION_PORTS: [(&str, u16); 3] = [
    ("us-east", 19_100),
    ("eu-west", 19_200),
    ("ap-south", 19_300),
];

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

// ── Per-worker stats ──────────────────────────────────────────────────────

#[derive(Default)]
struct NodeStats {
    beats_from_dc: HashMap<String, u64>,
}

type StatsRegistry = Arc<std::sync::Mutex<HashMap<String, Arc<Mutex<NodeStats>>>>>;

struct HeartbeatActor {
    stats: Arc<Mutex<NodeStats>>,
}

#[async_trait::async_trait]
impl Actor<HbMsg> for HeartbeatActor {
    async fn handle(&mut self, msg: HbMsg) -> Result<(), ActorProcessingErr> {
        if let Some(hb_msg::Kind::Beat(hb)) = msg.kind {
            let mut s = self.stats.lock().await;
            *s.beats_from_dc.entry(hb.from_dc.clone()).or_insert(0) += 1;
        }
        Ok(())
    }
}

/// Regional entry point — receives cross-DC probes and fans out to local workers.
struct RegionalGateway {
    workers: Vec<RemoteActorRef<HbMsg>>,
}

#[async_trait::async_trait]
impl Actor<HbMsg> for RegionalGateway {
    async fn handle(&mut self, msg: HbMsg) -> Result<(), ActorProcessingErr> {
        for worker in &self.workers {
            worker.send(msg.clone()).await?;
        }
        Ok(())
    }
}

struct RegionalSite {
    dc: String,
    port_base: u16,
    workers: DcWorkers<HbMsg>,
    gateway_addr: String,
    _gateway: lane_switchboards::distributed::NodeHandle<HbMsg>,
}

impl RegionalSite {
    async fn spawn(dc: &str, port_base: u16, stats: StatsRegistry) -> std::io::Result<Self> {
        let dc_name = dc.to_string();
        let stats_reg = stats.clone();
        let workers = DcWorkers::spawn(
            DcTopology::new().datacenter_with_ports(&dc_name, WORKERS_PER_DC, port_base),
            "heartbeat",
            move |_, node_name| {
                let entry = Arc::new(Mutex::new(NodeStats::default()));
                stats_reg
                    .lock()
                    .expect("stats")
                    .insert(node_name.to_string(), entry.clone());
                HeartbeatActor { stats: entry }
            },
        )
        .await?;

        let worker_refs: Vec<RemoteActorRef<HbMsg>> = workers
            .nodes()
            .iter()
            .map(|n| RemoteActorRef::new(&n.addr, "heartbeat"))
            .collect();

        let gateway_bind = format!("127.0.0.1:{port_base}");
        let gateway = serve_actor(
            format!("{dc}-gateway"),
            &gateway_bind,
            "gateway",
            RegionalGateway {
                workers: worker_refs,
            },
        )
        .await?;

        Ok(Self {
            dc: dc.to_string(),
            port_base,
            workers,
            gateway_addr: gateway.address().to_string(),
            _gateway: gateway,
        })
    }

    fn worker_port_range(&self) -> String {
        format!(
            "{}-{}",
            self.port_base + 1,
            self.port_base + WORKERS_PER_DC as u16
        )
    }
}

/// Build the coordinator roster: local workers + remote gateways only.
fn build_coordinator_cluster(sites: &[RegionalSite]) -> Cluster<HbMsg> {
    let mut cluster = Cluster::new();
    for site in sites {
        if site.dc == LOCAL_DC {
            for node in site.workers.nodes() {
                cluster.join(
                    ClusterMember::new(&node.name, &node.addr, "heartbeat")
                        .with_dc(site.dc.clone()),
                );
            }
        } else {
            cluster.join(
                ClusterMember::new(
                    format!("{}-gateway", site.dc),
                    &site.gateway_addr,
                    "gateway",
                )
                .with_dc(site.dc.clone()),
            );
        }
    }
    cluster
}

async fn probe_local_workers(cluster: &Cluster<HbMsg>, seq: u64) {
    let msg = HbMsg::beat(format!("{LOCAL_DC}-coordinator"), LOCAL_DC, seq);
    for worker in cluster.dc_members(LOCAL_DC) {
        let _ = worker.send(msg.clone()).await;
    }
}

async fn probe_remote_dc(cluster: &Cluster<HbMsg>, remote_dc: &str, seq: u64) -> usize {
    let msg = HbMsg::beat(format!("{LOCAL_DC}-coordinator"), LOCAL_DC, seq);
    let mut ok = 0;
    for gateway in cluster.dc_members(remote_dc) {
        if gateway
            .send_with_ack(msg.clone(), Duration::from_millis(500))
            .await
            .is_ok()
        {
            ok += 1;
        }
    }
    ok
}

async fn print_worker_summary(all_workers: &[NodeInfo], stats: &StatsRegistry) {
    let map = stats.lock().expect("stats");
    let mut totals: HashMap<String, HashMap<String, u64>> = HashMap::new();
    for node in all_workers {
        let beats = map[&node.name].lock().await.beats_from_dc.clone();
        let row = totals.entry(node.dc.clone()).or_default();
        for (from, count) in beats {
            *row.entry(from).or_insert(0) += count;
        }
    }
    let mut dcs: Vec<_> = totals.keys().collect();
    dcs.sort();
    for dc in dcs {
        let mut parts: Vec<String> = totals[dc]
            .iter()
            .map(|(from, n)| format!("{from}×{n}"))
            .collect();
        parts.sort();
        println!("  {dc:<12} ←  {}", parts.join("   "));
    }
}

fn all_worker_nodes(sites: &[RegionalSite]) -> Vec<NodeInfo> {
    sites
        .iter()
        .flat_map(|s| s.workers.nodes().iter().cloned())
        .collect()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    println!("=== multi_dc_heartbeat_topology — production layout ===\n");
    println!("Coordinator LOCAL_DC={LOCAL_DC}  (simulates Virginia ops plane)\n");

    let stats: StatsRegistry = Arc::new(std::sync::Mutex::new(HashMap::new()));

    println!("Regional sites (distinct port blocks = separate hosts):\n");
    let mut sites = Vec::new();
    for (dc, port_base) in REGION_PORTS {
        let site = RegionalSite::spawn(dc, port_base, stats.clone())
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let role = if dc == LOCAL_DC { "local" } else { "remote" };
        println!(
            "  {dc:<10}  gateway @ 127.0.0.1:{port_base:<5}  workers @ {}  ({role})",
            site.worker_port_range()
        );
        sites.push(site);
    }

    let cluster = build_coordinator_cluster(&sites);
    let worker_nodes = all_worker_nodes(&sites);

    println!(
        "\nCoordinator roster from {} ({} members — not all 18 workers):",
        LOCAL_DC,
        cluster.len()
    );
    println!(
        "  local  dc_members({LOCAL_DC:<10}) → {} worker refs",
        cluster.dc_members(LOCAL_DC).len()
    );
    for (dc, _) in REGION_PORTS {
        if dc != LOCAL_DC {
            println!(
                "  remote dc_members({dc:<10}) → {} gateway ref(s)",
                cluster.dc_members(dc).len()
            );
        }
    }

    let known = cluster.datacenters(LOCAL_DC);
    println!(
        "\ncluster.datacenters(\"{LOCAL_DC}\") → [{}]",
        known.join(", ")
    );
    println!(
        "  (uses coordinator home DC — not the \"local\" placeholder)\n"
    );

    tokio::time::sleep(Duration::from_millis(60)).await;

    // ── Round 1: intra-DC from coordinator ───────────────────────────────
    println!("─── Round 1: coordinator probes LOCAL workers only ────────────");
    let seq = 1;
    probe_local_workers(&cluster, seq).await;
    println!(
        "  {LOCAL_DC}-coordinator → {WORKERS_PER_DC} local workers @ :19101-19106  (seq={seq})"
    );
    tokio::time::sleep(Duration::from_millis(80)).await;
    println!("\nWorker stats (only {LOCAL_DC} should have beats):");
    print_worker_summary(&worker_nodes, &stats).await;

    // ── Round 2: cross-DC via gateways (1 RPC per remote DC) ───────────
    println!("\n─── Round 2: cross-DC via regional gateways ───────────────────");
    let mut seq = 2;
    for (dc, _) in REGION_PORTS {
        if dc == LOCAL_DC {
            continue;
        }
        let ok = probe_remote_dc(&cluster, dc, seq).await;
        println!(
            "  {LOCAL_DC}-coordinator → {dc}-gateway → {WORKERS_PER_DC} workers  (seq={seq}, gateways_ok={ok})"
        );
        seq += 1;
    }
    tokio::time::sleep(Duration::from_millis(80)).await;
    println!("\nWorker stats ({LOCAL_DC} workers hear remote coordinator beats):");
    print_worker_summary(&worker_nodes, &stats).await;

    // ── Round 3: eu-west partition — coordinator stops routing there ─────
    println!("\n─── Round 3: eu-west partition (coordinator stops probing) ────");
    let mut snap_eu: HashMap<String, u64> = HashMap::new();
    {
        let map = stats.lock().expect("stats");
        for node in worker_nodes.iter().filter(|n| n.dc == "eu-west") {
            let count = map[&node.name]
                .lock()
                .await
                .beats_from_dc
                .get(LOCAL_DC)
                .copied()
                .unwrap_or(0);
            snap_eu.insert(node.name.clone(), count);
        }
    }
    println!("  eu-west-gateway marked unreachable — coordinator skips remote probes");

    let seq = 4;
    probe_local_workers(&cluster, seq).await;
    let ap_ok = probe_remote_dc(&cluster, "ap-south", seq + 1).await;
    println!(
        "  {LOCAL_DC} local probe seq={seq}  |  ap-south gateway seq={} (ok={ap_ok})  |  eu-west SKIPPED",
        seq + 1
    );
    tokio::time::sleep(Duration::from_millis(80)).await;
    println!("\nWorker stats (eu-west frozen; us-east + ap-south advanced):");
    print_worker_summary(&worker_nodes, &stats).await;

    println!("\n─── Partition detection (eu-west workers stale vs coordinator) ─");
    let map = stats.lock().expect("stats");
    let mut stalled = 0;
    for node in worker_nodes.iter().filter(|n| n.dc == "eu-west") {
        let after = map[&node.name]
            .lock()
            .await
            .beats_from_dc
            .get(LOCAL_DC)
            .copied()
            .unwrap_or(0);
        let before = snap_eu[&node.name];
        if after == before {
            println!(
                "  ⚠  {:<14}  no new beats from {LOCAL_DC}  [had {before}, still {after}]",
                node.name
            );
            stalled += 1;
        }
    }
    println!("\n  → {stalled}/{WORKERS_PER_DC} eu-west workers stale (coordinator stopped probing)");

    // ── Local routing (coordinator home DC) ────────────────────────────
    println!("\n─── local_replicas_for_key (coordinator home DC only) ─────────");
    let key = "tenant-42/session-7";
    let local = cluster.local_replicas_for_key(&key, 3, LOCAL_DC);
    let names: Vec<String> = local
        .iter()
        .filter_map(|r| {
            worker_nodes
                .iter()
                .find(|n| n.addr == r.node_addr)
                .map(|n| n.name.clone())
        })
        .collect();
    println!(
        "  key={key:?}  local_dc={LOCAL_DC}  n=3  →  [{}]",
        names.join(", ")
    );
    println!("  (remote DC workers excluded — use dc_replicas_for_key for other regions)");

    println!("\nDone. Compare with: cargo run --example multi_dc_heartbeat");
    Ok(())
}
