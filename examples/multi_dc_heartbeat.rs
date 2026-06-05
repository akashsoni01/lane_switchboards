//! Multi-datacenter heartbeat — 3 DCs × 6 nodes each (18 nodes total).
//!
//! Demonstrates all DC-aware [`Cluster`] APIs:
//!
//! - [`ClusterMember::with_dc`] — tag each node with its datacenter
//! - [`Cluster::dc_members`]    — all remote refs for one DC
//! - [`Cluster::datacenters`]   — enumerate distinct DCs in the roster
//! - [`Cluster::dc_replicas_for_key`] — consistent-hash replicas within a DC
//! - [`Cluster::send_all`]      — global fan-out with per-node results
//!
//! Three scenarios are run back-to-back:
//!
//! 1. **Intra-DC broadcast** — every node receives a pulse from its own DC
//! 2. **Cross-DC probes**   — all 6 directed DC→DC pairs exchange one round
//! 3. **Partition simulation** — `eu-west` stops sending; `us-east` and
//!    `ap-south` nodes detect the missing beats in the stats summary
//!
//! Run: `cargo run --example multi_dc_heartbeat`
//! See: `examples/multi_dc_heartbeat.md`

use lane_switchboards::actor::{Actor, ActorProcessingErr};
use lane_switchboards::distributed::{serve_actor, Cluster, ClusterMember, NodeHandle};
use lane_switchboards::prost::Message;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

// ── Wire types (protobuf) ─────────────────────────────────────────────────

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

/// One heartbeat pulse.
#[derive(Clone, PartialEq, Message)]
pub struct Heartbeat {
    /// Logical name of the sending node (e.g. `"us-east-3"`).
    #[prost(string, tag = "1")]
    pub from_node: String,
    /// Datacenter of the sender (e.g. `"us-east"`).
    #[prost(string, tag = "2")]
    pub from_dc: String,
    /// Monotonically increasing sequence number.
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

// ── Shared per-node statistics ────────────────────────────────────────────

/// Accumulated heartbeat counts, keyed by *source datacenter*.
/// Written by the actor on every received beat; read by main for reporting.
#[derive(Default)]
struct NodeStats {
    beats_from_dc: HashMap<String, u64>,
}

// ── HeartbeatActor ────────────────────────────────────────────────────────

/// Receives heartbeat pulses and tallies them by source DC in shared stats.
struct HeartbeatActor {
    node_name: String,
    dc: String,
    stats: Arc<Mutex<NodeStats>>,
}

#[async_trait::async_trait]
impl Actor<HbMsg> for HeartbeatActor {
    async fn pre_start(&mut self) -> Result<(), ActorProcessingErr> {
        tracing::debug!(node = %self.node_name, dc = %self.dc, "heartbeat actor up");
        Ok(())
    }

    async fn handle(&mut self, msg: HbMsg) -> Result<(), ActorProcessingErr> {
        if let Some(hb_msg::Kind::Beat(hb)) = msg.kind {
            let mut s = self.stats.lock().await;
            *s.beats_from_dc.entry(hb.from_dc.clone()).or_insert(0) += 1;
            tracing::trace!(
                to   = %self.node_name,
                from = %hb.from_node,
                seq  = hb.seq,
                "♥ received"
            );
        }
        Ok(())
    }
}

// ── DcNode — one running gRPC node ───────────────────────────────────────

struct DcNode {
    name: String,
    dc: String,
    handle: NodeHandle<HbMsg>,
    stats: Arc<Mutex<NodeStats>>,
}

impl DcNode {
    /// Bind a gRPC node with a `HeartbeatActor` on a random loopback port.
    async fn spawn(node_name: &str, dc: &str) -> std::io::Result<Self> {
        let stats = Arc::new(Mutex::new(NodeStats::default()));
        let actor = HeartbeatActor {
            node_name: node_name.to_string(),
            dc: dc.to_string(),
            stats: stats.clone(),
        };
        // serve_actor binds a gRPC listener and registers the actor as "heartbeat".
        // Wrap with supervise_actor / spawn_child_spec for production fault tolerance.
        let handle = serve_actor(node_name, "127.0.0.1:0", "heartbeat", actor).await?;
        Ok(Self { name: node_name.to_string(), dc: dc.to_string(), handle, stats })
    }

    /// Build a [`ClusterMember`] tagged with this node's DC, ready for `cluster.join`.
    fn member(&self) -> ClusterMember {
        ClusterMember::new(&self.name, self.handle.address(), "heartbeat")
            .with_dc(self.dc.clone())
    }
}

// ── Heartbeat helpers ─────────────────────────────────────────────────────

/// Simulate `from_dc` broadcasting a heartbeat pulse to **all** its own members.
async fn beat_intra_dc(cluster: &Cluster<HbMsg>, dc: &str, seq: u64) {
    let msg = HbMsg::beat(dc, dc, seq);
    for r in cluster.dc_members(dc) {
        let _ = r.send(msg.clone()).await;
    }
}

/// Simulate `from_dc` sending a cross-DC probe to **all** members of `to_dc`.
async fn beat_cross_dc(cluster: &Cluster<HbMsg>, from_dc: &str, to_dc: &str, seq: u64) {
    let msg = HbMsg::beat(from_dc, from_dc, seq);
    for r in cluster.dc_members(to_dc) {
        let _ = r.send(msg.clone()).await;
    }
}

/// Print a 3-row summary matrix: `target_dc ← from_dc×count …`
async fn print_dc_summary(nodes: &[DcNode]) {
    // Aggregate per (target_dc, from_dc) across all nodes.
    let mut totals: HashMap<String, HashMap<String, u64>> = HashMap::new();
    for node in nodes {
        let beats = {
            let s = node.stats.lock().await;
            s.beats_from_dc.clone()
        };
        let row = totals.entry(node.dc.clone()).or_default();
        for (from_dc, count) in beats {
            *row.entry(from_dc).or_insert(0) += count;
        }
    }
    let mut target_dcs: Vec<&String> = totals.keys().collect();
    target_dcs.sort();
    for target_dc in target_dcs {
        let from_map = &totals[target_dc];
        let mut parts: Vec<String> = from_map
            .iter()
            .map(|(from, n)| format!("{from}×{n}"))
            .collect();
        parts.sort();
        println!("  {target_dc:<12} ←  {}", parts.join("   "));
    }
}

// ── Topology constants ────────────────────────────────────────────────────

const DCS: [&str; 3] = ["us-east", "eu-west", "ap-south"];
const NODES_PER_DC: usize = 6;

// ── main ──────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    println!("=== multi_dc_heartbeat — 3 DCs × 6 nodes ===\n");

    // ── 1. Spin up 18 nodes ──────────────────────────────────────────────
    println!("Spawning nodes…");
    let mut all_nodes: Vec<DcNode> = Vec::with_capacity(DCS.len() * NODES_PER_DC);
    for dc in DCS {
        for i in 1..=NODES_PER_DC {
            let name = format!("{dc}-{i}");
            let node = DcNode::spawn(&name, dc)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            println!("  {:<14}  dc={dc:<10}  addr={}", node.name, node.handle.address());
            all_nodes.push(node);
        }
    }
    println!("\n{} nodes online\n", all_nodes.len());

    // ── 2. Build a single 18-member Cluster, each tagged with its DC ─────
    let mut cluster = Cluster::<HbMsg>::new();
    for node in &all_nodes {
        cluster.join(node.member());
    }

    // Build addr → name lookup (for dc_replicas_for_key demo later).
    let addr_to_name: HashMap<String, String> = all_nodes
        .iter()
        .map(|n| (n.handle.address().to_string(), n.name.clone()))
        .collect();

    let known_dcs = cluster.datacenters("local");
    println!(
        "Cluster roster: {} nodes  |  DCs: [{}]",
        cluster.len(),
        known_dcs.join(", ")
    );
    for dc in &known_dcs {
        println!("  dc_members({dc:<10}) → {} nodes", cluster.dc_members(dc).len());
    }

    // Brief settle for pre_start to complete across all 18 actors.
    tokio::time::sleep(Duration::from_millis(60)).await;

    // ── Round 1: intra-DC broadcast ──────────────────────────────────────
    println!("\n─── Round 1: intra-DC broadcast ───────────────────────────────");
    let mut seq = 1u64;
    for dc in DCS {
        beat_intra_dc(&cluster, dc, seq).await;
        println!("  {dc} → {dc:<10}  (1 beat × 6 nodes, seq={seq})");
        seq += 1;
    }
    // Expected: each DC shows own_dc×6 (6 nodes × 1 beat each).
    tokio::time::sleep(Duration::from_millis(80)).await;
    println!("\nStats after round 1  (own_dc×6 per DC):");
    print_dc_summary(&all_nodes).await;

    // ── Round 2: all 6 cross-DC directed pairs ───────────────────────────
    println!("\n─── Round 2: cross-DC probes (all 6 directed pairs) ───────────");
    for from_dc in DCS {
        for to_dc in DCS {
            if from_dc != to_dc {
                beat_cross_dc(&cluster, from_dc, to_dc, seq).await;
                println!("  {from_dc} → {to_dc:<10}  seq={seq}");
                seq += 1;
            }
        }
    }
    // Expected: each DC shows own_dc×6  other_dc_a×6  other_dc_b×6.
    tokio::time::sleep(Duration::from_millis(80)).await;
    println!("\nStats after round 2  (each DC hears from all 3 DCs):");
    print_dc_summary(&all_nodes).await;

    // ── Round 3: eu-west partition ───────────────────────────────────────
    println!("\n─── Round 3: eu-west partition (eu-west stops sending) ────────");
    println!("  eu-west outbound heartbeats: SUPPRESSED");

    // Snapshot eu-west beat totals for non-eu-west nodes before round 3.
    let mut snap_eu: HashMap<String, u64> = HashMap::new();
    for node in all_nodes.iter().filter(|n| n.dc != "eu-west") {
        let count = {
            let s = node.stats.lock().await;
            s.beats_from_dc.get("eu-west").copied().unwrap_or(0)
        };
        snap_eu.insert(node.name.clone(), count);
    }

    // us-east and ap-south continue normally; eu-west skips all outbound sends.
    for from_dc in DCS {
        for to_dc in DCS {
            if from_dc == to_dc || from_dc == "eu-west" {
                continue; // eu-west is partitioned — no outbound beats
            }
            beat_cross_dc(&cluster, from_dc, to_dc, seq).await;
            println!("  {from_dc} → {to_dc:<10}  seq={seq}");
            seq += 1;
        }
    }
    // Intra-DC for the two healthy DCs.
    for dc in ["us-east", "ap-south"] {
        beat_intra_dc(&cluster, dc, seq).await;
        seq += 1;
    }
    tokio::time::sleep(Duration::from_millis(80)).await;

    // Expected: us-east and ap-south show eu-west×6 (unchanged — no new beats from eu-west).
    println!("\nStats after round 3  (eu-west count frozen on us-east and ap-south):");
    print_dc_summary(&all_nodes).await;

    // ── Partition detection ──────────────────────────────────────────────
    println!("\n─── Partition detection ────────────────────────────────────────");
    println!("  Nodes that received zero NEW beats from eu-west in round 3:\n");
    let mut missed = 0usize;
    for node in all_nodes.iter().filter(|n| n.dc != "eu-west") {
        let after = {
            let s = node.stats.lock().await;
            s.beats_from_dc.get("eu-west").copied().unwrap_or(0)
        };
        let before = snap_eu[&node.name];
        if after == before {
            println!("  ⚠  {:<14} ({})  eu-west silent  [had {before}, still {after}]",
                node.name, node.dc);
            missed += 1;
        }
    }
    println!("\n  → {missed} nodes detected the eu-west partition");

    // ── DC-aware routing demo ────────────────────────────────────────────
    println!("\n─── DC-aware routing (dc_replicas_for_key) ─────────────────────");
    let key = "session-abc123";
    for dc in DCS {
        let replicas = cluster.dc_replicas_for_key(&key, dc, "local", 3);
        let names: Vec<&str> = replicas
            .iter()
            .filter_map(|r| addr_to_name.get(&r.node_addr).map(String::as_str))
            .collect();
        println!("  key={key:?}  dc={dc:<10}  n=3  →  [{}]", names.join(", "));
    }

    // ── Cluster-wide broadcast ───────────────────────────────────────────
    println!("\n─── Global broadcast (send_all) ────────────────────────────────");
    let results = cluster
        .send_all(HbMsg::beat("coordinator", "control-plane", seq))
        .await;
    let (ok, err): (Vec<_>, Vec<_>) = results.iter().partition(|(_, r)| r.is_ok());
    println!("  broadcast to {} nodes → {} ok, {} err", results.len(), ok.len(), err.len());

    tokio::time::sleep(Duration::from_millis(60)).await;
    println!("\nFinal stats (includes coordinator broadcast):");
    print_dc_summary(&all_nodes).await;

    println!("\nDone. See examples/multi_dc_heartbeat.md");
    Ok(())
}
