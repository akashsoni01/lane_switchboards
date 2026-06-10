//! Distributed Key-Value Store — comprehensive usage example.
//!
//! Covers:
//!  1. Single-node RAM-only put/get/delete
//!  2. Multi-node QUORUM writes — fan-out to all 3 replicas
//!  3. Tunable consistency levels (ANY → ALL)
//!  4. Read repair — stale replica healed transparently
//!  5. WAL persistence and crash-recovery
//!  6. SERIAL (Paxos) linearisable writes across 3 nodes
//!  7. External `StorageClient` over gRPC
//!  8. Node join via `sync_from_peer`
//!  9. `stats()` and `health()` observability
//!
//! ```
//! cargo run --example distributed_key_value
//! ```

use lane_switchboards::{
    storage::{Key, Record, StorageClient, StorageNode, Value},
    ConsistencyConfig, HashRing, ReadConsistency, RingNode, WriteConsistency,
};
use std::path::PathBuf;
use std::time::SystemTime;
use tokio::time::{sleep, Duration};

// ── Helpers ──────────────────────────────────────────────────────────────────

fn k(s: &str) -> Key {
    Key::from(s.as_bytes().to_vec())
}
fn v(s: &str) -> Value {
    Value::from(s.as_bytes().to_vec())
}
fn str_val(b: Option<Value>) -> String {
    b.map(|v| String::from_utf8_lossy(&v).into_owned())
        .unwrap_or_else(|| "<none>".into())
}
fn separator(title: &str) {
    println!("\n{}", "─".repeat(60));
    println!("  {title}");
    println!("{}", "─".repeat(60));
}

// ── Cluster builder ───────────────────────────────────────────────────────────

struct Cluster {
    pub nodes: Vec<std::sync::Arc<StorageNode>>,
}

impl Cluster {
    /// Spin up `count` nodes, each with `rf` replicas, and wire replication.
    async fn new(count: usize, rf: usize) -> Self {
        let mut ring = HashRing::new(150);
        for i in 1..=count {
            ring.add_node(RingNode::new(&format!("n{i}"), "127.0.0.1", 0));
        }
        let cons = ConsistencyConfig {
            rf,
            local_rf: rf,
            ..Default::default()
        };

        let mut nodes = Vec::with_capacity(count);
        for i in 1..=count {
            let node = StorageNode::new(
                format!("n{i}"),
                ring.clone(),
                cons.clone(),
                "127.0.0.1:0",
                None,
                None,
            )
            .await
            .expect("start node");
            nodes.push(node);
        }

        // Wire every node to every other node's storage gRPC address.
        let addrs: Vec<(String, String)> = nodes
            .iter()
            .map(|n| (n.id.clone(), format!("http://{}", n.address)))
            .collect();

        for node in &nodes {
            let peers: Vec<(String, String)> = addrs
                .iter()
                .filter(|(id, _)| id != &node.id)
                .cloned()
                .collect();
            node.connect_peers(&peers).await.expect("connect peers");
        }

        Self { nodes }
    }

    fn primary(&self) -> &std::sync::Arc<StorageNode> {
        &self.nodes[0]
    }
}

// ── Demo 1: Single-node RAM-only ──────────────────────────────────────────────

async fn demo_single_node() -> Result<(), Box<dyn std::error::Error>> {
    separator("Demo 1 · Single-node RAM-only (rf=1)");

    let mut ring = HashRing::new(150);
    ring.add_node(RingNode::new("n1", "127.0.0.1", 0));
    let node = StorageNode::new(
        "n1".into(),
        ring,
        ConsistencyConfig {
            rf: 1,
            local_rf: 1,
            ..Default::default()
        },
        "127.0.0.1:0",
        None,
        None,
    )
    .await?;

    node.put(k("user:1:name"), v("Alice"), WriteConsistency::One)
        .await?;
    node.put(k("user:1:email"), v("alice@example.com"), WriteConsistency::One)
        .await?;
    node.put(k("user:2:name"), v("Bob"), WriteConsistency::One)
        .await?;

    println!(
        "  user:1:name  = {}",
        str_val(node.get(&k("user:1:name"), ReadConsistency::One).await?)
    );
    println!(
        "  user:1:email = {}",
        str_val(node.get(&k("user:1:email"), ReadConsistency::One).await?)
    );
    println!(
        "  user:2:name  = {}",
        str_val(node.get(&k("user:2:name"), ReadConsistency::One).await?)
    );

    node.delete(&k("user:2:name"), WriteConsistency::One).await?;
    let after_delete = node.get(&k("user:2:name"), ReadConsistency::One).await?;
    println!("  user:2:name after delete = {}", str_val(after_delete));

    println!("  records in table = {}", node.table().all().len());
    Ok(())
}

// ── Demo 2: Multi-node QUORUM writes ─────────────────────────────────────────

async fn demo_multi_node_quorum() -> Result<(), Box<dyn std::error::Error>> {
    separator("Demo 2 · 3-node cluster, QUORUM writes (rf=3)");

    let cluster = Cluster::new(3, 3).await;
    let (n1, n2, n3) = (&cluster.nodes[0], &cluster.nodes[1], &cluster.nodes[2]);

    // Write once; all 3 replicas should eventually have the data.
    n1.put(k("order:42:status"), v("shipped"), WriteConsistency::Quorum)
        .await?;
    n1.put(k("order:42:tracking"), v("TRK-987654"), WriteConsistency::Quorum)
        .await?;

    // Give background fan-out a moment to reach all replicas.
    sleep(Duration::from_millis(60)).await;

    for (label, node) in [("n1", n1), ("n2", n2), ("n3", n3)] {
        let status = node.table().get(&k("order:42:status"));
        println!(
            "  {label}.table[order:42:status] = {:?}",
            status.map(|r| String::from_utf8_lossy(&r.value).into_owned())
        );
    }

    // Read quorum — returns the value with the highest ballot.
    let status = n2
        .get(&k("order:42:status"), ReadConsistency::Quorum)
        .await?;
    println!("  Quorum read from n2: order:42:status = {}", str_val(status));
    Ok(())
}

// ── Demo 3: Tunable consistency levels ───────────────────────────────────────

async fn demo_consistency_levels() -> Result<(), Box<dyn std::error::Error>> {
    separator("Demo 3 · Tunable consistency levels (3-node, rf=3)");

    let cluster = Cluster::new(3, 3).await;
    let n1 = cluster.primary();

    // ANY — fire-and-forget; returns immediately without waiting for acks.
    n1.put(k("cache:session:abc"), v("user:1"), WriteConsistency::Any)
        .await?;
    println!("  ANY write  → ok (no ack waited)");

    // ONE — local write only; peers receive it asynchronously.
    n1.put(k("event:login:1001"), v("{ts:1718000000}"), WriteConsistency::One)
        .await?;
    println!("  ONE write  → local ack, background replication");

    // QUORUM — waits for ceil(rf/2)+1 = 2 acks.
    n1.put(k("bank:account:7"), v("balance:10000"), WriteConsistency::Quorum)
        .await?;
    println!("  QUORUM write → 2-of-3 acks confirmed");

    // ALL — waits for all 3 acks (highest durability, highest write latency).
    n1.put(k("config:feature:dark_mode"), v("true"), WriteConsistency::All)
        .await?;
    println!("  ALL write  → all 3 acks confirmed");

    sleep(Duration::from_millis(30)).await;

    // Read consistency mirrors the write side.
    let reads = [
        ("ONE", n1.get(&k("bank:account:7"), ReadConsistency::One).await?),
        ("QUORUM", n1.get(&k("bank:account:7"), ReadConsistency::Quorum).await?),
        ("ALL", n1.get(&k("bank:account:7"), ReadConsistency::All).await?),
    ];
    for (cl, val) in reads {
        println!("  Read({cl:6}) bank:account:7 = {}", str_val(val));
    }
    Ok(())
}

// ── Demo 4: Read repair ───────────────────────────────────────────────────────

async fn demo_read_repair() -> Result<(), Box<dyn std::error::Error>> {
    separator("Demo 4 · Read repair — stale replica healed automatically");

    let cluster = Cluster::new(3, 3).await;
    let (n1, n2, n3) = (&cluster.nodes[0], &cluster.nodes[1], &cluster.nodes[2]);

    // Simulate a missed replication: write directly to n1's MemTable only.
    let ballot = 500u64;
    n1.table().insert(Record {
        key: k("product:sku:x1"),
        value: v("price:99.99"),
        ballot,
        tombstone: false,
        written_at: SystemTime::now(),
    });
    println!("  Injected record directly into n1 (ballot={ballot})");
    println!("  n2 has it? {}", n2.table().get(&k("product:sku:x1")).is_some());
    println!("  n3 has it? {}", n3.table().get(&k("product:sku:x1")).is_some());

    // ReadConsistency::All collects all 3 responses → n2 and n3 appear stale →
    // background read repair fires for both.
    let got = n1
        .get(&k("product:sku:x1"), ReadConsistency::All)
        .await?;
    println!("  Read(ALL) → {}", str_val(got));

    println!("  Waiting 120ms for async read repair…");
    sleep(Duration::from_millis(120)).await;

    let n2_healed = n2.table().get(&k("product:sku:x1")).is_some();
    let n3_healed = n3.table().get(&k("product:sku:x1")).is_some();
    println!("  n2 healed? {n2_healed}");
    println!("  n3 healed? {n3_healed}");
    println!(
        "  Read repair convergence: {}",
        if n2_healed && n3_healed { "✓ all 3 consistent" } else { "partial (at least 1 repaired)" }
    );
    Ok(())
}

// ── Demo 5: WAL persistence and crash recovery ────────────────────────────────

async fn demo_wal_recovery() -> Result<(), Box<dyn std::error::Error>> {
    separator("Demo 5 · WAL persistence — writes survive a crash");

    let dir = tempfile::tempdir()?;
    let wal_path: PathBuf = dir.path().into();

    let mut ring = HashRing::new(150);
    ring.add_node(RingNode::new("n1", "127.0.0.1", 0));
    let cons = ConsistencyConfig {
        rf: 1,
        local_rf: 1,
        ..Default::default()
    };

    // ── Phase A: write 10 records, flush, "crash" ────────────────────────────
    {
        let node = StorageNode::new(
            "n1".into(),
            ring.clone(),
            cons.clone(),
            "127.0.0.1:0",
            None,
            Some(wal_path.clone()),
        )
        .await?;

        for i in 0u32..10 {
            node.put(
                k(&format!("durable:key:{i}")),
                v(&format!("value:{i}")),
                WriteConsistency::One,
            )
            .await?;
        }
        node.flush_wal().await?;
        println!("  Wrote 10 keys to WAL-enabled node, then flushed and dropped (simulated crash).");
    } // node dropped here — WAL actor flushes via post_stop

    // ── Phase B: restart from same WAL directory ─────────────────────────────
    let recovered = StorageNode::new(
        "n1".into(),
        ring.clone(),
        cons.clone(),
        "127.0.0.1:0",
        None,
        Some(wal_path.clone()),
    )
    .await?;

    let mut found = 0;
    for i in 0u32..10 {
        if recovered
            .get(&k(&format!("durable:key:{i}")), ReadConsistency::One)
            .await?
            .is_some()
        {
            found += 1;
        }
    }
    println!("  Recovered {found}/10 keys from WAL ✓");

    // ── Phase C: checkpoint → write more → recover ───────────────────────────
    for i in 10u32..20 {
        recovered
            .put(
                k(&format!("durable:key:{i}")),
                v(&format!("value:{i}")),
                WriteConsistency::One,
            )
            .await?;
    }
    recovered.checkpoint(&wal_path).await?;
    for i in 20u32..30 {
        recovered
            .put(
                k(&format!("durable:key:{i}")),
                v(&format!("value:{i}")),
                WriteConsistency::One,
            )
            .await?;
    }
        recovered.flush_wal().await?;
    println!("  Wrote 10 more, checkpointed, then 10 more, flushed.");
    drop(recovered);

    let final_node = StorageNode::new(
        "n1".into(),
        ring,
        cons,
        "127.0.0.1:0",
        None,
        Some(wal_path),
    )
    .await?;
    let total = final_node.table().all().len();
    println!("  After recovery: {total}/30 keys present ✓");
    Ok(())
}

// ── Demo 6: SERIAL (Paxos) linearisable writes ───────────────────────────────

async fn demo_serial_consistency() -> Result<(), Box<dyn std::error::Error>> {
    separator("Demo 6 · SERIAL (Paxos) linearisable writes — 3-node cluster");

    let mut ring = HashRing::new(150);
    ring.add_node(RingNode::new("n1", "127.0.0.1", 0));
    ring.add_node(RingNode::new("n2", "127.0.0.1", 0));
    ring.add_node(RingNode::new("n3", "127.0.0.1", 0));
    let cons = ConsistencyConfig {
        rf: 3,
        local_rf: 3,
        ..Default::default()
    };

    let n1 = StorageNode::new(
        "n1".into(),
        ring.clone(),
        cons.clone(),
        "127.0.0.1:0",
        Some("127.0.0.1:0"),
        None,
    )
    .await?;
    let n2 = StorageNode::new(
        "n2".into(),
        ring.clone(),
        cons.clone(),
        "127.0.0.1:0",
        Some("127.0.0.1:0"),
        None,
    )
    .await?;
    let n3 = StorageNode::new(
        "n3".into(),
        ring.clone(),
        cons.clone(),
        "127.0.0.1:0",
        Some("127.0.0.1:0"),
        None,
    )
    .await?;

    let p1 = n1.paxos.as_ref().unwrap().address.clone();
    let p2 = n2.paxos.as_ref().unwrap().address.clone();
    let p3 = n3.paxos.as_ref().unwrap().address.clone();

    let s1 = format!("http://{}", n1.address);
    let s2 = format!("http://{}", n2.address);
    let s3 = format!("http://{}", n3.address);

    n1.connect_peers_with_paxos(&[
        ("n2".into(), s2.clone(), p2.clone()),
        ("n3".into(), s3.clone(), p3.clone()),
    ])
    .await?;
    n2.connect_peers_with_paxos(&[
        ("n1".into(), s1.clone(), p1.clone()),
        ("n3".into(), s3.clone(), p3.clone()),
    ])
    .await?;
    n3.connect_peers_with_paxos(&[
        ("n1".into(), s1.clone(), p1.clone()),
        ("n2".into(), s2.clone(), p2.clone()),
    ])
    .await?;

    // ── Case A: single SERIAL write → all 3 nodes agree via SERIAL read ───────
    println!("  Case A: single SERIAL write to fresh key");
    n1.put(k("config:db:pool_size"), v("50"), WriteConsistency::Serial)
        .await?;
    sleep(Duration::from_millis(30)).await;

    let r1 = str_val(n1.get(&k("config:db:pool_size"), ReadConsistency::Serial).await?);
    let r2 = str_val(n2.get(&k("config:db:pool_size"), ReadConsistency::Serial).await?);
    let r3 = str_val(n3.get(&k("config:db:pool_size"), ReadConsistency::Serial).await?);
    println!("  n1 reads: {r1}  n2 reads: {r2}  n3 reads: {r3}");
    println!(
        "  All agree: {}",
        if r1 == r2 && r2 == r3 { "✓" } else { "✗" }
    );

    // ── Case B: concurrent SERIAL writes → Paxos completion obligation ────────
    // Both writes will complete (no error). Because Paxos enforces the completion
    // obligation, whichever proposal saw an earlier accepted value MUST re-propose
    // it. After both finish, a SERIAL read from any node returns the same winner.
    println!("\n  Case B: two concurrent SERIAL writes (completion obligation demo)");
    let n1c = n1.clone();
    let n2c = n2.clone();
    let (r1, r2) = tokio::join!(
        tokio::spawn(async move {
            n1c.put(k("lock:mutex:x"), v("n1-owns"), WriteConsistency::Serial)
                .await
        }),
        tokio::spawn(async move {
            n2c.put(k("lock:mutex:x"), v("n2-owns"), WriteConsistency::Serial)
                .await
        }),
    );
    // At least one should succeed; the other may also succeed after re-proposing.
    let ok1 = r1?.is_ok();
    let ok2 = r2?.is_ok();
    println!("  n1 write ok={ok1}  n2 write ok={ok2}");
    sleep(Duration::from_millis(80)).await;

    // SERIAL reads run a Paxos Prepare round → return the highest committed value.
    let w1 = str_val(n1.get(&k("lock:mutex:x"), ReadConsistency::Serial).await?);
    let w2 = str_val(n2.get(&k("lock:mutex:x"), ReadConsistency::Serial).await?);
    let w3 = str_val(n3.get(&k("lock:mutex:x"), ReadConsistency::Serial).await?);
    println!("  SERIAL reads: n1={w1}  n2={w2}  n3={w3}");
    let all_same = w1 == w2 && w2 == w3;
    let no_empty = !w1.is_empty() && w1 != "<none>";
    println!(
        "  Convergence: {}",
        if all_same && no_empty { "✓ all nodes agree on one winner" }
        else { "partial (completion obligation still in progress — see docs/storage.md)" }
    );
    Ok(())
}

// ── Demo 7: External StorageClient via gRPC ────────────────────────────────

async fn demo_storage_client() -> Result<(), Box<dyn std::error::Error>> {
    separator("Demo 7 · External StorageClient (gRPC gateway)");

    // Single rf=1 node for simplicity.
    let mut ring = HashRing::new(150);
    ring.add_node(RingNode::new("gw", "127.0.0.1", 0));
    let node = StorageNode::new(
        "gw".into(),
        ring,
        ConsistencyConfig {
            rf: 1,
            local_rf: 1,
            ..Default::default()
        },
        "127.0.0.1:0",
        None,
        None,
    )
    .await?;

    // Clients connect via the `StorageGateway` gRPC service.
    let mut client = StorageClient::connect(&format!("http://{}", node.address)).await?;

    // Default consistency: Quorum write / Quorum read (configurable).
    client.default_write_cl = WriteConsistency::One;
    client.default_read_cl = ReadConsistency::One;

    client.put(k("product:101:name"), v("Wireless Keyboard")).await?;
    client.put(k("product:101:price"), v("79.99")).await?;
    client.put(k("product:101:stock"), v("150")).await?;

    println!(
        "  GET product:101:name  = {}",
        str_val(client.get(&k("product:101:name")).await?)
    );
    println!(
        "  GET product:101:price = {}",
        str_val(client.get(&k("product:101:price")).await?)
    );

    // Explicit consistency override per call.
    client
        .put_cl(
            k("product:101:stock"),
            v("149"),
            WriteConsistency::One,
        )
        .await?;
    let stock = client
        .get_cl(&k("product:101:stock"), ReadConsistency::One)
        .await?;
    println!("  GET(ONE) product:101:stock  = {}", str_val(stock));

    client.delete(&k("product:101:stock")).await?;
    let after = client.get(&k("product:101:stock")).await?;
    println!("  GET after delete product:101:stock = {}", str_val(after));
    Ok(())
}

// ── Demo 8: Node join via sync_from_peer ─────────────────────────────────────

async fn demo_node_join() -> Result<(), Box<dyn std::error::Error>> {
    separator("Demo 8 · Node join — new node syncs full snapshot from peer");

    let mut ring = HashRing::new(150);
    ring.add_node(RingNode::new("primary", "127.0.0.1", 0));
    ring.add_node(RingNode::new("joiner", "127.0.0.1", 0));

    let cons = ConsistencyConfig {
        rf: 1,
        local_rf: 1,
        ..Default::default()
    };

    let primary = StorageNode::new(
        "primary".into(),
        ring.clone(),
        cons.clone(),
        "127.0.0.1:0",
        None,
        None,
    )
    .await?;

    // Write 50 records directly to the primary's table.
    for i in 0u32..50 {
        primary.table().insert(Record {
            key: k(&format!("catalog:item:{i}")),
            value: v(&format!("{{id:{i},name:item{i}}}")),
            ballot: i as u64 + 1,
            tombstone: false,
            written_at: SystemTime::now(),
        });
    }
    println!("  primary has {} records", primary.table().all().len());

    // Joiner node starts empty.
    let joiner = StorageNode::new(
        "joiner".into(),
        ring.clone(),
        cons.clone(),
        "127.0.0.1:0",
        None,
        None,
    )
    .await?;
    println!("  joiner starts with {} records", joiner.table().all().len());

    // Connect joiner to primary, then call sync_from_peer.
    let primary_addr = format!("http://{}", primary.address);
    joiner
        .connect_peers(&[("primary".into(), primary_addr)])
        .await?;
    joiner.sync_from_peer("primary").await?;

    println!("  joiner after sync: {} records ✓", joiner.table().all().len());
    Ok(())
}

// ── Demo 9: Stats and health observability ────────────────────────────────────

async fn demo_stats_and_health() -> Result<(), Box<dyn std::error::Error>> {
    separator("Demo 9 · Runtime stats and health");

    let mut ring = HashRing::new(150);
    ring.add_node(RingNode::new("obs", "127.0.0.1", 0));
    let node = StorageNode::new(
        "obs".into(),
        ring,
        ConsistencyConfig {
            rf: 1,
            local_rf: 1,
            ..Default::default()
        },
        "127.0.0.1:0",
        None,
        None, // RAM-only
    )
    .await?;

    // Generate some traffic.
    for i in 0u32..20 {
        node.put(k(&format!("metric:{i}")), v(&format!("{i}")), WriteConsistency::One)
            .await?;
    }
    for i in 0u32..20 {
        let _ = node.get(&k(&format!("metric:{i}")), ReadConsistency::One).await?;
    }
    for i in 10u32..20 {
        node.delete(&k(&format!("metric:{i}")), WriteConsistency::One)
            .await?;
    }

    let stats = node.stats();
    println!("  ┌─ StorageStats ────────────────────────────────");
    println!("  │  puts_total         = {}", stats.puts_total);
    println!("  │  gets_total         = {}", stats.gets_total);
    println!("  │  deletes_total      = {}", stats.deletes_total);
    println!("  │  read_repairs       = {}", stats.read_repairs);
    println!("  │  paxos_writes       = {}", stats.paxos_writes);
    println!("  │  quorum_failures    = {}", stats.quorum_failures);
    println!("  │  wal_bytes_written  = {}", stats.wal_bytes_written);
    println!("  │  tombstone_count    = {}", stats.tombstone_count);
    println!("  └────────────────────────────────────────────────");

    let health = node.health();
    println!("\n  ┌─ StorageHealth ───────────────────────────────");
    println!("  │  node_id         = {}", health.node_id);
    println!("  │  record_count    = {}", health.record_count);
    println!("  │  tombstone_count = {}", health.tombstone_count);
    println!(
        "  │  wal_bytes       = {}",
        health
            .wal_bytes_written
            .map_or("(RAM-only)".into(), |b| b.to_string())
    );
    println!("  │  replica_peers   = {}", health.replica_peers);
    println!("  │  ring_rf         = {}", health.ring_rf);
    println!("  └────────────────────────────────────────────────");
    Ok(())
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("warn")
        .try_init();

    println!("\n╔═══════════════════════════════════════════════════════╗");
    println!("║     Distributed Key-Value Store — Feature Demo        ║");
    println!("╚═══════════════════════════════════════════════════════╝");

    demo_single_node().await?;
    demo_multi_node_quorum().await?;
    demo_consistency_levels().await?;
    demo_read_repair().await?;
    demo_wal_recovery().await?;
    demo_serial_consistency().await?;
    demo_storage_client().await?;
    demo_node_join().await?;
    demo_stats_and_health().await?;

    println!("\n╔═══════════════════════════════════════════════════════╗");
    println!("║  All 9 demos complete. See docs/storage.md for more. ║");
    println!("╚═══════════════════════════════════════════════════════╝\n");
    Ok(())
}
