# lane_switchboards v0.9.0

Release notes for **v0.9.0** — embedded distributed key-value storage with Cassandra-style consistent hashing, Paxos linearisable writes, tunable consistency levels, and an actor-backed write-ahead log.

For the full project overview see [README.md](./README.md).  
Previous release: [v0.0.9](./READMEv0.0.9.md) · [Distributed KV example](./examples/distributed_key_value.md)

---

## What's new in v0.9.0

### Summary

| Area | What landed |
|------|-------------|
| **`StorageNode`** | Distributed KV node — consistent hash ring, tunable consistency, Paxos SERIAL writes, read repair |
| **`StorageClient`** | External gRPC client for `StorageGateway` — put/get/delete with per-call consistency override |
| **`WalActor`** | lane-core `Actor`-backed write-ahead log — sequential file I/O, checkpoint, crash recovery |
| **`MemTable`** | In-memory `BTreeMap` store — LWW via ballot numbers, tombstones, range scans |
| **`StorageRouter`** | Consistent-hash ring wrapper — replica-set selection, local-primary detection, DC-aware filtering |
| **`ReplicationClient`** | gRPC client for internal node-to-node replication (Replicate / ReadReplica / Snapshot) |
| **`StorageStats`** | Atomic counters — puts, gets, deletes, read repairs, Paxos writes, quorum failures, WAL bytes |
| **`StorageHealth`** | Lightweight health snapshot — record count, tombstones, WAL bytes, peer count, ring RF |
| **`docs/storage.md`** | Architecture guide — ASCII diagram, consistency matrix, Paxos walkthrough, WAL recovery |
| **`examples/distributed_key_value`** | 9 runnable demos + 7 Mermaid diagrams |

---

## Architecture

```
External Client
      │  gRPC (StorageGateway)
      ▼
┌──────────────────────────────────────────────────────────┐
│                      StorageNode                         │
│                                                          │
│  ┌──────────┐  ┌───────────────┐  ┌───────────────────┐ │
│  │ MemTable │  │   WalActor    │  │ PaxosAcceptorHandle│ │
│  │(RwLock   │  │ (lane_core    │  │ (lane_core Actor   │ │
│  │ BTreeMap)│  │  Actor)       │  │  + gRPC acceptor)  │ │
│  └──────────┘  └───────────────┘  └───────────────────┘ │
│                                                          │
│  ┌────────────────────────────────────────────────────┐  │
│  │  StorageRouter (HashRing)                          │  │
│  │  ReplicationClients  (one per peer, gRPC)          │  │
│  │  StorageStatsCell    (AtomicU64 × 7)               │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
      │  gRPC StorageService (internal replication)
   Peer Nodes
```

---

## New types at a glance

### `StorageNode`

The heart of the storage layer.  One node per process endpoint; exposes `put`, `get`, `delete` with any consistency level.

```rust
let node = StorageNode::new(
    "n1".into(),
    ring,
    ConsistencyConfig { rf: 3, local_rf: 3, ..Default::default() },
    "0.0.0.0:7001",        // storage + gateway gRPC
    Some("0.0.0.0:7002"),  // Paxos acceptor (None = no SERIAL support)
    Some(PathBuf::from("/var/data/n1")),  // WAL dir (None = RAM-only)
).await?;

// Connect replication clients to peers (call on every node after creation).
node.connect_peers(&[
    ("n2".into(), "http://10.0.0.2:7001".into()),
    ("n3".into(), "http://10.0.0.3:7001".into()),
]).await?;

// Write / read with any consistency level.
node.put(k, v, WriteConsistency::Quorum).await?;
let val = node.get(&k, ReadConsistency::Quorum).await?;

// SERIAL writes go through Paxos — requires paxos_addr + connect_peers_with_paxos.
node.put(k, v, WriteConsistency::Serial).await?;
let val = node.get(&k, ReadConsistency::Serial).await?;
```

### `StorageClient`

gRPC client for an external process connecting to `StorageGateway`.

```rust
let mut client = StorageClient::connect("http://10.0.0.1:7001").await?;
client.default_write_cl = WriteConsistency::Quorum;
client.default_read_cl  = ReadConsistency::Quorum;

client.put(k.clone(), v).await?;
let got = client.get(&k).await?;

// Per-call override:
client.put_cl(k.clone(), v2, WriteConsistency::All).await?;
client.get_cl(&k, ReadConsistency::Serial).await?;
```

### `WalActor` — actor-backed write-ahead log

Every `put` / `delete` goes through `WalActor`'s mailbox before any replication.  The actor's sequential message processing replaces locks on the file handle.

```rust
// WAL is transparent — configured in StorageNode::new(wal_path).

// Explicit flush (e.g. before a controlled shutdown):
node.flush_wal().await?;

// Checkpoint: snapshot MemTable → truncate WAL → WAL restarts from lsn=1.
node.checkpoint(&PathBuf::from("/var/data/n1")).await?;
```

**Recovery sequence on `StorageNode::new(wal_path)`:**

```
1. load_snapshot(snapshot.dat)  → MemTable populated from last checkpoint
2. replay(wal.dat, skip_lsn=0)  → apply all entries in the (truncated) WAL
3. WalActor spawned              → ready to accept new writes
```

### `StorageStats` / `StorageHealth`

```rust
let stats  = node.stats();    // snapshot of atomic counters
let health = node.health();   // structural summary

println!("puts={} quorum_failures={} wal_bytes={}",
    stats.puts_total, stats.quorum_failures, stats.wal_bytes_written);
println!("peers={} rf={} tombstones={}",
    health.replica_peers, health.ring_rf, health.tombstone_count);
```

---

## Consistency levels

### Write

| Level | Acks required | Notes |
|-------|--------------|-------|
| `ANY` | 0 — fire & forget | Highest throughput, no durability guarantee |
| `ONE` / `LOCAL_ONE` | 1 (local write) | Background replication to peers |
| `TWO` / `THREE` | 2 or 3 | Fixed count |
| `LOCAL_QUORUM` | ceil(local_rf / 2) + 1 | Same DC only |
| `QUORUM` | ceil(rf / 2) + 1 | **Recommended default** |
| `ALL` | rf | Highest durability, slowest write |
| `SERIAL` | Paxos quorum | Linearisable; requires Paxos acceptors |

### Read

| Level | Responses needed | Notes |
|-------|-----------------|-------|
| `ONE` | 1 | Local table lookup |
| `QUORUM` | ceil(rf / 2) + 1 | Read repair fires for stale replicas |
| `ALL` | rf | Repairs all stale replicas |
| `SERIAL` | Paxos quorum | Linearisable read via Prepare phase |

### Strong consistency pairs (W + R > N)

| Write | Read | Recommended for |
|-------|------|-----------------|
| `QUORUM` | `QUORUM` | General purpose — best balance |
| `ALL` | `ONE` | Write-heavy workloads |
| `ONE` | `ALL` | Read-heavy workloads |
| `SERIAL` | `SERIAL` | Distributed locks, compare-and-swap |

---

## Read repair

On every `get`, if a replica returns a lower ballot than the winner, a background
`tokio::spawn` task fans out `Replicate` RPCs to all stale replicas.  The caller
is not blocked.

```
get(key, QUORUM)
  │
  ├─ local read:  ballot=500 (winner)
  ├─ n2 read:     ballot=0   ← stale
  └─ n3 read:     ballot=0   ← stale

  ↓ return Some("value") to caller immediately

  ┌─ spawn (background) ─────────────────────────────┐
  │  Replicate(key, winner.value, ballot=500) → n2    │
  │  Replicate(key, winner.value, ballot=500) → n3    │
  └──────────────────────────────────────────────────┘
```

---

## QUORUM write — Cassandra fan-out model

All peer replications are **detached `tokio::spawn` tasks**.  The coordinator
collects acks via an mpsc channel but does not cancel the remaining tasks when
quorum is satisfied.  Every replica eventually receives the write.

```
put(key, val, QUORUM) on n1  (rf=3, quorum=2)
  │
  ├─ WAL.append(record)
  ├─ table.insert(record)                 ← local ack (1/2)
  │
  ├─ spawn → Replicate → n2  (detached)
  └─ spawn → Replicate → n3  (detached)
          │                 │
          ack received       ack received
          (2/2 ✓ return)     (arrives after return — table updated anyway)
```

---

## Paxos (SERIAL consistency)

Three-phase single-decree Paxos with the completion obligation:

```
Phase 1 — Prepare:
  Proposer sends Prepare{ballot, key} to all acceptors.
  Collects quorum Promises.
  If any Promise carries accepted_ballot > 0:
    value = accepted_value  ← completion obligation; must re-propose prior value.

Phase 2 — Propose:
  Proposer sends Propose{ballot, key, value} to all acceptors.
  Collects quorum AcceptReply{accepted=true}.
  On Reject{higher_ballot}: ballot = higher_ballot + 1, retry (max 3 rounds).

Phase 3 — Commit:
  Proposer sends Commit to all (fire-and-forget after quorum accepts).
```

Use `SERIAL` for operations where losing ordering between concurrent writes is a
correctness bug, not just a staleness issue:

- Distributed leader election
- Distributed locks (mutex / semaphore)
- Incrementing a shared counter atomically
- `IF NOT EXISTS` style conditional writes

---

## WAL file layout

```
/var/data/n1/
  wal.dat         ← live write-ahead log
                     (length-delimited WalEntry protobufs, lsn-stamped)
  snapshot.dat    ← last checkpoint (same format, all records)
  snapshot.lsn    ← 8-byte LE u64: 0 after truncation → replay all WAL entries
```

`WalEntry` wire format (proto3):

```proto
message WalEntry {
  uint64 lsn       = 1;
  bytes  key       = 2;
  bytes  value     = 3;
  uint64 ballot    = 4;
  bool   tombstone = 5;
  uint64 timestamp = 6;  // unix millis (diagnostics only)
}
```

---

## Node join / snapshot sync

When a new node joins a running cluster, call `sync_from_peer` to bootstrap
from an existing node's full snapshot before serving traffic:

```rust
// New node, empty MemTable.
let new_node = StorageNode::new("n4", ring, cons, "0.0.0.0:7004", None, wal_path).await?;

// Connect to an existing peer.
new_node.connect_peers(&[("n1", "http://10.0.0.1:7001")]).await?;

// Stream all records from n1 into new_node's MemTable.
// Higher-ballot records always win (LWW by ballot, not wall clock).
new_node.sync_from_peer("n1").await?;

// Now safe to accept writes.
```

---

## Actor usage in the storage layer

`lane_core` actors are used wherever sequential processing or resilience is
required, matching the OTP philosophy: **let it crash, let the supervisor fix it**.

| Component | Why actor | Benefit |
|-----------|-----------|---------|
| **`WalActor`** | Sequential file writes — one message at a time, no lock on the file handle | `post_stop` guarantees flush; supervisor can restart on I/O error without data loss |
| **`PaxosAcceptorHandle`** | State machine (promise → accept → commit) must process one request at a time | Inbox ordering prevents Paxos invariant violations under concurrent proposals |

`MemTable` uses `RwLock<BTreeMap>` (not an actor) because reads vastly
outnumber writes and all reads are pure — no side effects that need serialising.
This mirrors Mnesia's data-plane design: shared memory with locks for the hot
path, actors for the control plane.

---

## Known limitations

| Limitation | Status | Planned |
|------------|--------|---------|
| Tombstone GC | Not implemented — deleted keys persist as tombstones until restart | Phase 9: `gc_grace_seconds` sweep |
| Quorum scan (`StorageGateway::Scan`) | O(keys) ReadReplica RPCs — expensive for large ranges | Phase 10: streaming range read on primary |
| Anti-entropy repair | No background Merkle-tree comparison between nodes | Phase 9 |
| `table.insert` after `paxos_put` uses proposed value | If the completion obligation forced re-proposing a prior value, the local table reflects the original proposed value (not the committed value) | Phase 11: `PaxosProposerClient::write` returns committed value |
| Single-region WAL | No cross-node WAL coordination or batching | Phase 10 |
| No ring rebalancing | Adding/removing nodes requires full restart with updated ring | Phase 12 |

---

## gRPC services

### `StorageService` — internal replication

| RPC | Direction | Purpose |
|-----|-----------|---------|
| `Replicate` | coordinator → replica | Propagate a committed write |
| `ReadReplica` | coordinator → replica | Fetch value for read quorum |
| `Snapshot` | coordinator → peer | Stream all records for node join |

### `StorageGateway` — external client access

| RPC | Purpose |
|-----|---------|
| `Put` | Write with `write_consistency` string field |
| `Get` | Read with `read_consistency` string field |
| `Delete` | Write tombstone |
| `Scan` | Range query (start..=end) |

---

## File map (v0.9.0 touch points)

| File | Change |
|------|--------|
| `src/storage/mod.rs` | `StorageNode`, `StorageClient`, `StorageStats`, `StorageHealth`, all phase 3–8 logic |
| `src/storage/table.rs` | `MemTable`, `Record`, LWW ballot logic, tombstones, range scan |
| `src/storage/router.rs` | `StorageRouter`, `ReplicaSet`, DC-aware filtering |
| `src/storage/replication.rs` | `ReplicationClient` — gRPC client for Replicate/ReadReplica/Snapshot |
| `src/storage/wal.rs` | `WalActor`, `WalHandle`, `replay()`, `load_snapshot()` — **new** |
| `src/hash_ring.rs` | `RingNode.dc: Option<String>` field added |
| `src/consistency.rs` | `WriteConsistency::Serial` variant; `FromStr` for both enums |
| `src/paxos_grpc.rs` | `PaxosProposerClient::write()` — full Prepare→Propose→Commit |
| `proto/storage.proto` | `StorageService`, `StorageGateway`, `WalEntry` messages |
| `build.rs` | `storage.proto` added to compile list |
| `src/proto.rs` | `pub mod storage` include |
| `src/lib.rs` | Re-exports: `StorageClient`, `StorageHealth`, `StorageStats` |
| `docs/storage.md` | Architecture guide, consistency matrix, Paxos deep-dive — **new** |
| `examples/distributed_key_value.rs` | 9 demos — **new** |
| `examples/distributed_key_value.md` | 7 Mermaid diagrams — **new** |
| `Cargo.toml` | `[[example]] distributed_key_value` registered |

---

## Related reading

| Resource | Content |
|----------|---------|
| [`docs/storage.md`](./docs/storage.md) | Full architecture guide with ASCII diagrams, consistency pairs, Paxos completion obligation, WAL recovery |
| [`examples/distributed_key_value.md`](./examples/distributed_key_value.md) | 7 Mermaid diagrams: write fan-out, read repair, Paxos phases, WAL actor, node join, resilience model, roadmap |
| [`examples/distributed_key_value.rs`](./examples/distributed_key_value.rs) | Runnable 9-demo example (`cargo run --example distributed_key_value`) |
| [`READMEv0.0.9.md`](./READMEv0.0.9.md) | Previous release — `ArcSwap` `ChildRegistry`, supervision type guide |
| [`lane_core/README.md`](./lane_core/README.md) | `ActorMonitor`, `ActorStats`, `mean_handle_ms`, handle overhead estimates |
