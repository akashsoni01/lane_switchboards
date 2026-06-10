# Distributed Storage Layer

`lane_switchboards` ships an embedded distributed key-value store that combines:

- **Cassandra-style consistent hash ring** — determines which N nodes own each key.
- **Paxos** (via `PaxosProposerClient` / `PaxosAcceptorHandle`) — provides per-key
  linearisable writes at `SERIAL` consistency.
- **Tunable consistency levels** — choose between `ANY`, `ONE`, `QUORUM`, `ALL`,
  `SERIAL`, and their `LOCAL_*` variants.
- **Write-ahead log (WAL)** backed by a lane-core `Actor` for ordered, crash-safe writes.

---

## Architecture

```
External Client
      │
      ▼  gRPC (StorageGateway)
┌────────────────────────────────────────────────────┐
│                   StorageNode                      │
│                                                    │
│  ┌──────────┐  ┌────────────┐  ┌───────────────┐  │
│  │ MemTable │  │  WalActor  │  │ PaxosAcceptor │  │
│  │(RwLock   │  │ (lane_core │  │ (lane_core    │  │
│  │ BTreeMap)│  │  Actor)    │  │  Actor + gRPC)│  │
│  └──────────┘  └────────────┘  └───────────────┘  │
│                                                    │
│  ┌──────────────────────────────────────────────┐  │
│  │  ReplicationClients  (one per peer node)     │  │
│  │  StorageRouter       (HashRing wrapper)      │  │
│  └──────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────┘
      │                          │
      ▼ gRPC StorageService      ▼ gRPC PaxosService
   Peer Node                  Peer Node
```

**Request flow — QUORUM write:**

```
Client  →  Gateway.Put
              │
              ▼
         StorageNode.put(key, val, QUORUM)
              │
              ├─ ballot = atomic_incr()
              ├─ wal.append(record)             ← WAL first (crash-safe)
              ├─ table.insert(record)           ← local write
              │
              ├─ spawn(replicate → peer_n2)  ─╮ detached tokio tasks
              ├─ spawn(replicate → peer_n3)  ─╯ (all replicas get it)
              │
              └─ collect first quorum acks → Ok(())
```

**Request flow — SERIAL write (Paxos):**

```
Client  →  Gateway.Put (write_consistency: "SERIAL")
              │
              ▼
         StorageNode.paxos_put(key, val, local_only=false)
              │
         PaxosProposerClient.write(key, val, quorum, timeout)
              │
              ├─ Phase 1: Prepare  → collect quorum Promises
              │    (if prior accepted value found: must re-propose it)
              ├─ Phase 2: Propose  → collect quorum Accepts
              │    (on rejection: bump ballot, retry up to 3×)
              └─ Phase 3: Commit   → fire-and-forget
              │
         table.insert(record)  ← reflect locally after commit
```

---

## Consistency Level Guide

### Strong consistency: W + R > N

To guarantee that a read always sees the latest write, choose pairs where the
sum of acknowledgements exceeds the replication factor (N):

| Write            | Read            | W+R | Notes                             |
|------------------|-----------------|-----|-----------------------------------|
| `ALL` (N)        | `ONE` (1)       | N+1 | Strongest; write latency = slowest replica |
| `QUORUM` (N/2+1) | `QUORUM` (N/2+1)| N+1 | Best balance of durability+speed  |
| `SERIAL`         | `SERIAL`        | —   | Linearisable via Paxos            |
| `ONE`            | `ALL`           | N+1 | Slow reads; fast writes           |
| `ONE`            | `ONE`           | 2   | ✗ — NOT strong consistency       |

### Eventual consistency

`ANY`, `ONE`, or `LOCAL_ONE` writes paired with `ONE` reads deliver the best
latency but may return stale data.  Use **read repair** (enabled automatically
on every `get`) to converge stale replicas in the background.

---

## Paxos Path (SERIAL consistency)

Use `WriteConsistency::Serial` / `ReadConsistency::Serial` when you need
**linearisable** (compare-and-swap-level) guarantees across nodes.

**When to use it:**
- Leader election / distributed locks.
- Incrementing a shared counter atomically.
- Any operation where losing the ordering between concurrent writes would be
  a correctness bug (not just a staleness bug).

**What "completion obligation" means:**

If a Paxos Proposer sends `Prepare` and receives a `Promise` that already carries
an `accepted_ballot > 0` and `accepted_value`, it **must** propose that value
instead of its own.  This prevents a half-committed write from being permanently
lost.  `PaxosProposerClient::write` enforces this — if any promise in the quorum
carries an earlier accepted value, that value is re-proposed and committed before
the new write can proceed.

```rust
// Correct client code for a linearisable counter increment:
node.put(key.clone(), new_value, WriteConsistency::Serial).await?;
let current = node.get(&key, ReadConsistency::Serial).await?;
```

---

## WAL and Checkpoint

### Configuration

Pass `wal_path: Some(PathBuf)` to `StorageNode::new` to enable persistence.
RAM-only mode (`wal_path: None`) is suitable for caches and test fixtures.

```rust
let node = StorageNode::new(
    "node1".into(),
    ring,
    consistency,
    "0.0.0.0:7001",
    Some("0.0.0.0:7002"),  // paxos addr
    Some(PathBuf::from("/var/data/node1")),
).await?;
```

### File layout

```
/var/data/node1/
  wal.dat          ← live write-ahead log (length-delimited WalEntry protobufs)
  snapshot.dat     ← latest checkpoint (same format as WAL, all records)
  snapshot.lsn     ← 8-byte LE u64: checkpoint LSN (0 after truncation)
```

### Write path

Every `put` / `delete` goes through the `WalActor` before any replication:

```
put(key, val) → WAL.append(record) → table.insert() → replicate to peers
```

The WAL actor is a lane-core `Actor`: writes are processed one at a time from
the mailbox, ensuring a consistent, ordered log with no lock contention.

### Checkpoint

```rust
node.checkpoint(&PathBuf::from("/var/data/node1")).await?;
```

The checkpoint operation (handled atomically inside `WalActor`):

1. Flushes the BufWriter and fsyncs the WAL file.
2. Serialises the full MemTable to `snapshot.dat`.
3. Writes `0` to `snapshot.lsn` (WAL will be truncated).
4. Truncates `wal.dat` to zero and resets the internal LSN counter.

### Startup recovery

When `StorageNode::new` receives a `wal_path`:

1. If `snapshot.dat` exists → loads it into the MemTable.
2. Replays `wal.dat` entries with `lsn > checkpoint_lsn` into the MemTable.
3. Spawns the WAL actor pointing at the (truncated or empty) `wal.dat`.

---

## Observability

### Counters (`StorageNode::stats()`)

| Field               | Meaning                                       |
|---------------------|-----------------------------------------------|
| `puts_total`        | Total calls to `put()`                        |
| `gets_total`        | Total calls to `get()`                        |
| `deletes_total`     | Total calls to `delete()`                     |
| `read_repairs`      | Stale replicas repaired via background write  |
| `paxos_writes`      | SERIAL writes dispatched through Paxos        |
| `quorum_failures`   | Writes/reads that returned a quorum error     |
| `wal_bytes_written` | Approximate bytes appended to the WAL         |
| `tombstone_count`   | Number of deleted-but-not-GCed keys           |

### Health (`StorageNode::health()`)

```rust
let h = node.health();
println!("{} peers={} rf={} records={}", h.node_id, h.replica_peers, h.ring_rf, h.record_count);
```

### Tracing

All storage operations emit `tracing` events:

| Level   | Event                                         |
|---------|-----------------------------------------------|
| `info`  | `storage.put` / `storage.get` / `storage.delete` with `key_len` and `cl` |
| `debug` | `read repair triggered` with `stale_node`     |
| `warn`  | `write quorum shortfall` / `write timed out`  |
| `error` | `WAL append failure`                          |

---

## Known Limitations

- **Tombstone GC not implemented.** Deleted keys accumulate a tombstone record
  in the MemTable until the process is restarted.
  <!-- TODO: tombstone GC after gc_grace_seconds -->

- **Quorum scan is O(keys) RPCs.** `StorageGateway::Scan` fans out a
  `ReadReplica` RPC per key to enforce quorum, which is expensive for large
  ranges.
  <!-- TODO: replace with streaming range read on primary + async repair -->

- **No anti-entropy repair.** There is no background Merkle-tree comparison
  between nodes.  Consistency is maintained through read repair and
  `sync_from_peer` (full snapshot sync on node join) only.
  <!-- TODO: Phase 9 — anti-entropy with Merkle partitions per hash-ring segment -->

- **Single-region WAL.** Each `StorageNode` writes its own WAL independently.
  Cross-node WAL coordination (like Cassandra's commitlog batching) is not
  implemented.
