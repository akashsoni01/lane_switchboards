# Distributed Key-Value Store — Example Walkthrough

Run the example:

```bash
cargo run --example distributed_key_value
```

This demo exercises all nine capability areas of the embedded distributed KV
layer built into `lane_switchboards`. Each demo function is self-contained and
explains one concept.

---

## Architecture Overview

```mermaid
graph TB
    subgraph External["External Access"]
        Client["StorageClient\n(gRPC stub)"]
    end

    subgraph Node["StorageNode"]
        direction TB
        Gateway["StorageGateway\n(tonic server)"]
        Core["put / get / delete\n(routing + quorum logic)"]
        MemTable["MemTable\n(RwLock&lt;BTreeMap&gt;)"]
        WalActor["WalActor\n(lane_core Actor)\nserial file I/O"]
        Paxos["PaxosAcceptorHandle\n(lane_core Actor + gRPC)"]
        Router["StorageRouter\n(HashRing wrapper)"]
        RepClients["ReplicationClients\n(one per peer)"]
    end

    subgraph Peers["Peer Nodes"]
        P2["Peer n2\nStorageService gRPC"]
        P3["Peer n3\nStorageService gRPC"]
    end

    Client -->|"gRPC: Put/Get/Delete/Scan"| Gateway
    Gateway --> Core
    Core --> Router
    Core --> MemTable
    Core --> WalActor
    Core -->|"SERIAL writes"| Paxos
    Core --> RepClients
    RepClients -->|"gRPC: Replicate\nReadReplica\nSnapshot"| P2
    RepClients -->|"gRPC: Replicate\nReadReplica\nSnapshot"| P3
```

---

## Demo 1 — Single-node RAM-only

The simplest configuration: one node, replication factor 1, no WAL.

```mermaid
sequenceDiagram
    participant App
    participant StorageNode
    participant MemTable

    App->>StorageNode: put("user:1:name", "Alice", ONE)
    StorageNode->>MemTable: insert(Record{ballot=1, value="Alice"})
    StorageNode-->>App: Ok(())

    App->>StorageNode: get("user:1:name", ONE)
    StorageNode->>MemTable: get(key)
    MemTable-->>StorageNode: Some(Record{value="Alice"})
    StorageNode-->>App: Some("Alice")

    App->>StorageNode: delete("user:1:name", ONE)
    StorageNode->>MemTable: write tombstone(ballot=2)
    App->>StorageNode: get("user:1:name", ONE)
    StorageNode-->>App: None  (tombstone)
```

---

## Demo 2 — QUORUM write fan-out

With rf=3, a QUORUM write fans out to **all** replicas but only waits for 2 acks.
The remaining replica still receives the write via a detached `tokio::spawn` task.

```mermaid
sequenceDiagram
    participant App
    participant n1 as StorageNode n1
    participant n2 as StorageNode n2
    participant n3 as StorageNode n3

    App->>n1: put(key, val, QUORUM)
    Note over n1: ballot = atomic_incr()
    n1->>n1: WAL.append(record)
    n1->>n1: table.insert(record)     [local ack = 1]

    par Detached fan-out (all replicas)
        n1-)n2: Replicate(key, val, ballot, expect_ack=true)
        n1-)n3: Replicate(key, val, ballot, expect_ack=true)
    end

    n2-->>n1: Ack{ok=true}           [peer_acks = 1  ≥ needed=1]
    Note over n1: QUORUM satisfied → return Ok(())
    n1-->>App: Ok(())

    Note over n3: Still receives Replicate (detached task)
    n3-->>n1: Ack{ok=true}           [ignored — already returned]
```

> **Key insight:** Every replica eventually receives the write even if we stopped
> waiting after quorum. This is the Cassandra "send to all, wait for quorum" model.

---

## Demo 3 — Tunable consistency levels

```mermaid
graph LR
    subgraph "Write Consistency"
        ANY["ANY\nfire & forget\nno acks"] --> ONE
        ONE["ONE\nlocal ack\n+ background"] --> LOCAL_ONE
        LOCAL_ONE["LOCAL_ONE\nsame DC only"] --> LOCAL_QUORUM
        LOCAL_QUORUM["LOCAL_QUORUM\nceil(local_rf/2)+1"] --> QUORUM
        QUORUM["QUORUM\nceil(rf/2)+1\nrecommended"] --> ALL
        ALL["ALL\nrf acks\nhighest durability"]
    end

    subgraph "Read Consistency"
        R_ONE["ONE\nlocal read"] --> R_QUORUM
        R_QUORUM["QUORUM\nread repair\nrecommended"] --> R_ALL
        R_ALL["ALL\ncollect all\nrepairs stale"] --> R_SERIAL
        R_SERIAL["SERIAL\nPaxos Prepare\nlinearisable"]
    end
```

**Strong consistency formula: W + R > N**

| Write     | Read   | W  | R  | W+R | Strong? |
|-----------|--------|----|----|-----|---------|
| ALL (3)   | ONE (1)| 3  | 1  | 4   | ✓       |
| QUORUM(2) |QUORUM(2)| 2 | 2  | 4   | ✓ ← balanced choice |
| ONE (1)   | ALL (3)| 1  | 3  | 4   | ✓       |
| ONE (1)   | ONE (1)| 1  | 1  | 2   | ✗ eventual only |
| SERIAL    | SERIAL | —  | —  | —   | ✓ linearisable |

---

## Demo 4 — Read repair

Read repair heals stale replicas transparently on every `get`.

```mermaid
sequenceDiagram
    participant App
    participant n1 as n1 (has data)
    participant n2 as n2 (stale)
    participant n3 as n3 (stale)

    Note over n1: table[product:sku:x1] = {ballot=500, value="price:99.99"}
    Note over n2,n3: table[product:sku:x1] = (empty)

    App->>n1: get(key, ALL)
    n1->>n1: local read → {ballot=500}
    n1->>n2: ReadReplica(key) → {found=false, ballot=0}
    n1->>n3: ReadReplica(key) → {found=false, ballot=0}

    Note over n1: winner = ballot 500 (n1's record)\nstale = [n2, n3]

    n1-->>App: Some("price:99.99")

    Note over n1: spawn async repair task
    n1-)n2: Replicate(key, "price:99.99", ballot=500)
    n1-)n3: Replicate(key, "price:99.99", ballot=500)

    Note over n2,n3: Now consistent ✓
```

---

## Demo 5 — WAL persistence and crash recovery

The WAL is backed by a lane-core `Actor` — the actor's mailbox serialises all
file writes, and `post_stop` ensures a final flush on graceful shutdown.

```mermaid
sequenceDiagram
    participant App
    participant StorageNode
    participant WalActor as WalActor\n(lane_core Actor)
    participant Disk

    App->>StorageNode: put(key, val, ONE)
    StorageNode->>WalActor: Append{entry, reply_channel}
    WalActor->>Disk: write length-delimited WalEntry
    WalActor-->>StorageNode: Ok(lsn=1)
    StorageNode->>StorageNode: table.insert(record)
    StorageNode-->>App: Ok(())

    Note over App,Disk: Node dropped (crash simulation)
    WalActor->>Disk: flush on post_stop

    Note over App: StorageNode::new(wal_path) — restart
    StorageNode->>Disk: load_snapshot(snapshot.dat)
    StorageNode->>Disk: replay(wal.dat, skip_lsn=0)
    StorageNode->>StorageNode: table.insert(all replayed records)
    Note over StorageNode: All writes survive crash ✓
```

**Checkpoint + WAL truncation:**

```mermaid
sequenceDiagram
    participant App
    participant Node
    participant WalActor
    participant Disk

    App->>Node: checkpoint(dir)
    Node->>WalActor: Checkpoint{snapshot_path, records, lsn_path}
    WalActor->>Disk: write all records → snapshot.dat
    WalActor->>Disk: write 0 → snapshot.lsn
    WalActor->>Disk: truncate(wal.dat, 0) + seek(0)
    Note over WalActor: self.lsn = 0  (WAL restarts)
    WalActor-->>Node: Ok(())

    Note over App: Continue writing — WAL restarts from lsn=1
    App->>Node: put(new_key, …, ONE)
    Node->>WalActor: Append (lsn=1 in fresh WAL)
```

---

## Demo 6 — SERIAL (Paxos) linearisable writes

Single-decree Paxos provides per-key linearisability.  Every Paxos write goes
through three phases:

```mermaid
sequenceDiagram
    participant Proposer as PaxosProposerClient\n(on StorageNode n1)
    participant A1 as PaxosAcceptor\n(on n1)
    participant A2 as PaxosAcceptor\n(on n2)
    participant A3 as PaxosAcceptor\n(on n3)

    Note over Proposer: ballot = atomic_incr()

    Proposer->>A1: Prepare{ballot=B, key}
    Proposer->>A2: Prepare{ballot=B, key}
    Proposer->>A3: Prepare{ballot=B, key}

    A1-->>Proposer: Promise{accepted_ballot=0}
    A2-->>Proposer: Promise{accepted_ballot=0}
    Note over Proposer: quorum promises, no prior value → propose "v2.0.0"

    Proposer->>A1: Propose{ballot=B, value="v2.0.0"}
    Proposer->>A2: Propose{ballot=B, value="v2.0.0"}
    A1-->>Proposer: AcceptReply{accepted=true}
    A2-->>Proposer: AcceptReply{accepted=true}
    Note over Proposer: quorum accepts → commit

    Proposer-)A1: Commit{ballot=B}
    Proposer-)A2: Commit{ballot=B}
    Proposer-)A3: Commit{ballot=B}

    Note over Proposer: table.insert locally
```

**Completion obligation** — if a proposer's Prepare sees a previously accepted
value, it *must* re-propose that value (not its own) to avoid losing committed data:

```mermaid
sequenceDiagram
    participant P2 as New Proposer\n(ballot=B2)
    participant A1 as Acceptor n1\n(has accepted B1/"old")
    participant A2 as Acceptor n2\n(has accepted B1/"old")

    P2->>A1: Prepare{ballot=B2}
    P2->>A2: Prepare{ballot=B2}
    A1-->>P2: Promise{accepted_ballot=B1, accepted_value="old"}
    A2-->>P2: Promise{accepted_ballot=B1, accepted_value="old"}
    Note over P2: Completion obligation!\nMust propose "old", not "new"
    P2->>A1: Propose{ballot=B2, value="old"}
    P2->>A2: Propose{ballot=B2, value="old"}
    Note over P2: "old" is now committed with higher ballot
```

---

## Demo 7 — External StorageClient

`StorageClient` connects to a `StorageNode`'s `StorageGateway` gRPC server.

```mermaid
graph LR
    App["Application\nCode"]
    Client["StorageClient\n(default: QUORUM/QUORUM)"]
    Gateway["StorageGateway\n(tonic service)"]
    Node["StorageNode\nput/get/delete"]

    App -->|"put(key, val)"| Client
    App -->|"put_cl(key, val, ONE)"| Client
    App -->|"get(key)"| Client
    App -->|"get_cl(key, SERIAL)"| Client
    Client -->|"gRPC PutRequest\n{write_consistency:'QUORUM'}"| Gateway
    Gateway -->|"WriteConsistency::from_str"| Node
    Node -->|"MemTable + Replication"| Gateway
    Gateway -->|"Ack{ok=true}"| Client
    Client -->|"Ok(())"| App
```

---

## Demo 8 — Node join via `sync_from_peer`

New nodes bootstrap from an existing peer's full snapshot before accepting writes.

```mermaid
sequenceDiagram
    participant Joiner as New Node\n(empty MemTable)
    participant Primary as Primary Node\n(50 records)
    participant App

    App->>Primary: insert 50 records directly
    App->>Joiner: connect_peers([("primary", addr)])
    App->>Joiner: sync_from_peer("primary")

    Joiner->>Primary: Snapshot(from_ballot=0)

    loop Streaming chunks (500 records/chunk)
        Primary-->>Joiner: SnapshotChunk{records=[…], done=false}
        Joiner->>Joiner: table.insert if received.ballot > local.ballot
    end
    Primary-->>Joiner: SnapshotChunk{done=true}

    Note over Joiner: 50 records ✓ ready to serve traffic
```

---

## Demo 9 — Observability

```mermaid
graph TD
    subgraph StorageNode
        Stats["StorageStatsCell\n(AtomicU64 counters)"]
        Table["MemTable"]
        WAL["WalActor"]
        Clients["ReplicationClients"]
        Ring["StorageRouter"]
    end

    subgraph Snapshots
        SS["StorageStats\nputs_total, gets_total\ndeletes_total, read_repairs\npaxos_writes, quorum_failures\nwal_bytes_written, tombstone_count"]
        SH["StorageHealth\nnode_id, record_count\ntombstone_count, wal_bytes\nreplica_peers, ring_rf"]
    end

    Stats -->|"stats()"| SS
    Table -->|"tombstone_count()"| SS
    Table -->|"len(), tombstone_count()"| SH
    WAL -->|"bytes_appended"| SH
    Clients -->|"len()"| SH
    Ring -->|"rf"| SH
```

---

## Resilience Model

```mermaid
graph TD
    subgraph "Crash tolerance"
        WAL_A["WalActor<br/>(lane_core Actor)"]
        POST["post_stop flush<br/>on graceful shutdown"]
        SNAP["checkpoint()<br/>snapshot + WAL truncate"]
        REC["StorageNode::new(wal_path)<br/>load snapshot → replay WAL"]
    end

    subgraph "Network fault tolerance"
        RR["Read Repair<br/>stale replica → background Replicate"]
        SFP["sync_from_peer()<br/>streaming snapshot for joins"]
        QUORUM["Quorum acks<br/>tolerates rf - quorum failures"]
    end

    subgraph "Concurrent access safety"
        RWLOCK["RwLock&lt;BTreeMap&gt;<br/>(MemTable) — many readers"]
        ACTOR_MAILBOX["Actor mailbox<br/>(WalActor) — serial writes"]
        ATOMIC["AtomicU64 ballot<br/>no contention on hot path"]
    end

    WAL_A --> POST
    WAL_A --> SNAP
    SNAP --> REC
    POST --> REC
```

---

## Future Improvements (Roadmap)

```mermaid
graph LR
    subgraph Current["✅ Implemented"]
        P1["Phase 1-3\nMemTable, Ring, StorageNode\nBasic put/get/delete"]
        P4["Phase 4\nMulti-node replication\nRead repair, Node join"]
        P5["Phase 5\nPaxos SERIAL writes\nCompletion obligation"]
        P6["Phase 6\nWAL Actor, Checkpoint\nCrash recovery"]
        P7["Phase 7\nStorageClient gRPC\nExternal access"]
        P8["Phase 8\nStats, Health, Docs\nObservability"]
    end

    subgraph Phase9["🚧 Phase 9 — Anti-entropy"]
        AE["Merkle-tree partition\ncomparison per ring segment"]
        HH["Hinted Handoff\nwrite buffer for offline replicas"]
    end

    subgraph Phase10["🔮 Phase 10 — Persistence upgrade"]
        GC["Tombstone GC\n(gc_grace_seconds)"]
        SST["SSTable / LSM compaction\n(replace BTreeMap on disk)"]
        WAL2["Cross-node WAL\n(commitlog batching)"]
    end

    subgraph Phase11["🔮 Phase 11 — Multi-Paxos"]
        MP["Multi-Paxos log\n(overwrite semantics)"]
        CAS["Compare-and-swap\n(conditional writes)"]
        TX["Lightweight transactions\nIF NOT EXISTS"]
    end

    subgraph Phase12["🔮 Phase 12 — Operations"]
        REPAIR["Full cluster repair\n(manual anti-entropy scan)"]
        REBAL["Ring rebalancing\n(vnodes add/remove)"]
        BACKUP["Snapshot export\n(S3 / object storage)"]
    end

    P1 --> P4
    P4 --> P5
    P5 --> P6
    P6 --> P7
    P7 --> P8
    P8 --> Phase9
    Phase9 --> Phase10
    Phase9 --> Phase11
    Phase10 --> Phase12
    Phase11 --> Phase12
```

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **`WalActor` (lane_core Actor)** | Actor mailbox serialises writes without locks; `post_stop` guarantees flush; supervisor can restart on I/O error |
| **`RwLock<BTreeMap>` for MemTable** | Many concurrent readers (standard read path) with rare exclusive writes (replication receives); BTreeMap preserves key ordering for range scans and ring partition splits |
| **`PaxosAcceptorHandle` via Actor** | Acceptor state machine (promise/accept/commit) benefits from serial message processing and `lane_core` supervision |
| **Detached `tokio::spawn` for replication** | All replicas receive every write; quorum collection is decoupled from delivery; prevents 3rd replica starvation when QUORUM is satisfied by 2 |
| **Ballot = per-node `AtomicU64`** | Monotonically increasing, no coordination required for ballot generation; SystemTime stored for diagnostics only, never used for LWW ordering |
| **Tombstones, never physical deletes** | Safe concurrent reads; required for correct read repair (stale replica must not resurrect a deleted key with lower ballot) |
| **Single `StorageGateway` gRPC server** | One port per node handles both internal (StorageService: Replicate, ReadReplica, Snapshot) and external (StorageGateway: Put, Get, Delete, Scan) traffic |
