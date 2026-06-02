# Tunable consistency in the service mesh

The mesh routing layer implements Cassandra-style **tunable consistency**: you choose how many replicas must acknowledge a write (W) or read (R) before the call returns. The mesh fans out to the right replica set and waits for acks; **multi-master replication and conflict resolution remain the application's job**.

## Quick start

```rust
use lane_switchboards::{
    ConsistencyConfig, ServiceMesh, WriteConsistency, ReadConsistency,
};

let config = ConsistencyConfig {
    rf: 3,
    write_cl: WriteConsistency::Quorum,
    read_cl: ReadConsistency::Quorum,
    ..Default::default()
};
let mesh = ServiceMesh::with_consistency(config);

// Consistent write (waits for W=2 acks when rf=3)
mesh.invoke_consistent("orders", &order_id, msg).await?;

// Fire-and-forget (unchanged API)
mesh.invoke("orders", &order_id, msg).await?;
```

## The W + R > N formula

With replication factor **N = 3**:

| Level | Acks required |
|-------|---------------|
| ONE | 1 |
| QUORUM | 2 (`floor(3/2) + 1`) |
| ALL | 3 |

**Strong consistency** (reads see the latest write) is guaranteed when **W + R > N**:

| Write (W) | Read (R) | W + R | Strong? |
|-----------|----------|-------|---------|
| QUORUM (2) | QUORUM (2) | 4 > 3 | Yes |
| ONE (1) | QUORUM (2) | 3 ≯ 3 | No |
| QUORUM (2) | ONE (1) | 3 ≯ 3 | No |
| ALL (3) | ONE (1) | 4 > 3 | Yes |

Use **LOCAL_QUORUM** / **LOCAL_ONE** when you only need consistency within the local datacenter (lower latency, no cross-DC wait).

## Write consistency levels

| Level | Behaviour |
|-------|-----------|
| **ANY** | Fire-and-forget to one replica (via hash ring). No ack wait. |
| **ONE / TWO / THREE** | Wait for that many acks cluster-wide. |
| **LOCAL_ONE / LOCAL_QUORUM** | Same, but only local-DC replicas (`ConsistencyConfig::local_dc`). |
| **QUORUM** | `quorum_for(rf)` acks cluster-wide. |
| **EACH_QUORUM** | Quorum in **every** DC (see below). |
| **ALL** | All `rf` replicas must ack. |

Configure via `ConsistencyConfig::write_cl` or per-service override with `ServiceMesh::set_service_consistency`.

## Read consistency levels

| Level | Behaviour |
|-------|-----------|
| **ONE … ALL / LOCAL_*** | Fan out and wait for R replica **receipt** acks (response routing is app-level). |
| **SERIAL** | Paxos prepare round cluster-wide; linearizable read. |
| **LOCAL_SERIAL** | Paxos prepare round in local DC only. |

Use `read_consistent` for quorum ack reads and `read_serial_value` when you need the accepted Paxos value.

## QUORUM vs SERIAL

| | QUORUM | SERIAL |
|---|--------|--------|
| Protocol | Parallel ack wait | Paxos Prepare → Promise |
| Latency | Lower | Higher (extra round) |
| Guarantees | Tunable (depends on W+R) | Linearizable read |
| Use when | Eventual / bounded staleness OK | Must not read stale value during contention |

**QUORUM** is the default balance for most CRUD. **SERIAL** (or **LOCAL_SERIAL**) when you need Cassandra LWT-style linearizability on reads.

## EACH_QUORUM multi-DC setup

`EACH_QUORUM` requires a quorum in **each** datacenter independently:

```rust
ConsistencyConfig {
    write_cl: WriteConsistency::EachQuorum,
    dc_rfs: vec![2, 2],           // RF per DC
    dc_names: vec!["east".into(), "west".into()],
    local_dc: "east".into(),
    ..Default::default()
}
```

- Tag replicas with `ServiceRecord::dc` (or `ClusterMember::dc`). `None` = local DC.
- `dc_replicas_for_key` selects ring-first replicas per DC.
- Failure in one DC returns `ConsistencyError::NotEnoughAcks { dc: Some("east"), .. }`.

## Observability

### Tracing

`invoke_consistent` and `read_consistent` emit spans:

- `mesh.invoke` / `mesh.read`
- Fields: `service`, `consistency_level`, `replicas_contacted`, `acks_received`, `latency_ms`
- A `tracing::warn!` when the operation succeeds but `acks_received < rf` (degraded quorum)

### Metrics (feature `metrics`)

Enable in `Cargo.toml`:

```toml
lane_switchboards = { version = "0.5", features = ["metrics"] }
```

Register a callback on `ConsistencyConfig::on_metrics`:

```rust
use std::sync::Arc;
use lane_switchboards::ConsistencyMetrics;

config.on_metrics = Some(Arc::new(|m: ConsistencyMetrics| {
    println!("{} {:?} ok={}", m.service, m.consistency_level, m.succeeded);
}));
```

## Known limitations

- **No client-side CAS writes** — Paxos Propose/Accept from the client is not implemented; only SERIAL/LOCAL_SERIAL **reads** use Paxos today.
- **No tombstones or LWW** — last-write-wins and deletion semantics are application responsibility.
- **Ack = receipt, not execution** — quorum paths confirm the remote actor accepted the message into its mailbox, not that business logic completed.
- **Replication is not automatic** — the mesh routes and waits; your service must write/read the same key on each replica if you want replicated state.

## API reference

| Type / method | Role |
|---------------|------|
| `WriteConsistency` / `ReadConsistency` | Level enums |
| `ConsistencyConfig` | RF, DC tags, timeouts, optional metrics callback |
| `ConsistencyError` | `NotEnoughReplicas`, `NotEnoughAcks`, `Timeout`, `PaxosContention` |
| `ServiceMesh::invoke_consistent` | W-level write path |
| `ServiceMesh::read_consistent` | R-level read path |
| `ServiceMesh::read_serial_value` | Paxos read returning `Option<Vec<u8>>` |
| `Cluster::replicas_for_key` | Hash-ring replica selection |
| `Cluster::dc_replicas_for_key` | Per-DC replica selection for EACH_QUORUM |
