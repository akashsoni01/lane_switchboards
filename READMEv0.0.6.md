# lane_switchboards v0.0.6

Release notes for **v0.0.6** — Cassandra-style **tunable consistency** on the service mesh (quorum acks, multi-DC `EACH_QUORUM`, Paxos reads), optional **`tls`** / **`metrics`** features, and new examples/docs.

For the full project overview see [README.md](./README.md).  
Next release: [READMEv0.0.7.md](./READMEv0.0.7.md) (gRPC/protobuf wire).  
Previous release notes: [READMEv0.0.5.md](./READMEv0.0.5.md) · [READMEv0.0.4.md](./READMEv0.0.4.md) · [Ideas blog post](./docs/lane_switchboards_blog.md)

---

## What's new in v0.0.6

### 1) Tunable consistency — `src/consistency.rs`

Cassandra-style **W** / **R** levels for mesh writes and reads. The mesh fans out to replicas and waits for the required number of **receipt acks** before returning (application still owns replicated state).

| Type / fn | Role |
|-----------|------|
| `WriteConsistency` | `ANY`, `ONE`…`THREE`, `LOCAL_*`, `QUORUM`, `EACH_QUORUM`, `ALL` |
| `ReadConsistency` | `ONE`…`ALL`, `SERIAL`, `LOCAL_SERIAL` (Paxos path) |
| `ConsistencyConfig` | `rf`, `local_rf`, `dc_rfs`, `dc_names`, `write_cl`, `read_cl`, `ack_timeout` |
| `ConsistencyError` | `NotEnoughReplicas`, `NotEnoughAcks { dc }`, `Timeout`, `PaxosContention` |
| `quorum_for`, `write_acks_required`, `read_acks_required` | Quorum math (`floor(rf/2)+1`) |

Reference: [`docs/consistency.md`](docs/consistency.md)

---

### 2) Acknowledgement protocol — `src/distributed.rs`

Length-prefixed frames gain optional ack correlation:

| Field / type | Role |
|--------------|------|
| `Frame::frame_id`, `Frame::expect_ack` | Correlate inbound work with `AckFrame` (serde defaults preserve old peers) |
| `AckFrame` | `{ frame_id, ok, error?, data? }` — Paxos RPC payload on `data` when needed |
| `RemoteActorRef::send_with_ack` | Persistent connection + oneshot wait; maps to `ConsistencyError` |
| Reconnect loop | Exponential backoff (100ms → 30s) on write/ack failures |

Fire-and-forget `send()` still uses `expect_ack = false` (unchanged behaviour for `invoke()`).

---

### 3) Replica selection + multi-DC — `Cluster` / `ServiceRecord`

| API | Role |
|-----|------|
| `Cluster::replicas_for_key` | Hash-ring replica set (up to `count`) |
| `Cluster::local_replicas_for_key` | Filter by `local_dc` (`dc = None` → local) |
| `Cluster::all_replicas_for_key` | Every member on the ring |
| `Cluster::dc_members`, `dc_replicas_for_key`, `datacenters` | `EACH_QUORUM` per-DC fan-out |
| `ServiceRecord::dc`, `ClusterMember::dc` | Datacenter tags on registry records |

---

### 4) Consistency-aware mesh — `src/mesh.rs`

| API | Role |
|-----|------|
| `ServiceMesh::with_consistency` | Default `ConsistencyConfig` (`LOCAL_QUORUM` / rf=3) |
| `ServiceMesh::invoke_consistent` | Write path — quorum fan-out + ack wait |
| `ServiceMesh::read_consistent` | Read path — quorum acks or Paxos for `SERIAL` |
| `ServiceMesh::read_serial_value` | Linearizable Paxos read → `Option<Vec<u8>>` |
| `ServiceMesh::set_service_consistency` | Per-service override |
| `ServiceMesh::set_tls_connector` | TLS for all joined remote refs (`feature = "tls"`) |
| `MeshRouter::with_consistency`, `invoke_consistent`, `read_consistent` | Sidecar router wrappers |

Existing **`invoke` / `invoke_any` / `invoke_all`** remain fire-and-forget (no ack wait).

**Tracing:** `mesh.invoke` / `mesh.read` spans record `replicas_contacted`, `acks_received`, `latency_ms`.

---

### 5) Paxos read path — `src/paxos.rs`

Lightweight per-key Paxos for `ReadConsistency::Serial` / `LocalSerial`:

- Wire: `Prepare` → `Promise` (read); `Propose` / `Accept` / `Commit` defined for future CAS writes
- `PaxosNode` acceptor per service (`__paxos__{service}`)
- `PaxosProposer::read` — prepare quorum, highest accepted value wins
- Client-side conditional writes: **not implemented** (TODO in source)

---

### 6) Crate features — TLS and metrics optional

| Feature | Enables |
|---------|---------|
| `tls` (optional deps) | `rustls`, `tokio-rustls`, `rustls-pemfile`, `webpki-roots`, module `tls` |
| `metrics` | `ConsistencyMetrics` + `ConsistencyConfig::on_metrics` callback |

**Always on:** [`src/stream.rs`](src/stream.rs) — `MaybeTlsStream` (plain TCP), `connect` / `accept`. Without `tls`, passing a connector returns `Unsupported`.

```toml
[dependencies]
lane_switchboards = { version = "0.5", features = ["tls"] }
# or
lane_switchboards = { version = "0.5", features = ["tls", "metrics"] }
```

TLS-only APIs are `#[cfg(feature = "tls")]` (`serve_microservice_tls`, `Node::bind_tls_on_runtime`, `RemoteActorRef::with_tls`, etc.).

**Dependency policy:** crate dependencies use semver ranges (not `=x.y.z` pins) so downstream workspaces can resolve with other crates.

---

### 7) Examples and documentation

| Artifact | Description |
|----------|-------------|
| [`docs/consistency.md`](docs/consistency.md) | W+R>N, level tables, `EACH_QUORUM`, observability, limitations |
| [`examples/consistency.rs`](examples/consistency.rs) | Flash-sale inventory — `invoke` vs `QUORUM` vs outage (`required-features = ["tls"]`) |
| [`examples/consistency.md`](examples/consistency.md) | Story, mermaid architecture, Envoy sidecar notes (deployment pattern) |
| [`examples/hot_upgrade.rs`](examples/hot_upgrade.rs) | V1→V2 upgrade migrates count via snapshot before `upgrade()` |

```bash
cargo run --example consistency --features tls
cargo test --lib --features "tls,metrics"
```

---

### 8) Library exports (additions)

```rust
pub mod consistency;
pub mod stream;
#[cfg(feature = "tls")]
pub mod tls;
pub mod paxos;

pub use consistency::{
    WriteConsistency, ReadConsistency, ConsistencyConfig, ConsistencyError,
    quorum_for, write_acks_required, read_acks_required, /* ... */
};
#[cfg(feature = "metrics")]
pub use consistency::ConsistencyMetrics;

pub use stream::{MaybeTlsStream, connect as stream_connect, accept as stream_accept, host_from_addr};
#[cfg(feature = "tls")]
pub use tls::{build_acceptor, build_connector, server_config_from_pem, client_config_from_pem, /* ... */};

pub use mesh::{ServiceMesh::invoke_consistent, read_consistent, read_serial_value, /* ... */};
pub use paxos::{PaxosProposer, serve_paxos_acceptor, paxos_target, /* ... */};
```

---

## Migration notes from v0.0.5

### Plain TCP unchanged

Without features, behaviour matches v0.0.5 plain TCP. No TLS deps are pulled in.

### TLS moved behind `feature = "tls"`

1. Enable the feature in your `Cargo.toml` (see above).
2. Imports stay on `lane_switchboards::tls` when the feature is on; PEM helpers are unavailable without it.
3. Low-level framing is on `lane_switchboards::stream` (`MaybeTlsStream`, `stream_connect`).

```bash
# v0.0.5
cargo run --example tls_distributed

# v0.0.6
cargo run --example tls_distributed --features tls
```

### Adopting consistency levels

```rust
use lane_switchboards::{
    ConsistencyConfig, ServiceMesh, WriteConsistency, ReadConsistency,
};

let config = ConsistencyConfig {
    rf: 3,
    write_cl: WriteConsistency::Quorum,
    read_cl: ReadConsistency::Quorum,
    ack_timeout: Duration::from_secs(5),
    ..Default::default()
};
let mesh = ServiceMesh::with_consistency(config);

mesh.invoke_consistent("orders", &order_id, msg).await?;
```

- Keep **`invoke()`** for fire-and-forget (single hash-ring target).
- Use **`invoke_consistent`** when you need W acks across replicas.
- **`read_serial_value`** only for `SERIAL` / `LOCAL_SERIAL`; CAS writes are not supported yet.

### Multi-DC writes (`EACH_QUORUM`)

```rust
ConsistencyConfig {
    write_cl: WriteConsistency::EachQuorum,
    dc_rfs: vec![2, 2],
    dc_names: vec!["east".into(), "west".into()],
    local_dc: "east".into(),
    ..Default::default()
}
```

Tag instances with `ServiceRecord::dc`. Failure in one DC returns `NotEnoughAcks { dc: Some("east"), .. }`.

### Metrics (optional)

```rust
use std::sync::Arc;
use lane_switchboards::ConsistencyMetrics;

let mut config = ConsistencyConfig::default();
config.on_metrics = Some(Arc::new(|m: ConsistencyMetrics| {
    tracing::info!(service = %m.service, ok = m.succeeded, acks = m.acks_received);
}));
```

Requires `features = ["metrics"]`.

---

## Tests and checks

Validated on v0.0.6 changes:

- `cargo test --lib` ✅ (31 tests, no optional features)
- `cargo test --lib --features tls` ✅ (32 tests, + `tls_round_trip`)
- `cargo test --lib --features "tls,metrics"` ✅ (33 tests, + metrics callback)
- `cargo run --example consistency --features tls` ✅

---

## Quick consistency reference

```rust
use lane_switchboards::{
    ConsistencyConfig, ServiceMesh, WriteConsistency, ReadConsistency,
};
use std::time::Duration;

let mesh = ServiceMesh::with_consistency(ConsistencyConfig {
    rf: 3,
    write_cl: WriteConsistency::LocalQuorum,
    read_cl: ReadConsistency::LocalQuorum,
    ack_timeout: Duration::from_secs(5),
    ..Default::default()
});

// W=2 acks when rf=3
mesh.invoke_consistent("inventory", &sku, msg).await?;

// Fire-and-forget (legacy path)
mesh.invoke("inventory", &sku, msg).await?;
```

---

## File map (v0.0.6 touch points)

| File | Changes |
|------|---------|
| `src/consistency.rs` | **New** — levels, quorum math, `ConsistencyConfig`, errors |
| `src/stream.rs` | **New** — `MaybeTlsStream`, connect/accept; TLS types stub without feature |
| `src/tls.rs` | PEM/config only; gated by `feature = "tls"` |
| `src/distributed.rs` | Ack frames, `send_with_ack`, replica/DC helpers, Paxos RPC |
| `src/mesh.rs` | `invoke_consistent`, `read_consistent`, tracing, `set_tls_connector` |
| `src/paxos.rs` | **New** — acceptor + proposer read path |
| `src/lib.rs` | Re-exports, feature-gated `tls` |
| `Cargo.toml` | Features `tls`, `metrics`; optional rustls deps; semver ranges |
| `docs/consistency.md` | **New** |
| `examples/consistency.rs` / `.md` | **New** |
| `examples/hot_upgrade.rs` | Migrate V1 count into V2 on upgrade |
