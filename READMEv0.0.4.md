# lane_switchboards v0.0.4

Release notes for **v0.0.4** — production hardening across the registry, distributed TCP layer, consistent-hash ring, and service mesh.

For the full project overview see [README.md](./README.md).  
Previous release notes: [READMEv0.0.3.md](./READMEv0.0.3.md) · [READMEv0.0.2.md](./READMEv0.0.2.md)

**Ideas-first overview:** [docs/lane_switchboards_blog.md](./docs/lane_switchboards_blog.md)

---

## What's new in v0.0.4

### 1) Global registry — single actor index

`src/registry.rs` merges the former split maps into one `DashMap<ActorId, ActorEntry>`:

| Before | After |
|--------|-------|
| `ACTOR_CONTROL_SENDERS` + `SUPERVISOR_CHANNELS` | `ACTORS: DashMap<ActorId, ActorEntry>` |
| Two inserts on spawn | One atomic `register_actor(id, control, supervisor)` |
| `Registry` struct with broken `global()` | Free functions: `actor_count()`, `registered_ids()` |

Control and supervisor channels for an actor are always registered together, eliminating torn state if one insert failed.

---

### 2) Supervisor — review hardening

`src/supervisor.rs` fixes from the v0.0.4 review pass:

| Fix | Detail |
|-----|--------|
| No double `Arc<Mutex>` | `slots` owned directly by the supervisor task |
| No double restart log on failed restart | Failed attempts counted once toward intensity |
| `Handle::current()` at spec registration | Captured in `ChildSlot::child_spec`, `spawn_child_spec`, `supervise_actor` |
| `ChildRegistry` uses `Mutex` | Replaces `RwLock` for simpler async usage |
| Inline restart strategies | No `restart_indices` `Vec` alloc for `OneForOne` |
| `SupervisorHandle::stop()` | Graceful shutdown — stops children in reverse order |
| `AbandonChild` | Marks slot `ActorId::DEAD` |
| `spawn_child_spec` | Uses `track_and_bump` (generation bumps on restart) |

---

### 3) Distributed TCP — nine production fixes

`src/distributed.rs` — data-plane reliability and security:

| # | Issue | Fix |
|---|--------|-----|
| 1 | New TCP connection per `send` | Persistent per-peer write channel + background reconnect loop |
| 2 | Unbounded frame size (OOM) | `DistributedConfig::max_frame_bytes` (default 4 MiB) |
| 3 | Mutex held across `tx.send().await` | Clone sender under lock, release, then send |
| 4 | `broadcast` fails fast | Attempts all nodes; returns first error at end |
| 5 | `serve_actor` silent drop on spawn failure | Spawn actor **before** TCP bind; propagate error |
| 6 | `Cluster::leave` O(n) | `swap_remove` + single index map update |
| 7 | Round-robin u64 wrap | Documented as negligible; unchanged behavior |
| 8 | No read timeout | `DistributedConfig::read_timeout` (default 30s) on length + body |
| 9 | Redundant API | Removed `serve_actor_with_config`; kept `serve_actor`, `serve_actor_on_current_runtime`, `serve_actor_on_runtime` |

**Extended `DistributedConfig`:**

```rust
DistributedConfig {
    bridge_capacity: 32,
    max_in_flight: 32,
    max_frame_bytes: 4 * 1024 * 1024,   // NEW
    read_timeout: Duration::from_secs(30), // NEW
    remote_send_capacity: 32,           // NEW — outbound queue per RemoteActorRef
}
```

---

### 4) Hash ring — stable, correct, faster lookups

`src/hash_ring.rs` — consistent routing that cluster members can agree on:

| Fix | Detail |
|-----|--------|
| **Stable hasher** | Replaced `DefaultHasher` with **MurmurHash3 x64 128** (`murmur3` crate); lower 64 bits used for ring positions |
| **Collision visibility** | `add_node` logs `tracing::warn!` when a virtual-node hash collides |
| **`get_nodes` perf** | Lazy iterator chain — no intermediate `Vec` materialization |
| **Address parsing** | `RingNode::try_from_socket_addr` returns `Result`; `from_socket_addr` panics with clear message |
| **`Hash`/`Eq` contract** | `RingNode` derives `Hash` on all fields (removed id-only manual impl) |

**Dependency added:** `murmur3 = "=0.5.2"`

> **Migration note:** Ring placement changes when switching hash algorithms. All cluster members must run the same version. Keys remap — plan for brief routing churn on upgrade.

---

### 5) Service mesh — control-plane hardening

`src/mesh.rs` — eight fixes for registry, routing, and instance lifecycle:

| # | Fix | Detail |
|---|-----|--------|
| 1 | Control frame OOM | `MAX_CONTROL_FRAME` = 64 KiB on read/write |
| 2 | Connect per registry call | `MeshRegistryClient` holds persistent `TcpStream`, reconnects on IO error |
| 3 | O(n) registry storage | `HashMap<(service, instance_id), ServiceRecord>` |
| 4 | Upsert ring gap | `ServiceMesh::register` returns `Option<ServiceRecord>` for displaced instance |
| 5 | Dead instances forever | `registered_at` + `DEFAULT_RECORD_TTL` (30s) + background eviction; `renew()` on client |
| 6 | No read timeout | 30s timeout on control-plane reads |
| 7 | `target = service` collision | `serve_microservice` sets `target = instance_id` (unique dispatch slot) |
| 8 | Full mesh clear on sync | `apply_snapshot_diff` — removes stale instances only; mesh never fully empty mid-sync |

**API changes:**

```rust
// Before
MeshRegistryClient::register(addr, record).await?;
MeshRegistryClient::list(addr).await?;

// After
let mut client = MeshRegistryClient::new(addr);
client.register(record).await?;
client.list().await?;
client.renew(record).await?;  // lease refresh
```

`MeshRouter::with_registry` now owns a `MeshRegistryClient`. Use `router.registry_client()` for renewals.

**`ServiceRecord` new field:**

```rust
pub struct ServiceRecord {
    pub service: String,
    pub instance_id: String,
    pub address: String,
    pub target: String,        // now typically == instance_id
    pub registered_at: u64,      // NEW — unix seconds, set by registry
}
```

Exported: `DEFAULT_RECORD_TTL` (30 seconds).

---

## Migration notes from v0.0.3

### `MeshRegistryClient` is now stateful

```rust
// One-shot join at startup
if let Some(addr) = registry_addr {
    MeshRegistryClient::new(addr)
        .register(handle.record.clone())
        .await?;
}

// Long-lived router with periodic sync + renew
let mut router = MeshRouter::with_registry("127.0.0.1:9050");
router.sync().await?;
if let Some(client) = router.registry_client() {
    client.renew(handle.record.clone()).await?;
}
```

Run a renew loop (interval < `DEFAULT_RECORD_TTL`) for every registered instance, or the registry evicts stale records.

### `ServiceRecord.target` is per-instance

Remote frames route by `target`. After upgrade, ensure callers use `record.target` from discovery (not the service name). `serve_microservice` sets `target = instance_id`.

### `DistributedConfig` — add new fields

If you construct `DistributedConfig` manually, include the three new fields or use `DistributedConfig::default()`.

### Hash ring — expect remapping

MurmurHash3 replaces the previous unstable hasher. Same key may route to a different node after upgrade until all members run v0.0.4.

### `ServiceMesh::register` return value

Upserts now return the previous record. Refresh cached `RemoteActorRef` handles after address changes:

```rust
if let Some(old) = mesh.register(record.clone()) {
    tracing::info!(old = %old.address, new = %record.address, "instance upserted — refresh refs");
}
```

---

## Tests and checks

Validated on v0.0.4 changes:

- `cargo test` ✅ (22 tests: unit, integration, supervisor)
- Hash ring: stable lookup, collision-safe insert, address validation
- Mesh: multi-instance routing, registry control plane, diff-based snapshot sync
- Distributed: cluster hash routing, round-robin

---

## Quick reference defaults (v0.0.4)

```rust
ActorConfig {
    mailbox_capacity: 64,
    handle_timeout: None,
    slow_handle_threshold: None,
}

DistributedConfig {
    bridge_capacity: 32,
    max_in_flight: 32,
    max_frame_bytes: 4 * 1024 * 1024,
    read_timeout: Duration::from_secs(30),
    remote_send_capacity: 32,
}

SupervisorConfig {
    strategy: OneForOne,
    max_restarts: 5,
    within_secs: 10,
    intensity_action: ShutdownSupervisor,
    mailbox_capacity: 32,
}

// Mesh
DEFAULT_RECORD_TTL = 30 seconds
MAX_CONTROL_FRAME  = 64 KiB
```

---

## File map (v0.0.4 touch points)

| File | Changes |
|------|---------|
| `src/registry.rs` | Unified `ACTORS` DashMap |
| `src/supervisor.rs` | Hardening, `stop()`, generation tracking |
| `src/distributed.rs` | Persistent TCP, frame limits, timeouts, cluster fixes |
| `src/hash_ring.rs` | MurmurHash3, lazy `get_nodes`, address validation |
| `src/mesh.rs` | TTL registry, persistent client, diff sync, instance targets |
| `src/config.rs` | Extended `DistributedConfig` |
| `Cargo.toml` | `murmur3 = "=0.5.2"` |
| `examples/service_mesh.rs` | Updated `MeshRegistryClient` usage |
