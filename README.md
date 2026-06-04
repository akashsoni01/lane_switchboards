# lane_switchboards

**Rust actor runtime inspired by the telecom switchboard.** Lightweight isolated actors route messages through mailboxes; supervisors restart failed workers so one bad call never takes down the whole board.

OTP-style primitives in Rust: actors, supervision, linking, monitoring, distributed messaging, and hot code upgrade.

Actors run with strict OTP mailbox semantics: one message handled at a time (sequential runtime).

**Release notes:** [v0.7.0](READMEv0.0.7.md) · [v0.0.6](READMEv0.0.6.md) · [v0.0.5](READMEv0.0.5.md) · [v0.0.4](READMEv0.0.4.md) · [Ideas blog post](docs/lane_switchboards_blog.md)

In Erlang/OTP, linking and unlinking are built-in mechanisms for managing process lifecycles. They define how processes react if a related process crashes or terminates.

## The switchboard analogy

Erlang was built for telephone exchanges — physical **switchboards** where operators plugged cables to route calls. That hardware metaphor became the language’s concurrency model:

| Switchboard idea | Erlang/OTP | lane_switchboards |
|------------------|------------|-------------------|
| **Route calls to the right jack** | Isolated processes + message passing | `Actor` + `ActorRef::send` → mailbox |
| **Each operator’s local plug board** | Process mailbox | `mpsc` channel + `Envelope<M>` |
| **One bad line must not kill the exchange** | “Let it crash” + supervision | Supervisors restart failed children |
| **Replace a failed circuit automatically** | `OneForOne` / `RestForOne` restart | `supervisor.rs` strategies + intensity limits |
| **Know who’s up across the floor** | Process registry | `registry.rs` unified `ACTORS` index |
| **Calls between exchanges** | Distributed Erlang | `distributed.rs` gRPC/protobuf remote actors |

Telecom switches had to stay up under massive concurrent load — faults contained, components replaced in place. That heritage is why Erlang powers high-concurrency systems (messaging backends, IoT routing, soft real-time services). **lane_switchboards** brings the same *shape* — mailboxes, supervision trees, fault isolation — to Rust on top of Tokio, in a small readable core you can extend.

For the canonical reference, see the [Erlang/OTP System Documentation](https://www.erlang.org/doc/system).

## Why lane_switchboards?

Lane Switchboards is **not** a replacement for Tokio (runtime) or Actix Web (HTTP). It is a **small OTP-style actor layer** for fault-tolerant, message-driven domain logic. Use it when you want Erlang-like supervision and lifecycle primitives in Rust without adopting a larger framework.

| | **lane_switchboards** | **Tokio** (tasks + channels) | **Actix Web** | **Ractor** |
|---|----------------------|------------------------------|---------------|------------|
| **Primary role** | OTP actor runtime | Async runtime & I/O | HTTP server / web framework | Production actor framework |
| **Actor mailboxes** | Built-in (`Envelope`, `ActorRef`) | Roll your own (`mpsc`) | Not core (use for HTTP handlers) | Built-in |
| **Supervision trees** | OneForOne / OneForAll / RestForOne + intensity limits | None — manual restart loops | None | Via `ractor-supervisor` |
| **Link / monitor / trap_exit** | Yes — OTP semantics | None | None | Partial (monitoring exists) |
| **Hot code upgrade** | In-process `DynActor` swap | None | None | Not built-in |
| **Distributed actors** | gRPC/protobuf, bidi streams, cluster roster | Bring your own | Not included | Cluster features vary |
| **Global actor registry** | `DashMap` index for cross-actor routing | None | None | Registry patterns available |
| **Panic → supervisor path** | `catch_unwind` in `handle` | Task dies silently unless you wrap | Request fails; no actor tree | Framework-dependent |
| **Learning curve** | Small codebase (~1k LOC core) | Low-level, you design everything | Web-focused API | Medium, ecosystem docs |
| **Best for** | Supervised domain actors, demos, custom OTP, recoverable state patterns | Any async Rust | REST/gRPC gateways, HTTP | Production actor systems at scale |

### When to reach for lane_switchboards

- You want **OTP supervision** (restart strategies, fault isolation) without Erlang/Elixir.
- You need **linking, monitoring, or hot upgrade** as first-class mailbox messages.
- You are building **long-lived stateful workers** (calculators, switches, journals) that must survive panics.
- You want a **readable, forkable runtime** — the core is small enough to extend (journal replay, TLS on `Node`, etc.).

### When to use something else instead

| Need | Prefer |
|------|--------|
| HTTP APIs, routing, middleware | **Actix Web**, Axum, Poem |
| Raw concurrency, no actor model | **Tokio** tasks + channels |
| Mature actor ecosystem, PG groups, benchmarks | **Ractor** |
| Full distributed OTP (cluster, remote spawn) | **Erlang/Elixir** |
| High-throughput minimal actors | Dedicated actor crates or custom Tokio |

## Library (`src/`)

| Module | Capability |
|--------|------------|
| `actor.rs` | Actors, linking, monitoring, hot upgrade |
| `supervisor.rs` | OneForOne / OneForAll / RestForOne, `ChildRegistry`, `ChildSlot`, `spawn_child_spec`, `SupervisorHandle::stop()` |
| `registry.rs` | Unified `ACTORS` `DashMap` — control + supervisor channels registered atomically |
| `distributed.rs` | gRPC data plane (`ActorMessaging`), acked deliver, `Cluster`, `serve_actor` |
| `consistency.rs` | Write/read consistency levels, quorum math, `ConsistencyConfig` |
| `stream.rs` | `MaybeTlsStream`, plain TCP connect/accept |
| `tls.rs` | rustls PEM loaders (`feature = "tls"`) |
| `paxos.rs` | Paxos acceptor + linearizable read (`SERIAL` / `LOCAL_SERIAL`) |
| `hash_ring.rs` | `HashRing` / `RingNode` — MurmurHash3 consistent-hash (stable across builds) |
| `config.rs` | `ActorConfig` mailbox + timeout tuning; `DistributedConfig` bridge, in-flight, frame limits |
| `monitor.rs` | `ActorMonitor`, `ActorStats` — per-actor runtime counters |
| `mesh.rs` | gRPC service mesh — TTL registry, tonic client, diff sync, `serve_microservice` |

### When to use `supervisor.rs`

Use `src/supervisor.rs` when actor failure should be part of normal control flow (OTP style), not a process-ending bug.

| Situation | Why supervisor is the right fit | API to start with |
|-----------|----------------------------------|-------------------|
| One actor must auto-recover from panic/timeout | Keep mailbox endpoint alive across restarts | `supervise_actor`, `supervise_actor_with_config` |
| Several actors have dependency order | Restart downstream dependents on upstream failure | `Supervisor::new` + `RestartStrategy::RestForOne` |
| You need stable actor handles after restart | Ref IDs change after respawn | `ChildRegistry<M, K>` or `ChildSlot<M>` |
| You need restart storm protection | Prevent infinite crash loops | `SupervisorConfig { max_restarts, within_secs, intensity_action }` |
| Graceful supervisor shutdown | Stop children in reverse order | `SupervisorHandle::stop()` |
| Startup must finish before traffic | Ensure pre-start/register steps settle | `start_settled(Duration)` |

## One supervisor, many children

Yes — a single `Supervisor` manages **multiple child actors**. Pass several child specs to `Supervisor::new`; on `start()` the supervisor spawns every child and listens on one shared restart channel.

Use the built-in helpers so you do not reimplement spawn-and-track boilerplate in every example:

| Helper | When to use |
|--------|-------------|
| **`spawn_child_spec(order, name, registry, build)`** | Multiple named children under one supervisor (RestForOne chains, strategy demos) |
| **`ChildRegistry<M>`** | Stable `ActorRef` lookup by name after restart; optional generation counters |
| **`ChildSlot<M>`** + **`ChildSlot::child_spec`** | Single supervised child with a stable handle |
| **`Supervisor::start_settled(duration)`** | Start and wait briefly for initial spawns to settle |

| Strategy | When one child fails |
|----------|----------------------|
| `OneForOne` | Restart only the failed child |
| `OneForAll` | Restart all children |
| `RestForOne` | Restart the failed child and every child with higher `order` |

**Multiple children (RestForOne):**

```rust
use lane_switchboards::supervisor::{
    spawn_child_spec, ChildRegistry, RestartStrategy, Supervisor, SupervisorConfig,
};
use std::sync::Arc;
use std::time::Duration;

let registry = Arc::new(ChildRegistry::new());

let handle = Supervisor::new(
    SupervisorConfig {
        strategy: RestartStrategy::RestForOne,
        ..Default::default()
    },
    vec![
        spawn_child_spec(0, "upstream", registry.clone(), || UpstreamWorker { /* … */ }),
        spawn_child_spec(1, "downstream", registry.clone(), || DownstreamWorker { /* … */ }),
    ],
)
.start_settled(Duration::from_millis(50))
.await?;

// Look up live refs after restart:
let upstream = registry.get("upstream").await;
```

**Single child (OneForOne):**

```rust
use lane_switchboards::supervisor::{ChildSlot, Supervisor, SupervisorConfig};

let slot = Arc::new(ChildSlot::new());
let spec = ChildSlot::child_spec(0, slot.clone(), || MyWorker::default());

let _handle = Supervisor::new(SupervisorConfig::default(), vec![spec]).start().await?;
let worker = slot.require().await?;
```

Low-level `child_spec(order, factory)` is still available when you need a custom spawn factory.

**Constraints**

| Topic | Detail |
|-------|--------|
| Message type | All children under one supervisor must use the same `M` (`Supervisor<M>`). Unify with a shared enum if needed. |
| `supervise_actor` | Convenience helper for **one** child only — use `Supervisor::new` + `vec![…]` for multiple. |
| Restart intensity | `max_restarts` / `within_secs` is shared across the whole supervisor, not per child. When too many restart events land inside the sliding `within_secs` window, the supervisor stops restarting (`ShutdownSupervisor` by default). See [rest_for_one_calculator_timer.md](examples/rest_for_one_calculator_timer.md#intensity-limits-max_restarts-within_secs). |
| Child handles | `start()` does not return `ActorRef`s — use `ChildRegistry` (named) or `ChildSlot` (single child); both are updated on every restart |
| `order` | First argument to `spawn_child_spec(order, …)` or `child_spec(order, …)`; used by `RestForOne` for startup/restart dependency order |

See [`supervisor_strategies.md`](examples/supervisor_strategies.md) (`cargo run --example supervisor_strategies`) for live demos of each strategy and intensity limits.

## Horizontal scaling (cluster roster)

Add computing capacity by binding new TCP nodes and joining them to a shared [`Cluster`](src/distributed.rs) roster. Existing nodes keep running — no restart required.

**Data-plane reliability (v0.0.4):** each `RemoteActorRef` keeps a persistent TCP write channel with automatic reconnect; inbound frames are capped (`max_frame_bytes`, default 4 MiB) and reads time out (`read_timeout`, default 30s). `serve_actor` spawns the local actor **before** binding TCP so failed spawns never leave a listener with no handler.

| Helper | When to use |
|--------|-------------|
| **`serve_actor(name, bind_addr, target, actor)`** | Bind a local node and bridge frames to a local actor |
| **`serve_actor_on_runtime(..., distributed, actor_config)`** | Full control over runtime + `DistributedConfig` |
| **`Cluster::join(member)`** | Register a remote node's address in the roster |
| **`Cluster::send_by_key(key, msg)`** | Route to the node chosen by consistent hash |
| **`Cluster::broadcast(msg)`** | Send to every member; returns first error after attempting all |
| **`Cluster::send_all(msg)`** | Send to every member; returns per-node results |
| **`Cluster::send_to(names, msg)`** | Send to a named subset |
| **`Cluster::send_replicas(key, n, msg)`** | Primary + next nodes on hash ring |
| **`Cluster::send_round_robin(msg)`** | Spread work across all members (no stickiness) |
| **`HashRing`** | Standalone MurmurHash3 consistent-hash ring ([`hash_ring.rs`](src/hash_ring.rs)) |

```rust
use lane_switchboards::distributed::{serve_actor, Cluster};

let node_a = serve_actor("worker-a", "127.0.0.1:0", "worker", MyWorker::default()).await?;
let node_b = serve_actor("worker-b", "127.0.0.1:0", "worker", MyWorker::default()).await?;

let mut cluster = Cluster::new();
cluster.join(node_a.member.clone());
cluster.join(node_b.member.clone());

// Later: scale out on new hardware
let node_c = serve_actor("worker-c", "0.0.0.0:9002", "worker", MyWorker::default()).await?;
cluster.join(node_c.member.clone());

cluster.send_by_key(&job_id, WorkMsg::Process { job_id }).await?;
// Same job_id always maps to the same node until the ring changes.
```

See [`horizontal_scaling.md`](examples/horizontal_scaling.md) (`cargo run --example horizontal_scaling`) and [`horizontal_scaling_rest_for_one.md`](examples/horizontal_scaling_rest_for_one.md) (RestForOne processor + reporter per site, multi-send APIs).

## TLS on gRPC (`feature = "tls"`)

Optional **rustls** via [`TlsConfig`](src/config.rs) PEM fields. Plain HTTP/2 is the default when `tls` is `None`.

| Layer | Server | Client |
|-------|--------|--------|
| Distributed data plane | `DistributedConfig.tls` on `Node::bind_on_current_runtime` | `RemoteActorRef::connect` with same config |
| Mesh registry | `MeshRegistryHandle::bind_with_tls` | `MeshRegistryClient::connect_with_tls` |
| Microservice | `serve_microservice_tls` | `ServiceMesh::set_tls` for fan-out |

See [`tls_distributed.md`](examples/tls_distributed.md) (`cargo run --example tls_distributed --features tls`) and [`docs/wire_protocol.md`](docs/wire_protocol.md).

## gRPC service mesh

Microservices over gRPC with a **control plane** (`MeshRegistry`) and **data plane** (`ActorMessaging` bidi `Deliver` streams).

| Component | Role |
|-----------|------|
| **`MeshRegistryHandle`** | gRPC registry with lease TTL (`DEFAULT_RECORD_TTL`, 30s) and background eviction |
| **`MeshRegistryClient`** | tonic client; `register`, `list`, `watch` |
| **`serve_microservice`** | Bind one instance — deliver `target` is the unique `instance_id` |
| **`ServiceMesh` / `MeshRouter`** | Route `invoke(service, key, msg)` via hash ring per service |
| **`join_mesh`** | Register locally + with gRPC registry |

Wire types must implement [`RemoteMessage`](src/distributed.rs) (`prost::Message` + `Default`). See [`service_mesh.md`](examples/service_mesh.md) (`cargo run --example service_mesh`).

## Deadlock / slow-handle prevention

Stuck or slow `handle()` calls are bounded via [`ActorConfig`](src/config.rs):

| Field | Role |
|-------|------|
| `handle_timeout` | Max wall time per `handle()` — overrun → `on_handle_stuck` → `ExitReason::HandleTimeout` → supervisor restart |
| `slow_handle_threshold` | Warn + count handles that finish but exceed this duration (defaults to `handle_timeout`) |

**Lifecycle hooks** on [`Actor`](src/actor.rs):

| Method | When |
|--------|------|
| `on_handle_begin(&msg)` | Before each `handle` — store pending work for recovery |
| `on_handle_stuck(ctx)` | After timeout — persist journal / stuck action |
| `handle(msg)` | Normal processing |

**Monitor:** `ActorMonitor::global().get(actor_id)` / `.all()` — messages handled, panics, timeouts, in-flight, last/max handle ms.

```rust
use lane_switchboards::{ActorConfig, ActorMonitor, spawn_with_config};
use std::time::Duration;

let config = ActorConfig {
    handle_timeout: Some(Duration::from_secs(5)),
    ..Default::default()
};
let (actor, _) = spawn_with_config(MyWorker, None, &config).await?;
// ...
let stats = ActorMonitor::global().get(actor.id);
```

### Latest latency snapshot (sequential runtime)

Measured on the current design (`cargo run --example handle_timeout_calculator_timer_latency`, 2026-06-01, debug build):

| Metric | Value |
|--------|-------|
| warmup / samples | 5 / 50 |
| `add` end-to-end | min **40 µs**, avg **91 µs**, max **1321 µs** |
| `slow_div` (0ms delay) end-to-end | min **1165 µs**, avg **1237 µs**, max **1499 µs** |
| `last_result` end-to-end | min **38 µs**, avg **63 µs**, max **386 µs** |
| Full demo wall-clock | **~3.4 s** |

The wall-clock run includes demonstration sleeps, restart choreography, and timeout phases; the microsecond figures above are success-path request latency.

## Configuration defaults

```rust
use lane_switchboards::supervisor::{IntensityAction, RestartStrategy, SupervisorConfig};
use lane_switchboards::{ActorConfig, DistributedConfig};
use std::time::Duration;

ActorConfig {
    mailbox_capacity: 64,
    handle_timeout: None,
    slow_handle_threshold: None,
}

DistributedConfig {
    bridge_capacity: 32,
    max_in_flight: 32,                              // per-node dispatch semaphore
    max_frame_bytes: 4 * 1024 * 1024,               // reject oversized inbound frames
    read_timeout: Duration::from_secs(30),          // TCP read timeout per frame
    remote_send_capacity: 32,                       // outbound queue per RemoteActorRef
}

SupervisorConfig {
    strategy: RestartStrategy::OneForOne,
    max_restarts: 5,
    within_secs: 10,
    intensity_action: IntensityAction::ShutdownSupervisor,
    mailbox_capacity: 32,
}
```

See [READMEv0.0.5.md](READMEv0.0.5.md) for migration notes (hash ring remapping, mesh client API, `ServiceRecord.target`, TLS).

## Examples

| Example | Command |
|---------|---------|
| Hot code upgrade | `cargo run --example hot_upgrade` |
| Envelope variants (link, monitor, upgrade, …) | `cargo run --example envelope_demo` — see [envelope_demo.md](examples/envelope_demo.md) |
| Supervisor strategies + intensity limits | `cargo run --example supervisor_strategies` — see [supervisor_strategies.md](examples/supervisor_strategies.md) |
| Calculator (add, sub, mul, div) | `cargo run --example calculator` — see [calculator.md](examples/calculator.md) |
| Resilient calculator (survives panic) | `cargo run --example resilient_calculator` — see [resilient_calculator.md](examples/resilient_calculator.md) |
| Resilient calculator + last-result timer | `cargo run --example resilient_calculator_timer` |
| Recoverable calculator + journal timer | `cargo run --example recoverable_timer_calc` — see [recoverable_timer_calc.md](examples/recoverable_timer_calc.md) |
| RestForOne calculator + timer | `cargo run --example rest_for_one_calculator_timer` — see [rest_for_one_calculator_timer.md](examples/rest_for_one_calculator_timer.md) (includes `max_restarts` / `within_secs` intensity breach) |
| RestForOne calculator + timer (optimized macros) | `cargo run --example rest_for_one_calculator_timer_optimized` — see [rest_for_one_calculator_timer_optimized.md](examples/rest_for_one_calculator_timer_optimized.md) |
| Latency + deadlock recovery benchmark | `cargo run --example handle_timeout_calculator_timer_latency` — typed child keys (`ChildRegistry<M, K>`) + success-path latency probes |
| Distributed messaging (gRPC) | `cargo run --example distributed_demo` |
| gRPC cluster (hash ring + round-robin) | `cargo run --example grpc_cluster` |
| Distributed messaging (TLS gRPC) | `cargo run --example tls_distributed --features tls` — see [tls_distributed.md](examples/tls_distributed.md) |
| Write consistency / quorum (`--features tls`) | `cargo run --example consistency --features tls` — see [consistency.md](examples/consistency.md) |
| Horizontal scaling (add cluster nodes) | `cargo run --example horizontal_scaling` — see [horizontal_scaling.md](examples/horizontal_scaling.md) |
| Horizontal scaling + RestForOne multi-actor sites | `cargo run --example horizontal_scaling_rest_for_one` — see [horizontal_scaling_rest_for_one.md](examples/horizontal_scaling_rest_for_one.md) |
| gRPC service mesh (orders / inventory / billing) | `cargo run --example service_mesh` — see [service_mesh.md](examples/service_mesh.md) |
| Supervised services + autoscale cluster | `cargo run --example service_complex_cluster` |
| **E-commerce flash sale** (mesh + supervision + autoscale + QUORUM) | `cargo run --example ecommerce_flash_sale` — see [ecommerce_flash_sale.md](examples/ecommerce_flash_sale.md) |
| Calculator on service mesh (RestForOne + prost) | `cargo run --example calculator_mesh` — see [calculator_mesh.md](examples/calculator_mesh.md) |
| Calculator mesh (minimal) | `cargo run --example calculator_mesh_simplified` — see [calculator_mesh_simplified.md](examples/calculator_mesh_simplified.md) |

## Benchmarks

gRPC wire benchmarks live in [`benches/wire.rs`](benches/wire.rs) (Criterion). They measure localhost paths on a single machine (release build, no TLS):

```bash
cargo bench --bench wire
cargo bench --bench ecommerce   # full checkout saga
```

Results below are from one run on **macOS (Apple Silicon), release profile, Tokio multi-thread runtime, all peers on `127.0.0.1`**. Your numbers will vary with CPU load and OS; use these as relative comparisons between operations, not SLA guarantees.

| Benchmark | Typical time (median) | What it measures |
|-----------|----------------------|------------------|
| `remote_actor_ref_send` | **~1.8 µs** | One fire-and-forget `RemoteActorRef::send` on a warm bidi `Deliver` stream |
| `mesh_registry_list_32` | **~187 µs** | `MeshRegistryClient::list` with 32 registered instances |
| `invoke_consistent_quorum_rf3` | **~139 µs** | `ServiceMesh::invoke_consistent` (QUORUM, W=2 of rf=3) across three local replicas |
| `ecommerce_checkout_pipeline` | **~84 µs** | Order send + QUORUM inventory reserve + billing invoke ([`ecommerce_flash_sale`](examples/ecommerce_flash_sale.rs)) |

Criterion output (same run):

```
remote_actor_ref_send        [1.68 µs … 2.02 µs]   median ≈ 1.80 µs
mesh_registry_list_32        [180.8 µs … 193.2 µs] median ≈ 187 µs
invoke_consistent_quorum     [135.3 µs … 142.6 µs] median ≈ 139 µs
ecommerce_checkout_pipeline  [83.0 µs … 85.6 µs]   median ≈ 84 µs
```

See [`docs/wire_protocol.md`](docs/wire_protocol.md) and [`examples/ecommerce_flash_sale.md`](examples/ecommerce_flash_sale.md) for architecture.

## Tests

```bash
cargo test
```
