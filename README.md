# lane_switchboards

**Rust actor runtime inspired by the telecom switchboard.** Lightweight isolated actors route messages through mailboxes; supervisors restart failed workers so one bad call never takes down the whole board.

OTP-style primitives in Rust: actors, supervision, linking, monitoring, distributed messaging, and hot code upgrade.

In Erlang/OTP, linking and unlinking are built-in mechanisms for managing process lifecycles. They define how processes react if a related process crashes or terminates.

## The switchboard analogy

Erlang was built for telephone exchanges — physical **switchboards** where operators plugged cables to route calls. That hardware metaphor became the language’s concurrency model:

| Switchboard idea | Erlang/OTP | lane_switchboards |
|------------------|------------|-------------------|
| **Route calls to the right jack** | Isolated processes + message passing | `Actor` + `ActorRef::send` → mailbox |
| **Each operator’s local plug board** | Process mailbox | `mpsc` channel + `Envelope<M>` |
| **One bad line must not kill the exchange** | “Let it crash” + supervision | Supervisors restart failed children |
| **Replace a failed circuit automatically** | `OneForOne` / `RestForOne` restart | `supervisor.rs` strategies + intensity limits |
| **Know who’s up across the floor** | Process registry | `registry.rs` global `DashMap` |
| **Calls between exchanges** | Distributed Erlang | `distributed.rs` TCP-framed remote actors |

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
| **Distributed actors** | TCP JSON frames in core (`distributed.rs`) | Bring your own | Not included | Cluster features vary |
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
| `supervisor.rs` | OneForOne / OneForAll / RestForOne, `ChildRegistry`, `ChildSlot`, `spawn_child_spec` |
| `registry.rs` | Global `DashMap` actor index |
| `distributed.rs` | TCP-framed remote actors, `Cluster` roster, `serve_actor`, hash-ring routing |
| `hash_ring.rs` | `HashRing` / `RingNode` consistent-hash discovery |
| `mesh.rs` | TCP service mesh — `ServiceMesh`, registry, `MeshRouter`, `serve_microservice` |

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

| Helper | When to use |
|--------|-------------|
| **`serve_actor(name, bind_addr, target, actor)`** | Bind a local node and bridge frames to a local actor |
| **`Cluster::join(member)`** | Register a remote node's address in the roster |
| **`Cluster::send_by_key(key, msg)`** | Route to the node chosen by consistent hash |
| **`Cluster::broadcast(msg)`** | Send to every member (`M: Clone`; fails on first error) |
| **`Cluster::send_all(msg)`** | Send to every member; returns per-node results |
| **`Cluster::send_to(names, msg)`** | Send to a named subset |
| **`Cluster::send_replicas(key, n, msg)`** | Primary + next nodes on hash ring |
| **`Cluster::send_round_robin(msg)`** | Spread work across all members (no stickiness) |
| **`HashRing`** | Standalone consistent-hash discovery ([`hash_ring.rs`](src/hash_ring.rs)) |

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

## TCP service mesh

Microservices over TCP with a **control plane** (register / discover) and **data plane** (route frames by service name).

| Component | Role |
|-----------|------|
| **`MeshRegistryServer`** | TCP registry — instances call `Register` / clients call `List` |
| **`serve_microservice`** | Bind one service instance (data plane) |
| **`ServiceMesh` / `MeshRouter`** | Route `invoke(service, key, msg)` via hash ring per service |
| **`join_mesh`** | Register locally + with TCP registry |

```rust
use lane_switchboards::mesh::{
    join_mesh, serve_microservice, MeshRegistryServer, MeshRouter,
};

let registry = MeshRegistryServer::bind("127.0.0.1:9050").await?;
let handle = serve_microservice("orders", "orders-1", "127.0.0.1:0", OrdersActor).await?;
join_mesh(&mut mesh, Some(&registry.address), &handle).await?;

let mut router = MeshRouter::with_registry(&registry.address);
router.sync().await?;
router.invoke("orders", &order_id, msg).await?;           // one instance (sticky)
router.invoke_all("orders", health_msg).await;            // every replica
```

See [`service_mesh.md`](examples/service_mesh.md) (`cargo run --example service_mesh`).

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
| Distributed messaging | `cargo run --example distributed_demo` |
| Horizontal scaling (add cluster nodes) | `cargo run --example horizontal_scaling` — see [horizontal_scaling.md](examples/horizontal_scaling.md) |
| Horizontal scaling + RestForOne multi-actor sites | `cargo run --example horizontal_scaling_rest_for_one` — see [horizontal_scaling_rest_for_one.md](examples/horizontal_scaling_rest_for_one.md) |
| TCP service mesh (orders / inventory / billing) | `cargo run --example service_mesh` — see [service_mesh.md](examples/service_mesh.md) |

## Tests

```bash
cargo test
```
