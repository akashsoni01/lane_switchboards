# lane_switchboards

**Rust actor runtime inspired by the telecom switchboard.** Lightweight isolated actors route messages through mailboxes; supervisors restart failed workers so one bad call never takes down the whole board.

OTP-style primitives in Rust: actors, supervision, linking, monitoring, distributed messaging, and hot code upgrade.

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
| `supervisor.rs` | OneForOne / OneForAll / RestForOne |
| `registry.rs` | Global `DashMap` actor index |
| `distributed.rs` | TCP-framed remote actors |

## One supervisor, many children

Yes — a single `Supervisor` manages **multiple child actors**. Pass several `child_spec`s to `Supervisor::new`; on `start()` the supervisor spawns every child and listens on one shared restart channel.

| Strategy | When one child fails |
|----------|----------------------|
| `OneForOne` | Restart only the failed child |
| `OneForAll` | Restart all children |
| `RestForOne` | Restart the failed child and every child with higher `order` |

```rust
use lane_switchboards::actor::{spawn, Actor, ActorRef};
use lane_switchboards::supervisor::{
    child_spec, RestartStrategy, Supervisor, SupervisorConfig,
};

enum WorkerMsg { Ping }

struct WorkerA;
struct WorkerB;

// Both actors must share the same message type M (here: WorkerMsg).
let spec_a = child_spec(0, |sup_tx| {
    Box::pin(async move { spawn(WorkerA, Some(sup_tx)).await.map(|(r, _)| r) })
});
let spec_b = child_spec(1, |sup_tx| {
    Box::pin(async move { spawn(WorkerB, Some(sup_tx)).await.map(|(r, _)| r) })
});

let sup = Supervisor::new(
    SupervisorConfig {
        strategy: RestartStrategy::OneForOne,
        ..Default::default()
    },
    vec![spec_a, spec_b],
);
let _handle = sup.start().await?;
```

**Constraints**

| Topic | Detail |
|-------|--------|
| Message type | All children under one supervisor must use the same `M` (`Supervisor<M>`). Unify with a shared enum if needed. |
| `supervise_actor` | Convenience helper for **one** child only — use `Supervisor::new` + `vec![…]` for multiple. |
| Restart intensity | `max_restarts` / `within_secs` is shared across the whole supervisor, not per child. When too many restart events land inside the sliding `within_secs` window, the supervisor stops restarting (`ShutdownSupervisor` by default). See [rest_for_one_calculator_timer.md](examples/rest_for_one_calculator_timer.md#intensity-limits-max_restarts-within_secs). |
| Child handles | `start()` does not return `ActorRef`s — capture them in the factory (see [recoverable_timer_calc.rs](examples/recoverable_timer_calc.rs)) or read from the registry. |
| `order` | Set on each `child_spec(order, …)`; used by `RestForOne` to define startup/restart dependency order. |

See [`supervisor_strategies.md`](examples/supervisor_strategies.md) (`cargo run --example supervisor_strategies`) for live demos of each strategy and intensity limits.

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

## Tests

```bash
cargo test
```
