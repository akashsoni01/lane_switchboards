# lane_switchboards

OTP-style actor runtime in Rust: actors, supervision, linking, monitoring, distributed messaging, and hot code upgrade.

See **[architecture.md](./architecture.md)** for Mermaid diagrams and module breakdown.

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

The **gateway** example combines Actix Web (HTTP edge) with Ractor (client pool) — lane_switchboards handles the **supervised actor core**; web and HTTP layers stay in their own crates.

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
| Restart intensity | `max_restarts` / `within_secs` is shared across the whole supervisor, not per child. |
| Child handles | `start()` does not return `ActorRef`s — capture them in the factory (see [recoverable_timer_calc.rs](examples/recoverable_timer_calc.rs)) or read from the registry. |
| `order` | Set on each `child_spec(order, …)`; used by `RestForOne` to define startup/restart dependency order. |

## Examples

| Example | Command |
|---------|---------|
| Actix gateway + Ractor RestTemplate | `cargo run --example gateway` |
| Hot code upgrade | `cargo run --example hot_upgrade` |
| Envelope variants (link, monitor, upgrade, …) | `cargo run --example envelope_demo` — see [envelope_demo.md](examples/envelope_demo.md) |
| Calculator (add, sub, mul, div) | `cargo run --example calculator` — see [calculator.md](examples/calculator.md) |
| Resilient calculator (survives panic) | `cargo run --example resilient_calculator` — see [resilient_calculator.md](examples/resilient_calculator.md) |
| Resilient calculator + last-result timer | `cargo run --example resilient_calculator_timer` |
| Recoverable calculator + journal timer | `cargo run --example recoverable_timer_calc` — see [recoverable_timer_calc.md](examples/recoverable_timer_calc.md) |
| Distributed messaging | `cargo run --example distributed_demo` |
| ONDC signing (testecom → dummy server) | `cargo run --example ondc_demo` — see [ondc.md](examples/gateway/ondc.md) |

Gateway (after `cargo run --example gateway`):

- `GET http://127.0.0.1:8080/health`
- `GET http://127.0.0.1:8080/fetch` — single request (pooled connection reuse)
- `GET http://127.0.0.1:8080/fetch-parallel` — parallel GETs to multiple httpbin endpoints

```rust
// Parallel calls from code
handle.get_parallel(["https://api.a.com/x", "https://api.b.com/y"]).await?;
handle.execute_parallel(vec![/* RestRequest */]).await?;
```

RestTemplate lives under `examples/gateway/` — see **[rest_template.md](examples/gateway/rest_template.md)** (architecture, connection pooling, **security / signing**).

## Tests

```bash
cargo test
```
