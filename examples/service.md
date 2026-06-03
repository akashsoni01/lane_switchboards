# Service supervisors — ServiceA and ServiceB

[`service.rs`](./service.rs) runs **`ServiceASupervisor`** and **`ServiceBSupervisor`** as **separate** OTP supervisors. Each supervises two DAO actors under **`OneForOne`**: a crash restarts only that child, not its sibling or the other supervisor.

```bash
cargo run --example service
```

Related: [`supervisor_strategies.md`](./supervisor_strategies.md) (all restart strategies).

---

## Layout

| Supervisor | Children | Actor | Message type |
|------------|----------|-------|----------------|
| **`ServiceASupervisor`** | `dao-a`, `dao-b` | `DaoAActor`, `DaoBActor` | `DaoAMsg`, `DaoBMsg` |
| **`ServiceBSupervisor`** | `dao-b`, `dao-c` | `DaoBActor`, `DaoCActor` | `DaoBMsg`, `DaoCMsg` |

Each DAO uses its **own message enum** (`Actor<DaoAMsg>`, etc.). A single `Supervisor<M>` cannot mix message types, so each child runs under its own one-child `OneForOne` supervisor inside `ServiceASupervisor` / `ServiceBSupervisor`.

`DaoB` under `ServiceASupervisor` and `DaoB` under `ServiceBSupervisor` are **different actor instances** (separate `ChildRegistry` + `Supervisor` task). Crashing one does not restart the other.

```mermaid
flowchart TB
    subgraph sa ["ServiceASupervisor"]
        A1["DaoAActor dao-a"]
        A2["DaoBActor dao-b"]
    end

    subgraph sb ["ServiceBSupervisor"]
        B1["DaoBActor dao-b"]
        B2["DaoCActor dao-c"]
    end
```

---

## OneForOne behaviour

```mermaid
sequenceDiagram
    participant Main
    participant SupA as ServiceASupervisor
    participant DaoA as DaoA
    participant DaoB as DaoB

    Main->>DaoB: Fail
    DaoB-->>SupA: handle Err
    SupA->>SupA: restart dao-b only
    Note over DaoA: generation unchanged
    SupA->>DaoB: new DaoBActor instance
```

The example prints **generation counters** from `ChildRegistry::bump_generation` in each actor's `pre_start`:

| Delta | Meaning |
|-------|---------|
| `+0` | Child was not restarted |
| `+2` | Child restarted once (`track_and_bump` + `pre_start` bump per spawn) |
| `+1` | Would indicate one bump only if you drop `bump_generation` from `pre_start` |

---

## What the demo runs

| Step | Action | Expected |
|------|--------|----------|
| 1 | `ServiceASupervisor::start` + `ServiceBSupervisor::start` | Two supervisor start lines + four DAO spawns |
| 2 | `ping_all` on both | Four ping lines |
| 3 | `ServiceASupervisor::fail_dao_b` | ServiceA: `dao-b` bumps; `dao-a` +0 |
| 4 | Snapshot `ServiceBSupervisor` | Generations unchanged |
| 5 | `ServiceBSupervisor::fail_dao_c` | ServiceB: `dao-c` bumps; `dao-b` +0 |
| 6 | Snapshot `ServiceASupervisor` | Generations unchanged |

---

## Core code pattern

```rust
let service_a = ServiceASupervisor::start().await?;
let service_b = ServiceBSupervisor::start().await?;

service_a.ping_all().await?;
service_a.fail_dao_b().await?; // only dao-b restarts inside ServiceASupervisor
```

Inside `ServiceASupervisor::start`:

```rust
let children = vec![
    spawn_child_spec(0, "dao-a", registry.clone(), || DaoAActor {
        supervisor: "ServiceASupervisor",
        registry: registry.clone(),
    }),
    spawn_child_spec(1, "dao-b", registry.clone(), || DaoBActor { /* ... */ }),
];
let _handle = Supervisor::new(one_for_one_config(), children)
    .start_settled(Duration::from_millis(50))
    .await?;
```

- **`spawn_child_spec`** — one child per inner supervisor + typed `ChildRegistry`.
- **`DaoAMsg::Fail` / `DaoBMsg::Fail` / `DaoCMsg::Fail`** — `handle` returns `Err` to trigger that child's supervisor restart.

---

## When to use this pattern

| Use case | Why two supervisors |
|----------|---------------------|
| Domain boundary A vs B | Isolate failure blast radius per `Service*Supervisor` |
| Shared actor *name* (`DaoB`) | Different supervisors/registries — no cross-supervisor restart |
| Independent restart policy | Each supervisor can use its own `SupervisorConfig` later |

For **OneForAll** or **RestForOne**, change `SupervisorConfig::strategy` on one service and compare with [`supervisor_strategies`](./supervisor_strategies.rs).

---

## File map

| File | Role |
|------|------|
| [`service.rs`](./service.rs) | Runnable demo |
| [`service.md`](./service.md) | This doc |
