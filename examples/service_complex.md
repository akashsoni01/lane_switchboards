# Service supervisors as actors — `service_complex`

[`service_complex.rs`](./service_complex.rs) extends [`service.rs`](./service.rs): same DAO layout and isolation demo, but **each service boundary is a supervised actor**.

```bash
cargo run --example service_complex
cargo run --example service_complex_cluster   # 10 replicas × ServiceA + ServiceB on TCP nodes
```

Compare with: [`service.md`](./service.md) (coordinator structs in `main` only). Cluster layout follows [`horizontal_scaling.md`](./horizontal_scaling.md).

---

## Layout

```mermaid
flowchart TB
    subgraph root ["main — supervise_actor"]
        SA["ServiceASupervisorActor"]
        SB["ServiceBSupervisorActor"]
    end

    subgraph inner_a ["inside Service A actor pre_start"]
        A1["supervise_named_child dao-a"]
        A2["supervise_named_child dao-b"]
    end

    subgraph inner_b ["inside Service B actor pre_start"]
        B1["supervise_named_child dao-b"]
        B2["supervise_named_child dao-c"]
    end

    SA --> A1
    SA --> A2
    SB --> B1
    SB --> B2
```

| Layer | What | Restart scope |
|-------|------|----------------|
| **Outer** | `supervise_actor(Service*SupervisorActor)` | If the **service actor** fails, the whole service restarts (`pre_start` respawns both DAO supervisors) |
| **Inner** | `supervise_named_child!` per DAO | `OneForOne` — only the failed DAO restarts |

---

## vs `service.rs`

| | `service` | `service_complex` |
|---|-----------|-------------------|
| Service A / B | Plain struct in `main` | `Actor<ServiceAMsg>` / `Actor<ServiceBMsg>` + outer `supervise_actor` |
| `main` talks to DAOs | Via struct methods | Via `ActorRef::send(Service*Msg::…)` |
| Service actor crash | N/A (not an actor) | Outer supervisor restarts service actor → new DAO trees in `pre_start` |
| DAO crash | Inner `supervise_named_child!` | Same |

---

## One-child supervisor helper (no proc-macro)

Spawning one named child under `OneForOne` used to require `spawn_child_spec` + `Supervisor::new` + `start_settled`. The library now provides:

| API | Role |
|-----|------|
| [`supervise_named_child`](../src/supervisor.rs) | Async function |
| [`supervise_named_child!`](../src/macros.rs) | Declarative macro — `move \|\| actor` boilerplate removed |

```rust
let sup = supervise_named_child!(
    "dao-a",
    registry.clone(),
    one_for_one_config(),
    Duration::from_millis(50),
    DaoAActor { supervisor: SERVICE_A, registry: registry.clone() }
)
.await?;
```

**Feature flags:** not required. Declarative macros ship with the crate (like `registry_child_spec!`). A proc-macro crate would be optional future work; it would not change runtime behaviour.

For actors **without** a `ChildRegistry`, use [`supervise_actor`](../src/supervisor.rs) (cloneable prototype actor).

---

## Crash / panic (service_complex)

| Failure | Effect |
|---------|--------|
| DAO `Fail` / panic in `handle` | Inner `supervise_named_child!` restarts that DAO only |
| **Service actor** fails (if you add `ServiceAMsg::Fail`) | Outer `supervise_actor` restarts the service actor; `post_stop` stops inner DAO supervisors; `pre_start` spawns fresh DAOs |
| Service A vs B | Still isolated — separate outer supervisors and registries |

---

## Multi-node cluster (`service_complex_cluster`)

[`service_complex_cluster.rs`](./service_complex_cluster.rs) starts with **`CLUSTER_REPLICAS_INITIAL` (2)** replicas per service and **autoscales** up to **`CLUSTER_REPLICAS_MAX` (10)** when load per replica exceeds a threshold.

| Piece | Role |
|-------|------|
| **`serve_actor`** | One node per replica — binds `127.0.0.1:0`, target name `"service"` |
| **`AutoscalingCluster<M>`** | Tracks dispatches/replica; calls `serve_actor` + `Cluster::join` on scale-out |
| **`Cluster<ServiceACommand>`** | Roster inside autoscaling wrapper (grows 2 → up to 10) |
| **`Cluster<ServiceBCommand>`** | Separate roster for Service B |
| **`ServiceACommand` / `ServiceBCommand`** | `Serialize` remote commands (`PingAll`, `FailDaoB` / `FailDaoC`) |

```mermaid
flowchart TB
    Coord["Coordinator main"]
    ASA["AutoscalingCluster ServiceA"]
    ASB["AutoscalingCluster ServiceB"]

    Coord --> ASA
    Coord --> ASB

    ASA -->|"load high"| ScaleA["serve_actor + join"]
    ASB -->|"load high"| ScaleB["serve_actor + join"]
```

### Autoscaling on load increase

| Constant | Default | Meaning |
|----------|---------|---------|
| `CLUSTER_REPLICAS_INITIAL` | 2 | Nodes at boot |
| `CLUSTER_REPLICAS_MAX` | 10 | Ceiling per service |
| `AUTOSCALE_REQUESTS_PER_REPLICA` | 12 | Scale out when window avg dispatches/replica ≥ this |
| `AUTOSCALE_LOAD_WAVE_REQUESTS` | 24 | Synthetic load per wave in the demo |

After each load wave, `maybe_scale_up` compares dispatches since the last scale (or boot) to roster size. If average load per replica is above the threshold and roster &lt; max, it launches another `serve_actor` and `join`s it. Service A and Service B scale **independently**.

Tune in [`service_complex_shared.rs`](./service_complex_shared.rs): `AutoscaleConfig` (`max_replicas`, `requests_per_replica_threshold`, `scale_step`).

### What the cluster demo runs

| Step | API | Expected |
|------|-----|----------|
| 1 | Boot `CLUSTER_REPLICAS_INITIAL` per service | 4 nodes total |
| 2 | Load waves: `send_round_robin(PingAll)` × N, then `maybe_scale_up` | Roster grows toward 10 as load rises |
| 3 | `broadcast(PingAll)` | All replicas in final roster ping DAOs |
| 4 | `send_by_key(&3, FailDaoB)` on A (if replica exists) | Only that replica’s DaoB restarts |
| 5 | `send_by_key(&7, FailDaoC)` on B | Only replica 7’s DaoC restarts |

Unlike single-node `service_complex`, there is **no** outer `supervise_actor` on each replica — `serve_actor` owns the service actor mailbox; **inner** `supervise_named_child!` per DAO is unchanged.

### Shared code

| File | Role |
|------|------|
| [`service_complex_shared.rs`](./service_complex_shared.rs) | DAO + service actors, commands, helpers |
| [`service_complex.rs`](./service_complex.rs) | Local supervised demo + generation counters |
| [`service_complex_cluster.rs`](./service_complex_cluster.rs) | Multi-node 10+10 replicas |

---

## File map

| File | Role |
|------|------|
| [`service_complex.rs`](./service_complex.rs) | Single-node runnable demo |
| [`service_complex_cluster.rs`](./service_complex_cluster.rs) | 10 replicas per service on TCP nodes |
| [`service_complex_shared.rs`](./service_complex_shared.rs) | Shared actors and commands |
| [`service_complex.md`](./service_complex.md) | This doc |
| [`service.rs`](./service.rs) | Simpler coordinator version (same macros) |
