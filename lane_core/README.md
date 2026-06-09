# lane_core

Core OTP actor primitives for the **lane_switchboards** runtime.

`lane_core` is a dependency-minimal crate that contains everything needed to spawn, supervise, link, monitor, and hot-upgrade actors. It has no gRPC, no TLS, no distributed messaging — just the mailbox loop and supervision tree.

`lane_switchboards` re-exports every public symbol from `lane_core`, so code that imports `lane_switchboards` gets the full API without directly depending on this crate.

---

## Modules

| Module | What it provides |
|--------|-----------------|
| [`actor`] | `Actor` trait, `ActorRef`, `ActorId`, `Envelope`, `ExitReason`, spawn functions, link / monitor / upgrade |
| [`config`] | `ActorConfig` (mailbox + timeout), `DistributedConfig`, `DedicatedRuntime`, `RuntimeOptions` |
| [`monitor`] | `ActorMonitor`, `ActorStats` — per-actor runtime counters, post-mortem snapshots |
| [`registry`] | Process-global control-channel and supervisor-channel index (internal) |
| [`supervisor`] | OTP restart strategies, `Supervisor`, `ChildRegistry`, `ChildSlot`, `SupervisorHandle` |

---

## Key types

### `actor`

| Type / Function | Role |
|-----------------|------|
| `Actor<M>` | Trait to implement: `pre_start`, `handle`, `on_handle_begin`, `on_handle_stuck`, `post_stop`, `trap_exit` |
| `ActorRef<M>` | Cheap clone handle; `send`, `stop`, `kill`, `link`, `unlink`, `monitor`, `demonitor`, `upgrade` |
| `ActorId` | Unique `u64` identifier; `ActorId::DEAD` sentinel |
| `Envelope<M>` | Mailbox wire type — `Msg`, `Link`, `Unlink`, `Monitor`, `Kill`, `Stop`, `Upgrade` |
| `ExitReason` | `Normal`, `Shutdown`, `Error`, `HandleTimeout`, `Linked`, `Killed` |
| `spawn(actor, sup_tx)` | Spawn on current Tokio runtime with default config |
| `spawn_with_config(actor, sup_tx, &config)` | Spawn with explicit `ActorConfig` |
| `spawn_on_runtime(&handle, actor, sup_tx, &config)` | Spawn on a specific runtime handle |

### `config`

| Type | Role |
|------|------|
| `ActorConfig` | `mailbox_capacity`, `handle_timeout`, `slow_handle_threshold` |
| `DistributedConfig` | gRPC/distributed tuning (ack timeout, TLS) |
| `DedicatedRuntime` | Owned multi-thread Tokio runtime for actor isolation |
| `RuntimeOptions` | `worker_threads` for `DedicatedRuntime` |

### `monitor`

| API | Role |
|-----|------|
| `ActorMonitor::global()` | Process-singleton monitor |
| `.get(id)` | Snapshot for a live **or** recently-stopped actor (post-mortem) |
| `.all()` | Snapshots for every currently-live actor |
| `.unregister(id)` | Called automatically on actor exit; moves cell to post-mortem store |
| `.snapshot_and_unregister(id)` | Consume final snapshot once and evict from post-mortem |
| `.purge(id)` | Discard a post-mortem entry |

`ActorStats` fields:

| Field | Meaning |
|-------|---------|
| `messages_handled` | Successful `handle()` completions |
| `handle_errors` | `handle()` returned `Err` |
| `panics` | `handle()` panicked (caught by `catch_unwind`) |
| `handle_timeouts` | `handle_timeout` fired before `handle()` finished |
| `slow_handles` | `handle()` completed but exceeded `slow_handle_threshold` |
| `in_flight` | Handles started but not yet finished (0 for stopped actors) |
| `last_handle_ms` | Duration of the most recent handle call |
| `max_handle_ms` | Longest handle call ever recorded |
| `total_handle_ms` | Sum of all successful handle durations |
| `mean_handle_ms` | `total_handle_ms / messages_handled`; `0` when no messages handled yet |

### `supervisor`

| Type / Function | Role |
|-----------------|------|
| `Supervisor<M>` | Runs the restart loop; `new`, `with_actor_config`, `start`, `start_settled` |
| `SupervisorConfig` | `strategy`, `max_restarts`, `within_secs`, `intensity_action`, `mailbox_capacity` |
| `RestartStrategy` | `OneForOne`, `OneForAll`, `RestForOne` |
| `IntensityAction` | `ShutdownSupervisor`, `AbandonChild` |
| `SupervisorHandle<M>` | Running supervisor; `initial_ref`, `initial_refs`, `stop` |
| `ChildRegistry<M, K>` | Named stable refs after restart; lock-free `get` via `ArcSwap` |
| `ChildSlot<M>` | Single-child stable ref; lock-free `get` / `require` |
| `child_spec(order, factory)` | Low-level child spec builder |
| `spawn_child_spec(order, name, registry, build)` | Named child that registers in `ChildRegistry` |
| `supervise_actor(actor, config)` | Convenience: one child, returns `(ActorRef, SupervisorHandle)` |
| `supervise_named_child(name, registry, config, build)` | One child registered by name |

---

## Dependency tree

```
lane_core
├── tokio (full)
├── tracing
├── async-trait
├── arc-swap
├── once_cell
└── futures-util
```

`lane_core` has no `prost`, no `tonic`, no `serde`, no TLS dependencies. All those live in `lane_switchboards`.

---

## Using lane_core directly

Add to `Cargo.toml`:

```toml
[dependencies]
lane_core = { path = "../lane_core" }   # or version once published
tokio = { version = "1", features = ["full"] }
async-trait = "0.1"
```

```rust
use lane_core::actor::{spawn, Actor, ActorProcessingErr};
use lane_core::supervisor::{ChildSlot, Supervisor, SupervisorConfig};
use lane_core::monitor::ActorMonitor;
use std::sync::Arc;

struct Worker;

#[async_trait::async_trait]
impl Actor<String> for Worker {
    async fn handle(&mut self, msg: String) -> Result<(), ActorProcessingErr> {
        println!("received: {msg}");
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let (worker, _join) = spawn(Worker, None).await.unwrap();
    worker.send("hello".into()).await.unwrap();
    worker.stop().await.unwrap();

    // Stats available post-mortem:
    if let Some(stats) = ActorMonitor::global().get(worker.id) {
        println!("handled: {}", stats.messages_handled);
    }
}
```

---

## Related

- [`lane_switchboards`](../README.md) — full runtime (gRPC, service mesh, distributed actors, Paxos)
- [`examples/resilient_monitor.rs`](../examples/resilient_monitor.rs) — live demo of `ActorMonitor`
- [`examples/resilient_calculator.rs`](../examples/resilient_calculator.rs) — supervised panic recovery
- [`examples/envelope_demo.rs`](../examples/envelope_demo.rs) — link, monitor, upgrade demo
