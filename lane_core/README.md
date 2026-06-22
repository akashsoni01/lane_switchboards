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

`ActorStats` fields (all [`usize`]; counters saturate at [`usize::MAX`] with a warning on overflow):

| Field | Type | Meaning |
|-------|------|---------|
| `messages_handled` | `usize` | Successful `handle()` completions |
| `handle_errors` | `usize` | `handle()` returned `Err` |
| `panics` | `usize` | `handle()` panicked (caught by `catch_unwind`) |
| `handle_timeouts` | `usize` | `handle_timeout` fired before `handle()` finished |
| `slow_handles` | `usize` | `handle()` completed but exceeded `slow_handle_threshold` |
| `in_flight` | `usize` | Handles started but not yet finished (0 for stopped actors) |
| `last_handle_ms` | `usize` | Duration of the most recent handle call |
| `max_handle_ms` | `usize` | Longest handle call ever recorded |
| `total_handle_ms` | `usize` | Sum of all successful handle durations |
| `mean_handle_ms` | `usize` | `total_handle_ms / messages_handled`; `0` when no messages handled yet |

Counter updates use saturating arithmetic — values never wrap. When an increment would
exceed [`usize::MAX`], the counter is clamped and `tracing::warn!` records the field
and actor id.

Monitor updates are wrapped in `catch_unwind` and [`RwLock`] poison is recovered, so a
bug or panic in stats/Prometheus code cannot crash actors or the rest of the service.

### Prometheus export (`metrics` feature)

Enable with `lane_core = { features = ["metrics"] }` or `lane_switchboards = { features = ["metrics"] }`.

| API | Role |
|-----|------|
| `ActorConfig::monitor_meta` | Set `actor_name`, `actor_type`, etc. at spawn |
| `init_metrics(MetricsConfig)` | Process-wide `node` / `dc` labels |
| `render_prometheus_text()` | Grafana-ready text exposition |
| `record_supervisor_restart` / `sync_supervisor_scrape` | Supervisor restart + intensity gauges |
| `set_child_generation` | `ChildRegistry` generation gauge |
| `sync_storage_stats` | Diff `StorageStats` into Prometheus counters |
| `StorageNode::export_prometheus_stats` | Convenience wrapper in `lane_switchboards` |
| `record_consistency_operation` | Mesh consistency (auto via `emit_metrics`) |
| `record_remote_send` / `record_remote_ack_timeout` | gRPC remote actor dispatches |
| `record_mesh_dispatch` | Mesh `invoke_consistent` / `read_consistent` entry |
| `serve_metrics_http(addr)` | Standalone `/metrics` HTTP server |
| `MetricsConfig::on_scrape` | Optional callback after each scrape render |

```bash
cargo run --example metrics_exporter --features metrics
# scrape http://127.0.0.1:9090/metrics
```

Import [`docs/grafana/actor-runtime.json`](../docs/grafana/actor-runtime.json) into Grafana, or run `docker compose -f docs/grafana/docker-compose.yml up` (Prometheus on host **9091**, Grafana on **3000**).

### Prometheus series mapping

| `ActorStats` field | Prometheus series |
|--------------------|-------------------|
| `messages_handled` | `lane_actor_messages_handled_total` |
| `handle_errors` | `lane_actor_handle_errors_total` |
| `panics` | `lane_actor_panics_total` |
| `handle_timeouts` | `lane_actor_handle_timeouts_total` |
| `slow_handles` | `lane_actor_slow_handles_total` |
| `in_flight` | `lane_actor_in_flight` (gauge) |
| `last_handle_ms` | `lane_actor_last_handle_seconds` |
| `max_handle_ms` | `lane_actor_max_handle_seconds` |
| `mailbox_depth` | `lane_actor_mailbox_depth` |
| `mailbox_capacity` | `lane_actor_mailbox_capacity` |
| (on exit) | `lane_actor_exits_total{reason}` |
| (saturate) | `lane_actor_counter_saturated_total{field}` |
| handle wall time | `lane_actor_handle_duration_seconds` (histogram) |
| mailbox queue wait | `lane_actor_mailbox_wait_seconds` (histogram) |
| full-channel async send | `lane_actor_mailbox_send_blocked_total` |
| rejected try_send | `lane_actor_mailbox_send_rejected_total` |

Use histogram quantiles in Grafana — not `mean_handle_ms` — for latency SLOs.

See [`docs/todo.md`](../docs/todo.md) for the full Grafana roadmap.

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

## Monitor overhead & throughput context

Understanding the cost of a single instrumented `handle()` call, and how that cost
compares to common Rust operations, helps you decide whether monitoring is "free" at
your message rate or something to profile.

### Per-handle cost breakdown

Every `handle()` call goes through these monitor operations:

| Phase | Operations | Typical cost |
|-------|-----------|-------------|
| `begin_handle` | 1 × `RwLock::read()` + Arc clone + 1 × `fetch_add` | ~40–80 ns |
| `finish_handle` | 1 × `RwLock::read()` + Arc clone + 4 × atomics + 1 × CAS loop | ~80–180 ns |
| **Total per message** | | **~120–260 ns** |

This is the monitoring overhead alone — not your `handle()` body.  A trivial
`handle()` that only does an integer increment (≈ 1 ns) sees the monitor as the
dominant cost.  A `handle()` that does real work (I/O, allocations, parsing) barely
notices it.

### Throughput reference numbers

These are single-threaded, release-mode estimates on modern x86-64 hardware.
They show where monitoring fits on the cost ladder.

| Operation (×100 000 iterations) | Estimated wall time | Notes |
|----------------------------------|--------------------:|-------|
| `i64` additions (sum loop) | ~0.05 ms | ~0.5 ns / op — pure ALU |
| `AtomicUsize::fetch_add` | ~0.4 ms | ~4 ns / op — L1 cache hit |
| Monitor `begin + finish_handle` | ~15–25 ms | ~150–250 ns / op — lock + atomics |
| `HashMap::insert` (pre-allocated) | ~10–15 ms | ~100–150 ns / op — hash + store |
| `HashMap::insert` (with growth) | ~20–40 ms | ~200–400 ns / op — realloc hits |
| `tokio::mpsc::send` (unbuffered) | ~30–60 ms | ~300–600 ns / op — channel wake |

Key takeaways:

- **100 000 `handle()` calls with monitoring** ≈ **15–25 ms total overhead** from the
  monitor alone — roughly the same order as 100 000 `HashMap::insert` calls.
- **100 000 simple sums** finish in **< 0.1 ms** — three orders of magnitude faster.
  The monitor overhead is irrelevant at that operation density because the actual
  message rate that saturates an actor channel is far lower than raw ALU speed.
- At **1 M messages/sec** (a heavily-loaded actor), monitor cost is ≈ 150–250 µs/s —
  0.015–0.025 % of a single CPU core.

### Minimum latency for a trivial handler

```text
actor mailbox dequeue          ~100 ns   (tokio task wake + channel recv)
begin_handle (monitor)         ~60  ns
your handle() body: i64 += n   ~1   ns
finish_handle (monitor)        ~120 ns
                              --------
total                          ~280 ns   ≈ 0.3 µs per message
```

A single supervised child doing nothing but increment a counter can sustain
roughly **3–4 M messages/sec** on a single core before the runtime itself
(channel wake-ups and task scheduling) becomes the bottleneck — not the monitor.

---

## Related

- [`lane_switchboards`](../README.md) — full runtime (gRPC, service mesh, distributed actors, Paxos)
- [`examples/resilient_monitor.rs`](../examples/resilient_monitor.rs) — live demo of `ActorMonitor`
- [`examples/resilient_calculator.rs`](../examples/resilient_calculator.rs) — supervised panic recovery
- [`examples/envelope_demo.rs`](../examples/envelope_demo.rs) — link, monitor, upgrade demo
