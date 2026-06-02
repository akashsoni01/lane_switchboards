# lane_switchboards v0.0.3

Release notes for **v0.0.3** — OTP-style sequential actor execution, supervisor correctness fixes, and config/API cleanup.

For the full project overview see [README.md](./README.md).  
Previous release notes: [READMEv0.0.2.md](./READMEv0.0.2.md)

---

## What's new in v0.0.3

### 1) Actor runtime is now strictly sequential (OTP semantics)

Actors process one message at a time. The non-OTP concurrent actor handling path has been removed.

- Removed concurrent execution path from `src/actor.rs`.
- `run_actor` is now the single mailbox loop.
- Removed actor-side concurrency machinery:
  - `JoinSet` handler tasks
  - actor `Arc<Mutex<...>>` indirection
  - actor-side `Semaphore`
  - failure fan-in channel (`fail_tx`/`fail_rx`)
  - shutdown atomic coordination for handler tasks
- Links and monitors are plain stack-local state in the actor task.
- Control messages (`ControlMsg`) are prioritized with `tokio::select! { biased; ... }`.

Result: simpler runtime behavior, lower lock contention risk, and closer OTP process semantics.

### 2) `ActorConfig` simplified

`ActorConfig.max_in_flight` has been removed.

`ActorConfig` now contains:

- `mailbox_capacity`
- `handle_timeout`
- `slow_handle_threshold`

`DistributedConfig.max_in_flight` remains unchanged and still controls TCP node dispatch concurrency.

### 3) Supervisor fixes and hardening

`src/supervisor.rs` now includes the following corrections:

- **Fixed double-spawn bug** in `supervise_actor_with_config`:
  - Returned child ref now comes from supervisor startup (`SupervisorHandle::initial_ref()`).
- **No mutex held across `.await` during initial spawn**:
  - Startup takes ownership of child specs before async restarts.
- **Restart intensity log switched to `VecDeque<Instant>`** for efficient sliding-window pruning.
- **`ChildRegistry` unified under one mutexed inner struct** for consistent ref+generation updates.
- **`OneForAll` and `RestForOne` restart ordering now honors `ChildSpec::order()`.**
- **Failed restart handling improved**:
  - error logged
  - attempt counted toward restart intensity
  - child slot marked with `ActorId::DEAD`
- `ChildSpec::restart` now takes `ActorConfig` by value (`Copy`), simplifying call sites.
- `SupervisorHandle` no longer exposes a synthetic/unbacked supervisor id.

### 4) Actor/supervisor behavior fixes included

- Removed unsafe sender transmute path for cross-type link signaling.
- Link propagation is bidirectional.
- `Demonitor` now removes the correct observer entry.
- Linked-exit propagation occurs once, from shutdown path only.
- Linked exits are propagated only for abnormal exits (not `Normal` / `Shutdown`).
- Added upgrade lifecycle hook (`on_upgrade`) used by hot upgrade flow.

### 5) Latest latency (sequential design)

From `cargo run --example handle_timeout_calculator_timer_latency` (2026-06-01, debug build, warmup=5, samples=50):

- `add` e2e: min 40 µs, avg 91 µs, max 1321 µs
- `slow_div 0ms` e2e: min 1165 µs, avg 1237 µs, max 1499 µs
- `last_result` e2e: min 38 µs, avg 63 µs, max 386 µs
- Full demo wall clock: ~3.4 s

---

## Tests and checks

Validated on v0.0.3 changes:

- `cargo clippy -- -D warnings` (library target) ✅
- `cargo test` ✅

Note: repository examples may still emit unrelated Rust warnings (dead code/private interfaces), but the actor/supervisor/runtime changes are test-green.

---

## Migration notes from v0.0.2

### If you used `ActorConfig.max_in_flight`

Remove it from initializers:

```rust
let config = ActorConfig {
    mailbox_capacity: 64,
    handle_timeout: Some(Duration::from_millis(150)),
    slow_handle_threshold: Some(Duration::from_millis(150)),
};
```

### If you relied on concurrent `handle()` calls

That mode is no longer supported. Model concurrency via:

- multiple actors/workers
- supervised worker pools
- routing/work partitioning across actor instances

---

## Quick reference defaults (v0.0.3)

```rust
ActorConfig {
    mailbox_capacity: 64,
    handle_timeout: None,
    slow_handle_threshold: None,
}

DistributedConfig {
    bridge_capacity: 32,
    max_in_flight: 32,
}

SupervisorConfig {
    strategy: OneForOne,
    max_restarts: 5,
    within_secs: 10,
    intensity_action: ShutdownSupervisor,
    mailbox_capacity: 32,
}
```
