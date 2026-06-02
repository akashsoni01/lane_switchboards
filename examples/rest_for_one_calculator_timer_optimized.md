# RestForOne calculator + timer (optimized)

Lean RestForOne demo that keeps OTP behavior but cuts repetitive wiring using library macros.

```bash
cargo run --example rest_for_one_calculator_timer_optimized
```

Source: [`rest_for_one_calculator_timer_optimized.rs`](./rest_for_one_calculator_timer_optimized.rs)

---

## Cheatsheet: when to use what

| Need | Use | Why |
|------|-----|-----|
| Supervise multiple named actors | `ChildRegistry<M>` + `registry_child_spec!` | Stable named lookups after restart with less spawn boilerplate |
| One request/reply actor call | `actor_ask!` | Avoid repeating `oneshot::channel + send + await` ceremony |
| Single supervised child | `ChildSlot::child_spec` | Smallest API when you only have one actor |
| Restart only failing child | `RestartStrategy::OneForOne` | Isolate failures |
| Restart dependency chain | `RestartStrategy::RestForOne` | Restart failed child and later-order dependents |
| Restart whole local tree | `RestartStrategy::OneForAll` | Keep tightly coupled children in sync |
| Guard restart storms | `max_restarts` + `within_secs` | Intensity window limits endless restart loops |
| Keep startup deterministic | `start_settled(Duration)` | Give children time to pre-start/register before traffic |

---

## Imports explained (`rest_for_one_calculator_timer_optimized.rs` 9-14)

| Import group | Purpose in this example |
|--------------|-------------------------|
| `actor::{Actor, ActorProcessingErr, ActorRef}` | Defines actors (`Calculator`, `ResultTimer`), common actor error type, and refs for mailbox sends |
| `supervisor::{ChildRegistry, IntensityAction, RestartStrategy, Supervisor, SupervisorConfig, SupervisorHandle}` | Builds RestForOne tree, intensity policy, and registry-backed child discovery |
| `{actor_ask, registry_child_spec}` | Macro helpers to remove oneshot and child-spec boilerplate |
| `Arc`, `Duration`, `oneshot` | Shared registry ownership, timer intervals/sleeps, typed request/response channels inside messages |

---

## What this optimized variant demonstrates

- Same behavior as the classic RestForOne calculator/timer demo:
  - divide-by-zero panic in calculator triggers RestForOne restart
  - intensity breach shuts down supervisor when failures exceed budget
- Less user-side ceremony:
  - `actor_ask!` for typed request/reply
  - `registry_child_spec!` for child registration/wiring
- Cleaner `App::start` via helper functions:
  - `rest_for_one_config(...)`
  - `child_specs(...)`

For the full non-optimized walkthrough, see [`rest_for_one_calculator_timer.md`](./rest_for_one_calculator_timer.md).
