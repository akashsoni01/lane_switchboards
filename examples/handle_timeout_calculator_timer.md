# Deadlock prevention — calculator + timer + ledger

RestForOne supervision with **`ActorConfig::handle_timeout`**, **`on_handle_begin`**, **`on_handle_stuck`**, and **`ActorMonitor`** — demonstrates slow handlers and **real actor deadlocks**, and how the runtime breaks the cycle instead of hanging forever.

```bash
cargo run --example handle_timeout_calculator_timer
```

Source: [`handle_timeout_calculator_timer.rs`](./handle_timeout_calculator_timer.rs)

Based on [`rest_for_one_calculator_timer.rs`](./rest_for_one_calculator_timer.rs). For panic-based RestForOne see that example; for panic journaling see [`recoverable_timer_calc.rs`](./recoverable_timer_calc.rs).

---

## Deadlock in actor systems

An actor processes one mailbox. With **`max_in_flight = 1`** (the default), a second message cannot enter `handle()` while the first is still running.

**Deadlock** happens when `handle()` blocks waiting for a reply that requires the same mailbox (or a circular chain of mailboxes) to make progress:

| Pattern | This example | Why it stalls |
|---------|--------------|---------------|
| **Slow handler** | `SlowDiv` sleeps 400ms | Mailbox occupied; looks like deadlock to callers |
| **Self-deadlock** | `SelfDeadlockProbe` → `Ping` to self | Calculator is inside `handle()`; `Ping` never runs |
| **Cross-actor deadlock** | `CrossDeadlockProbe` → `LedgerFetch` → `LastResult` | Calculator waits on ledger; ledger waits on calculator |

Lane Switchboards does **not** run a circular-wait graph algorithm. Prevention is **operational**: bound wall time, journal inputs, exit, supervise, monitor.

---

## Deadlock prevention stack

```mermaid
flowchart TD
    subgraph trap["Stall detected"]
        A["handle blocks in await"]
        B["handle_timeout elapses"]
    end

    subgraph recover["Recovery"]
        C["on_handle_stuck — persist pending inputs"]
        D["ExitReason HandleTimeout"]
        E["Supervisor RestForOne restart"]
        F["ActorMonitor record_timeout"]
    end

    A --> B
    B --> C
    C --> D
    D --> E
    B --> F
```

| Layer | Setting / API | Role in this example |
|-------|---------------|----------------------|
| Sequential mailbox | `max_in_flight: 1` | One in-flight `handle()` — reentrant self-calls deadlock |
| Wall-clock bound | `handle_timeout: 150ms` | Cancels stuck `handle()` future |
| Input snapshot | `on_handle_begin(&msg)` | Copy operands before `handle()` runs |
| Stuck persistence | `on_handle_stuck(ctx)` | Push snapshot to shared stuck journal |
| Process recovery | RestForOne supervisor | Restart calculator, timer, **and** ledger |
| Observability | `ActorMonitor::global()` | `handle_timeouts`, `messages_handled`, handle ms |

Shared `Arc<Mutex<SharedState>>` holds `last_result` (survives restart) and `stuck_actions` (populated on every timeout).

---

## Child tree

```mermaid
flowchart LR
    Sup["Supervisor RestForOne"]
    Calc["calculator order=0"]
    Timer["timer order=1"]
    Ledger["ledger order=2"]

    Sup --> Calc
    Sup --> Timer
    Sup --> Ledger
    Timer -->|"LastResult"| Calc
    Calc -->|"CrossDeadlockProbe"| Ledger
    Ledger -->|"LedgerFetch → LastResult"| Calc
```

When **calculator** hits `HandleTimeout`, RestForOne restarts calculator and every child with **order ≥ 0** (timer + ledger).

---

## Demo phases

### Phase 1 — slow handler (deadlock-like)

1. Fast `add 10 + 4` — completes within 150ms.
2. `slow_div 20 / 4` with 400ms sleep — timeout → journal `{ SlowDiv(20, 4, 400) }` → restart.

### Phase 2 — deadlock prevention

**2a Self-deadlock**

```
calculator.handle(SelfDeadlockProbe)
  └─ send Ping to calculator ──► mailbox busy ──► never runs
```

**2b Cross-actor deadlock**

```
calculator.handle(CrossDeadlockProbe)
  └─ ledger.handle(LedgerFetch)
       └─ calculator.handle(LastResult) ──► mailbox busy ──► never runs
  └─ calculator blocks on pending() until handle_timeout
```

The calculator dispatches `LedgerFetch` then waits forever (`pending()`). It does **not** await the ledger reply — otherwise a ledger timeout could drop the channel and let the handler finish without journaling. Both actors may record `handle_timeouts` in `ActorMonitor`; journaling happens on the calculator via `on_handle_stuck`.

After each probe:

- Stuck journal contains `SelfDeadlockProbe` or `CrossDeadlockProbe(99.0)`
- Generations increase for calculator, timer, ledger
- `ActorMonitor` shows rising `handle_timeouts`
- Healthy `add` and fast `slow_div` work again after restart

---

## Config

```rust
ActorConfig {
    handle_timeout: Some(Duration::from_millis(150)),
    slow_handle_threshold: Some(Duration::from_millis(150)),
    max_in_flight: 1,  // sequential mailbox — required for self-deadlock demo
    ..Default::default()
}
```

Supervisor uses `Supervisor::with_actor_config(actor_config, ...)` so every child shares the timeout.

---

## Design notes

- **`on_handle_begin` runs while `msg` is still available by reference** — store what you need before `handle()` may block forever.
- When timeout fires, the in-flight `handle()` future is **dropped**; recovery reads from `on_handle_begin` / `on_handle_stuck` state, not from the dropped future.
- **Avoid synchronous actor-to-actor calls from inside `handle()`** unless you accept timeout + restart as the failure mode (or use `max_in_flight > 1` with careful design — still risky for cycles).
- Pair timeouts with a **supervisor** so `HandleTimeout` triggers child restart rather than a permanently wedged process.

See also [README.md](../README.md#deadlock--slow-handle-prevention) and [READMEv0.0.2.md](../READMEv0.0.2.md).
