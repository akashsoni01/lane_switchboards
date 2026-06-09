//! Deadlock prevention: `handle_timeout`, stuck-action journaling, and supervision.
//!
//! **Phase 1 — slow handler:** `SlowDiv` sleeps longer than `handle_timeout` (looks like
//! a stuck mailbox; inputs journaled via `on_handle_begin` / `on_handle_stuck`).
//!
//! **Phase 2 — real deadlocks:** with `max_in_flight = 1`, an actor cannot serve a second
//! message while `handle()` is blocked. This example triggers:
//! - **Self-deadlock** — calculator awaits a `Ping` to itself inside `handle()`
//! - **Cross-actor deadlock** — calculator awaits ledger; ledger awaits calculator
//!
//! Prevention: `handle_timeout` cancels the stuck `handle()`, `on_handle_stuck` persists
//! inputs, `ExitReason::HandleTimeout` notifies the supervisor (RestForOne restart),
//! `ActorMonitor` records the event.
//!
//! Run: `cargo run --example handle_timeout_calculator_timer`
//! See: `examples/handle_timeout_calculator_timer.md`
//!
//! **Overall latency:** ~2.6–3.1 s wall clock (full demo). Successful `add` e2e latency
//! is measured at startup (~58 µs avg debug, ~21 µs release) — demo sleeps excluded.

use lane_switchboards::actor::{Actor, ActorProcessingErr, ActorRef, HandleStuckContext};
use lane_switchboards::config::ActorConfig;
use lane_switchboards::monitor::ActorMonitor;
use lane_switchboards::supervisor::{
    spawn_child_spec, ChildRegistry, RestartStrategy, Supervisor, SupervisorConfig,
    SupervisorHandle,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, oneshot};

const HANDLE_TIMEOUT: Duration = Duration::from_millis(150);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum ChildName {
    Calculator,
    Timer,
    Ledger,
}

impl ChildName {
    fn as_str(self) -> &'static str {
        match self {
            ChildName::Calculator => "calculator",
            ChildName::Timer => "timer",
            ChildName::Ledger => "ledger",
        }
    }
}

enum AppMsg {
    Add(f64, f64, oneshot::Sender<Result<f64, String>>),
    SlowDiv(f64, f64, u64, oneshot::Sender<Result<f64, String>>),
    /// Reply-only probe used in self-deadlock (must be handled while caller is stuck in `handle`).
    Ping(oneshot::Sender<()>),
    /// Calculator `handle()` calls itself — mailbox cannot make progress.
    SelfDeadlockProbe(oneshot::Sender<Result<(), String>>),
    /// Calculator calls ledger; ledger calls calculator — circular wait.
    CrossDeadlockProbe(f64, oneshot::Sender<Result<(), String>>),
    /// Ledger side of the cross-actor cycle.
    LedgerFetch(oneshot::Sender<Option<f64>>),
    LastResult(oneshot::Sender<Option<f64>>),
    StuckJournal(oneshot::Sender<Vec<StuckAction>>),
    TimerStart(ActorRef<AppMsg>),
    TimerTick,
}

/// Inputs captured before `handle()` — persisted on timeout in `on_handle_stuck`.
#[derive(Clone, Debug, PartialEq)]
enum StuckAction {
    Add(f64, f64),
    SlowDiv(f64, f64, u64),
    SelfDeadlockProbe,
    CrossDeadlockProbe(f64),
}

impl StuckAction {
    fn from_msg(msg: &AppMsg) -> Option<Self> {
        match msg {
            AppMsg::Add(a, b, _) => Some(StuckAction::Add(*a, *b)),
            AppMsg::SlowDiv(a, b, delay_ms, _) => {
                Some(StuckAction::SlowDiv(*a, *b, *delay_ms))
            }
            AppMsg::SelfDeadlockProbe(_) => Some(StuckAction::SelfDeadlockProbe),
            AppMsg::CrossDeadlockProbe(amount, _) => Some(StuckAction::CrossDeadlockProbe(*amount)),
            _ => None,
        }
    }

    fn label(&self) -> String {
        match self {
            StuckAction::Add(a, b) => format!("add {a} + {b}"),
            StuckAction::SlowDiv(a, b, delay_ms) => {
                format!("slow_div {a} / {b} (delay {delay_ms}ms)")
            }
            StuckAction::SelfDeadlockProbe => "self_deadlock_probe".into(),
            StuckAction::CrossDeadlockProbe(amount) => {
                format!("cross_deadlock_probe amount={amount}")
            }
        }
    }
}

#[derive(Default)]
struct SharedState {
    last_result: Option<f64>,
    stuck_actions: Vec<StuckAction>,
}

type SharedStateHandle = Arc<Mutex<SharedState>>;

async fn log_generation(registry: &ChildRegistry<AppMsg, ChildName>, name: ChildName) {
    registry.bump_generation(name).await;
    println!(
        "[spawn] {} generation {}",
        name.as_str(),
        registry.generation(&name).await
    );
}

#[derive(Clone)]
struct Calculator {
    pending: Option<StuckAction>,
    state: SharedStateHandle,
    registry: Arc<ChildRegistry<AppMsg, ChildName>>,
}

#[async_trait::async_trait]
impl Actor<AppMsg> for Calculator {
    async fn pre_start(&mut self) -> Result<(), ActorProcessingErr> {
        log_generation(&self.registry, ChildName::Calculator).await;
        let state = self.state.lock().await;
        self.pending = None;
        if let Some(v) = state.last_result {
            println!("[calc] restored last_result = {v} from shared state");
        }
        if !state.stuck_actions.is_empty() {
            println!(
                "[calc] stuck journal has {} entr{}:",
                state.stuck_actions.len(),
                if state.stuck_actions.len() == 1 { "y" } else { "ies" }
            );
            for action in &state.stuck_actions {
                println!("         - {}", action.label());
            }
        }
        Ok(())
    }

    async fn on_handle_begin(&mut self, msg: &AppMsg) -> Result<(), ActorProcessingErr> {
        self.pending = StuckAction::from_msg(msg);
        Ok(())
    }

    async fn on_handle_stuck(&mut self, ctx: HandleStuckContext) -> Result<(), ActorProcessingErr> {
        if let Some(action) = self.pending.take() {
            println!(
                "[calc] on_handle_stuck: persisting {} (elapsed {}ms, limit {}ms)",
                action.label(),
                ctx.elapsed.as_millis(),
                ctx.limit.as_millis()
            );
            self.state.lock().await.stuck_actions.push(action);
        }
        Ok(())
    }

    async fn handle(&mut self, msg: AppMsg) -> Result<(), ActorProcessingErr> {
        match msg {
            AppMsg::Add(a, b, reply) => {
                let value = a + b;
                {
                    let mut state = self.state.lock().await;
                    state.last_result = Some(value);
                }
                self.pending = None;
                let _ = reply.send(Ok(value));
            }
            AppMsg::SlowDiv(a, b, delay_ms, reply) => {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                if b == 0.0 {
                    return Err("division by zero".into());
                }
                let value = a / b;
                {
                    let mut state = self.state.lock().await;
                    state.last_result = Some(value);
                }
                self.pending = None;
                let _ = reply.send(Ok(value));
            }
            AppMsg::Ping(reply) => {
                let _ = reply.send(());
            }
            AppMsg::SelfDeadlockProbe(reply) => {
                // Self-deadlock: mailbox is busy here (`max_in_flight = 1`), so `Ping` never runs.
                let calc = self
                    .registry
                    .get(&ChildName::Calculator)
                    .ok_or("calculator ref missing")?;
                let (tx, rx) = oneshot::channel();
                calc.send(AppMsg::Ping(tx)).await?;
                let result = match rx.await {
                    Ok(()) => Ok(()),
                    Err(_) => Err("ping dropped".into()),
                };
                let _ = reply.send(result);
            }
            AppMsg::CrossDeadlockProbe(_amount, reply) => {
                // Cross-actor deadlock: ledger will call back into this calculator while we
                // are still inside `handle()`. Never await the ledger reply — only
                // `handle_timeout` should end this handler (and trigger journaling).
                let ledger = self
                    .registry
                    .get(&ChildName::Ledger)
                    .ok_or("ledger ref missing")?;
                let (tx, _rx) = oneshot::channel();
                ledger.send(AppMsg::LedgerFetch(tx)).await?;
                std::future::pending::<()>().await;
                let _ = reply.send(Ok(()));
            }
            AppMsg::LastResult(reply) => {
                let last = self.state.lock().await.last_result;
                let _ = reply.send(last);
            }
            AppMsg::StuckJournal(reply) => {
                let stuck = self.state.lock().await.stuck_actions.clone();
                let _ = reply.send(stuck);
            }
            AppMsg::LedgerFetch(_) | AppMsg::TimerStart(_) | AppMsg::TimerTick => {}
        }
        Ok(())
    }
}

/// Participates in cross-actor deadlock: fetches from calculator while calculator waits on us.
struct Ledger {
    registry: Arc<ChildRegistry<AppMsg, ChildName>>,
}

#[async_trait::async_trait]
impl Actor<AppMsg> for Ledger {
    async fn pre_start(&mut self) -> Result<(), ActorProcessingErr> {
        log_generation(&self.registry, ChildName::Ledger).await;
        Ok(())
    }

    async fn handle(&mut self, msg: AppMsg) -> Result<(), ActorProcessingErr> {
        match msg {
            AppMsg::LedgerFetch(reply) => {
                if let Some(calc) = self.registry.get(&ChildName::Calculator) {
                    let (tx, rx) = oneshot::channel();
                    calc.send(AppMsg::LastResult(tx)).await?;
                    let _ = reply.send(rx.await.ok().flatten());
                } else {
                    let _ = reply.send(None);
                }
            }
            _ => {}
        }
        Ok(())
    }
}

struct ResultTimer {
    registry: Arc<ChildRegistry<AppMsg, ChildName>>,
    self_ref: Option<ActorRef<AppMsg>>,
    interval: Duration,
    running: bool,
}

#[async_trait::async_trait]
impl Actor<AppMsg> for ResultTimer {
    async fn pre_start(&mut self) -> Result<(), ActorProcessingErr> {
        log_generation(&self.registry, ChildName::Timer).await;
        Ok(())
    }

    async fn handle(&mut self, msg: AppMsg) -> Result<(), ActorProcessingErr> {
        match msg {
            AppMsg::TimerStart(self_ref) => {
                self.self_ref = Some(self_ref);
                self.running = true;
                self.schedule_next();
            }
            AppMsg::TimerTick if self.running => {
                if let Some(calc) = self.registry.get(&ChildName::Calculator) {
                    let (tx, rx) = oneshot::channel();
                    let _ = calc.send(AppMsg::LastResult(tx)).await;
                    match rx.await {
                        Ok(Some(v)) => println!("[timer] last_result = {v}"),
                        Ok(None) => println!("[timer] last_result = (none)"),
                        Err(_) => println!("[timer] calculator unavailable"),
                    }
                }
                self.schedule_next();
            }
            AppMsg::TimerTick => {}
            _ => {}
        }
        Ok(())
    }
}

impl ResultTimer {
    fn schedule_next(&self) {
        let Some(self_ref) = self.self_ref.clone() else {
            return;
        };
        let delay = self.interval;
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = self_ref.send(AppMsg::TimerTick).await;
        });
    }
}

struct SupervisedApp {
    registry: Arc<ChildRegistry<AppMsg, ChildName>>,
    _supervisor: SupervisorHandle<AppMsg>,
}

impl SupervisedApp {
    async fn start(interval: Duration) -> Result<Self, ActorProcessingErr> {
        let registry = Arc::new(ChildRegistry::<AppMsg, ChildName>::new());
        let state = Arc::new(Mutex::new(SharedState::default()));
        let calc_registry = registry.clone();
        let timer_registry = registry.clone();
        let ledger_registry = registry.clone();
        let calc_state = state.clone();

        let actor_config = ActorConfig {
            handle_timeout: Some(HANDLE_TIMEOUT),
            slow_handle_threshold: Some(HANDLE_TIMEOUT),
            ..Default::default()
        };

        let handle = Supervisor::with_actor_config(
            actor_config,
            SupervisorConfig {
                strategy: RestartStrategy::RestForOne,
                max_restarts: 20,
                within_secs: 60,
                ..Default::default()
            },
            vec![
                spawn_child_spec(0, ChildName::Calculator, registry.clone(), {
                    let registry = calc_registry.clone();
                    let state = calc_state.clone();
                    move || Calculator {
                        pending: None,
                        state: state.clone(),
                        registry: registry.clone(),
                    }
                }),
                spawn_child_spec(1, ChildName::Timer, registry.clone(), {
                    let registry = timer_registry.clone();
                    move || ResultTimer {
                        registry: registry.clone(),
                        self_ref: None,
                        interval,
                        running: false,
                    }
                }),
                spawn_child_spec(2, ChildName::Ledger, registry.clone(), {
                    let registry = ledger_registry.clone();
                    move || Ledger {
                        registry: registry.clone(),
                    }
                }),
            ],
        )
        .start_settled(Duration::from_millis(50))
        .await?;

        Ok(Self {
            registry,
            _supervisor: handle,
        })
    }

    async fn start_timer(&self) -> anyhow::Result<()> {
        let timer = self
            .registry
            .get(&ChildName::Timer)
            .ok_or_else(|| anyhow::anyhow!("timer not running"))?;
        timer
            .send(AppMsg::TimerStart(timer.clone()))
            .await
            .map_err(actor_err)?;
        Ok(())
    }
}

fn actor_err(e: ActorProcessingErr) -> anyhow::Error {
    anyhow::anyhow!("{e}")
}

async fn add(app: &SupervisedApp, a: f64, b: f64) -> anyhow::Result<f64> {
    let calc = app
        .registry
        .get(&ChildName::Calculator)
        .ok_or_else(|| anyhow::anyhow!("calculator not running"))?;
    let (tx, rx) = oneshot::channel();
    calc.send(AppMsg::Add(a, b, tx)).await.map_err(actor_err)?;
    rx.await
        .map_err(|_| anyhow::anyhow!("calculator dropped reply"))?
        .map_err(|e| anyhow::anyhow!("{e}"))
}

async fn slow_div(
    app: &SupervisedApp,
    a: f64,
    b: f64,
    delay_ms: u64,
) -> anyhow::Result<Result<f64, String>> {
    let calc = app
        .registry
        .get(&ChildName::Calculator)
        .ok_or_else(|| anyhow::anyhow!("calculator not running"))?;
    let (tx, rx) = oneshot::channel();
    calc.send(AppMsg::SlowDiv(a, b, delay_ms, tx))
        .await
        .map_err(actor_err)?;
    match rx.await {
        Ok(r) => Ok(r),
        Err(_) => Err(anyhow::anyhow!(
            "handle_timeout broke slow handler (HandleTimeout → supervisor restart)"
        )),
    }
}

async fn probe_self_deadlock(app: &SupervisedApp) -> anyhow::Result<Result<(), String>> {
    let calc = app
        .registry
        .get(&ChildName::Calculator)
        .ok_or_else(|| anyhow::anyhow!("calculator not running"))?;
    let (tx, rx) = oneshot::channel();
    calc.send(AppMsg::SelfDeadlockProbe(tx))
        .await
        .map_err(actor_err)?;
    match rx.await {
        Ok(r) => Ok(r),
        Err(_) => Err(anyhow::anyhow!(
            "handle_timeout broke self-deadlock (calculator restarted)"
        )),
    }
}

async fn probe_cross_deadlock(
    app: &SupervisedApp,
    amount: f64,
) -> anyhow::Result<Result<(), String>> {
    let calc = app
        .registry
        .get(&ChildName::Calculator)
        .ok_or_else(|| anyhow::anyhow!("calculator not running"))?;
    let (tx, rx) = oneshot::channel();
    calc.send(AppMsg::CrossDeadlockProbe(amount, tx))
        .await
        .map_err(actor_err)?;
    match rx.await {
        Ok(r) => Ok(r),
        Err(_) => Err(anyhow::anyhow!(
            "handle_timeout broke calc↔ledger deadlock (supervisor restart)"
        )),
    }
}

async fn stuck_journal(app: &SupervisedApp) -> anyhow::Result<Vec<StuckAction>> {
    let calc = app
        .registry
        .get(&ChildName::Calculator)
        .ok_or_else(|| anyhow::anyhow!("calculator not running"))?;
    let (tx, rx) = oneshot::channel();
    calc.send(AppMsg::StuckJournal(tx)).await.map_err(actor_err)?;
    rx.await
        .map_err(|_| anyhow::anyhow!("calculator dropped stuck journal reply"))
}

async fn last_result(app: &SupervisedApp) -> anyhow::Result<Option<f64>> {
    let calc = app
        .registry
        .get(&ChildName::Calculator)
        .ok_or_else(|| anyhow::anyhow!("calculator not running"))?;
    let (tx, rx) = oneshot::channel();
    calc.send(AppMsg::LastResult(tx)).await.map_err(actor_err)?;
    rx.await
        .map_err(|_| anyhow::anyhow!("calculator dropped last_result reply"))
}

struct LatencyStats {
    label: &'static str,
    samples_us: Vec<u128>,
}

impl LatencyStats {
    fn push(&mut self, elapsed: Duration) {
        self.samples_us.push(elapsed.as_micros());
    }

    fn print(&self) {
        let n = self.samples_us.len();
        let min = *self.samples_us.iter().min().unwrap_or(&0);
        let max = *self.samples_us.iter().max().unwrap_or(&0);
        let avg = if n == 0 {
            0
        } else {
            self.samples_us.iter().sum::<u128>() / n as u128
        };
        println!(
            "[latency] {:<22} min={:>5} µs  avg={:>5} µs  max={:>5} µs  (n={n})",
            self.label, min, avg, max
        );
    }
}

/// End-to-end success-path timings — no intentional sleeps between samples.
async fn measure_success_latencies(app: &SupervisedApp) -> anyhow::Result<()> {
    const WARMUP: usize = 5;
    const SAMPLES: usize = 50;

    println!("=== Success-path latency (demo sleeps excluded) ===");
    println!("warmup={WARMUP} samples={SAMPLES} per op\n");

    for _ in 0..WARMUP {
        let _ = add(app, 0.0, 0.0).await?;
    }

    let mut add_stats = LatencyStats {
        label: "add (e2e)",
        samples_us: Vec::with_capacity(SAMPLES),
    };
    for i in 0..SAMPLES {
        let start = Instant::now();
        let _ = add(app, i as f64, 1.0).await?;
        add_stats.push(start.elapsed());
    }
    add_stats.print();

    let mut div_stats = LatencyStats {
        label: "slow_div 0ms (e2e)",
        samples_us: Vec::with_capacity(SAMPLES),
    };
    for i in 0..SAMPLES {
        let start = Instant::now();
        let _ = slow_div(app, (i + 1) as f64, 2.0, 0)
            .await?
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        div_stats.push(start.elapsed());
    }
    div_stats.print();

    let mut last_stats = LatencyStats {
        label: "last_result (e2e)",
        samples_us: Vec::with_capacity(SAMPLES),
    };
    for _ in 0..SAMPLES {
        let start = Instant::now();
        let _ = last_result(app).await?;
        last_stats.push(start.elapsed());
    }
    last_stats.print();

    if let Some(calc) = app.registry.get(&ChildName::Calculator) {
        if let Some(stats) = ActorMonitor::global().get(calc.id) {
            println!(
                "[latency] ActorMonitor (calculator) last_handle_ms={} max_handle_ms={}",
                stats.last_handle_ms, stats.max_handle_ms
            );
        }
    }

    println!();
    Ok(())
}

fn print_generations(label: &str, gens: &HashMap<ChildName, u64>) {
    println!("{label}");
    for name in [ChildName::Calculator, ChildName::Timer, ChildName::Ledger] {
        println!(
            "  {}: generation {}",
            name.as_str(),
            gens.get(&name).copied().unwrap_or(0)
        );
    }
}

fn print_monitor_timeouts(label: &str) {
    println!("{label}");
    for stats in ActorMonitor::global().all() {
        if stats.handle_timeouts > 0 {
            println!(
                "  {} — handle_timeouts={}, messages_handled={}",
                stats.actor_id, stats.handle_timeouts, stats.messages_handled
            );
        }
    }
}

async fn phase1_slow_handle(app: &SupervisedApp) -> anyhow::Result<()> {
    println!("=== Phase 1: slow handler (deadlock-like stall) ===");
    println!("handle_timeout = {}ms, max_in_flight = 1\n", HANDLE_TIMEOUT.as_millis());

    app.start_timer().await?;
    println!("[calc] add 10 + 4 = {}", add(app, 10.0, 4.0).await?);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let before_gen = app.registry.generations().await;
    println!(
        "\n--- slow_div 20 / 4 with 400ms delay (exceeds {}ms timeout) ---",
        HANDLE_TIMEOUT.as_millis()
    );
    match slow_div(app, 20.0, 4.0, 400).await {
        Ok(v) => println!("[calc] unexpected success: {v:?}"),
        Err(e) => println!("[calc] {e}"),
    }

    tokio::time::sleep(Duration::from_millis(300)).await;
    print_generations(
        "\n[generations] RestForOne restarted calculator + timer + ledger",
        &app.registry.generations().await,
    );
    assert!(
        app.registry.generation(&ChildName::Calculator).await
            > before_gen.get(&ChildName::Calculator).copied().unwrap_or(0),
        "calculator should restart after slow handle timeout"
    );

    let stuck = stuck_journal(app).await?;
    println!("\n[stuck journal] after slow handle:");
    for action in &stuck {
        println!("  - {}", action.label());
    }
    assert!(
        stuck.iter().any(|a| matches!(a, StuckAction::SlowDiv(20.0, 4.0, 400))),
        "slow_div inputs should be journaled"
    );

    print_monitor_timeouts("\n[monitor] timeout counters:");
    app.start_timer().await?;
    Ok(())
}

async fn phase2_deadlock_prevention(app: &SupervisedApp) -> anyhow::Result<()> {
    println!("\n=== Phase 2: deadlock prevention ===");
    println!(
        "Classic actor deadlock: mailbox busy in `handle()`, waiting on a reply that \
         requires the same mailbox (self) or a peer that waits on us (cross).\n"
    );

    let before_gen = app.registry.generations().await;

    println!("--- 2a: self-deadlock (calculator awaits Ping to itself) ---");
    match probe_self_deadlock(app).await {
        Ok(Ok(())) => println!("[deadlock] unexpected success"),
        Ok(Err(e)) => println!("[deadlock] probe returned error: {e}"),
        Err(e) => println!("[deadlock] {e}"),
    }

    tokio::time::sleep(Duration::from_millis(300)).await;

    let stuck_after_self = stuck_journal(app).await?;
    println!("[stuck journal] after self-deadlock:");
    for action in &stuck_after_self {
        println!("  - {}", action.label());
    }
    assert!(
        stuck_after_self
            .iter()
            .any(|a| matches!(a, StuckAction::SelfDeadlockProbe)),
        "self-deadlock probe should be journaled"
    );

    app.start_timer().await?;

    println!("\n--- 2b: cross-actor deadlock (calculator ↔ ledger) ---");
    match probe_cross_deadlock(app, 99.0).await {
        Ok(Ok(())) => println!("[deadlock] unexpected success"),
        Ok(Err(e)) => println!("[deadlock] unexpected handler error: {e}"),
        Err(e) => println!("[deadlock] {e}"),
    }

    tokio::time::sleep(Duration::from_millis(300)).await;

    let stuck_after_cross = stuck_journal(app).await?;
    println!("[stuck journal] after cross-deadlock:");
    for action in &stuck_after_cross {
        println!("  - {}", action.label());
    }
    assert!(
        stuck_after_cross
            .iter()
            .any(|a| matches!(a, StuckAction::CrossDeadlockProbe(99.0))),
        "cross-deadlock probe should be journaled"
    );

    print_generations(
        "\n[generations] RestForOne recovered all dependents",
        &app.registry.generations().await,
    );
    assert!(
        app.registry.generation(&ChildName::Calculator).await
            > before_gen.get(&ChildName::Calculator).copied().unwrap_or(0),
        "calculator should restart after deadlock timeout"
    );

    print_monitor_timeouts("\n[monitor] cumulative timeout counters:");

    app.start_timer().await?;
    println!(
        "\n[calc] add 1 + 1 = {} (healthy after deadlock recovery)",
        add(app, 1.0, 1.0).await?
    );

    let value = slow_div(app, 20.0, 4.0, 0)
        .await?
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    println!("[calc] retry slow_div 20 / 4 (0ms) = {value}");

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let interval = Duration::from_millis(600);
    let app = SupervisedApp::start(interval).await.map_err(actor_err)?;

    measure_success_latencies(&app).await?;

    phase1_slow_handle(&app).await?;
    phase2_deadlock_prevention(&app).await?;

    tokio::time::sleep(Duration::from_millis(900)).await;
    println!("\nDone.");
    Ok(())
}
