//! Single-child supervisor with [`ChildSlot`] — stable `ActorRef` across restarts.
//!
//! Simplified from [`handle_timeout_calculator_timer_latency`](./handle_timeout_calculator_timer_latency.rs):
//! one supervised calculator, **OneForOne**, no `ChildRegistry`.
//!
//! - **`ChildSlot`** — updated on every spawn/restart; callers use `slot.require().await`
//! - **`handle_timeout`** — slow handler cancelled; inputs journaled via `on_handle_stuck`
//! - **Shared state** — `last_result` and stuck journal survive actor restart
//! - **Panic recovery** — divide-by-zero restarts the lone child
//!
//! Run: `cargo run --example single_child_supervisor`
//! See: `examples/single_child_supervisor.md`

use lane_switchboards::actor::{Actor, ActorProcessingErr, ActorRef, HandleStuckContext};
use lane_switchboards::config::ActorConfig;
use lane_switchboards::monitor::ActorMonitor;
use lane_switchboards::supervisor::{
    ChildSlot, RestartStrategy, Supervisor, SupervisorConfig, SupervisorHandle,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, oneshot};

const HANDLE_TIMEOUT: Duration = Duration::from_millis(150);

enum CalcMsg {
    Add(f64, f64, oneshot::Sender<Result<f64, String>>),
    Div(f64, f64, oneshot::Sender<Result<f64, String>>),
    SlowDiv(f64, f64, u64, oneshot::Sender<Result<f64, String>>),
    Ping(oneshot::Sender<()>),
    /// Mailbox busy in `handle()` — awaits a `Ping` to the same actor via `ChildSlot`.
    SelfDeadlockProbe(oneshot::Sender<Result<(), String>>),
    LastResult(oneshot::Sender<Option<f64>>),
    StuckJournal(oneshot::Sender<Vec<StuckAction>>),
}

#[derive(Clone, Debug, PartialEq)]
enum StuckAction {
    Add(f64, f64),
    SlowDiv(f64, f64, u64),
    SelfDeadlockProbe,
}

impl StuckAction {
    fn from_msg(msg: &CalcMsg) -> Option<Self> {
        match msg {
            CalcMsg::Add(a, b, _) => Some(StuckAction::Add(*a, *b)),
            CalcMsg::SlowDiv(a, b, delay_ms, _) => {
                Some(StuckAction::SlowDiv(*a, *b, *delay_ms))
            }
            CalcMsg::SelfDeadlockProbe(_) => Some(StuckAction::SelfDeadlockProbe),
            _ => None,
        }
    }

    fn label(&self) -> String {
        match self {
            StuckAction::Add(a, b) => format!("add {a} + {b}"),
            StuckAction::SlowDiv(a, b, ms) => format!("slow_div {a} / {b} (delay {ms}ms)"),
            StuckAction::SelfDeadlockProbe => "self_deadlock_probe".into(),
        }
    }
}

#[derive(Default)]
struct SharedInner {
    last_result: Option<f64>,
    stuck_actions: Vec<StuckAction>,
}

struct SharedState {
    inner: Mutex<SharedInner>,
    restarts: AtomicU64,
}

type SharedStateHandle = Arc<SharedState>;

impl SharedState {
    fn new() -> SharedStateHandle {
        Arc::new(Self {
            inner: Mutex::new(SharedInner::default()),
            restarts: AtomicU64::new(0),
        })
    }
}

#[derive(Clone)]
struct Calculator {
    pending: Option<StuckAction>,
    state: SharedStateHandle,
    slot: Arc<ChildSlot<CalcMsg>>,
}

#[async_trait::async_trait]
impl Actor<CalcMsg> for Calculator {
    async fn pre_start(&mut self) -> Result<(), ActorProcessingErr> {
        let gen = self.state.restarts.fetch_add(1, Ordering::Relaxed) + 1;
        println!("[calc] spawn generation {gen}");
        self.pending = None;
        let inner = self.state.inner.lock().await;
        if let Some(v) = inner.last_result {
            println!("[calc] restored last_result = {v}");
        }
        if !inner.stuck_actions.is_empty() {
            println!(
                "[calc] stuck journal ({} entr{}):",
                inner.stuck_actions.len(),
                if inner.stuck_actions.len() == 1 {
                    "y"
                } else {
                    "ies"
                }
            );
            for action in &inner.stuck_actions {
                println!("         - {}", action.label());
            }
        }
        Ok(())
    }

    async fn on_handle_begin(&mut self, msg: &CalcMsg) -> Result<(), ActorProcessingErr> {
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
            self.state.inner.lock().await.stuck_actions.push(action);
        }
        Ok(())
    }

    async fn handle(&mut self, msg: CalcMsg) -> Result<(), ActorProcessingErr> {
        match msg {
            CalcMsg::Add(a, b, reply) => {
                let value = a + b;
                self.state.inner.lock().await.last_result = Some(value);
                self.pending = None;
                let _ = reply.send(Ok(value));
            }
            CalcMsg::Div(a, b, reply) => {
                if b == 0.0 {
                    panic!("division by zero");
                }
                let value = a / b;
                self.state.inner.lock().await.last_result = Some(value);
                self.pending = None;
                let _ = reply.send(Ok(value));
            }
            CalcMsg::SlowDiv(a, b, delay_ms, reply) => {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                if b == 0.0 {
                    return Err("division by zero".into());
                }
                let value = a / b;
                self.state.inner.lock().await.last_result = Some(value);
                self.pending = None;
                let _ = reply.send(Ok(value));
            }
            CalcMsg::Ping(reply) => {
                let _ = reply.send(());
            }
            CalcMsg::SelfDeadlockProbe(reply) => {
                let calc = self
                    .slot
                    .get()
                    .ok_or("calculator slot empty")?;
                let (tx, rx) = oneshot::channel();
                calc.send(CalcMsg::Ping(tx)).await?;
                let result = match rx.await {
                    Ok(()) => Ok(()),
                    Err(_) => Err("ping dropped".into()),
                };
                let _ = reply.send(result);
            }
            CalcMsg::LastResult(reply) => {
                let last = self.state.inner.lock().await.last_result;
                let _ = reply.send(last);
            }
            CalcMsg::StuckJournal(reply) => {
                let stuck = self.state.inner.lock().await.stuck_actions.clone();
                let _ = reply.send(stuck);
            }
        }
        Ok(())
    }
}

/// Application handle: `ChildSlot` for stable refs + supervisor lifetime.
struct CalcApp {
    slot: Arc<ChildSlot<CalcMsg>>,
    state: SharedStateHandle,
    _supervisor: SupervisorHandle<CalcMsg>,
}

impl CalcApp {
    async fn start() -> Result<Self, ActorProcessingErr> {
        let slot = Arc::new(ChildSlot::new());
        let state = SharedState::new();
        let slot_for_actor = slot.clone();
        let state_for_actor = state.clone();

        let actor_config = ActorConfig {
            handle_timeout: Some(HANDLE_TIMEOUT),
            slow_handle_threshold: Some(HANDLE_TIMEOUT),
            ..Default::default()
        };

        let spec = ChildSlot::child_spec(0, slot.clone(), move || Calculator {
            pending: None,
            state: state_for_actor.clone(),
            slot: slot_for_actor.clone(),
        });

        let sup = Supervisor::with_actor_config(
            actor_config,
            SupervisorConfig {
                strategy: RestartStrategy::OneForOne,
                max_restarts: 20,
                within_secs: 60,
                ..Default::default()
            },
            vec![spec],
        )
        .start_settled(Duration::from_millis(50))
        .await?;

        slot.require()?;

        Ok(Self {
            slot,
            state,
            _supervisor: sup,
        })
    }

    async fn actor(&self) -> ActorRef<CalcMsg> {
        self.slot
            .require()
            .expect("supervised calculator running")
    }

    fn restart_count(&self) -> u64 {
        self.state.restarts.load(Ordering::Relaxed)
    }
}

fn actor_err(e: ActorProcessingErr) -> anyhow::Error {
    anyhow::anyhow!("{e}")
}

async fn add(app: &CalcApp, a: f64, b: f64) -> anyhow::Result<f64> {
    let calc = app.actor().await;
    let (tx, rx) = oneshot::channel();
    calc.send(CalcMsg::Add(a, b, tx)).await.map_err(actor_err)?;
    rx.await
        .map_err(|_| anyhow::anyhow!("calculator dropped reply"))?
        .map_err(|e| anyhow::anyhow!("{e}"))
}

async fn div(app: &CalcApp, a: f64, b: f64) -> anyhow::Result<f64> {
    let calc = app.actor().await;
    let (tx, rx) = oneshot::channel();
    calc.send(CalcMsg::Div(a, b, tx)).await.map_err(actor_err)?;
    rx.await
        .map_err(|_| anyhow::anyhow!("calculator dropped reply"))?
        .map_err(|e| anyhow::anyhow!("{e}"))
}

async fn slow_div(
    app: &CalcApp,
    a: f64,
    b: f64,
    delay_ms: u64,
) -> anyhow::Result<Result<f64, String>> {
    let calc = app.actor().await;
    let (tx, rx) = oneshot::channel();
    calc.send(CalcMsg::SlowDiv(a, b, delay_ms, tx))
        .await
        .map_err(actor_err)?;
    match rx.await {
        Ok(r) => Ok(r),
        Err(_) => Err(anyhow::anyhow!(
            "handle_timeout broke slow handler (HandleTimeout → supervisor restart)"
        )),
    }
}

async fn stuck_journal(app: &CalcApp) -> anyhow::Result<Vec<StuckAction>> {
    let calc = app.actor().await;
    let (tx, rx) = oneshot::channel();
    calc.send(CalcMsg::StuckJournal(tx)).await.map_err(actor_err)?;
    rx.await
        .map_err(|_| anyhow::anyhow!("calculator dropped stuck journal reply"))
}

async fn probe_self_deadlock(app: &CalcApp) -> anyhow::Result<Result<(), String>> {
    let calc = app.actor().await;
    let (tx, rx) = oneshot::channel();
    calc.send(CalcMsg::SelfDeadlockProbe(tx))
        .await
        .map_err(actor_err)?;
    match rx.await {
        Ok(r) => Ok(r),
        Err(_) => Err(anyhow::anyhow!(
            "handle_timeout broke self-deadlock (calculator restarted)"
        )),
    }
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
        if n == 0 {
            return;
        }
        let min = *self.samples_us.iter().min().unwrap();
        let max = *self.samples_us.iter().max().unwrap();
        let avg = self.samples_us.iter().sum::<u128>() / n as u128;
        println!(
            "[latency] {:<18} min={:>5} µs  avg={:>5} µs  max={:>5} µs  (n={n})",
            self.label, min, avg, max
        );
    }
}

async fn measure_add_latency(app: &CalcApp) -> anyhow::Result<()> {
    const WARMUP: usize = 5;
    const SAMPLES: usize = 30;

    println!("=== Success-path latency (ChildSlot → actor) ===\n");
    for _ in 0..WARMUP {
        let _ = add(app, 0.0, 0.0).await?;
    }

    let mut stats = LatencyStats {
        label: "add (e2e)",
        samples_us: Vec::with_capacity(SAMPLES),
    };
    for i in 0..SAMPLES {
        let start = Instant::now();
        let _ = add(app, i as f64, 1.0).await?;
        stats.push(start.elapsed());
    }
    stats.print();

    if let Some(mon) = ActorMonitor::global().get(app.actor().await.id) {
        println!(
            "[latency] ActorMonitor last_handle_ms={} max_handle_ms={}",
            mon.last_handle_ms, mon.max_handle_ms
        );
    }
    println!();
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    println!("=== Single-child supervisor (ChildSlot + OneForOne) ===\n");
    let app = CalcApp::start().await.map_err(actor_err)?;

    measure_add_latency(&app).await?;

    println!("--- Add ---");
    println!("10 + 4 = {}", add(&app, 10.0, 4.0).await?);

    println!("\n--- Slow div (400ms > {}ms handle_timeout) ---", HANDLE_TIMEOUT.as_millis());
    let before = app.restart_count();
    match slow_div(&app, 20.0, 4.0, 400).await {
        Ok(v) => println!("[calc] unexpected success: {v:?}"),
        Err(e) => println!("[calc] {e}"),
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    println!(
        "[slot] restarts: {before} → {} (OneForOne respawned child)",
        app.restart_count()
    );
    assert!(app.restart_count() > before, "calculator should restart after timeout");

    let stuck = stuck_journal(&app).await?;
    println!("\n[stuck journal]:");
    for action in &stuck {
        println!("  - {}", action.label());
    }

    println!("\n--- Div by zero (panic → restart) ---");
    let before = app.restart_count();
    let _ = div(&app, 10.0, 0.0).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    println!(
        "[slot] restarts: {before} → {}",
        app.restart_count()
    );
    println!("1 + 1 = {}", add(&app, 1.0, 1.0).await?);

    println!("\n--- Self-deadlock probe (slot lookup inside handle) ---");
    let before = app.restart_count();
    match probe_self_deadlock(&app).await {
        Ok(Ok(())) => println!("[deadlock] unexpected success"),
        Ok(Err(e)) => println!("[deadlock] handler error: {e}"),
        Err(e) => println!("[deadlock] {e}"),
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    println!(
        "[slot] restarts: {before} → {}",
        app.restart_count()
    );

    println!("\n--- Healthy after recovery ---");
    println!("3 × 7 path: add then last_result");
    let _ = add(&app, 3.0, 7.0).await?;
    let calc = app.actor().await;
    let (tx, rx) = oneshot::channel();
    calc.send(CalcMsg::LastResult(tx)).await.map_err(actor_err)?;
    println!("last_result = {:?}", rx.await.ok());

    println!("\nSee examples/single_child_supervisor.md");
    println!("Multi-child variant: cargo run --example handle_timeout_calculator_timer_latency\n");
    Ok(())
}
