//! RestForOne: calculator (order=0) + result timer (order=1) under one supervisor.
//!
//! When the calculator fails, RestForOne restarts the calculator **and** the timer.
//!
//! **Intensity limits** (`max_restarts`, `within_secs`):
//! - Every child failure adds one timestamp to a sliding window of length `within_secs`.
//! - When restart events in that window exceed `max_restarts`, intensity is breached.
//! - Default `IntensityAction::ShutdownSupervisor`: the supervisor task exits and
//!   **stops restarting any child** (calculator and timer are left dead).
//! - See phase 2 in `main` for a live breach (`max_restarts: 2`, `within_secs: 10`).
//!
//! Run: `cargo run --example rest_for_one_calculator_timer`
//! See: `examples/rest_for_one_calculator_timer.md`

use lane_switchboards::actor::{spawn, Actor, ActorProcessingErr, ActorRef};
use lane_switchboards::supervisor::{
    child_spec, IntensityAction, RestartStrategy, Supervisor, SupervisorConfig,
    SupervisorHandle,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, Mutex};

enum AppMsg {
    Add(f64, f64, oneshot::Sender<Result<f64, String>>),
    Div(f64, f64, oneshot::Sender<Result<f64, String>>),
    LastResult(oneshot::Sender<Option<f64>>),
    TimerStart(ActorRef<AppMsg>),
    TimerTick,
}

struct ChildRefs {
    by_name: Arc<Mutex<HashMap<String, ActorRef<AppMsg>>>>,
    generations: Arc<Mutex<HashMap<String, u64>>>,
}

impl ChildRefs {
    fn new() -> Self {
        Self {
            by_name: Arc::new(Mutex::new(HashMap::new())),
            generations: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn get(&self, name: &str) -> Option<ActorRef<AppMsg>> {
        self.by_name.lock().await.get(name).cloned()
    }

    async fn bump_generation(&self, name: &str) {
        let mut gens = self.generations.lock().await;
        *gens.entry(name.to_string()).or_insert(0) += 1;
        println!(
            "[spawn] {name} generation {}",
            gens.get(name).copied().unwrap_or(0)
        );
    }

    async fn snapshot(&self) -> HashMap<String, u64> {
        self.generations.lock().await.clone()
    }
}

#[derive(Clone)]
struct Calculator {
    last_result: Option<f64>,
    refs: Arc<ChildRefs>,
}

#[async_trait::async_trait]
impl Actor<AppMsg> for Calculator {
    async fn pre_start(&mut self) -> Result<(), ActorProcessingErr> {
        self.refs.bump_generation("calculator").await;
        Ok(())
    }

    async fn handle(&mut self, msg: AppMsg) -> Result<(), ActorProcessingErr> {
        match msg {
            AppMsg::Add(a, b, reply) => {
                let value = a + b;
                self.last_result = Some(value);
                let _ = reply.send(Ok(value));
            }
            AppMsg::Div(a, b, reply) => {
                if b == 0.0 {
                    panic!("division by zero");
                }
                let value = a / b;
                self.last_result = Some(value);
                let _ = reply.send(Ok(value));
            }
            AppMsg::LastResult(reply) => {
                let _ = reply.send(self.last_result);
            }
            AppMsg::TimerStart(_) | AppMsg::TimerTick => {}
        }
        Ok(())
    }
}

struct ResultTimer {
    refs: Arc<ChildRefs>,
    self_ref: Option<ActorRef<AppMsg>>,
    interval: Duration,
    running: bool,
}

#[async_trait::async_trait]
impl Actor<AppMsg> for ResultTimer {
    async fn pre_start(&mut self) -> Result<(), ActorProcessingErr> {
        self.refs.bump_generation("timer").await;
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
                if let Some(calc) = self.refs.get("calculator").await {
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
    refs: Arc<ChildRefs>,
    _supervisor: SupervisorHandle<AppMsg>,
}

impl SupervisedApp {
    async fn start(interval: Duration, config: SupervisorConfig) -> Result<Self, ActorProcessingErr> {
        let refs = Arc::new(ChildRefs::new());

        let calc_refs = refs.clone();
        let calc_spec = child_spec(0, move |sup_tx| {
            let refs = calc_refs.clone();
            Box::pin(async move {
                let worker = Calculator {
                    last_result: None,
                    refs: refs.clone(),
                };
                let (actor_ref, _) = spawn(worker, Some(sup_tx)).await?;
                refs.by_name
                    .lock()
                    .await
                    .insert("calculator".into(), actor_ref.clone());
                Ok(actor_ref)
            })
        });

        let timer_refs = refs.clone();
        let timer_spec = child_spec(1, move |sup_tx| {
            let refs = timer_refs.clone();
            Box::pin(async move {
                let worker = ResultTimer {
                    refs: refs.clone(),
                    self_ref: None,
                    interval,
                    running: false,
                };
                let (actor_ref, _) = spawn(worker, Some(sup_tx)).await?;
                refs.by_name
                    .lock()
                    .await
                    .insert("timer".into(), actor_ref.clone());
                Ok(actor_ref)
            })
        });

        let config = SupervisorConfig {
            strategy: RestartStrategy::RestForOne,
            ..config
        };

        let supervisor = Supervisor::new(config, vec![calc_spec, timer_spec]);
        let handle = supervisor.start().await?;
        tokio::time::sleep(Duration::from_millis(50)).await;

        Ok(Self {
            refs,
            _supervisor: handle,
        })
    }

    async fn start_timer(&self) -> anyhow::Result<()> {
        let timer = self
            .refs
            .get("timer")
            .await
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
        .refs
        .get("calculator")
        .await
        .ok_or_else(|| anyhow::anyhow!("calculator not running"))?;
    let (tx, rx) = oneshot::channel();
    calc.send(AppMsg::Add(a, b, tx)).await.map_err(actor_err)?;
    rx.await
        .map_err(|_| anyhow::anyhow!("calculator dropped reply"))?
        .map_err(|e| anyhow::anyhow!("{e}"))
}

async fn div(app: &SupervisedApp, a: f64, b: f64) -> anyhow::Result<Result<f64, String>> {
    let calc = app
        .refs
        .get("calculator")
        .await
        .ok_or_else(|| anyhow::anyhow!("calculator not running"))?;
    let (tx, rx) = oneshot::channel();
    calc.send(AppMsg::Div(a, b, tx)).await.map_err(actor_err)?;
    match rx.await {
        Ok(r) => Ok(r),
        Err(_) => Err(anyhow::anyhow!(
            "calculator crashed (RestForOne will restart calculator + timer)"
        )),
    }
}

fn print_generations(label: &str, gens: &HashMap<String, u64>) {
    println!("{label}");
    for name in ["calculator", "timer"] {
        println!("  {name}: generation {}", gens.get(name).copied().unwrap_or(0));
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let interval = Duration::from_millis(600);

    // Phase 1: normal RestForOne recovery (generous intensity budget)
    let app = SupervisedApp::start(
        interval,
        SupervisorConfig {
            max_restarts: 10,
            within_secs: 60,
            intensity_action: IntensityAction::ShutdownSupervisor,
            ..Default::default()
        },
    )
    .await
    .map_err(actor_err)?;

    println!("=== Phase 1: RestForOne (calculator order=0, timer order=1) ===\n");
    demo_rest_for_one_restart(&app, interval).await?;

    // Phase 2: intensity breach — too many failures inside within_secs
    println!("\n=== Phase 2: intensity limit (max_restarts=2, within_secs=10) ===");
    println!("Each div-by-zero counts as one restart event in the sliding window.");
    println!("When events in the window exceed max_restarts, ShutdownSupervisor stops the supervisor.\n");

    let app = SupervisedApp::start(
        interval,
        SupervisorConfig {
            max_restarts: 2,
            within_secs: 10,
            intensity_action: IntensityAction::ShutdownSupervisor,
            ..Default::default()
        },
    )
    .await
    .map_err(actor_err)?;

    app.start_timer().await?;
    let _ = add(&app, 2.0, 2.0).await?;

    for i in 1..=4 {
        println!("--- intensity test failure {i} ---");
        let _ = div(&app, 1.0, 0.0).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    let gens = app.refs.snapshot().await;
    print_generations("[after intensity breach]", &gens);

    match add(&app, 99.0, 1.0).await {
        Ok(v) => println!("[calc] unexpected success: {v}"),
        Err(e) => println!("[calc] supervisor dead — add failed: {e}"),
    }

    println!("\nDone.");
    Ok(())
}

async fn demo_rest_for_one_restart(app: &SupervisedApp, interval: Duration) -> anyhow::Result<()> {
    let _ = interval;
    app.start_timer().await?;

    tokio::time::sleep(Duration::from_millis(300)).await;
    println!("[calc] add 10 + 4 = {}", add(app, 10.0, 4.0).await?);

    tokio::time::sleep(Duration::from_millis(900)).await;
    println!("[calc] add 5 + 3 = {}", add(app, 5.0, 3.0).await?);

    tokio::time::sleep(Duration::from_millis(400)).await;
    let before = app.refs.snapshot().await;
    print_generations("\n[before] generations", &before);

    println!("\n--- calculator divide by zero (RestForOne restarts timer too) ---");
    let _ = div(app, 10.0, 0.0).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let after = app.refs.snapshot().await;
    print_generations("[after] generations (both should increase)", &after);

    app.start_timer().await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    println!("[calc] add 1 + 1 = {}", add(app, 1.0, 1.0).await?);

    tokio::time::sleep(Duration::from_millis(900)).await;
    Ok(())
}
