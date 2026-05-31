//! RestForOne: calculator (order=0) + result timer (order=1) under one supervisor.
//!
//! When the calculator fails, RestForOne restarts the calculator **and** the timer.
//!
//! Run: `cargo run --example rest_for_one_calculator_timer`
//! See: `examples/rest_for_one_calculator_timer.md`

use lane_switchboards::actor::{spawn, Actor, ActorProcessingErr, ActorRef};
use lane_switchboards::supervisor::{
    child_spec, RestartStrategy, Supervisor, SupervisorConfig, SupervisorHandle,
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
    async fn start(interval: Duration) -> Result<Self, ActorProcessingErr> {
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
            max_restarts: 10,
            within_secs: 60,
            ..Default::default()
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
    let app = SupervisedApp::start(interval).await.map_err(actor_err)?;

    println!("RestForOne supervisor: calculator order=0, timer order=1\n");
    app.start_timer().await?;

    tokio::time::sleep(Duration::from_millis(300)).await;
    println!("[calc] add 10 + 4 = {}", add(&app, 10.0, 4.0).await?);

    tokio::time::sleep(Duration::from_millis(900)).await;
    println!("[calc] add 5 + 3 = {}", add(&app, 5.0, 3.0).await?);

    tokio::time::sleep(Duration::from_millis(400)).await;
    let before = app.refs.snapshot().await;
    print_generations("\n[before] generations", &before);

    println!("\n--- calculator divide by zero (RestForOne restarts timer too) ---");
    let _ = div(&app, 10.0, 0.0).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let after = app.refs.snapshot().await;
    print_generations("[after] generations (both should increase)", &after);

    app.start_timer().await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    println!("[calc] add 1 + 1 = {}", add(&app, 1.0, 1.0).await?);

    tokio::time::sleep(Duration::from_millis(900)).await;
    println!("\nDone.");
    Ok(())
}
