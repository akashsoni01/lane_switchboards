//! Supervised calculator plus a timer actor that prints `last_result` on an interval.
//!
//! Run: `cargo run --example resilient_calculator_timer`

use lane_switchboards::actor::{spawn, Actor, ActorProcessingErr, ActorRef};
use lane_switchboards::supervisor::{
    child_spec, RestartStrategy, Supervisor, SupervisorConfig, SupervisorHandle,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, Mutex};

enum CalcMsg {
    Add(f64, f64, oneshot::Sender<Result<f64, String>>),
    Sub(f64, f64, oneshot::Sender<Result<f64, String>>),
    Mul(f64, f64, oneshot::Sender<Result<f64, String>>),
    Div(f64, f64, oneshot::Sender<Result<f64, String>>),
    LastResult(oneshot::Sender<Option<f64>>),
    Panic(oneshot::Sender<()>),
}

#[derive(Clone, Default)]
struct ResilientCalculator {
    last_result: Option<f64>,
}

impl ResilientCalculator {
    fn compute(op: &str, a: f64, b: f64) -> Result<f64, String> {
        match op {
            "add" => Ok(a + b),
            "sub" => Ok(a - b),
            "mul" => Ok(a * b),
            "div" if b == 0.0 => panic!("division by zero"),
            "div" => Ok(a / b),
            _ => Err(format!("unknown operation: {op}")),
        }
    }
}

#[async_trait::async_trait]
impl Actor<CalcMsg> for ResilientCalculator {
    async fn handle(&mut self, msg: CalcMsg) -> Result<(), ActorProcessingErr> {
        match msg {
            CalcMsg::Panic(reply) => {
                let _ = reply.send(());
                panic!("simulated calculator bug");
            }
            CalcMsg::LastResult(reply) => {
                let _ = reply.send(self.last_result);
            }
            CalcMsg::Add(a, b, reply) => {
                let result = Self::compute("add", a, b);
                if let Ok(value) = result {
                    self.last_result = Some(value);
                }
                let _ = reply.send(result);
            }
            CalcMsg::Sub(a, b, reply) => {
                let result = Self::compute("sub", a, b);
                if let Ok(value) = result {
                    self.last_result = Some(value);
                }
                let _ = reply.send(result);
            }
            CalcMsg::Mul(a, b, reply) => {
                let result = Self::compute("mul", a, b);
                if let Ok(value) = result {
                    self.last_result = Some(value);
                }
                let _ = reply.send(result);
            }
            CalcMsg::Div(a, b, reply) => {
                let result = Self::compute("div", a, b);
                if let Ok(value) = result {
                    self.last_result = Some(value);
                }
                let _ = reply.send(result);
            }
        }
        Ok(())
    }
}

struct CalcHandle {
    current: Arc<Mutex<Option<ActorRef<CalcMsg>>>>,
    _supervisor: SupervisorHandle<CalcMsg>,
}

impl CalcHandle {
    async fn start() -> Result<Self, ActorProcessingErr> {
        let current = Arc::new(Mutex::new(None));
        let slot_for_spec = current.clone();

        let spec = child_spec(0, move |sup_tx| {
            let slot = slot_for_spec.clone();
            Box::pin(async move {
                let (actor_ref, _) = spawn(ResilientCalculator::default(), Some(sup_tx)).await?;
                *slot.lock().await = Some(actor_ref.clone());
                Ok(actor_ref)
            })
        });

        let config = SupervisorConfig {
            strategy: RestartStrategy::OneForOne,
            max_restarts: 10,
            within_secs: 60,
            ..Default::default()
        };

        let supervisor = Supervisor::new(config, vec![spec]);
        let sup_handle = supervisor.start().await?;

        if current.lock().await.is_none() {
            return Err("supervised calculator not started".into());
        }

        Ok(Self {
            current,
            _supervisor: sup_handle,
        })
    }

    async fn actor(&self) -> ActorRef<CalcMsg> {
        self.current
            .lock()
            .await
            .clone()
            .expect("supervised calculator running")
    }
}

enum TimerMsg {
    Start(ActorRef<TimerMsg>),
    Tick,
    Stop,
}

struct LastResultTimer {
    calc: Arc<CalcHandle>,
    self_ref: Option<ActorRef<TimerMsg>>,
    interval: Duration,
    running: bool,
}

impl LastResultTimer {
    fn new(calc: Arc<CalcHandle>, interval: Duration) -> Self {
        Self {
            calc,
            self_ref: None,
            interval,
            running: false,
        }
    }

    fn schedule_next(&self) {
        let Some(self_ref) = self.self_ref.clone() else {
            return;
        };
        let delay = self.interval;
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = self_ref.send(TimerMsg::Tick).await;
        });
    }
}

#[async_trait::async_trait]
impl Actor<TimerMsg> for LastResultTimer {
    async fn handle(&mut self, msg: TimerMsg) -> Result<(), ActorProcessingErr> {
        match msg {
            TimerMsg::Start(self_ref) => {
                self.self_ref = Some(self_ref);
                self.running = true;
                self.schedule_next();
            }
            TimerMsg::Tick if self.running => {
                match query_last_result(&self.calc).await {
                    Ok(Some(value)) => println!("[timer] last result = {value}"),
                    Ok(None) => println!("[timer] last result = (none)"),
                    Err(e) => println!("[timer] query failed: {e}"),
                }
                self.schedule_next();
            }
            TimerMsg::Tick => {}
            TimerMsg::Stop => {
                self.running = false;
            }
        }
        Ok(())
    }
}

fn actor_err(e: ActorProcessingErr) -> anyhow::Error {
    anyhow::anyhow!("{e}")
}

async fn query_last_result(handle: &CalcHandle) -> anyhow::Result<Option<f64>> {
    let (tx, rx) = oneshot::channel();
    let calc = handle.actor().await;
    calc.send(CalcMsg::LastResult(tx)).await.map_err(actor_err)?;
    rx.await
        .map_err(|_| anyhow::anyhow!("calculator dropped last-result reply"))
}

async fn request(
    handle: &CalcHandle,
    build: impl FnOnce(oneshot::Sender<Result<f64, String>>) -> CalcMsg,
) -> anyhow::Result<Result<f64, String>> {
    let (tx, rx) = oneshot::channel();
    let calc = handle.actor().await;
    calc.send(build(tx)).await.map_err(actor_err)?;
    match rx.await {
        Ok(result) => Ok(result),
        Err(_) => Err(anyhow::anyhow!(
            "calculator crashed before reply (supervisor will restart it)"
        )),
    }
}

async fn add(handle: &CalcHandle, a: f64, b: f64) -> anyhow::Result<Result<f64, String>> {
    request(handle, |reply| CalcMsg::Add(a, b, reply)).await
}

async fn div(handle: &CalcHandle, a: f64, b: f64) -> anyhow::Result<Result<f64, String>> {
    request(handle, |reply| CalcMsg::Div(a, b, reply)).await
}

fn print_op(op: &str, a: f64, b: f64, outcome: anyhow::Result<Result<f64, String>>) {
    match outcome {
        Ok(Ok(value)) => println!("[calc] {op}: {a} and {b} = {value}"),
        Ok(Err(e)) => println!("[calc] {op}: {a} and {b} -> error: {e}"),
        Err(e) => println!("[calc] {op}: {a} and {b} -> {e}"),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let calc = Arc::new(CalcHandle::start().await.map_err(actor_err)?);
    let interval = Duration::from_millis(50);

    let (timer, timer_join) = spawn(
        LastResultTimer::new(calc.clone(), interval),
        None,
    )
    .await
    .map_err(actor_err)?;

    timer
        .send(TimerMsg::Start(timer.clone()))
        .await
        .map_err(actor_err)?;

    println!("Supervised calculator + timer started (every {}ms)\n", interval.as_millis());

    tokio::time::sleep(Duration::from_millis(400)).await;
    print_op("add", 10.0, 4.0, add(&calc, 10.0, 4.0).await);

    tokio::time::sleep(Duration::from_millis(1200)).await;
    print_op("add", 5.0, 3.0, add(&calc, 5.0, 3.0).await);

    tokio::time::sleep(Duration::from_millis(400)).await;
    println!("\n--- panic: divide by zero ---");
    print_op("div", 10.0, 0.0, div(&calc, 10.0, 0.0).await);

    tokio::time::sleep(Duration::from_millis(1200)).await;
    print_op("add", 1.0, 1.0, add(&calc, 1.0, 1.0).await);

    tokio::time::sleep(Duration::from_millis(1200)).await;

    timer.stop().await.map_err(actor_err)?;
    timer_join.await?;

    let actor = calc.actor().await;
    actor.stop().await.map_err(actor_err)?;

    println!("\nTimer and calculator stopped.");
    Ok(())
}
