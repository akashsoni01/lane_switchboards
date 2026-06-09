//! Supervised calculator that survives panics via OTP-style restart.
//!
//! Run: `cargo run --example resilient_calculator`
//! See: `examples/resilient_calculator.md`

use lane_switchboards::actor::{Actor, ActorProcessingErr, ActorRef};
use lane_switchboards::supervisor::{
    ChildSlot, RestartStrategy, Supervisor, SupervisorConfig, SupervisorHandle,
};
use std::sync::Arc;
use tokio::sync::oneshot;

enum CalcMsg {
    Add(f64, f64, oneshot::Sender<Result<f64, String>>),
    Sub(f64, f64, oneshot::Sender<Result<f64, String>>),
    Mul(f64, f64, oneshot::Sender<Result<f64, String>>),
    Div(f64, f64, oneshot::Sender<Result<f64, String>>),
    /// Intentionally panic to demonstrate supervisor recovery.
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

    async fn post_stop(&mut self) -> Result<(), ActorProcessingErr> {
        tracing::info!("resilient calculator stopped (supervisor may restart)");
        Ok(())
    }
}

/// Stable handle: always points at the current supervised calculator `ActorRef`.
struct CalcHandle {
    slot: Arc<ChildSlot<CalcMsg>>,
    _supervisor: SupervisorHandle<CalcMsg>,
}

impl CalcHandle {
    async fn start() -> Result<Self, ActorProcessingErr> {
        let slot = Arc::new(ChildSlot::new());
        let spec = ChildSlot::child_spec(0, slot.clone(), || ResilientCalculator::default());

        let config = SupervisorConfig {
            strategy: RestartStrategy::OneForOne,
            max_restarts: 10,
            within_secs: 60,
            ..Default::default()
        };

        let sup_handle = Supervisor::new(config, vec![spec]).start().await?;
        slot.require()?;

        Ok(Self {
            slot,
            _supervisor: sup_handle,
        })
    }

    async fn actor(&self) -> ActorRef<CalcMsg> {
        self.slot
            .get()
            .expect("supervised calculator running")
    }
}

fn actor_err(e: ActorProcessingErr) -> anyhow::Error {
    anyhow::anyhow!("{e}")
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

async fn sub(handle: &CalcHandle, a: f64, b: f64) -> anyhow::Result<Result<f64, String>> {
    request(handle, |reply| CalcMsg::Sub(a, b, reply)).await
}

async fn mul(handle: &CalcHandle, a: f64, b: f64) -> anyhow::Result<Result<f64, String>> {
    request(handle, |reply| CalcMsg::Mul(a, b, reply)).await
}

async fn div(handle: &CalcHandle, a: f64, b: f64) -> anyhow::Result<Result<f64, String>> {
    request(handle, |reply| CalcMsg::Div(a, b, reply)).await
}

async fn trigger_panic(handle: &CalcHandle) -> anyhow::Result<()> {
    let (tx, rx) = oneshot::channel();
    let calc = handle.actor().await;
    calc.send(CalcMsg::Panic(tx)).await.map_err(actor_err)?;
    let _ = rx.await;
    Ok(())
}

fn print_result(op: &str, a: f64, b: f64, outcome: anyhow::Result<Result<f64, String>>) {
    match outcome {
        Ok(Ok(value)) => println!("{op}: {a} and {b} = {value}"),
        Ok(Err(e)) => println!("{op}: {a} and {b} -> error: {e}"),
        Err(e) => println!("{op}: {a} and {b} -> {e}"),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let calc = CalcHandle::start().await.map_err(actor_err)?;
    println!("Supervised resilient calculator started\n");

    print_result("add", 10.0, 4.0, add(&calc, 10.0, 4.0).await);
    print_result("sub", 10.0, 4.0, sub(&calc, 10.0, 4.0).await);
    print_result("mul", 10.0, 4.0, mul(&calc, 10.0, 4.0).await);
    print_result("div", 10.0, 4.0, div(&calc, 10.0, 4.0).await);

    println!("\n--- panic: divide by zero ---");
    print_result("div", 10.0, 0.0, div(&calc, 10.0, 0.0).await);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    print_result("add", 5.0, 5.0, add(&calc, 5.0, 5.0).await);

    println!("\n--- panic: simulated bug ---");
    if let Err(e) = trigger_panic(&calc).await {
        println!("panic: {e}");
    }
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    print_result("mul", 3.0, 7.0, mul(&calc, 3.0, 7.0).await);

    let actor = calc.actor().await;
    actor.stop().await.map_err(actor_err)?;
    println!("\nCalculator stopped cleanly.");
    Ok(())
}
