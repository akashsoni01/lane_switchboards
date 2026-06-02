//! Basic calculator actor: add, subtract, multiply, divide.
//!
//! Run: `cargo run --example calculator`
//! See: `examples/calculator.md`

use lane_switchboards::actor::{spawn, Actor, ActorProcessingErr, ActorRef};
use tokio::sync::oneshot;

enum CalcMsg {
    Add(f64, f64, oneshot::Sender<Result<f64, String>>),
    Sub(f64, f64, oneshot::Sender<Result<f64, String>>),
    Mul(f64, f64, oneshot::Sender<Result<f64, String>>),
    Div(f64, f64, oneshot::Sender<Result<f64, String>>),
}

struct Calculator {
    last_result: Option<f64>,
}

impl Calculator {
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
impl Actor<CalcMsg> for Calculator {
    async fn handle(&mut self, msg: CalcMsg) -> Result<(), ActorProcessingErr> {
        let (op, a, b, reply) = match msg {
            CalcMsg::Add(a, b, reply) => ("add", a, b, reply),
            CalcMsg::Sub(a, b, reply) => ("sub", a, b, reply),
            CalcMsg::Mul(a, b, reply) => ("mul", a, b, reply),
            CalcMsg::Div(a, b, reply) => ("div", a, b, reply),
        };

        let result = Self::compute(op, a, b);
        if let Ok(value) = result {
            self.last_result = Some(value);
        }
        let _ = reply.send(result);
        Ok(())
    }
}

fn actor_err(e: ActorProcessingErr) -> anyhow::Error {
    anyhow::anyhow!("{e}")
}

async fn request(
    calc: &ActorRef<CalcMsg>,
    build: impl FnOnce(oneshot::Sender<Result<f64, String>>) -> CalcMsg,
) -> anyhow::Result<Result<f64, String>> {
    let (tx, rx) = oneshot::channel();
    calc.send(build(tx)).await.map_err(actor_err)?;
    rx.await
        .map_err(|_| anyhow::anyhow!("calculator dropped reply channel"))
}

async fn add(calc: &ActorRef<CalcMsg>, a: f64, b: f64) -> anyhow::Result<Result<f64, String>> {
    request(calc, |reply| CalcMsg::Add(a, b, reply)).await
}

async fn sub(calc: &ActorRef<CalcMsg>, a: f64, b: f64) -> anyhow::Result<Result<f64, String>> {
    request(calc, |reply| CalcMsg::Sub(a, b, reply)).await
}

async fn mul(calc: &ActorRef<CalcMsg>, a: f64, b: f64) -> anyhow::Result<Result<f64, String>> {
    request(calc, |reply| CalcMsg::Mul(a, b, reply)).await
}

async fn div(calc: &ActorRef<CalcMsg>, a: f64, b: f64) -> anyhow::Result<Result<f64, String>> {
    request(calc, |reply| CalcMsg::Div(a, b, reply)).await
}

fn print_result(op: &str, a: f64, b: f64, result: Result<f64, String>) {
    match result {
        Ok(value) => println!("{op}: {a} and {b} = {value}"),
        Err(e) => println!("{op}: {a} and {b} -> error: {e}"),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let (calc, join) = spawn(Calculator { last_result: None }, None)
        .await
        .map_err(actor_err)?;

    print_result("add", 10.0, 4.0, add(&calc, 10.0, 4.0).await?);
    print_result("sub", 10.0, 4.0, sub(&calc, 10.0, 4.0).await?);
    print_result("mul", 10.0, 4.0, mul(&calc, 10.0, 4.0).await?);
    print_result("div", 10.0, 4.0, div(&calc, 10.0, 4.0).await?);
    print_result("div", 10.0, 0.0, div(&calc, 10.0, 0.0).await?);
    print_result("div", 10.0, 4.0, div(&calc, 10.0, 4.0).await?);
    print_result("sub", 10.0, 4.0, sub(&calc, 10.0, 4.0).await?);

    calc.stop().await.map_err(actor_err)?;
    join.await?;
    Ok(())
}
