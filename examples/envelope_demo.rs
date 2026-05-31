//! Demonstrates every [`Envelope`] variant in the actor runtime.
//!
//! | Variant | Demo section |
//! |---------|--------------|
//! | `Msg(M)` | Application ping |
//! | `Link` / `Unlink` | Bidirectional link, then unlink before failure |
//! | `Monitor` / `Demonitor` | One-shot exit notification |
//! | `LinkedExit` | Linked peer failure propagation |
//! | `Upgrade(DynActor)` | Hot swap V1 → V2 |
//! | `Stop` / `Kill` | Graceful vs forced shutdown |
//!
//! Run: `cargo run --example envelope_demo`
//! See: `examples/envelope_demo.md`

use lane_switchboards::actor::{spawn, Actor, ActorId, ActorProcessingErr, ExitReason};

fn actor_err(e: ActorProcessingErr) -> anyhow::Error {
    anyhow::anyhow!("{e}")
}

// --- shared worker (Msg, Link, LinkedExit, Stop) ---

enum WorkerMsg {
    Ping,
    Fail,
}

struct Worker {
    name: String,
}

#[async_trait::async_trait]
impl Actor<WorkerMsg> for Worker {
    async fn handle(&mut self, msg: WorkerMsg) -> Result<(), ActorProcessingErr> {
        match msg {
            WorkerMsg::Ping => {
                println!("[Msg] {} received Ping", self.name);
                Ok(())
            }
            WorkerMsg::Fail => Err(format!("{} intentional failure", self.name).into()),
        }
    }

    async fn post_stop(&mut self) -> Result<(), ActorProcessingErr> {
        println!("[Stop/Kill/LinkedExit] {} post_stop", self.name);
        Ok(())
    }
}

// --- hot upgrade (Upgrade) ---

enum CounterMsg {
    Inc,
    Get(tokio::sync::oneshot::Sender<u64>),
}

struct CounterV1 {
    count: u64,
}

struct CounterV2 {
    count: u64,
}

#[async_trait::async_trait]
impl Actor<CounterMsg> for CounterV1 {
    async fn handle(&mut self, msg: CounterMsg) -> Result<(), ActorProcessingErr> {
        match msg {
            CounterMsg::Inc => self.count += 1,
            CounterMsg::Get(tx) => {
                let _ = tx.send(self.count);
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl Actor<CounterMsg> for CounterV2 {
    async fn handle(&mut self, msg: CounterMsg) -> Result<(), ActorProcessingErr> {
        match msg {
            CounterMsg::Inc => self.count += 2,
            CounterMsg::Get(tx) => {
                let _ = tx.send(self.count);
            }
        }
        Ok(())
    }
}

fn section(title: &str) {
    println!("\n=== {title} ===");
}

async fn demo_msg() -> anyhow::Result<()> {
    section("Msg(M) — application message");
    let (worker, join) = spawn(Worker { name: "worker".into() }, None)
        .await
        .map_err(actor_err)?;
    worker.send(WorkerMsg::Ping).await.map_err(actor_err)?;
    worker.stop().await.map_err(actor_err)?;
    join.await?;
    Ok(())
}

async fn demo_monitor() -> anyhow::Result<()> {
    section("Monitor — one-shot exit notification");
    let (target, target_join) = spawn(Worker { name: "monitored".into() }, None)
        .await
        .map_err(actor_err)?;
    let exit_rx = target.monitor(ActorId::new()).await;

    target.stop().await.map_err(actor_err)?;
    let reason = exit_rx.await?;
    println!("[Monitor] observed exit: {reason:?}");
    target_join.await?;
    Ok(())
}

async fn demo_demonitor() -> anyhow::Result<()> {
    section("Demonitor — remove monitor (no-op in current runtime)");
    let (target, target_join) = spawn(Worker { name: "demonitored".into() }, None)
        .await
        .map_err(actor_err)?;
    let observer = ActorId::new();
    target.demonitor(observer).await.map_err(actor_err)?;
    target.stop().await.map_err(actor_err)?;
    target_join.await?;
    Ok(())
}

async fn demo_link_and_linked_exit() -> anyhow::Result<()> {
    section("Link + LinkedExit — failure propagates to linked peer");
    let (a, a_join) = spawn(Worker { name: "alpha".into() }, None)
        .await
        .map_err(actor_err)?;
    let (b, b_join) = spawn(Worker { name: "beta".into() }, None)
        .await
        .map_err(actor_err)?;

    a.link(b.id).await.map_err(actor_err)?;
    b.link(a.id).await.map_err(actor_err)?;
    println!("[Link] alpha <-> beta linked");

    a.send(WorkerMsg::Fail).await.map_err(actor_err)?;
    let (a_res, b_res) = tokio::join!(a_join, b_join);
    a_res?;
    b_res?;
    Ok(())
}

async fn demo_unlink() -> anyhow::Result<()> {
    section("Unlink — peer survives when link is removed");
    let (a, a_join) = spawn(Worker { name: "solo-fail".into() }, None)
        .await
        .map_err(actor_err)?;
    let (b, b_join) = spawn(Worker { name: "survivor".into() }, None)
        .await
        .map_err(actor_err)?;

    a.link(b.id).await.map_err(actor_err)?;
    b.link(a.id).await.map_err(actor_err)?;
    a.unlink(b.id).await.map_err(actor_err)?;
    b.unlink(a.id).await.map_err(actor_err)?;
    println!("[Unlink] peers unlinked before failure");

    a.send(WorkerMsg::Fail).await.map_err(actor_err)?;
    a_join.await?;
    b.send(WorkerMsg::Ping).await.map_err(actor_err)?;
    b.stop().await.map_err(actor_err)?;
    b_join.await?;
    Ok(())
}

async fn demo_upgrade() -> anyhow::Result<()> {
    section("Upgrade(DynActor) — swap implementation in-place");
    let (counter, join) = spawn(CounterV1 { count: 0 }, None)
        .await
        .map_err(actor_err)?;

    counter.send(CounterMsg::Inc).await.map_err(actor_err)?;
    let (tx, rx) = tokio::sync::oneshot::channel();
    counter.send(CounterMsg::Get(tx)).await.map_err(actor_err)?;
    println!("[Upgrade] V1 count: {}", rx.await?);

    counter.upgrade(CounterV2 { count: 1 }).await.map_err(actor_err)?;
    counter.send(CounterMsg::Inc).await.map_err(actor_err)?;
    let (tx, rx) = tokio::sync::oneshot::channel();
    counter.send(CounterMsg::Get(tx)).await.map_err(actor_err)?;
    println!("[Upgrade] V2 count after +2 increment: {}", rx.await?);

    counter.stop().await.map_err(actor_err)?;
    join.await?;
    Ok(())
}

async fn demo_stop_vs_kill() -> anyhow::Result<()> {
    section("Stop vs Kill — graceful vs forced shutdown");

    let (graceful, graceful_join) = spawn(Worker { name: "graceful".into() }, None)
        .await
        .map_err(actor_err)?;
    let exit_rx = graceful.monitor(ActorId::new()).await;
    graceful.stop().await.map_err(actor_err)?;
    match exit_rx.await? {
        ExitReason::Shutdown => println!("[Stop] graceful shutdown confirmed"),
        other => println!("[Stop] unexpected reason: {other:?}"),
    }
    graceful_join.await?;

    let (forced, forced_join) = spawn(Worker { name: "forced".into() }, None)
        .await
        .map_err(actor_err)?;
    let exit_rx = forced.monitor(ActorId::new()).await;
    forced.kill().await.map_err(actor_err)?;
    match exit_rx.await? {
        ExitReason::Killed => println!("[Kill] forced shutdown confirmed"),
        other => println!("[Kill] unexpected reason: {other:?}"),
    }
    forced_join.await?;

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    demo_msg().await?;
    demo_monitor().await?;
    demo_demonitor().await?;
    demo_link_and_linked_exit().await?;
    demo_unlink().await?;
    demo_upgrade().await?;
    demo_stop_vs_kill().await?;

    println!("\nAll envelope variants demonstrated.");
    Ok(())
}
