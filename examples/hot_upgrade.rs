//! Hot code upgrade: swap `CounterV1` → `CounterV2` without stopping the actor.

use lane_switchboards::actor::{spawn, Actor, ActorProcessingErr};

struct CounterV1 {
    count: u64,
}

struct CounterV2 {
    count: u64,
    label: String,
}

impl CounterV2 {
    /// Build V2 from a V1 snapshot (count is read via `Get` before `upgrade`).
    fn from_v1_count(count: u64) -> Self {
        Self {
            count,
            label: format!("migrated from v1 (count was {count})"),
        }
    }
}

enum CounterMsg {
    Inc,
    Get(tokio::sync::oneshot::Sender<u64>),
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let (actor, join) = spawn(CounterV1 { count: 0 }, None)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    for _ in 0..3 {
        actor
            .send(CounterMsg::Inc)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
    }

    let (tx, rx) = tokio::sync::oneshot::channel();
    actor
        .send(CounterMsg::Get(tx))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let v1_count = rx.await?;
    println!("V1 count: {v1_count}");

    // Hot upgrade: carry V1 state into the new implementation (not a fresh counter).
    let v2 = CounterV2::from_v1_count(v1_count);
    println!("{}", v2.label);
    actor
        .upgrade(v2)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    actor
        .send(CounterMsg::Inc)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let (tx, rx) = tokio::sync::oneshot::channel();
    actor
        .send(CounterMsg::Get(tx))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    println!("V2 count after +2 increment: {}", rx.await?);

    actor
        .stop()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    join.await?;
    Ok(())
}
