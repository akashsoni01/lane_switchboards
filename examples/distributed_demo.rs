//! Two-node distributed messaging demo.

use lane_switchboards::actor::{spawn, Actor, ActorProcessingErr};
use lane_switchboards::distributed::{Node, RemoteActorRef};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

#[derive(Debug, Clone, Serialize, Deserialize)]
enum PingMsg {
    Ping(String),
}

struct PingActor;

#[async_trait::async_trait]
impl Actor<PingMsg> for PingActor {
    async fn handle(&mut self, msg: PingMsg) -> Result<(), ActorProcessingErr> {
        let PingMsg::Ping(s) = msg;
        println!("received remote ping: {s}");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let node_b = Node::<PingMsg>::bind("node-b", "127.0.0.1:0").await?;
    let addr_b = node_b.address().to_string();
    let (tx_b, mut rx_b) = mpsc::channel(16);
    node_b.register("worker", tx_b).await;

    tokio::spawn(async move {
        let (actor, _) = spawn(PingActor, None).await.unwrap();
        while let Some(PingMsg::Ping(s)) = rx_b.recv().await {
            actor.send(PingMsg::Ping(s)).await.ok();
        }
    });

    let remote = RemoteActorRef::<PingMsg>::new(&addr_b, "worker");
    remote
        .send(PingMsg::Ping("hello from remote".into()))
        .await?;

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    Ok(())
}
