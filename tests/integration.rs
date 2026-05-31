use lane_switchboards::actor::{spawn, Actor, ActorProcessingErr};
use lane_switchboards::supervisor::{supervise_actor, RestartStrategy, SupervisorConfig};

#[derive(Clone)]
struct Echo;

enum EchoMsg {
    Ping,
}

#[async_trait::async_trait]
impl Actor<EchoMsg> for Echo {
    async fn handle(&mut self, _msg: EchoMsg) -> Result<(), ActorProcessingErr> {
        Ok(())
    }
}

#[tokio::test]
async fn actor_spawns_and_stops() {
    let (actor, join) = spawn(Echo, None).await.expect("spawn");
    actor.stop().await.expect("stop");
    join.await.expect("join");
}

#[tokio::test]
async fn supervisor_restarts_child() {
    let config = SupervisorConfig {
        strategy: RestartStrategy::OneForOne,
        ..Default::default()
    };
    let (child, _sup) = supervise_actor(Echo, config).await.expect("supervise");
    child.send(EchoMsg::Ping).await.expect("send");
}
