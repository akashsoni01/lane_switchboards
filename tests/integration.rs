use lane_switchboards::actor::{spawn, Actor, ActorProcessingErr};
use lane_switchboards::supervisor::{
    spawn_child_spec, supervise_actor, ChildRegistry, ChildSlot, RestartStrategy,
    Supervisor, SupervisorConfig,
};

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

#[tokio::test]
async fn child_registry_tracks_named_child() {
    let registry = std::sync::Arc::new(ChildRegistry::new());
    let spec = spawn_child_spec(0, "echo", registry.clone(), || Echo);

    let config = SupervisorConfig {
        strategy: RestartStrategy::OneForOne,
        ..Default::default()
    };
    let _sup = Supervisor::new(config, vec![spec]).start().await.expect("start");

    let echo = registry.get("echo").await.expect("spawned");
    echo.send(EchoMsg::Ping).await.expect("send");
}

#[derive(Clone)]
struct EchoHolder {
    registry: std::sync::Arc<ChildRegistry<EchoMsg>>,
}

#[async_trait::async_trait]
impl Actor<EchoMsg> for EchoHolder {
    async fn pre_start(&mut self) -> Result<(), ActorProcessingErr> {
        self.registry.bump_generation("echo").await;
        Ok(())
    }

    async fn handle(&mut self, _msg: EchoMsg) -> Result<(), ActorProcessingErr> {
        Ok(())
    }
}

#[tokio::test]
async fn child_registry_updates_on_restart() {
    let registry = std::sync::Arc::new(ChildRegistry::new());
    let spec = spawn_child_spec(0, "echo", registry.clone(), {
        let registry = registry.clone();
        move || EchoHolder {
            registry: registry.clone(),
        }
    });

    let config = SupervisorConfig {
        strategy: RestartStrategy::OneForOne,
        ..Default::default()
    };
    let _sup = Supervisor::new(config, vec![spec]).start().await.expect("start");

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert_eq!(registry.generation("echo").await, 1);
}

#[tokio::test]
async fn child_slot_holds_current_ref() {
    let slot = std::sync::Arc::new(ChildSlot::new());
    let spec = ChildSlot::child_spec(0, slot.clone(), || Echo);

    let config = SupervisorConfig {
        strategy: RestartStrategy::OneForOne,
        ..Default::default()
    };
    let _sup = Supervisor::new(config, vec![spec]).start().await.expect("start");

    let child = slot.require().await.expect("spawned");
    child.send(EchoMsg::Ping).await.expect("send");
}
