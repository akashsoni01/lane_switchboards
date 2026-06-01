use lane_switchboards::actor::{spawn, Actor, ActorProcessingErr};
use std::sync::Arc;
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
    let registry: Arc<ChildRegistry<EchoMsg, String>> = std::sync::Arc::new(ChildRegistry::new());
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct RemotePing(u64);

struct PingCounter(Arc<std::sync::atomic::AtomicU64>);

#[async_trait::async_trait]
impl Actor<RemotePing> for PingCounter {
    async fn handle(&mut self, msg: RemotePing) -> Result<(), ActorProcessingErr> {
        let _ = msg;
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}

#[tokio::test]
async fn cluster_round_robin_across_nodes() {
    use lane_switchboards::distributed::{serve_actor, Cluster};

    let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let a = serve_actor(
        "a",
        "127.0.0.1:0",
        "worker",
        PingCounter(counter.clone()),
    )
    .await
    .expect("node a");
    let b = serve_actor(
        "b",
        "127.0.0.1:0",
        "worker",
        PingCounter(Arc::new(std::sync::atomic::AtomicU64::new(0))),
    )
    .await
    .expect("node b");

    let mut cluster = Cluster::new();
    cluster.join(a.member.clone());
    cluster.join(b.member.clone());

    for i in 0..4 {
        cluster
            .send_round_robin(RemotePing(i))
            .await
            .expect("send");
    }

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 2);
}

#[tokio::test]
async fn cluster_hash_ring_routes_same_key_to_same_node() {
    use lane_switchboards::distributed::{serve_actor, Cluster};

    let a = serve_actor(
        "a",
        "127.0.0.1:0",
        "worker",
        PingCounter(Arc::new(std::sync::atomic::AtomicU64::new(0))),
    )
    .await
    .expect("node a");
    let b = serve_actor(
        "b",
        "127.0.0.1:0",
        "worker",
        PingCounter(Arc::new(std::sync::atomic::AtomicU64::new(0))),
    )
    .await
    .expect("node b");

    let mut cluster = Cluster::new();
    cluster.join(a.member.clone());
    cluster.join(b.member.clone());

    let key = 42u64;
    let first = cluster.member_for_key(&key).map(|m| m.name.clone());
    assert_eq!(cluster.member_for_key(&key).map(|m| m.name.clone()), first);

    for _ in 0..3 {
        cluster
            .send_by_key(&key, RemotePing(key))
            .await
            .expect("send");
    }

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(first.is_some());
}

#[derive(Clone)]
struct SlowMsg {
    sleep_ms: u64,
}

struct SlowWorker {
    pending: Option<SlowMsg>,
    stuck_journal: Arc<std::sync::Mutex<Vec<SlowMsg>>>,
}

#[async_trait::async_trait]
impl Actor<SlowMsg> for SlowWorker {
    async fn on_handle_begin(&mut self, msg: &SlowMsg) -> Result<(), ActorProcessingErr> {
        self.pending = Some(msg.clone());
        Ok(())
    }

    async fn handle(&mut self, msg: SlowMsg) -> Result<(), ActorProcessingErr> {
        tokio::time::sleep(std::time::Duration::from_millis(msg.sleep_ms)).await;
        self.pending = None;
        Ok(())
    }

    async fn on_handle_stuck(
        &mut self,
        ctx: lane_switchboards::actor::HandleStuckContext,
    ) -> Result<(), ActorProcessingErr> {
        if let Some(pending) = self.pending.take() {
            self.stuck_journal.lock().unwrap().push(pending);
        }
        tracing::info!(
            actor = %ctx.actor_id,
            elapsed_ms = ctx.elapsed.as_millis(),
            "persisted stuck action"
        );
        Ok(())
    }
}

#[tokio::test]
async fn handle_timeout_triggers_stuck_recovery_and_stats() {
    use lane_switchboards::actor::spawn_with_config;
    use lane_switchboards::config::ActorConfig;
    use lane_switchboards::monitor::ActorMonitor;
    use std::time::Duration;

    let journal = Arc::new(std::sync::Mutex::new(Vec::new()));
    let config = ActorConfig {
        handle_timeout: Some(Duration::from_millis(50)),
        ..Default::default()
    };

    let (actor, join) = spawn_with_config(
        SlowWorker {
            pending: None,
            stuck_journal: journal.clone(),
        },
        None,
        &config,
    )
    .await
    .expect("spawn");

    let id = actor.id;
    actor
        .send(SlowMsg { sleep_ms: 500 })
        .await
        .expect("send slow msg");

    tokio::time::sleep(Duration::from_millis(120)).await;

    let stats = ActorMonitor::global().get(id).expect("stats");
    assert_eq!(stats.handle_timeouts, 1);
    assert_eq!(journal.lock().unwrap().len(), 1);
    assert_eq!(journal.lock().unwrap()[0].sleep_ms, 500);

    join.await.expect("join");
}
