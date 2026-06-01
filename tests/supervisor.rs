use lane_switchboards::actor::{spawn, Actor, ActorId, ActorProcessingErr};
use lane_switchboards::monitor::ActorMonitor;
use lane_switchboards::supervisor::{
    child_spec, supervise_actor, ChildRegistry, RestartStrategy, Supervisor, SupervisorConfig,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::runtime::Handle;

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
async fn supervise_actor_spawns_single_child() {
    let config = SupervisorConfig {
        strategy: RestartStrategy::OneForOne,
        ..Default::default()
    };
    let (child, sup) = supervise_actor(Echo, config).await.expect("supervise");
    assert_eq!(child.id, sup.initial_ref().expect("initial ref").id);
    assert!(ActorMonitor::global().get(child.id).is_some());
    child.stop().await.expect("stop");
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
}

#[tokio::test]
async fn supervise_actor_initial_ref_matches_registry() {
    let config = SupervisorConfig::default();
    let (child, sup) = supervise_actor(Echo, config).await.expect("supervise");
    let initial = sup.initial_ref().expect("initial ref");
    assert_eq!(child.id, initial.id);
}

#[tokio::test]
async fn child_registry_get_with_generation_is_consistent() {
    let registry: Arc<ChildRegistry<EchoMsg, String>> = Arc::new(ChildRegistry::new());
    let (actor, join) = spawn(Echo, None).await.expect("spawn");
    registry.track_and_bump("echo", actor.clone()).await;

    let (got, gen) = registry
        .get_with_generation("echo")
        .await
        .expect("tracked");
    assert_eq!(got.id, actor.id);
    assert_eq!(gen, 1);
    assert_eq!(registry.generation("echo").await, 1);

    actor.stop().await.expect("stop");
    join.await.expect("join");
}

#[tokio::test]
async fn one_for_all_restarts_children_in_order() {
    #[derive(Clone)]
    struct OrderRecorder {
        log: Arc<tokio::sync::Mutex<Vec<usize>>>,
        order: usize,
        fail: Arc<AtomicBool>,
    }

    enum OrderMsg {
        Ping,
    }

    #[async_trait::async_trait]
    impl Actor<OrderMsg> for OrderRecorder {
        async fn handle(&mut self, _msg: OrderMsg) -> Result<(), ActorProcessingErr> {
            if self.fail.load(Ordering::Relaxed) {
                return Err("fail".into());
            }
            Ok(())
        }
    }

    let log = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let fail_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let make_spec = |order: usize| {
        let log = log.clone();
        let fail_flag = fail_flag.clone();
        child_spec(order, move |sup_tx, actor_config| {
            let log = log.clone();
            let fail_flag = fail_flag.clone();
            Box::pin(async move {
                log.lock().await.push(order);
                let (actor_ref, _) = lane_switchboards::actor::spawn_on_runtime(
                    &Handle::current(),
                    OrderRecorder {
                        log: log.clone(),
                        order,
                        fail: fail_flag.clone(),
                    },
                    Some(sup_tx),
                    &actor_config,
                )
                .await?;
                Ok(actor_ref)
            })
        })
    };

    let config = SupervisorConfig {
        strategy: RestartStrategy::OneForAll,
        max_restarts: 20,
        ..Default::default()
    };
    let sup = Supervisor::with_actor_config(
        lane_switchboards::config::ActorConfig::default(),
        config,
        vec![make_spec(2), make_spec(0), make_spec(1)],
    );
    let handle = sup.start().await.expect("start");
    assert_eq!(handle.initial_refs().len(), 3);

    fail_flag.store(true, Ordering::Relaxed);
    handle.initial_refs()[1]
        .send(OrderMsg::Ping)
        .await
        .expect("send fail");

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let recorded = log.lock().await.clone();
    assert!(recorded.len() >= 6, "expected restarts, got {recorded:?}");
    let restart_orders: Vec<usize> = recorded
        .iter()
        .copied()
        .skip(3)
        .collect();
    assert_eq!(restart_orders, vec![0, 1, 2]);
}

#[tokio::test]
async fn demonitor_prevents_exit_notification() {
    use lane_switchboards::actor::spawn;

    #[derive(Clone)]
    struct Quiet;

    enum QuietMsg {
        Ping,
    }

    #[async_trait::async_trait]
    impl Actor<QuietMsg> for Quiet {
        async fn handle(&mut self, _msg: QuietMsg) -> Result<(), ActorProcessingErr> {
            Ok(())
        }
    }

    let (target, join) = spawn(Quiet, None).await.expect("spawn");
    let observer_id = ActorId::new();
    let rx = target.monitor(observer_id).await;
    target.demonitor(observer_id).await.expect("demonitor");
    target.stop().await.expect("stop");

    tokio::select! {
        result = rx => {
            assert!(result.is_err(), "demonitored observer should not receive exit reason");
        }
        _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {}
    }

    join.await.expect("join");
}

#[tokio::test]
async fn bidirectional_link_notifies_when_peer_exits_first() {
    use lane_switchboards::actor::spawn;
    use std::time::Duration;

    #[derive(Clone)]
    struct LinkedWorker {
        name: &'static str,
        fail: bool,
    }

    enum LinkMsg {
        Ping,
    }

    #[async_trait::async_trait]
    impl Actor<LinkMsg> for LinkedWorker {
        async fn handle(&mut self, _msg: LinkMsg) -> Result<(), ActorProcessingErr> {
            if self.fail {
                return Err("linked fail".into());
            }
            Ok(())
        }
    }

    let (alpha, alpha_join) = spawn(
        LinkedWorker {
            name: "alpha",
            fail: true,
        },
        None,
    )
    .await
    .expect("spawn alpha");
    let (beta, beta_join) = spawn(
        LinkedWorker {
            name: "beta",
            fail: false,
        },
        None,
    )
    .await
    .expect("spawn beta");

    alpha.link(beta.id).await.expect("link");
    tokio::time::sleep(Duration::from_millis(20)).await;

    alpha.send(LinkMsg::Ping).await.expect("trigger alpha fail");

    tokio::time::sleep(Duration::from_millis(100)).await;

    assert!(alpha_join.is_finished());
    assert!(beta_join.is_finished());
}
