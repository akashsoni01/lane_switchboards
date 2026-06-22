//! Integration test: actor traffic appears in Prometheus export.

use lane_switchboards::actor::{spawn, Actor, ActorProcessingErr};
use lane_switchboards::config::ActorConfig;
use lane_switchboards::metrics::render_prometheus_text;
use lane_switchboards::monitor::{ActorMeta, ActorMonitor};
use std::time::Duration;

struct Echo;

#[async_trait::async_trait]
impl Actor<u32> for Echo {
    async fn handle(&mut self, _msg: u32) -> Result<(), ActorProcessingErr> {
        Ok(())
    }
}

#[tokio::test]
async fn prometheus_export_reflects_handled_messages() {
    let config = ActorConfig {
        monitor_meta: ActorMeta::default().with_name("echo"),
        ..Default::default()
    };
    let (actor, join) = lane_switchboards::actor::spawn_with_config(Echo, None, &config)
        .await
        .expect("spawn");

    for _ in 0..5 {
        actor.send(1).await.expect("send");
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    let stats = ActorMonitor::global().get(actor.id).expect("stats");
    assert!(stats.messages_handled >= 5, "expected handled >= 5");

    let body = render_prometheus_text().expect("render");
    assert!(body.contains("lane_actor_messages_handled_total"));
    assert!(body.contains("lane_actor_mailbox_wait_seconds"));

    actor.stop().await.expect("stop");
    join.await.expect("join");
}
