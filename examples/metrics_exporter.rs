//! Prometheus `/metrics` exporter for [`ActorMonitor`].
//!
//! ```bash
//! cargo run --example metrics_exporter --features metrics
//! ```
//!
//! Scrape `http://127.0.0.1:9090/metrics` with Prometheus or curl.
//! Import Grafana dashboards from `docs/grafana/` (see `docs/todo.md`).

use lane_switchboards::actor::{Actor, ActorProcessingErr};
use lane_switchboards::config::ActorConfig;
use lane_switchboards::metrics::{init_metrics, render_prometheus_text, serve_metrics_http, MetricsConfig};
use lane_switchboards::monitor::{ActorMeta, ActorMonitor};
use lane_switchboards::supervisor::{supervise_actor_with_config, SupervisorConfig};
use std::net::SocketAddr;
use std::time::Duration;

enum WorkMsg {
    Ping,
}

#[derive(Clone)]
struct Worker;

#[async_trait::async_trait]
impl Actor<WorkMsg> for Worker {
    async fn handle(&mut self, msg: WorkMsg) -> Result<(), ActorProcessingErr> {
        match msg {
            WorkMsg::Ping => {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    init_metrics(MetricsConfig {
        node: Some("metrics_exporter".into()),
        ..Default::default()
    });

    let actor_config = ActorConfig {
        monitor_meta: ActorMeta::default()
            .with_name("worker")
            .with_actor_type("Worker"),
        slow_handle_threshold: Some(Duration::from_millis(1)),
        ..Default::default()
    };

    let (worker, _sup) = supervise_actor_with_config(
        Worker,
        SupervisorConfig::default(),
        &actor_config,
    )
    .await
    .expect("supervise worker");

    let addr: SocketAddr = "127.0.0.1:9090".parse()?;
    println!("metrics_exporter listening on http://{addr}/metrics");
    println!("PromQL example: rate(lane_actor_messages_handled_total[1m])");
    println!("Docker stack: docs/grafana/docker-compose.yml");

    let metrics_task = tokio::spawn(serve_metrics_http(addr));

    for _ in 0..20 {
        worker
            .send(WorkMsg::Ping)
            .await
            .expect("send ping");
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    if let Some(stats) = ActorMonitor::global().get(worker.id) {
        println!(
            "local stats: handled={} mailbox_depth={} mean_ms={}",
            stats.messages_handled, stats.mailbox_depth, stats.mean_handle_ms
        );
    }

    let sample = render_prometheus_text()?;
    assert!(sample.contains("lane_actor_mailbox_wait_seconds"));
    let lines: usize = sample.lines().filter(|l| !l.starts_with('#')).count();
    println!("prometheus sample lines (non-comment): {lines}");

    metrics_task.abort();
    Ok(())
}
