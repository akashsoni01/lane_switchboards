//! Prometheus `/metrics` exporter for [`ActorMonitor`].
//!
//! ```bash
//! cargo run --example metrics_exporter --features metrics
//! ```
//!
//! Scrape `http://127.0.0.1:9090/metrics` with Prometheus or curl.
//! Import Grafana dashboards from `docs/grafana/` (see `examples/metrics_exporter.md`).
//!
//! Press Ctrl-C to stop. See the README for Docker + manual Prometheus/Grafana setup.

use lane_switchboards::actor::{Actor, ActorProcessingErr};
use lane_switchboards::config::ActorConfig;
use lane_switchboards::metrics::{init_metrics, serve_metrics_http, MetricsConfig};
use lane_switchboards::monitor::ActorMeta;
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
    println!("Docker stack: docker compose -f docs/grafana/docker-compose.yml up");
    println!("Press Ctrl-C to stop.");

    tokio::spawn(serve_metrics_http(addr));

    let mut interval = tokio::time::interval(Duration::from_secs(2));
    loop {
        tokio::select! {
            _ = interval.tick() => {
                if worker.send(WorkMsg::Ping).await.is_err() {
                    eprintln!("worker send failed — stopping ping loop");
                    break;
                }
            }
            _ = tokio::signal::ctrl_c() => {
                println!("\nshutting down");
                break;
            }
        }
    }

    Ok(())
}
