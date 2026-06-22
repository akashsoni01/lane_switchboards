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
use lane_switchboards::metrics::{init_metrics, render_prometheus_text, MetricsConfig};
use lane_switchboards::monitor::{ActorMeta, ActorMonitor};
use lane_switchboards::supervisor::{supervise_actor_with_config, SupervisorConfig};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

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

async fn serve_metrics(listener: TcpListener) -> std::io::Result<()> {
    loop {
        let (mut stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let n = stream.read(&mut buf).await.unwrap_or(0);
            let req = String::from_utf8_lossy(&buf[..n]);
            let (status, body) = if req.starts_with("GET /metrics") {
                match render_prometheus_text() {
                    Ok(text) => ("200 OK", text),
                    Err(e) => ("500 Internal Server Error", format!("render error: {e}")),
                }
            } else if req.starts_with("GET /health") {
                ("200 OK", "ok".into())
            } else {
                ("404 Not Found", "not found".into())
            };

            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes()).await;
        });
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

    let listener = TcpListener::bind("127.0.0.1:9090").await?;
    println!("metrics_exporter listening on http://127.0.0.1:9090/metrics");
    println!("PromQL example: rate(lane_actor_messages_handled_total[1m])");

    let metrics_task = tokio::spawn(serve_metrics(listener));

    for _ in 0..20 {
        worker
            .send(WorkMsg::Ping)
            .await
            .expect("send ping");
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    if let Some(stats) = ActorMonitor::global().get(worker.id) {
        println!(
            "local stats: handled={} mean_ms={}",
            stats.messages_handled, stats.mean_handle_ms
        );
    }

    let sample = render_prometheus_text()?;
    let lines: usize = sample.lines().filter(|l| !l.starts_with('#')).count();
    println!("prometheus sample lines (non-comment): {lines}");

    metrics_task.abort();
    Ok(())
}
