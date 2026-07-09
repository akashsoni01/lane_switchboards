//! Prometheus export for [`crate::monitor::ActorMonitor`] (`metrics` feature).
//!
//! Metrics are split by domain:
//! - [`actor`] — always registered (hot path, ~90% of usage)
//! - [`supervisor`], [`mesh`], [`remote`], [`storage`] — lazy-registered on first use
//!
//! Hot-path actor counters are pre-bound per actor at register time.
//! Gauges for uptime / idle / alive are refreshed when [`render_prometheus_text`] is called.

mod actor;
mod common;
mod mesh;
mod remote;
mod storage;
mod supervisor;

use crate::actor::ExitReason;
use crate::monitor::ActorMonitor;
use crate::supervisor::{IntensityAction, RestartStrategy};
use once_cell::sync::{Lazy, OnceCell};
use prometheus::{Encoder, Registry, TextEncoder};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::time::{SystemTime, UNIX_EPOCH};

pub use actor::actor_label_strings;
pub use common::metric_prefix;
pub use mesh::ConsistencyOpSnapshot;
pub use storage::StorageMetricsSnapshot;

pub(crate) use actor::PromActorMetrics;

use actor::ActorMetricsRegistry;
use common::{metrics_try, on_scrape_callback};
use mesh::MeshMetricsRegistry;
use remote::RemoteMetricsRegistry;
use storage::StorageMetricsRegistry;
use supervisor::SupervisorMetricsRegistry;

/// Process-wide label defaults applied when [`ActorMeta`] fields are unset.
#[derive(Clone, Default)]
pub struct MetricsConfig {
    pub node: Option<String>,
    pub dc: Option<String>,
    pub environment: Option<String>,
    /// Prometheus metric name prefix (default `lane`). Series become `{prefix}_actor_*`, etc.
    ///
    /// Opt-in only — unset keeps `lane_*` so existing Grafana dashboards and PromQL keep working.
    /// Only this process is affected; other services on the same Prometheus are unchanged.
    /// Overridden by `LANE_METRICS_PREFIX` when unset here. Invalid values fall back to `lane`.
    pub metric_prefix: Option<String>,
    /// Optional hook invoked after each successful [`render_prometheus_text`] (push sinks, logging).
    pub on_scrape: Option<std::sync::Arc<dyn Fn(&str) + Send + Sync>>,
}

impl std::fmt::Debug for MetricsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsConfig")
            .field("node", &self.node)
            .field("dc", &self.dc)
            .field("environment", &self.environment)
            .field("metric_prefix", &self.metric_prefix)
            .field("on_scrape", &self.on_scrape.as_ref().map(|_| "<callback>"))
            .finish()
    }
}

/// Shared Prometheus registry; actor metrics are always present.
struct MetricsHub {
    registry: Registry,
    actor: ActorMetricsRegistry,
}

static HUB: Lazy<MetricsHub> = Lazy::new(MetricsHub::new);

static SUPERVISOR: OnceCell<SupervisorMetricsRegistry> = OnceCell::new();
static MESH: OnceCell<MeshMetricsRegistry> = OnceCell::new();
static REMOTE: OnceCell<RemoteMetricsRegistry> = OnceCell::new();
static STORAGE: OnceCell<StorageMetricsRegistry> = OnceCell::new();

impl MetricsHub {
    fn new() -> Self {
        let registry = Registry::new();
        let actor = ActorMetricsRegistry::register(&registry);
        Self { registry, actor }
    }

    fn render(&self) -> Result<String, prometheus::Error> {
        let encoder = TextEncoder::new();
        let families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&families, &mut buffer)?;
        Ok(String::from_utf8_lossy(&buffer).into_owned())
    }
}

/// Process-wide registry for registering additional metric families (e.g. messenger).
pub fn shared_registry() -> &'static Registry {
    &HUB.registry
}

fn actor_metrics() -> &'static ActorMetricsRegistry {
    &HUB.actor
}

fn supervisor_metrics() -> &'static SupervisorMetricsRegistry {
    SUPERVISOR.get_or_init(|| SupervisorMetricsRegistry::register(&HUB.registry))
}

fn mesh_metrics() -> &'static MeshMetricsRegistry {
    MESH.get_or_init(|| MeshMetricsRegistry::register(&HUB.registry))
}

fn remote_metrics() -> &'static RemoteMetricsRegistry {
    REMOTE.get_or_init(|| RemoteMetricsRegistry::register(&HUB.registry))
}

fn storage_metrics() -> &'static StorageMetricsRegistry {
    STORAGE.get_or_init(|| StorageMetricsRegistry::register(&HUB.registry))
}

/// Set global defaults for Prometheus labels (call once at process start).
pub fn init_metrics(config: MetricsConfig) {
    common::init_global_config(config);
}

// --- actor (always on) ---

pub(crate) fn bind_actor_metrics(
    meta: &crate::monitor::ActorMeta,
    mailbox_capacity: usize,
) -> Option<PromActorMetrics> {
    actor::bind_actor_metrics(actor_metrics(), meta, mailbox_capacity)
}

pub(crate) fn record_counter_saturated(field: &'static str, meta: &crate::monitor::ActorMeta) {
    actor::record_counter_saturated(actor_metrics(), field, meta);
}

pub(crate) fn record_exit(prom: &PromActorMetrics, reason: &ExitReason) {
    actor::record_exit(prom, reason);
}

pub(crate) fn sync_scrape_gauges(
    registered_at: std::time::Instant,
    last_handle_unix_ms: u64,
    prom: &PromActorMetrics,
    in_flight: usize,
    last_handle_ms: usize,
    max_handle_ms: usize,
    mailbox_depth: usize,
) {
    actor::sync_scrape_gauges(
        registered_at,
        last_handle_unix_ms,
        prom,
        in_flight,
        last_handle_ms,
        max_handle_ms,
        mailbox_depth,
    );
}

pub(crate) fn observe_handle_duration(prom: &PromActorMetrics, elapsed: std::time::Duration) {
    actor::observe_handle_duration(prom, elapsed);
}

// --- supervisor (lazy) ---

pub fn record_supervisor_restart(child: &str, strategy: RestartStrategy) {
    supervisor_metrics().record_restart(child, strategy);
}

pub fn record_intensity_exceeded(action: IntensityAction) {
    supervisor_metrics().record_intensity_exceeded(action);
}

pub fn sync_supervisor_scrape(
    restarts_in_window: usize,
    max_restarts: usize,
    children_alive: usize,
) {
    supervisor::sync_supervisor_scrape(restarts_in_window, max_restarts, children_alive);
}

pub fn set_child_generation(child: &str, generation: u64) {
    supervisor_metrics().set_child_generation(child, generation);
}

// --- mesh (lazy) ---

pub fn record_mesh_dispatch(service: &str) {
    mesh_metrics().record_dispatch(service);
}

pub fn record_consistency_operation(op: &ConsistencyOpSnapshot) {
    mesh_metrics().record_consistency_operation(op);
}

// --- remote (lazy) ---

pub fn record_remote_send(target: &str, ok: bool) {
    remote_metrics().record_send(target, ok);
}

pub fn record_remote_ack_timeout(target: &str) {
    remote_metrics().record_ack_timeout(target);
}

// --- storage (lazy) ---

pub fn sync_storage_stats(snapshot: &StorageMetricsSnapshot) {
    storage_metrics().sync_stats(snapshot);
}

// --- labels & helpers ---

pub fn unix_now_ms() -> Result<u64, std::time::SystemTimeError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Map [`ExitReason`] to a bounded Prometheus `reason` label.
pub fn exit_reason_label(reason: &ExitReason) -> &'static str {
    match reason {
        ExitReason::Normal => "normal",
        ExitReason::Shutdown => "shutdown",
        ExitReason::Killed => "killed",
        ExitReason::HandleTimeout { .. } => "handle_timeout",
        ExitReason::Linked(_, inner) => match inner.as_ref() {
            ExitReason::HandleTimeout { .. } => "linked_handle_timeout",
            ExitReason::Error(_) => "linked_error",
            ExitReason::Killed => "linked_killed",
            _ => "linked",
        },
        ExitReason::Error(msg) => {
            if msg.contains("panic") {
                "panic"
            } else {
                "error"
            }
        }
    }
}

/// Map [`RestartStrategy`] to a bounded Prometheus label.
pub fn restart_strategy_label(strategy: RestartStrategy) -> &'static str {
    match strategy {
        RestartStrategy::OneForOne => "one_for_one",
        RestartStrategy::OneForAll => "one_for_all",
        RestartStrategy::RestForOne => "rest_for_one",
    }
}

fn sync_supervisor_prometheus_gauges() {
    let state = supervisor::supervisor_scrape_state();
    supervisor_metrics().sync_scrape_gauges(&state);
}

/// Serve Prometheus text exposition over HTTP (`GET /metrics`, `GET /health`).
pub async fn serve_metrics_http(addr: std::net::SocketAddr) -> std::io::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind(addr).await?;
    loop {
        let (mut stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let n = stream.read(&mut buf).await.unwrap_or(0);
            let req = String::from_utf8_lossy(&buf[..n]);
            let (status, body) = if req.starts_with("GET /metrics") {
                match catch_unwind(AssertUnwindSafe(render_prometheus_text)) {
                    Ok(Ok(text)) => ("200 OK", text),
                    Ok(Err(e)) => ("500 Internal Server Error", format!("render error: {e}")),
                    Err(_) => (
                        "500 Internal Server Error",
                        "metrics render panicked".into(),
                    ),
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

/// Refresh scrape-time gauges, then render Prometheus text exposition format.
pub fn render_prometheus_text() -> Result<String, prometheus::Error> {
    metrics_try("sync_prometheus_gauges", || {
        ActorMonitor::global().sync_prometheus_gauges();
    });
    sync_supervisor_prometheus_gauges();
    let text = HUB.render()?;
    if let Some(cb) = on_scrape_callback() {
        let text_for_cb = text.clone();
        metrics_try("on_scrape", || cb(&text_for_cb));
    }
    Ok(text)
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::metric_name;

    #[test]
    fn exit_reason_labels_are_bounded() {
        assert_eq!(
            exit_reason_label(&ExitReason::HandleTimeout {
                elapsed_ms: 1,
                limit_ms: 1
            }),
            "handle_timeout"
        );
        assert_eq!(exit_reason_label(&ExitReason::Normal), "normal");
    }

    #[test]
    fn actor_metrics_always_registered() {
        let body = HUB.render().expect("render");
        assert!(body.contains(&metric_name("actor_messages_handled_total")));
    }

    #[test]
    fn optional_supervisor_metrics_lazy_register() {
        record_supervisor_restart("worker", RestartStrategy::OneForOne);
        let body = render_prometheus_text().expect("render");
        assert!(body.contains(&metric_name("supervisor_restarts_total")));
    }

    #[test]
    fn optional_mesh_metrics_lazy_register() {
        record_mesh_dispatch("orders");
        let body = render_prometheus_text().expect("render");
        assert!(body.contains(&metric_name("mesh_dispatches_total")));
    }

    #[test]
    fn default_metric_prefix_is_lane() {
        assert_eq!(super::common::DEFAULT_METRIC_PREFIX, "lane");
        assert_eq!(metric_prefix(), "lane");
        let body = HUB.render().expect("render");
        assert!(body.contains("lane_actor_messages_handled_total"));
    }
}
