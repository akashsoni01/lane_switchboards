use once_cell::sync::OnceCell;
use std::panic::{catch_unwind, AssertUnwindSafe};

use super::MetricsConfig;

/// Run metrics work; panics are caught so export never crashes actors or HTTP scrapes.
pub(crate) fn metrics_try<F: FnOnce()>(op: &'static str, f: F) {
    if catch_unwind(AssertUnwindSafe(f)).is_err() {
        tracing::warn!(op, "metrics panicked; update dropped");
    }
}

static GLOBAL_CONFIG: OnceCell<MetricsConfig> = OnceCell::new();

pub fn init_global_config(config: MetricsConfig) {
    let _ = GLOBAL_CONFIG.set(config);
}

pub fn global_node() -> String {
    GLOBAL_CONFIG
        .get()
        .and_then(|c| c.node.clone())
        .or_else(|| std::env::var("LANE_NODE").ok())
        .unwrap_or_else(|| "unknown".into())
}

pub fn on_scrape_callback() -> Option<std::sync::Arc<dyn Fn(&str) + Send + Sync>> {
    GLOBAL_CONFIG.get().and_then(|c| c.on_scrape.clone())
}

pub(crate) const ACTOR_LABEL_NAMES: &[&str] =
    &["actor_name", "actor_type", "supervisor", "node", "service"];

pub(crate) const EXIT_LABEL_NAMES: &[&str] = &[
    "reason",
    "actor_name",
    "actor_type",
    "supervisor",
    "node",
    "service",
];

pub(crate) fn register_counter_vec(
    registry: &prometheus::Registry,
    name: &str,
    help: &str,
    labels: &[&str],
) -> prometheus::IntCounterVec {
    let vec = prometheus::IntCounterVec::new(prometheus::Opts::new(name, help), labels)
        .unwrap_or_else(|e| panic!("{name}: {e}"));
    registry
        .register(Box::new(vec.clone()))
        .unwrap_or_else(|e| panic!("register {name}: {e}"));
    vec
}

pub(crate) fn register_gauge_vec(
    registry: &prometheus::Registry,
    name: &str,
    help: &str,
    labels: &[&str],
) -> prometheus::GaugeVec {
    let vec = prometheus::GaugeVec::new(prometheus::Opts::new(name, help), labels)
        .unwrap_or_else(|e| panic!("{name}: {e}"));
    registry
        .register(Box::new(vec.clone()))
        .unwrap_or_else(|e| panic!("register {name}: {e}"));
    vec
}

pub(crate) fn register_histogram_vec(
    registry: &prometheus::Registry,
    name: &str,
    help: &str,
    buckets: Vec<f64>,
    labels: &[&str],
) -> prometheus::HistogramVec {
    let vec = prometheus::HistogramVec::new(
        prometheus::HistogramOpts::new(name, help).buckets(buckets),
        labels,
    )
    .unwrap_or_else(|e| panic!("{name}: {e}"));
    registry
        .register(Box::new(vec.clone()))
        .unwrap_or_else(|e| panic!("register {name}: {e}"));
    vec
}
