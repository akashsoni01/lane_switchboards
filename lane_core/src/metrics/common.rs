use once_cell::sync::{Lazy, OnceCell};
use std::panic::{catch_unwind, AssertUnwindSafe};

use super::MetricsConfig;

/// Run metrics work; panics are caught so export never crashes actors or HTTP scrapes.
pub(crate) fn metrics_try<F: FnOnce()>(op: &'static str, f: F) {
    if catch_unwind(AssertUnwindSafe(f)).is_err() {
        tracing::warn!(op, "metrics panicked; update dropped");
    }
}

static GLOBAL_CONFIG: OnceCell<MetricsConfig> = OnceCell::new();

/// Default prefix — matches bundled Grafana dashboards (`docs/grafana/actor-runtime.json`).
pub const DEFAULT_METRIC_PREFIX: &str = "lane";

/// Resolved once on first metric registration (call [`super::init_metrics`] before that).
static METRIC_PREFIX: Lazy<String> = Lazy::new(resolve_metric_prefix);

fn resolve_metric_prefix() -> String {
    let raw = GLOBAL_CONFIG
        .get()
        .and_then(|c| c.metric_prefix.clone())
        .or_else(|| std::env::var("LANE_METRICS_PREFIX").ok());

    match raw {
        None => DEFAULT_METRIC_PREFIX.into(),
        Some(p) if p.is_empty() => DEFAULT_METRIC_PREFIX.into(),
        Some(p) if is_valid_metric_prefix(&p) => p,
        Some(p) => {
            tracing::warn!(
                prefix = %p,
                default = DEFAULT_METRIC_PREFIX,
                "invalid metric prefix (use ASCII letters, digits, underscore; start with a letter); \
                 falling back to default so existing Grafana/Prometheus queries stay valid"
            );
            DEFAULT_METRIC_PREFIX.into()
        }
    }
}

/// Prometheus-safe prefix: `[a-zA-Z][a-zA-Z0-9_]*`, max 64 chars.
fn is_valid_metric_prefix(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_') && s.len() <= 64
}

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

/// Active Prometheus metric name prefix (`lane` by default).
///
/// **Shared stacks:** the default `lane` prefix is unchanged and matches the bundled Grafana
/// dashboards. A custom prefix applies only to **this process** — it does not rename or delete
/// metrics from other scrape targets. Prefer labels (`node`, `service`, `actor_name`) to filter
/// within `lane_*` before changing the prefix.
///
/// Set via [`MetricsConfig::metric_prefix`] in [`super::init_metrics`] or `LANE_METRICS_PREFIX`
/// (must be set before the first metric is registered). Invalid values fall back to `lane`.
pub fn metric_prefix() -> &'static str {
    METRIC_PREFIX.as_str()
}

/// Build a fully qualified Prometheus metric name: `{prefix}_{suffix}`.
///
/// Called only during registry setup (once per process), not on the actor handle hot path.
pub(crate) fn metric_name(suffix: &str) -> String {
    format!("{}_{}", METRIC_PREFIX.as_str(), suffix)
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

#[cfg(test)]
mod tests {
    use super::{is_valid_metric_prefix, DEFAULT_METRIC_PREFIX};

    #[test]
    fn default_prefix_constant() {
        assert_eq!(DEFAULT_METRIC_PREFIX, "lane");
    }

    #[test]
    fn valid_metric_prefixes() {
        assert!(is_valid_metric_prefix("lane"));
        assert!(is_valid_metric_prefix("mysvc"));
        assert!(is_valid_metric_prefix("orders_lane"));
        assert!(!is_valid_metric_prefix(""));
        assert!(!is_valid_metric_prefix("9bad"));
        assert!(!is_valid_metric_prefix("http-server"));
    }
}
