//! Prometheus export for [`crate::monitor::ActorMonitor`] (`metrics` feature).
//!
//! Hot-path counters are pre-bound per actor at register time (no per-message allocations).
//! Gauges for uptime / idle / alive are refreshed when [`render_prometheus_text`] is called.

use crate::actor::ExitReason;
use crate::monitor::{ActorMeta, ActorMonitor};
use once_cell::sync::{Lazy, OnceCell};
use prometheus::{
    Encoder, Gauge, Histogram, IntCounter, Opts, Registry, TextEncoder,
    HistogramOpts, HistogramVec, IntCounterVec,
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Process-wide label defaults applied when [`ActorMeta`] fields are unset.
#[derive(Debug, Clone, Default)]
pub struct MetricsConfig {
    pub node: Option<String>,
    pub dc: Option<String>,
    pub environment: Option<String>,
}

static GLOBAL_CONFIG: OnceCell<MetricsConfig> = OnceCell::new();

/// Set global defaults for Prometheus labels (call once at process start).
pub fn init_metrics(config: MetricsConfig) {
    let _ = GLOBAL_CONFIG.set(config);
}

fn global_node() -> String {
    GLOBAL_CONFIG
        .get()
        .and_then(|c| c.node.clone())
        .or_else(|| std::env::var("LANE_NODE").ok())
        .unwrap_or_else(|| "unknown".into())
}

const ACTOR_LABEL_NAMES: &[&str] = &["actor_name", "actor_type", "supervisor", "node", "service"];
const EXIT_LABEL_NAMES: &[&str] = &[
    "reason",
    "actor_name",
    "actor_type",
    "supervisor",
    "node",
    "service",
];

/// Pre-bound Prometheus handles for one actor (no label allocation on the hot path).
pub(crate) struct PromActorMetrics {
    pub messages_handled: IntCounter,
    pub handle_errors: IntCounter,
    pub panics: IntCounter,
    pub handle_timeouts: IntCounter,
    pub slow_handles: IntCounter,
    pub in_flight: Gauge,
    pub last_handle_seconds: Gauge,
    pub max_handle_seconds: Gauge,
    pub mailbox_capacity: Gauge,
    pub mailbox_depth: Gauge,
    pub handle_duration: Histogram,
    pub uptime_seconds: Gauge,
    pub idle_seconds: Gauge,
    pub alive: Gauge,
    exits: IntCounterVec,
    exit_labels: [String; 6],
}

struct MetricsRegistry {
    registry: Registry,
    messages_handled: IntCounterVec,
    handle_errors: IntCounterVec,
    panics: IntCounterVec,
    handle_timeouts: IntCounterVec,
    slow_handles: IntCounterVec,
    counter_saturated: IntCounterVec,
    exits: IntCounterVec,
    in_flight: prometheus::GaugeVec,
    last_handle_seconds: prometheus::GaugeVec,
    max_handle_seconds: prometheus::GaugeVec,
    mailbox_capacity: prometheus::GaugeVec,
    mailbox_depth: prometheus::GaugeVec,
    handle_duration: HistogramVec,
    uptime_seconds: prometheus::GaugeVec,
    idle_seconds: prometheus::GaugeVec,
    alive: prometheus::GaugeVec,
}

static METRICS: Lazy<MetricsRegistry> = Lazy::new(MetricsRegistry::new);

impl MetricsRegistry {
    fn new() -> Self {
        let registry = Registry::new();
        let messages_handled = IntCounterVec::new(
            Opts::new(
                "lane_actor_messages_handled_total",
                "Successful actor handle() completions",
            ),
            ACTOR_LABEL_NAMES,
        )
        .expect("lane_actor_messages_handled_total");
        registry
            .register(Box::new(messages_handled.clone()))
            .expect("register messages_handled");

        let handle_errors = IntCounterVec::new(
            Opts::new("lane_actor_handle_errors_total", "handle() returned Err"),
            ACTOR_LABEL_NAMES,
        )
        .expect("lane_actor_handle_errors_total");
        registry
            .register(Box::new(handle_errors.clone()))
            .expect("register handle_errors");

        let panics = IntCounterVec::new(
            Opts::new("lane_actor_panics_total", "handle() panicked"),
            ACTOR_LABEL_NAMES,
        )
        .expect("lane_actor_panics_total");
        registry
            .register(Box::new(panics.clone()))
            .expect("register panics");

        let handle_timeouts = IntCounterVec::new(
            Opts::new(
                "lane_actor_handle_timeouts_total",
                "handle() exceeded handle_timeout",
            ),
            ACTOR_LABEL_NAMES,
        )
        .expect("lane_actor_handle_timeouts_total");
        registry
            .register(Box::new(handle_timeouts.clone()))
            .expect("register handle_timeouts");

        let slow_handles = IntCounterVec::new(
            Opts::new(
                "lane_actor_slow_handles_total",
                "handle() finished but exceeded slow_handle_threshold",
            ),
            ACTOR_LABEL_NAMES,
        )
        .expect("lane_actor_slow_handles_total");
        registry
            .register(Box::new(slow_handles.clone()))
            .expect("register slow_handles");

        let counter_saturated = IntCounterVec::new(
            Opts::new(
                "lane_actor_counter_saturated_total",
                "Internal stat counter clamped at usize::MAX",
            ),
            &[
                "field",
                "actor_name",
                "actor_type",
                "supervisor",
                "node",
                "service",
            ],
        )
        .expect("lane_actor_counter_saturated_total");
        registry
            .register(Box::new(counter_saturated.clone()))
        .expect("register counter_saturated");

        let exits = IntCounterVec::new(
            Opts::new("lane_actor_exits_total", "Actor exits by reason"),
            EXIT_LABEL_NAMES,
        )
        .expect("lane_actor_exits_total");
        registry
            .register(Box::new(exits.clone()))
            .expect("register exits");

        let gauge_opts = |name: &str, help: &str| {
            prometheus::GaugeVec::new(Opts::new(name, help), ACTOR_LABEL_NAMES)
                .expect(name)
        };

        let in_flight = gauge_opts("lane_actor_in_flight", "Handles started but not finished");
        registry
            .register(Box::new(in_flight.clone()))
            .expect("register in_flight");

        let last_handle_seconds = gauge_opts(
            "lane_actor_last_handle_seconds",
            "Wall time of the most recent handle()",
        );
        registry
            .register(Box::new(last_handle_seconds.clone()))
            .expect("register last_handle_seconds");

        let max_handle_seconds = gauge_opts(
            "lane_actor_max_handle_seconds",
            "Longest handle() wall time recorded",
        );
        registry
            .register(Box::new(max_handle_seconds.clone()))
            .expect("register max_handle_seconds");

        let mailbox_capacity = gauge_opts(
            "lane_actor_mailbox_capacity",
            "Configured actor mailbox capacity",
        );
        registry
            .register(Box::new(mailbox_capacity.clone()))
            .expect("register mailbox_capacity");

        let mailbox_depth = gauge_opts(
            "lane_actor_mailbox_depth",
            "Approximate queued messages in the actor mailbox",
        );
        registry
            .register(Box::new(mailbox_depth.clone()))
            .expect("register mailbox_depth");

        let handle_duration = HistogramVec::new(
            HistogramOpts::new(
                "lane_actor_handle_duration_seconds",
                "Wall time per handle() call",
            )
            .buckets(vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ]),
            ACTOR_LABEL_NAMES,
        )
        .expect("lane_actor_handle_duration_seconds");
        registry
            .register(Box::new(handle_duration.clone()))
            .expect("register handle_duration");

        let uptime_seconds = gauge_opts(
            "lane_actor_uptime_seconds",
            "Seconds since the actor was registered",
        );
        registry
            .register(Box::new(uptime_seconds.clone()))
            .expect("register uptime_seconds");

        let idle_seconds = gauge_opts(
            "lane_actor_idle_seconds",
            "Seconds since the last completed handle()",
        );
        registry
            .register(Box::new(idle_seconds.clone()))
            .expect("register idle_seconds");

        let alive = gauge_opts("lane_actor_alive", "1 while the actor is running, 0 after exit");
        registry
            .register(Box::new(alive.clone()))
            .expect("register alive");

        Self {
            registry,
            messages_handled,
            handle_errors,
            panics,
            handle_timeouts,
            slow_handles,
            counter_saturated,
            exits,
            in_flight,
            last_handle_seconds,
            max_handle_seconds,
            mailbox_capacity,
            mailbox_depth,
            handle_duration,
            uptime_seconds,
            idle_seconds,
            alive,
        }
    }

    fn bind_actor(&self, meta: &ActorMeta, mailbox_capacity: usize) -> Option<PromActorMetrics> {
        let labels = actor_label_strings(meta);
        let label_refs: [&str; 5] = [
            &labels[0],
            &labels[1],
            &labels[2],
            &labels[3],
            &labels[4],
        ];

        let exit_labels = [
            String::new(),
            labels[0].clone(),
            labels[1].clone(),
            labels[2].clone(),
            labels[3].clone(),
            labels[4].clone(),
        ];

        let prom = PromActorMetrics {
            messages_handled: self
                .messages_handled
                .get_metric_with_label_values(&label_refs)
                .ok()?,
            handle_errors: self
                .handle_errors
                .get_metric_with_label_values(&label_refs)
                .ok()?,
            panics: self.panics.get_metric_with_label_values(&label_refs).ok()?,
            handle_timeouts: self
                .handle_timeouts
                .get_metric_with_label_values(&label_refs)
                .ok()?,
            slow_handles: self
                .slow_handles
                .get_metric_with_label_values(&label_refs)
                .ok()?,
            in_flight: self.in_flight.get_metric_with_label_values(&label_refs).ok()?,
            last_handle_seconds: self
                .last_handle_seconds
                .get_metric_with_label_values(&label_refs)
                .ok()?,
            max_handle_seconds: self
                .max_handle_seconds
                .get_metric_with_label_values(&label_refs)
                .ok()?,
            mailbox_capacity: self
                .mailbox_capacity
                .get_metric_with_label_values(&label_refs)
                .ok()?,
            mailbox_depth: self
                .mailbox_depth
                .get_metric_with_label_values(&label_refs)
                .ok()?,
            handle_duration: self
                .handle_duration
                .get_metric_with_label_values(&label_refs)
                .ok()?,
            uptime_seconds: self
                .uptime_seconds
                .get_metric_with_label_values(&label_refs)
                .ok()?,
            idle_seconds: self
                .idle_seconds
                .get_metric_with_label_values(&label_refs)
                .ok()?,
            alive: self.alive.get_metric_with_label_values(&label_refs).ok()?,
            exits: self.exits.clone(),
            exit_labels,
        };
        prom.mailbox_capacity.set(mailbox_capacity as f64);
        prom.alive.set(1.0);
        Some(prom)
    }

    fn render(&self) -> Result<String, prometheus::Error> {
        let encoder = TextEncoder::new();
        let families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&families, &mut buffer)?;
        Ok(String::from_utf8_lossy(&buffer).into_owned())
    }
}

pub(crate) fn bind_actor_metrics(
    meta: &ActorMeta,
    mailbox_capacity: usize,
) -> Option<PromActorMetrics> {
    METRICS.bind_actor(meta, mailbox_capacity)
}

pub(crate) fn record_counter_saturated(field: &'static str, meta: &ActorMeta) {
    let labels = actor_label_strings(meta);
    if let Ok(counter) = METRICS.counter_saturated.get_metric_with_label_values(&[
        field,
        &labels[0],
        &labels[1],
        &labels[2],
        &labels[3],
        &labels[4],
    ]) {
        counter.inc();
    }
}

pub(crate) fn record_exit(prom: &PromActorMetrics, reason: &ExitReason) {
    let mut labels = prom.exit_labels.clone();
    labels[0] = exit_reason_label(reason).to_string();
    let refs: [&str; 6] = [
        &labels[0],
        &labels[1],
        &labels[2],
        &labels[3],
        &labels[4],
        &labels[5],
    ];
    if let Ok(counter) = prom.exits.get_metric_with_label_values(&refs) {
        counter.inc();
    }
    prom.alive.set(0.0);
}

pub(crate) fn sync_scrape_gauges(
    registered_at: Instant,
    last_handle_unix_ms: u64,
    prom: &PromActorMetrics,
    in_flight: usize,
    last_handle_ms: usize,
    max_handle_ms: usize,
    mailbox_depth: usize,
) {
    prom.uptime_seconds.set(registered_at.elapsed().as_secs_f64());
    prom.in_flight.set(in_flight as f64);
    prom.last_handle_seconds.set(last_handle_ms as f64 / 1000.0);
    prom.max_handle_seconds.set(max_handle_ms as f64 / 1000.0);
    prom.mailbox_depth.set(mailbox_depth as f64);

    if last_handle_unix_ms == 0 {
        prom.idle_seconds.set(0.0);
    } else if let Ok(now_ms) = unix_now_ms() {
        prom.idle_seconds.set((now_ms.saturating_sub(last_handle_unix_ms)) as f64 / 1000.0);
    }
}

pub(crate) fn observe_handle_duration(prom: &PromActorMetrics, elapsed: Duration) {
    prom.handle_duration.observe(elapsed.as_secs_f64());
}

pub fn unix_now_ms() -> Result<u64, std::time::SystemTimeError> {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| {
        u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
    })
}

pub fn actor_label_strings(meta: &ActorMeta) -> [String; 5] {
    [
        meta.name
            .clone()
            .unwrap_or_else(|| "unknown".into()),
        meta.actor_type
            .clone()
            .unwrap_or_else(|| "unknown".into()),
        meta.supervisor_id
            .map(|id| id.0.to_string())
            .unwrap_or_else(|| "unknown".into()),
        meta.node.clone().unwrap_or_else(global_node),
        meta.service.clone().unwrap_or_default(),
    ]
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

/// Refresh scrape-time gauges, then render Prometheus text exposition format.
pub fn render_prometheus_text() -> Result<String, prometheus::Error> {
    ActorMonitor::global().sync_prometheus_gauges();
    METRICS.render()
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn render_prometheus_text_contains_help() {
        let body = render_prometheus_text().expect("render");
        assert!(body.contains("lane_actor_messages_handled_total"));
        assert!(body.contains("# HELP"));
    }
}
