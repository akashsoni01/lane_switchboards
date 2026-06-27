//! Actor runtime metrics — always registered (primary hot path).

use crate::actor::ExitReason;
use crate::monitor::ActorMeta;
use prometheus::{Gauge, Histogram, IntCounter, IntCounterVec, Registry};
use std::time::{Duration, Instant};

use super::common::{
    metrics_try, metric_name, register_counter_vec, register_gauge_vec, register_histogram_vec,
    ACTOR_LABEL_NAMES, EXIT_LABEL_NAMES, global_node,
};

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
    pub mailbox_send_rejected: IntCounter,
    pub mailbox_send_blocked: IntCounter,
    pub mailbox_wait: Histogram,
    pub handle_duration: Histogram,
    pub uptime_seconds: Gauge,
    pub idle_seconds: Gauge,
    pub alive: Gauge,
    exits: IntCounterVec,
    exit_labels: [String; 6],
}

pub(crate) struct ActorMetricsRegistry {
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
    handle_duration: prometheus::HistogramVec,
    uptime_seconds: prometheus::GaugeVec,
    idle_seconds: prometheus::GaugeVec,
    alive: prometheus::GaugeVec,
    mailbox_send_rejected: IntCounterVec,
    mailbox_send_blocked: IntCounterVec,
    mailbox_wait: prometheus::HistogramVec,
}

impl ActorMetricsRegistry {
    pub(crate) fn register(registry: &Registry) -> Self {
        let messages_handled = register_counter_vec(
            registry,
            &metric_name("actor_messages_handled_total"),
            "Successful actor handle() completions",
            ACTOR_LABEL_NAMES,
        );
        let handle_errors = register_counter_vec(
            registry,
            &metric_name("actor_handle_errors_total"),
            "handle() returned Err",
            ACTOR_LABEL_NAMES,
        );
        let panics = register_counter_vec(
            registry,
            &metric_name("actor_panics_total"),
            "handle() panicked",
            ACTOR_LABEL_NAMES,
        );
        let handle_timeouts = register_counter_vec(
            registry,
            &metric_name("actor_handle_timeouts_total"),
            "handle() exceeded handle_timeout",
            ACTOR_LABEL_NAMES,
        );
        let slow_handles = register_counter_vec(
            registry,
            &metric_name("actor_slow_handles_total"),
            "handle() finished but exceeded slow_handle_threshold",
            ACTOR_LABEL_NAMES,
        );
        let counter_saturated = register_counter_vec(
            registry,
            &metric_name("actor_counter_saturated_total"),
            "Internal stat counter clamped at usize::MAX",
            &[
                "field",
                "actor_name",
                "actor_type",
                "supervisor",
                "node",
                "service",
            ],
        );
        let exits = register_counter_vec(
            registry,
            &metric_name("actor_exits_total"),
            "Actor exits by reason",
            EXIT_LABEL_NAMES,
        );
        let in_flight = register_gauge_vec(
            registry,
            &metric_name("actor_in_flight"),
            "Handles started but not finished",
            ACTOR_LABEL_NAMES,
        );
        let last_handle_seconds = register_gauge_vec(
            registry,
            &metric_name("actor_last_handle_seconds"),
            "Wall time of the most recent handle()",
            ACTOR_LABEL_NAMES,
        );
        let max_handle_seconds = register_gauge_vec(
            registry,
            &metric_name("actor_max_handle_seconds"),
            "Longest handle() wall time recorded",
            ACTOR_LABEL_NAMES,
        );
        let mailbox_capacity = register_gauge_vec(
            registry,
            &metric_name("actor_mailbox_capacity"),
            "Configured actor mailbox capacity",
            ACTOR_LABEL_NAMES,
        );
        let mailbox_depth = register_gauge_vec(
            registry,
            &metric_name("actor_mailbox_depth"),
            "Approximate queued messages in the actor mailbox",
            ACTOR_LABEL_NAMES,
        );
        let handle_duration = register_histogram_vec(
            registry,
            &metric_name("actor_handle_duration_seconds"),
            "Wall time per handle() call",
            vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ],
            ACTOR_LABEL_NAMES,
        );
        let uptime_seconds = register_gauge_vec(
            registry,
            &metric_name("actor_uptime_seconds"),
            "Seconds since the actor was registered",
            ACTOR_LABEL_NAMES,
        );
        let idle_seconds = register_gauge_vec(
            registry,
            &metric_name("actor_idle_seconds"),
            "Seconds since the last completed handle()",
            ACTOR_LABEL_NAMES,
        );
        let alive = register_gauge_vec(
            registry,
            &metric_name("actor_alive"),
            "1 while the actor is running, 0 after exit",
            ACTOR_LABEL_NAMES,
        );
        let mailbox_send_rejected = register_counter_vec(
            registry,
            &metric_name("actor_mailbox_send_rejected_total"),
            "Mailbox sends rejected (full or actor exited)",
            ACTOR_LABEL_NAMES,
        );
        let mailbox_send_blocked = register_counter_vec(
            registry,
            &metric_name("actor_mailbox_send_blocked_total"),
            "Async mailbox sends that waited on a full channel",
            ACTOR_LABEL_NAMES,
        );
        let mailbox_wait = register_histogram_vec(
            registry,
            &metric_name("actor_mailbox_wait_seconds"),
            "Time from enqueue to begin_handle for actor messages",
            vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
            ACTOR_LABEL_NAMES,
        );

        Self {
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
            mailbox_send_rejected,
            mailbox_send_blocked,
            mailbox_wait,
        }
    }

    pub(crate) fn bind_actor(
        &self,
        meta: &ActorMeta,
        mailbox_capacity: usize,
    ) -> Option<PromActorMetrics> {
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
            mailbox_send_rejected: self
                .mailbox_send_rejected
                .get_metric_with_label_values(&label_refs)
                .ok()?,
            mailbox_send_blocked: self
                .mailbox_send_blocked
                .get_metric_with_label_values(&label_refs)
                .ok()?,
            mailbox_wait: self
                .mailbox_wait
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

    pub(crate) fn record_counter_saturated(&self, field: &'static str, meta: &ActorMeta) {
        metrics_try("record_counter_saturated", || {
            let labels = actor_label_strings(meta);
            if let Ok(counter) = self.counter_saturated.get_metric_with_label_values(&[
                field,
                &labels[0],
                &labels[1],
                &labels[2],
                &labels[3],
                &labels[4],
            ]) {
                counter.inc();
            }
        });
    }
}

pub(crate) fn bind_actor_metrics(
    registry: &ActorMetricsRegistry,
    meta: &ActorMeta,
    mailbox_capacity: usize,
) -> Option<PromActorMetrics> {
    registry.bind_actor(meta, mailbox_capacity)
}

pub(crate) fn record_counter_saturated(
    registry: &ActorMetricsRegistry,
    field: &'static str,
    meta: &ActorMeta,
) {
    registry.record_counter_saturated(field, meta);
}

pub(crate) fn record_exit(prom: &PromActorMetrics, reason: &ExitReason) {
    metrics_try("record_exit", || {
        let mut labels = prom.exit_labels.clone();
        labels[0] = super::exit_reason_label(reason).to_string();
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
    });
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
    metrics_try("sync_scrape_gauges", || {
        prom.uptime_seconds.set(registered_at.elapsed().as_secs_f64());
        prom.in_flight.set(in_flight as f64);
        prom.last_handle_seconds.set(last_handle_ms as f64 / 1000.0);
        prom.max_handle_seconds.set(max_handle_ms as f64 / 1000.0);
        prom.mailbox_depth.set(mailbox_depth as f64);

        if last_handle_unix_ms == 0 {
            prom.idle_seconds.set(0.0);
        } else if let Ok(now_ms) = super::unix_now_ms() {
            prom.idle_seconds
                .set((now_ms.saturating_sub(last_handle_unix_ms)) as f64 / 1000.0);
        }
    });
}

pub(crate) fn observe_handle_duration(prom: &PromActorMetrics, elapsed: Duration) {
    metrics_try("observe_handle_duration", || {
        prom.handle_duration.observe(elapsed.as_secs_f64());
    });
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
