//! Prometheus export for [`crate::monitor::ActorMonitor`] (`metrics` feature).
//!
//! Hot-path counters are pre-bound per actor at register time (no per-message allocations).
//! Gauges for uptime / idle / alive are refreshed when [`render_prometheus_text`] is called.

use crate::actor::ExitReason;
use crate::monitor::{ActorMeta, ActorMonitor};
use crate::supervisor::{IntensityAction, RestartStrategy};
use once_cell::sync::{Lazy, OnceCell};
use prometheus::{
    Encoder, Gauge, Histogram, IntCounter, Opts, Registry, TextEncoder,
    HistogramOpts, HistogramVec, IntCounterVec,
};
use std::collections::HashMap;
use std::sync::{Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Process-wide label defaults applied when [`ActorMeta`] fields are unset.
#[derive(Clone, Default)]
pub struct MetricsConfig {
    pub node: Option<String>,
    pub dc: Option<String>,
    pub environment: Option<String>,
    /// Optional hook invoked after each successful [`render_prometheus_text`] (push sinks, logging).
    pub on_scrape: Option<std::sync::Arc<dyn Fn(&str) + Send + Sync>>,
}

impl std::fmt::Debug for MetricsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsConfig")
            .field("node", &self.node)
            .field("dc", &self.dc)
            .field("environment", &self.environment)
            .field("on_scrape", &self.on_scrape.as_ref().map(|_| "<callback>"))
            .finish()
    }
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
    mailbox_send_rejected: IntCounterVec,
    mailbox_send_blocked: IntCounterVec,
    mailbox_wait: HistogramVec,
    // --- mesh ---
    mesh_dispatches: IntCounterVec,
    // --- remote ---
    remote_send: IntCounterVec,
    remote_ack_timeouts: IntCounterVec,
    supervisor_restarts: IntCounterVec,
    supervisor_intensity_exceeded: IntCounterVec,
    supervisor_children_alive: prometheus::GaugeVec,
    supervisor_intensity_remaining: prometheus::GaugeVec,
    supervisor_child_generation: prometheus::GaugeVec,
    // --- storage (counters synced from snapshots) ---
    storage_puts: IntCounterVec,
    storage_gets: IntCounterVec,
    storage_deletes: IntCounterVec,
    storage_read_repairs: IntCounterVec,
    storage_paxos_writes: IntCounterVec,
    storage_quorum_failures: IntCounterVec,
    storage_wal_bytes: IntCounterVec,
    storage_tombstones: prometheus::GaugeVec,
    storage_live_records: prometheus::GaugeVec,
    // --- consistency ---
    consistency_operations: IntCounterVec,
    consistency_duration: HistogramVec,
    consistency_acks_required: HistogramVec,
    consistency_acks_received: HistogramVec,
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

        let mailbox_send_rejected = IntCounterVec::new(
            Opts::new(
                "lane_actor_mailbox_send_rejected_total",
                "Mailbox sends rejected (full or actor exited)",
            ),
            ACTOR_LABEL_NAMES,
        )
        .expect("lane_actor_mailbox_send_rejected_total");
        registry
            .register(Box::new(mailbox_send_rejected.clone()))
            .expect("register mailbox_send_rejected");

        let mailbox_send_blocked = IntCounterVec::new(
            Opts::new(
                "lane_actor_mailbox_send_blocked_total",
                "Async mailbox sends that waited on a full channel",
            ),
            ACTOR_LABEL_NAMES,
        )
        .expect("lane_actor_mailbox_send_blocked_total");
        registry
            .register(Box::new(mailbox_send_blocked.clone()))
            .expect("register mailbox_send_blocked");

        let mailbox_wait = HistogramVec::new(
            HistogramOpts::new(
                "lane_actor_mailbox_wait_seconds",
                "Time from enqueue to begin_handle for actor messages",
            )
            .buckets(vec![
                0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0,
            ]),
            ACTOR_LABEL_NAMES,
        )
        .expect("lane_actor_mailbox_wait_seconds");
        registry
            .register(Box::new(mailbox_wait.clone()))
            .expect("register mailbox_wait");

        let mesh_dispatches = IntCounterVec::new(
            Opts::new(
                "lane_mesh_dispatches_total",
                "Mesh invoke_consistent / read_consistent dispatches",
            ),
            &["service", "node"],
        )
        .expect("lane_mesh_dispatches_total");
        registry
            .register(Box::new(mesh_dispatches.clone()))
            .expect("register mesh_dispatches");

        let remote_send = IntCounterVec::new(
            Opts::new(
                "lane_remote_send_total",
                "Remote actor frame dispatches over gRPC",
            ),
            &["target", "result", "node"],
        )
        .expect("lane_remote_send_total");
        registry
            .register(Box::new(remote_send.clone()))
            .expect("register remote_send");

        let remote_ack_timeouts = IntCounterVec::new(
            Opts::new(
                "lane_remote_ack_timeouts_total",
                "Remote send_with_ack timeouts",
            ),
            &["target", "node"],
        )
        .expect("lane_remote_ack_timeouts_total");
        registry
            .register(Box::new(remote_ack_timeouts.clone()))
            .expect("register remote_ack_timeouts");

        let node_label = &["node"];
        let supervisor_restarts = IntCounterVec::new(
            Opts::new(
                "lane_supervisor_restarts_total",
                "Supervised child restarts",
            ),
            &["child", "strategy", "node"],
        )
        .expect("lane_supervisor_restarts_total");
        registry
            .register(Box::new(supervisor_restarts.clone()))
            .expect("register supervisor_restarts");

        let supervisor_intensity_exceeded = IntCounterVec::new(
            Opts::new(
                "lane_supervisor_intensity_exceeded_total",
                "Restart intensity limit breached",
            ),
            &["action", "node"],
        )
        .expect("lane_supervisor_intensity_exceeded_total");
        registry
            .register(Box::new(supervisor_intensity_exceeded.clone()))
            .expect("register supervisor_intensity_exceeded");

        let supervisor_children_alive = prometheus::GaugeVec::new(
            Opts::new(
                "lane_supervisor_children_alive",
                "Currently live children under the supervisor",
            ),
            node_label,
        )
        .expect("lane_supervisor_children_alive");
        registry
            .register(Box::new(supervisor_children_alive.clone()))
            .expect("register supervisor_children_alive");

        let supervisor_intensity_remaining = prometheus::GaugeVec::new(
            Opts::new(
                "lane_supervisor_restart_intensity_remaining",
                "Restart budget remaining in the current window",
            ),
            node_label,
        )
        .expect("lane_supervisor_restart_intensity_remaining");
        registry
            .register(Box::new(supervisor_intensity_remaining.clone()))
            .expect("register supervisor_intensity_remaining");

        let supervisor_child_generation = prometheus::GaugeVec::new(
            Opts::new(
                "lane_supervisor_child_generation",
                "Child restart generation from ChildRegistry",
            ),
            &["child", "node"],
        )
        .expect("lane_supervisor_child_generation");
        registry
            .register(Box::new(supervisor_child_generation.clone()))
            .expect("register supervisor_child_generation");

        let storage_node_label = &["node"];
        let storage_puts = IntCounterVec::new(
            Opts::new("lane_storage_puts_total", "Storage put operations"),
            storage_node_label,
        )
        .expect("lane_storage_puts_total");
        registry
            .register(Box::new(storage_puts.clone()))
            .expect("register storage_puts");

        let storage_gets = IntCounterVec::new(
            Opts::new("lane_storage_gets_total", "Storage get operations"),
            storage_node_label,
        )
        .expect("lane_storage_gets_total");
        registry
            .register(Box::new(storage_gets.clone()))
            .expect("register storage_gets");

        let storage_deletes = IntCounterVec::new(
            Opts::new("lane_storage_deletes_total", "Storage delete operations"),
            storage_node_label,
        )
        .expect("lane_storage_deletes_total");
        registry
            .register(Box::new(storage_deletes.clone()))
            .expect("register storage_deletes");

        let storage_read_repairs = IntCounterVec::new(
            Opts::new("lane_storage_read_repairs_total", "Read repair operations"),
            storage_node_label,
        )
        .expect("lane_storage_read_repairs_total");
        registry
            .register(Box::new(storage_read_repairs.clone()))
            .expect("register storage_read_repairs");

        let storage_paxos_writes = IntCounterVec::new(
            Opts::new("lane_storage_paxos_writes_total", "Paxos write rounds"),
            storage_node_label,
        )
        .expect("lane_storage_paxos_writes_total");
        registry
            .register(Box::new(storage_paxos_writes.clone()))
            .expect("register storage_paxos_writes");

        let storage_quorum_failures = IntCounterVec::new(
            Opts::new(
                "lane_storage_quorum_failures_total",
                "Quorum failures on storage operations",
            ),
            storage_node_label,
        )
        .expect("lane_storage_quorum_failures_total");
        registry
            .register(Box::new(storage_quorum_failures.clone()))
            .expect("register storage_quorum_failures");

        let storage_wal_bytes = IntCounterVec::new(
            Opts::new(
                "lane_storage_wal_bytes_written_total",
                "WAL bytes appended",
            ),
            storage_node_label,
        )
        .expect("lane_storage_wal_bytes_written_total");
        registry
            .register(Box::new(storage_wal_bytes.clone()))
            .expect("register storage_wal_bytes");

        let storage_tombstones = prometheus::GaugeVec::new(
            Opts::new("lane_storage_tombstone_count", "Live tombstone records"),
            storage_node_label,
        )
        .expect("lane_storage_tombstone_count");
        registry
            .register(Box::new(storage_tombstones.clone()))
            .expect("register storage_tombstones");

        let storage_live_records = prometheus::GaugeVec::new(
            Opts::new("lane_storage_live_records", "Live records in the MemTable"),
            storage_node_label,
        )
        .expect("lane_storage_live_records");
        registry
            .register(Box::new(storage_live_records.clone()))
            .expect("register storage_live_records");

        let consistency_operations = IntCounterVec::new(
            Opts::new(
                "lane_consistency_operations_total",
                "Mesh consistency operations",
            ),
            &["service", "level", "result", "node"],
        )
        .expect("lane_consistency_operations_total");
        registry
            .register(Box::new(consistency_operations.clone()))
            .expect("register consistency_operations");

        let consistency_duration = HistogramVec::new(
            HistogramOpts::new(
                "lane_consistency_duration_seconds",
                "Mesh consistency operation wall time",
            )
            .buckets(vec![0.001, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]),
            &["service", "level", "node"],
        )
        .expect("lane_consistency_duration_seconds");
        registry
            .register(Box::new(consistency_duration.clone()))
            .expect("register consistency_duration");

        let consistency_acks_required = HistogramVec::new(
            HistogramOpts::new(
                "lane_consistency_acks_required",
                "Acknowledgements required per consistency operation",
            )
            .buckets(vec![1.0, 2.0, 3.0, 5.0, 7.0, 9.0, 15.0, 31.0]),
            &["service", "level", "node"],
        )
        .expect("lane_consistency_acks_required");
        registry
            .register(Box::new(consistency_acks_required.clone()))
            .expect("register consistency_acks_required");

        let consistency_acks_received = HistogramVec::new(
            HistogramOpts::new(
                "lane_consistency_acks_received",
                "Acknowledgements received per consistency operation",
            )
            .buckets(vec![0.0, 1.0, 2.0, 3.0, 5.0, 7.0, 9.0, 15.0, 31.0]),
            &["service", "level", "node"],
        )
        .expect("lane_consistency_acks_received");
        registry
            .register(Box::new(consistency_acks_received.clone()))
            .expect("register consistency_acks_received");

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
            mailbox_send_rejected,
            mailbox_send_blocked,
            mailbox_wait,
            mesh_dispatches,
            remote_send,
            remote_ack_timeouts,
            supervisor_restarts,
            supervisor_intensity_exceeded,
            supervisor_children_alive,
            supervisor_intensity_remaining,
            supervisor_child_generation,
            storage_puts,
            storage_gets,
            storage_deletes,
            storage_read_repairs,
            storage_paxos_writes,
            storage_quorum_failures,
            storage_wal_bytes,
            storage_tombstones,
            storage_live_records,
            consistency_operations,
            consistency_duration,
            consistency_acks_required,
            consistency_acks_received,
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

/// Map [`RestartStrategy`] to a bounded Prometheus label.
pub fn restart_strategy_label(strategy: RestartStrategy) -> &'static str {
    match strategy {
        RestartStrategy::OneForOne => "one_for_one",
        RestartStrategy::OneForAll => "one_for_all",
        RestartStrategy::RestForOne => "rest_for_one",
    }
}

fn intensity_action_label(action: IntensityAction) -> &'static str {
    match action {
        IntensityAction::ShutdownSupervisor => "shutdown_supervisor",
        IntensityAction::AbandonChild => "abandon_child",
    }
}

#[derive(Default, Clone)]
struct SupervisorScrapeState {
    restarts_in_window: usize,
    max_restarts: usize,
    children_alive: usize,
}

static SUPERVISOR_SCRAPE: Lazy<RwLock<SupervisorScrapeState>> =
    Lazy::new(|| RwLock::new(SupervisorScrapeState::default()));

#[derive(Debug, Clone, Default)]
pub struct StorageMetricsSnapshot {
    pub node: String,
    pub puts_total: u64,
    pub gets_total: u64,
    pub deletes_total: u64,
    pub read_repairs: u64,
    pub paxos_writes: u64,
    pub quorum_failures: u64,
    pub wal_bytes_written: u64,
    pub tombstone_count: u64,
    pub live_records: u64,
}

static STORAGE_LAST: Lazy<Mutex<HashMap<String, StorageMetricsSnapshot>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

/// Snapshot of a mesh consistency operation for Prometheus export.
#[derive(Debug, Clone)]
pub struct ConsistencyOpSnapshot {
    pub service: String,
    pub consistency_level: String,
    pub succeeded: bool,
    pub duration_ms: u64,
    pub acks_required: usize,
    pub acks_received: usize,
}

/// Record a supervised child restart.
pub fn record_supervisor_restart(child: &str, strategy: RestartStrategy) {
    let node = global_node();
    let strategy = restart_strategy_label(strategy);
    if let Ok(counter) = METRICS
        .supervisor_restarts
        .get_metric_with_label_values(&[child, strategy, &node])
    {
        counter.inc();
    }
}

/// Record restart-intensity limit breach.
pub fn record_intensity_exceeded(action: IntensityAction) {
    let node = global_node();
    let action = intensity_action_label(action);
    if let Ok(counter) = METRICS
        .supervisor_intensity_exceeded
        .get_metric_with_label_values(&[action, &node])
    {
        counter.inc();
    }
}

/// Update supervisor scrape gauges (call from the supervisor loop).
pub fn sync_supervisor_scrape(
    restarts_in_window: usize,
    max_restarts: usize,
    children_alive: usize,
) {
    if let Ok(mut state) = SUPERVISOR_SCRAPE.write() {
        state.restarts_in_window = restarts_in_window;
        state.max_restarts = max_restarts;
        state.children_alive = children_alive;
    }
}

/// Set [`ChildRegistry`] generation gauge for a named child.
pub fn set_child_generation(child: &str, generation: u64) {
    let node = global_node();
    if let Ok(gauge) = METRICS
        .supervisor_child_generation
        .get_metric_with_label_values(&[child, &node])
    {
        gauge.set(generation as f64);
    }
}

/// Diff storage cumulative counters into Prometheus counters on each scrape/sync.
pub fn sync_storage_stats(snapshot: &StorageMetricsSnapshot) {
    let mut last_map = STORAGE_LAST.lock().unwrap_or_else(|e| e.into_inner());
    let prev = last_map
        .entry(snapshot.node.clone())
        .or_insert_with(|| snapshot.clone());
    let node = &snapshot.node;

    inc_storage_delta(
        &METRICS.storage_puts,
        node,
        snapshot.puts_total,
        &mut prev.puts_total,
    );
    inc_storage_delta(
        &METRICS.storage_gets,
        node,
        snapshot.gets_total,
        &mut prev.gets_total,
    );
    inc_storage_delta(
        &METRICS.storage_deletes,
        node,
        snapshot.deletes_total,
        &mut prev.deletes_total,
    );
    inc_storage_delta(
        &METRICS.storage_read_repairs,
        node,
        snapshot.read_repairs,
        &mut prev.read_repairs,
    );
    inc_storage_delta(
        &METRICS.storage_paxos_writes,
        node,
        snapshot.paxos_writes,
        &mut prev.paxos_writes,
    );
    inc_storage_delta(
        &METRICS.storage_quorum_failures,
        node,
        snapshot.quorum_failures,
        &mut prev.quorum_failures,
    );
    inc_storage_delta(
        &METRICS.storage_wal_bytes,
        node,
        snapshot.wal_bytes_written,
        &mut prev.wal_bytes_written,
    );

    if let Ok(gauge) = METRICS
        .storage_tombstones
        .get_metric_with_label_values(&[node.as_str()])
    {
        gauge.set(snapshot.tombstone_count as f64);
    }
    if let Ok(gauge) = METRICS
        .storage_live_records
        .get_metric_with_label_values(&[node.as_str()])
    {
        gauge.set(snapshot.live_records as f64);
    }
}

fn inc_storage_delta(
    vec: &IntCounterVec,
    node: &str,
    current: u64,
    prev: &mut u64,
) {
    if current > *prev {
        if let Ok(counter) = vec.get_metric_with_label_values(&[node]) {
            counter.inc_by(current - *prev);
        }
        *prev = current;
    }
}

/// Record a mesh consistency operation.
pub fn record_consistency_operation(op: &ConsistencyOpSnapshot) {
    let node = global_node();
    let result = if op.succeeded { "ok" } else { "err" };
    if let Ok(counter) = METRICS.consistency_operations.get_metric_with_label_values(&[
        &op.service,
        &op.consistency_level,
        result,
        &node,
    ]) {
        counter.inc();
    }
    if let Ok(hist) = METRICS.consistency_duration.get_metric_with_label_values(&[
        &op.service,
        &op.consistency_level,
        &node,
    ]) {
        hist.observe(op.duration_ms as f64 / 1000.0);
    }
    if let Ok(hist) = METRICS.consistency_acks_required.get_metric_with_label_values(&[
        &op.service,
        &op.consistency_level,
        &node,
    ]) {
        hist.observe(op.acks_required as f64);
    }
    if let Ok(hist) = METRICS.consistency_acks_received.get_metric_with_label_values(&[
        &op.service,
        &op.consistency_level,
        &node,
    ]) {
        hist.observe(op.acks_received as f64);
    }
}

fn sync_supervisor_prometheus_gauges() {
    let state = SUPERVISOR_SCRAPE
        .read()
        .unwrap_or_else(|e| e.into_inner());
    let node = global_node();
    if let Ok(gauge) = METRICS
        .supervisor_children_alive
        .get_metric_with_label_values(&[&node])
    {
        gauge.set(state.children_alive as f64);
    }
    if let Ok(gauge) = METRICS
        .supervisor_intensity_remaining
        .get_metric_with_label_values(&[&node])
    {
        gauge.set(
            state
                .max_restarts
                .saturating_sub(state.restarts_in_window) as f64,
        );
    }
}

/// Record a mesh invoke/read dispatch (call at the start of `invoke_consistent` / `read_consistent`).
pub fn record_mesh_dispatch(service: &str) {
    let node = global_node();
    if let Ok(counter) = METRICS
        .mesh_dispatches
        .get_metric_with_label_values(&[service, &node])
    {
        counter.inc();
    }
}

/// Record a remote actor send attempt.
pub fn record_remote_send(target: &str, ok: bool) {
    let node = global_node();
    let result = if ok { "ok" } else { "err" };
    if let Ok(counter) = METRICS
        .remote_send
        .get_metric_with_label_values(&[target, result, &node])
    {
        counter.inc();
    }
}

/// Record a `send_with_ack` timeout to a remote actor.
pub fn record_remote_ack_timeout(target: &str) {
    let node = global_node();
    if let Ok(counter) = METRICS
        .remote_ack_timeouts
        .get_metric_with_label_values(&[target, &node])
    {
        counter.inc();
    }
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

/// Refresh scrape-time gauges, then render Prometheus text exposition format.
pub fn render_prometheus_text() -> Result<String, prometheus::Error> {
    ActorMonitor::global().sync_prometheus_gauges();
    sync_supervisor_prometheus_gauges();
    let text = METRICS.render()?;
    if let Some(cb) = GLOBAL_CONFIG.get().and_then(|c| c.on_scrape.clone()) {
        cb(&text);
    }
    Ok(text)
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
    fn render_prometheus_text_contains_supervisor_series() {
        record_supervisor_restart("worker", RestartStrategy::OneForOne);
        let body = render_prometheus_text().expect("render");
        assert!(body.contains("lane_supervisor_restarts_total"));
    }

    #[test]
    fn render_prometheus_text_contains_mesh_dispatch_series() {
        record_mesh_dispatch("orders");
        let body = render_prometheus_text().expect("render");
        assert!(body.contains("lane_mesh_dispatches_total"));
    }
}
