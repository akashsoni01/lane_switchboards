//! Messenger gateway Prometheus metrics (`metrics` feature).

use std::sync::Once;
use std::time::Instant;

use lane_core::metrics::{metric_prefix, shared_registry};
use once_cell::sync::Lazy;
use prometheus::{
    Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec,
    Registry,
};

static INIT: Once = Once::new();

struct MessengerMetrics {
    sessions: IntGaugeVec,
    messages_in: IntCounterVec,
    messages_out: IntCounterVec,
    ack_latency: HistogramVec,
    inbox_depth: IntGaugeVec,
    fanout_duration: Histogram,
    dropped_frames: IntCounter,
    peer_links: IntGauge,
}

static METRICS: Lazy<MessengerMetrics> = Lazy::new(|| {
    let registry = shared_registry();
    MessengerMetrics::register(registry)
});

impl MessengerMetrics {
    fn register(registry: &Registry) -> Self {
        let prefix = metric_prefix();
        let sessions = IntGaugeVec::new(
            prometheus::Opts::new(
                format!("{prefix}_messenger_sessions_connected"),
                "Live client sessions per gateway node",
            ),
            &["node"],
        )
        .expect("messenger sessions gauge");
        registry.register(Box::new(sessions.clone())).expect("register sessions");

        let messages_in = IntCounterVec::new(
            prometheus::Opts::new(
                format!("{prefix}_messenger_messages_in_total"),
                "Frames received from clients and peer links",
            ),
            &["node", "packet_type"],
        )
        .expect("messenger messages_in");
        registry.register(Box::new(messages_in.clone())).expect("register messages_in");

        let messages_out = IntCounterVec::new(
            prometheus::Opts::new(
                format!("{prefix}_messenger_messages_out_total"),
                "Frames sent to clients and peer links",
            ),
            &["node", "packet_type"],
        )
        .expect("messenger messages_out");
        registry.register(Box::new(messages_out.clone())).expect("register messages_out");

        let ack_latency = HistogramVec::new(
            HistogramOpts::new(
                format!("{prefix}_messenger_ack_latency_seconds"),
                "Time from ChatMessage receipt to ServerAck on the home shard",
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]),
            &["node"],
        )
        .expect("messenger ack_latency");
        registry.register(Box::new(ack_latency.clone())).expect("register ack_latency");

        let inbox_depth = IntGaugeVec::new(
            prometheus::Opts::new(
                format!("{prefix}_messenger_inbox_pending"),
                "Undelivered offline messages per user inbox on this node",
            ),
            &["node", "user_id"],
        )
        .expect("messenger inbox_depth");
        registry.register(Box::new(inbox_depth.clone())).expect("register inbox_depth");

        let fanout_duration = Histogram::with_opts(
            HistogramOpts::new(
                format!("{prefix}_messenger_fanout_duration_seconds"),
                "Group fan-out duration on the group home shard",
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0]),
        )
        .expect("messenger fanout_duration");
        registry.register(Box::new(fanout_duration.clone())).expect("register fanout");

        let dropped_frames = IntCounter::with_opts(prometheus::Opts::new(
            format!("{prefix}_messenger_dropped_frames_total"),
            "Frames dropped due to peer-link backpressure",
        ))
        .expect("messenger dropped_frames");
        registry.register(Box::new(dropped_frames.clone())).expect("register dropped");

        let peer_links = IntGauge::with_opts(prometheus::Opts::new(
            format!("{prefix}_messenger_peer_links_connected"),
            "Established outbound peer links from this gateway",
        ))
        .expect("messenger peer_links");
        registry.register(Box::new(peer_links.clone())).expect("register peer_links");

        Self {
            sessions,
            messages_in,
            messages_out,
            ack_latency,
            inbox_depth,
            fanout_duration,
            dropped_frames,
            peer_links,
        }
    }
}

fn ensure_init() {
    INIT.call_once(|| {
        let _ = &*METRICS;
    });
}

pub fn init() {
    ensure_init();
}

pub fn set_sessions(node: &str, count: i64) {
    ensure_init();
    METRICS.sessions.with_label_values(&[node]).set(count);
}

pub fn set_peer_links(count: i64) {
    ensure_init();
    METRICS.peer_links.set(count);
}

pub fn record_message_in(node: &str, packet_type: &str) {
    ensure_init();
    METRICS
        .messages_in
        .with_label_values(&[node, packet_type])
        .inc();
}

pub fn record_message_out(node: &str, packet_type: &str) {
    ensure_init();
    METRICS
        .messages_out
        .with_label_values(&[node, packet_type])
        .inc();
}

pub fn observe_ack_latency(node: &str, elapsed: std::time::Duration) {
    ensure_init();
    METRICS
        .ack_latency
        .with_label_values(&[node])
        .observe(elapsed.as_secs_f64());
}

pub fn set_inbox_depth(node: &str, user_id: &str, depth: i64) {
    ensure_init();
    METRICS
        .inbox_depth
        .with_label_values(&[node, user_id])
        .set(depth);
}

pub fn observe_fanout(elapsed: std::time::Duration) {
    ensure_init();
    METRICS.fanout_duration.observe(elapsed.as_secs_f64());
}

pub fn record_dropped_frame() {
    ensure_init();
    METRICS.dropped_frames.inc();
}

/// Hash user ids for metrics labels (privacy on shared scrapes).
pub fn hash_user_id(user_id: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    user_id.hash(&mut h);
    format!("u{:016x}", h.finish())
}

/// Timer helper for ack-latency histograms.
pub struct AckTimer {
    node: String,
    started: Instant,
}

impl AckTimer {
    pub fn start(node: impl Into<String>) -> Self {
        ensure_init();
        Self {
            node: node.into(),
            started: Instant::now(),
        }
    }

    pub fn observe(self) {
        observe_ack_latency(&self.node, self.started.elapsed());
    }
}
