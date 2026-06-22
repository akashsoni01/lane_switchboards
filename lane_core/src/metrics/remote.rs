//! Remote gRPC actor dispatch metrics (lazy-registered).

use prometheus::{IntCounterVec, Registry};

use super::common::{metrics_try, register_counter_vec, global_node};

pub(crate) struct RemoteMetricsRegistry {
    send: IntCounterVec,
    ack_timeouts: IntCounterVec,
}

impl RemoteMetricsRegistry {
    pub(crate) fn register(registry: &Registry) -> Self {
        Self {
            send: register_counter_vec(
                registry,
                "lane_remote_send_total",
                "Remote actor frame dispatches over gRPC",
                &["target", "result", "node"],
            ),
            ack_timeouts: register_counter_vec(
                registry,
                "lane_remote_ack_timeouts_total",
                "Remote send_with_ack timeouts",
                &["target", "node"],
            ),
        }
    }

    pub(crate) fn record_send(&self, target: &str, ok: bool) {
        metrics_try("record_remote_send", || {
            let node = global_node();
            let result = if ok { "ok" } else { "err" };
            if let Ok(counter) = self
                .send
                .get_metric_with_label_values(&[target, result, &node])
            {
                counter.inc();
            }
        });
    }

    pub(crate) fn record_ack_timeout(&self, target: &str) {
        metrics_try("record_remote_ack_timeout", || {
            let node = global_node();
            if let Ok(counter) = self
                .ack_timeouts
                .get_metric_with_label_values(&[target, &node])
            {
                counter.inc();
            }
        });
    }
}
