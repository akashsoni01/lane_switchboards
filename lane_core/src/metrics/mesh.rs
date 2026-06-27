//! Mesh dispatch and consistency operation metrics (lazy-registered).

use prometheus::{IntCounterVec, Registry};

use super::common::{
    metrics_try, metric_name, register_counter_vec, register_histogram_vec, global_node,
};

pub(crate) struct MeshMetricsRegistry {
    dispatches: IntCounterVec,
    consistency_operations: IntCounterVec,
    consistency_duration: prometheus::HistogramVec,
    consistency_acks_required: prometheus::HistogramVec,
    consistency_acks_received: prometheus::HistogramVec,
}

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

impl MeshMetricsRegistry {
    pub(crate) fn register(registry: &Registry) -> Self {
        Self {
            dispatches: register_counter_vec(
                registry,
                &metric_name("mesh_dispatches_total"),
                "Mesh invoke_consistent / read_consistent dispatches",
                &["service", "node"],
            ),
            consistency_operations: register_counter_vec(
                registry,
                &metric_name("consistency_operations_total"),
                "Mesh consistency operations",
                &["service", "level", "result", "node"],
            ),
            consistency_duration: register_histogram_vec(
                registry,
                &metric_name("consistency_duration_seconds"),
                "Mesh consistency operation wall time",
                vec![0.001, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
                &["service", "level", "node"],
            ),
            consistency_acks_required: register_histogram_vec(
                registry,
                &metric_name("consistency_acks_required"),
                "Acknowledgements required per consistency operation",
                vec![1.0, 2.0, 3.0, 5.0, 7.0, 9.0, 15.0, 31.0],
                &["service", "level", "node"],
            ),
            consistency_acks_received: register_histogram_vec(
                registry,
                &metric_name("consistency_acks_received"),
                "Acknowledgements received per consistency operation",
                vec![0.0, 1.0, 2.0, 3.0, 5.0, 7.0, 9.0, 15.0, 31.0],
                &["service", "level", "node"],
            ),
        }
    }

    pub(crate) fn record_dispatch(&self, service: &str) {
        metrics_try("record_mesh_dispatch", || {
            let node = global_node();
            if let Ok(counter) = self
                .dispatches
                .get_metric_with_label_values(&[service, &node])
            {
                counter.inc();
            }
        });
    }

    pub(crate) fn record_consistency_operation(&self, op: &ConsistencyOpSnapshot) {
        metrics_try("record_consistency_operation", || {
            let node = global_node();
            let result = if op.succeeded { "ok" } else { "err" };
            if let Ok(counter) = self.consistency_operations.get_metric_with_label_values(&[
                &op.service,
                &op.consistency_level,
                result,
                &node,
            ]) {
                counter.inc();
            }
            if let Ok(hist) = self.consistency_duration.get_metric_with_label_values(&[
                &op.service,
                &op.consistency_level,
                &node,
            ]) {
                hist.observe(op.duration_ms as f64 / 1000.0);
            }
            if let Ok(hist) = self.consistency_acks_required.get_metric_with_label_values(&[
                &op.service,
                &op.consistency_level,
                &node,
            ]) {
                hist.observe(op.acks_required as f64);
            }
            if let Ok(hist) = self.consistency_acks_received.get_metric_with_label_values(&[
                &op.service,
                &op.consistency_level,
                &node,
            ]) {
                hist.observe(op.acks_received as f64);
            }
        });
    }
}
