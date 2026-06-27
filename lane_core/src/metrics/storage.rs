//! Distributed storage metrics (lazy-registered).

use once_cell::sync::Lazy;
use prometheus::{IntCounterVec, Registry};
use std::collections::HashMap;
use std::sync::Mutex;

use super::common::{metrics_try, metric_name, register_counter_vec, register_gauge_vec};

pub(crate) struct StorageMetricsRegistry {
    puts: IntCounterVec,
    gets: IntCounterVec,
    deletes: IntCounterVec,
    read_repairs: IntCounterVec,
    paxos_writes: IntCounterVec,
    quorum_failures: IntCounterVec,
    wal_bytes: IntCounterVec,
    tombstones: prometheus::GaugeVec,
    live_records: prometheus::GaugeVec,
}

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

impl StorageMetricsRegistry {
    pub(crate) fn register(registry: &Registry) -> Self {
        let node_label = &["node"];
        Self {
            puts: register_counter_vec(
                registry,
                &metric_name("storage_puts_total"),
                "Storage put operations",
                node_label,
            ),
            gets: register_counter_vec(
                registry,
                &metric_name("storage_gets_total"),
                "Storage get operations",
                node_label,
            ),
            deletes: register_counter_vec(
                registry,
                &metric_name("storage_deletes_total"),
                "Storage delete operations",
                node_label,
            ),
            read_repairs: register_counter_vec(
                registry,
                &metric_name("storage_read_repairs_total"),
                "Read repair operations",
                node_label,
            ),
            paxos_writes: register_counter_vec(
                registry,
                &metric_name("storage_paxos_writes_total"),
                "Paxos write rounds",
                node_label,
            ),
            quorum_failures: register_counter_vec(
                registry,
                &metric_name("storage_quorum_failures_total"),
                "Quorum failures on storage operations",
                node_label,
            ),
            wal_bytes: register_counter_vec(
                registry,
                &metric_name("storage_wal_bytes_written_total"),
                "WAL bytes appended",
                node_label,
            ),
            tombstones: register_gauge_vec(
                registry,
                &metric_name("storage_tombstone_count"),
                "Live tombstone records",
                node_label,
            ),
            live_records: register_gauge_vec(
                registry,
                &metric_name("storage_live_records"),
                "Live records in the MemTable",
                node_label,
            ),
        }
    }

    pub(crate) fn sync_stats(&self, snapshot: &StorageMetricsSnapshot) {
        metrics_try("sync_storage_stats", || self.sync_stats_inner(snapshot));
    }

    fn sync_stats_inner(&self, snapshot: &StorageMetricsSnapshot) {
        let mut last_map = STORAGE_LAST.lock().unwrap_or_else(|e| e.into_inner());
        let prev = last_map
            .entry(snapshot.node.clone())
            .or_insert_with(|| snapshot.clone());
        let node = &snapshot.node;

        inc_storage_delta(&self.puts, node, snapshot.puts_total, &mut prev.puts_total);
        inc_storage_delta(&self.gets, node, snapshot.gets_total, &mut prev.gets_total);
        inc_storage_delta(
            &self.deletes,
            node,
            snapshot.deletes_total,
            &mut prev.deletes_total,
        );
        inc_storage_delta(
            &self.read_repairs,
            node,
            snapshot.read_repairs,
            &mut prev.read_repairs,
        );
        inc_storage_delta(
            &self.paxos_writes,
            node,
            snapshot.paxos_writes,
            &mut prev.paxos_writes,
        );
        inc_storage_delta(
            &self.quorum_failures,
            node,
            snapshot.quorum_failures,
            &mut prev.quorum_failures,
        );
        inc_storage_delta(
            &self.wal_bytes,
            node,
            snapshot.wal_bytes_written,
            &mut prev.wal_bytes_written,
        );

        if let Ok(gauge) = self.tombstones.get_metric_with_label_values(&[node.as_str()]) {
            gauge.set(snapshot.tombstone_count as f64);
        }
        if let Ok(gauge) = self.live_records.get_metric_with_label_values(&[node.as_str()]) {
            gauge.set(snapshot.live_records as f64);
        }
    }
}

fn inc_storage_delta(vec: &IntCounterVec, node: &str, current: u64, prev: &mut u64) {
    if current > *prev {
        if let Ok(counter) = vec.get_metric_with_label_values(&[node]) {
            counter.inc_by(current - *prev);
        }
        *prev = current;
    }
}
