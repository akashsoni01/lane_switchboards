//! Per-actor runtime stats and handle-duration monitoring.

use crate::actor::ActorId;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

static MONITOR: Lazy<ActorMonitor> = Lazy::new(ActorMonitor::new);

/// Snapshot of one actor's runtime counters (deadlock / slow-handle detection).
#[derive(Debug, Clone)]
pub struct ActorStats {
    pub actor_id: ActorId,
    pub messages_handled: u64,
    pub handle_errors: u64,
    pub panics: u64,
    pub handle_timeouts: u64,
    pub in_flight: u64,
    pub last_handle_ms: u64,
    pub max_handle_ms: u64,
    pub slow_handles: u64,
}

struct StatsCell {
    messages_handled: AtomicU64,
    handle_errors: AtomicU64,
    panics: AtomicU64,
    handle_timeouts: AtomicU64,
    in_flight: AtomicU64,
    last_handle_ms: AtomicU64,
    max_handle_ms: AtomicU64,
    slow_handles: AtomicU64,
}

impl StatsCell {
    fn new() -> Self {
        Self {
            messages_handled: AtomicU64::new(0),
            handle_errors: AtomicU64::new(0),
            panics: AtomicU64::new(0),
            handle_timeouts: AtomicU64::new(0),
            in_flight: AtomicU64::new(0),
            last_handle_ms: AtomicU64::new(0),
            max_handle_ms: AtomicU64::new(0),
            slow_handles: AtomicU64::new(0),
        }
    }

    fn snapshot(&self, id: ActorId) -> ActorStats {
        ActorStats {
            actor_id: id,
            messages_handled: self.messages_handled.load(Ordering::Relaxed),
            handle_errors: self.handle_errors.load(Ordering::Relaxed),
            panics: self.panics.load(Ordering::Relaxed),
            handle_timeouts: self.handle_timeouts.load(Ordering::Relaxed),
            in_flight: self.in_flight.load(Ordering::Relaxed),
            last_handle_ms: self.last_handle_ms.load(Ordering::Relaxed),
            max_handle_ms: self.max_handle_ms.load(Ordering::Relaxed),
            slow_handles: self.slow_handles.load(Ordering::Relaxed),
        }
    }
}

/// Process-global actor monitor (stats for every registered actor).
#[derive(Clone)]
pub struct ActorMonitor {
    cells: Arc<DashMap<ActorId, Arc<StatsCell>>>,
}

impl ActorMonitor {
    pub fn new() -> Self {
        Self {
            cells: Arc::new(DashMap::new()),
        }
    }

    pub fn global() -> &'static ActorMonitor {
        &MONITOR
    }

    pub fn register(&self, id: ActorId) {
        self.cells.entry(id).or_insert_with(|| Arc::new(StatsCell::new()));
    }

    pub fn unregister(&self, id: ActorId) {
        self.cells.remove(&id);
    }

    /// Keep stats after actor exit but clear in-flight counter.
    pub fn mark_inactive(&self, id: ActorId) {
        if let Some(cell) = self.cells.get(&id) {
            cell.in_flight.store(0, Ordering::Relaxed);
        }
    }

    pub fn get(&self, id: ActorId) -> Option<ActorStats> {
        self.cells.get(&id).map(|e| e.value().snapshot(id))
    }

    pub fn all(&self) -> Vec<ActorStats> {
        let mut out: Vec<_> = self
            .cells
            .iter()
            .map(|e| e.value().snapshot(*e.key()))
            .collect();
        out.sort_by_key(|s| s.actor_id.0);
        out
    }

    pub(crate) fn begin_handle(&self, id: ActorId) {
        if let Some(cell) = self.cells.get(&id) {
            cell.in_flight.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn finish_handle(&self, id: ActorId, elapsed: Duration, slow_threshold: Option<Duration>) {
        let Some(cell) = self.cells.get(&id) else {
            return;
        };
        let ms = elapsed.as_millis().min(u64::MAX as u128) as u64;
        cell.in_flight.fetch_sub(1, Ordering::Relaxed);
        cell.messages_handled.fetch_add(1, Ordering::Relaxed);
        cell.last_handle_ms.store(ms, Ordering::Relaxed);
        loop {
            let prev = cell.max_handle_ms.load(Ordering::Relaxed);
            if ms <= prev {
                break;
            }
            if cell
                .max_handle_ms
                .compare_exchange(prev, ms, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
        if slow_threshold.is_some_and(|t| elapsed > t) {
            cell.slow_handles.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                %id,
                handle_ms = ms,
                threshold_ms = slow_threshold.map(|t| t.as_millis()).unwrap_or(0),
                "actor handle exceeded slow threshold"
            );
        }
    }

    pub(crate) fn record_error(&self, id: ActorId) {
        if let Some(cell) = self.cells.get(&id) {
            cell.handle_errors.fetch_add(1, Ordering::Relaxed);
            cell.in_flight.fetch_sub(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn record_panic(&self, id: ActorId) {
        if let Some(cell) = self.cells.get(&id) {
            cell.panics.fetch_add(1, Ordering::Relaxed);
            cell.in_flight.fetch_sub(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn record_timeout(&self, id: ActorId, elapsed: Duration) {
        if let Some(cell) = self.cells.get(&id) {
            cell.handle_timeouts.fetch_add(1, Ordering::Relaxed);
            cell.in_flight.fetch_sub(1, Ordering::Relaxed);
            let ms = elapsed.as_millis().min(u64::MAX as u128) as u64;
            cell.last_handle_ms.store(ms, Ordering::Relaxed);
            tracing::error!(
                %id,
                handle_ms = ms,
                "actor handle timeout — possible deadlock or slow handler"
            );
        }
    }
}

impl Default for ActorMonitor {
    fn default() -> Self {
        Self::new()
    }
}
