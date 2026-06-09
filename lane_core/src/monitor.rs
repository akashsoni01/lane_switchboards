//! Per-actor runtime stats and handle-duration monitoring.
//!
//! # Locking discipline
//!
//! `cells` and `post_mortem` are guarded by a `RwLock<HashMap>`.
//! The read lock is held only long enough to clone the `Arc<StatsCell>`;
//! all counter updates happen on the `Arc` afterwards — no lock held on the hot path.
//! Writes (`register`, `unregister`) take the write lock briefly and do no I/O inside it.

use crate::actor::ActorId;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

static MONITOR: Lazy<ActorMonitor> = Lazy::new(ActorMonitor::new);

/// Clamp a `Duration` to milliseconds that fit in `u64`.
#[inline(always)]
fn duration_ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

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
    /// Sum of all successful handle durations — divide by `messages_handled` for mean.
    pub total_handle_ms: u64,
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
    total_handle_ms: AtomicU64,
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
            total_handle_ms: AtomicU64::new(0),
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
            total_handle_ms: self.total_handle_ms.load(Ordering::Relaxed),
            slow_handles: self.slow_handles.load(Ordering::Relaxed),
        }
    }

    /// Saturating decrement of `in_flight` — never wraps to `u64::MAX`.
    fn dec_in_flight(&self) {
        self.in_flight
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            })
            .ok();
    }
}

/// Process-global actor monitor (stats for every registered actor).
///
/// Live stats are held in `cells` (one `Arc<StatsCell>` per running actor).  On exit
/// the cell is removed and a final `ActorStats` snapshot is stored in `post_mortem`
/// so callers can still read stats for a recently-stopped actor.
/// Post-mortem entries are evicted by `purge` or when the same id re-registers
/// (which never happens in practice — ids are monotonically assigned).
#[derive(Clone)]
pub struct ActorMonitor {
    cells: Arc<RwLock<HashMap<ActorId, Arc<StatsCell>>>>,
    post_mortem: Arc<RwLock<HashMap<ActorId, ActorStats>>>,
}

impl ActorMonitor {
    pub fn new() -> Self {
        Self {
            cells: Arc::new(RwLock::new(HashMap::new())),
            post_mortem: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn global() -> &'static ActorMonitor {
        &MONITOR
    }

    pub fn register(&self, id: ActorId) {
        self.post_mortem.write().unwrap().remove(&id);
        self.cells
            .write()
            .unwrap()
            .entry(id)
            .or_insert_with(|| Arc::new(StatsCell::new()));
    }

    /// Capture a final snapshot into the post-mortem store, then drop the live cell.
    ///
    /// After this call `get(id)` returns the frozen snapshot; `in_flight` is
    /// forced to 0 in the snapshot so external readers see a clean final state.
    pub fn unregister(&self, id: ActorId) {
        if let Some(cell) = self.cells.write().unwrap().remove(&id) {
            let mut snapshot = cell.snapshot(id);
            snapshot.in_flight = 0;
            self.post_mortem.write().unwrap().insert(id, snapshot);
        }
    }

    /// Capture the final stats snapshot and remove both live and post-mortem entries.
    ///
    /// Use this when you want to consume the final stats exactly once (e.g. structured
    /// logging on exit) without retaining a post-mortem entry.
    pub fn snapshot_and_unregister(&self, id: ActorId) -> Option<ActorStats> {
        self.post_mortem
            .write()
            .unwrap()
            .remove(&id)
            .or_else(|| {
                self.cells
                    .write()
                    .unwrap()
                    .remove(&id)
                    .map(|cell| cell.snapshot(id))
            })
    }

    /// Remove the post-mortem entry for `id` once you have consumed the final snapshot.
    pub fn purge(&self, id: ActorId) {
        self.post_mortem.write().unwrap().remove(&id);
    }

    /// Look up stats for a live *or* recently-stopped actor.
    pub fn get(&self, id: ActorId) -> Option<ActorStats> {
        if let Some(cell) = self.cells.read().unwrap().get(&id).cloned() {
            return Some(cell.snapshot(id));
        }
        self.post_mortem.read().unwrap().get(&id).cloned()
    }

    pub fn all(&self) -> Vec<ActorStats> {
        let mut out: Vec<_> = self
            .cells
            .read()
            .unwrap()
            .iter()
            .map(|(&id, cell)| cell.snapshot(id))
            .collect();
        out.sort_by_key(|s| s.actor_id.0);
        out
    }

    // --- hot-path helpers: clone Arc under brief read lock, then update lock-free ---

    fn cell(&self, id: ActorId) -> Option<Arc<StatsCell>> {
        self.cells.read().unwrap().get(&id).cloned()
    }

    pub(crate) fn begin_handle(&self, id: ActorId) {
        if let Some(cell) = self.cell(id) {
            cell.in_flight.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn finish_handle(
        &self,
        id: ActorId,
        elapsed: Duration,
        slow_threshold: Option<Duration>,
    ) {
        let Some(cell) = self.cell(id) else { return };
        let ms = duration_ms(elapsed);
        cell.dec_in_flight();
        cell.messages_handled.fetch_add(1, Ordering::Relaxed);
        cell.last_handle_ms.store(ms, Ordering::Relaxed);
        cell.total_handle_ms.fetch_add(ms, Ordering::Relaxed);

        // Spin to update the running maximum. compare_exchange_weak avoids an
        // extra barrier; Acquire on failure ensures the reload of `prev` sees
        // the latest stored value.
        loop {
            let prev = cell.max_handle_ms.load(Ordering::Relaxed);
            if ms <= prev {
                break;
            }
            if cell
                .max_handle_ms
                .compare_exchange_weak(prev, ms, Ordering::Relaxed, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }

        if let Some(threshold) = slow_threshold {
            if elapsed > threshold {
                cell.slow_handles.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    %id,
                    handle_ms = ms,
                    threshold_ms = threshold.as_millis(),
                    "actor handle exceeded slow threshold"
                );
            }
        }
    }

    pub(crate) fn record_error(&self, id: ActorId) {
        if let Some(cell) = self.cell(id) {
            cell.handle_errors.fetch_add(1, Ordering::Relaxed);
            cell.dec_in_flight();
        }
    }

    pub(crate) fn record_panic(&self, id: ActorId) {
        if let Some(cell) = self.cell(id) {
            cell.panics.fetch_add(1, Ordering::Relaxed);
            cell.dec_in_flight();
        }
    }

    pub(crate) fn record_timeout(&self, id: ActorId, elapsed: Duration) {
        if let Some(cell) = self.cell(id) {
            cell.handle_timeouts.fetch_add(1, Ordering::Relaxed);
            cell.dec_in_flight();
            let ms = duration_ms(elapsed);
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
