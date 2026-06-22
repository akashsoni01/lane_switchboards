//! Per-actor runtime stats and handle-duration monitoring.
//!
//! # Locking discipline
//!
//! `cells` and `post_mortem` are guarded by a `RwLock<HashMap>`.
//! The read lock is held only long enough to clone the `Arc<StatsCell>`;
//! all counter updates happen on the `Arc` afterwards — no lock held on the hot path.
//! Writes (`register`, `unregister`) take the write lock briefly and do no I/O inside it.
//!
//! # Counter limits
//!
//! All counters and millisecond fields use [`usize`]. Hot-path updates use
//! saturating arithmetic; when a value would exceed [`usize::MAX`], it is clamped
//! and a `tracing::warn!` is emitted once per overflow attempt (field + actor id).

use crate::actor::ActorId;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

static MONITOR: Lazy<ActorMonitor> = Lazy::new(ActorMonitor::new);

/// Clamp a `Duration` to milliseconds that fit in [`usize`].
#[inline(always)]
fn duration_ms(d: Duration) -> usize {
    usize::try_from(d.as_millis()).unwrap_or(usize::MAX)
}

/// Saturating add to `counter`; warn when `prev + delta` would overflow [`usize`].
fn fetch_add_saturating(
    counter: &AtomicUsize,
    delta: usize,
    field: &'static str,
    id: ActorId,
) {
    loop {
        let prev = counter.load(Ordering::Relaxed);
        if prev == usize::MAX {
            return;
        }
        let next = match prev.checked_add(delta) {
            Some(n) => n,
            None => {
                tracing::warn!(
                    %id,
                    field,
                    prev,
                    delta,
                    "actor stat counter overflow; clamped to usize::MAX"
                );
                usize::MAX
            }
        };
        if counter
            .compare_exchange_weak(prev, next, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            break;
        }
    }
}

fn inc_counter(counter: &AtomicUsize, field: &'static str, id: ActorId) {
    fetch_add_saturating(counter, 1, field, id);
}

/// Snapshot of one actor's runtime counters (deadlock / slow-handle detection).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorStats {
    pub actor_id: ActorId,
    pub messages_handled: usize,
    pub handle_errors: usize,
    pub panics: usize,
    pub handle_timeouts: usize,
    pub in_flight: usize,
    pub last_handle_ms: usize,
    pub max_handle_ms: usize,
    /// Sum of all successful handle durations.
    pub total_handle_ms: usize,
    /// Mean duration per successful handle call (`total_handle_ms / messages_handled`).
    /// `0` when no messages have been handled yet.
    pub mean_handle_ms: usize,
    pub slow_handles: usize,
}

struct StatsCell {
    messages_handled: AtomicUsize,
    handle_errors: AtomicUsize,
    panics: AtomicUsize,
    handle_timeouts: AtomicUsize,
    in_flight: AtomicUsize,
    last_handle_ms: AtomicUsize,
    max_handle_ms: AtomicUsize,
    total_handle_ms: AtomicUsize,
    slow_handles: AtomicUsize,
}

impl StatsCell {
    fn new() -> Self {
        Self {
            messages_handled: AtomicUsize::new(0),
            handle_errors: AtomicUsize::new(0),
            panics: AtomicUsize::new(0),
            handle_timeouts: AtomicUsize::new(0),
            in_flight: AtomicUsize::new(0),
            last_handle_ms: AtomicUsize::new(0),
            max_handle_ms: AtomicUsize::new(0),
            total_handle_ms: AtomicUsize::new(0),
            slow_handles: AtomicUsize::new(0),
        }
    }

    fn snapshot(&self, id: ActorId) -> ActorStats {
        let messages_handled = self.messages_handled.load(Ordering::Relaxed);
        let total_handle_ms = self.total_handle_ms.load(Ordering::Relaxed);
        let mean_handle_ms = total_handle_ms.checked_div(messages_handled).unwrap_or(0);
        ActorStats {
            actor_id: id,
            messages_handled,
            handle_errors: self.handle_errors.load(Ordering::Relaxed),
            panics: self.panics.load(Ordering::Relaxed),
            handle_timeouts: self.handle_timeouts.load(Ordering::Relaxed),
            in_flight: self.in_flight.load(Ordering::Relaxed),
            last_handle_ms: self.last_handle_ms.load(Ordering::Relaxed),
            max_handle_ms: self.max_handle_ms.load(Ordering::Relaxed),
            total_handle_ms,
            mean_handle_ms,
            slow_handles: self.slow_handles.load(Ordering::Relaxed),
        }
    }

    /// Saturating decrement of `in_flight` — never wraps to [`usize::MAX`].
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
            inc_counter(&cell.in_flight, "in_flight", id);
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
        inc_counter(&cell.messages_handled, "messages_handled", id);
        cell.last_handle_ms.store(ms, Ordering::Relaxed);
        fetch_add_saturating(&cell.total_handle_ms, ms, "total_handle_ms", id);

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
                inc_counter(&cell.slow_handles, "slow_handles", id);
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
            inc_counter(&cell.handle_errors, "handle_errors", id);
            cell.dec_in_flight();
        }
    }

    pub(crate) fn record_panic(&self, id: ActorId) {
        if let Some(cell) = self.cell(id) {
            inc_counter(&cell.panics, "panics", id);
            cell.dec_in_flight();
        }
    }

    pub(crate) fn record_timeout(&self, id: ActorId, elapsed: Duration) {
        if let Some(cell) = self.cell(id) {
            inc_counter(&cell.handle_timeouts, "handle_timeouts", id);
            // Count as a handled message so messages_handled is consistent with
            // total_handle_ms (both include the timed-out call).
            inc_counter(&cell.messages_handled, "messages_handled", id);
            cell.dec_in_flight();
            let ms = duration_ms(elapsed);
            cell.last_handle_ms.store(ms, Ordering::Relaxed);
            fetch_add_saturating(&cell.total_handle_ms, ms, "total_handle_ms", id);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_saturates_at_usize_max() {
        let counter = AtomicUsize::new(usize::MAX - 1);
        let id = ActorId(42);
        inc_counter(&counter, "messages_handled", id);
        assert_eq!(counter.load(Ordering::Relaxed), usize::MAX - 1 + 1);
        inc_counter(&counter, "messages_handled", id);
        assert_eq!(counter.load(Ordering::Relaxed), usize::MAX);
        inc_counter(&counter, "messages_handled", id);
        assert_eq!(counter.load(Ordering::Relaxed), usize::MAX);
    }

    #[test]
    fn fetch_add_saturating_clamps_large_delta() {
        let counter = AtomicUsize::new(usize::MAX - 5);
        let id = ActorId(7);
        fetch_add_saturating(&counter, 10, "total_handle_ms", id);
        assert_eq!(counter.load(Ordering::Relaxed), usize::MAX);
    }

    #[test]
    fn snapshot_mean_handle_ms_zero_when_no_messages() {
        let cell = StatsCell::new();
        let stats = cell.snapshot(ActorId(1));
        assert_eq!(stats.mean_handle_ms, 0);
        assert_eq!(stats.messages_handled, 0);
    }

    #[test]
    fn in_flight_never_wraps_on_decrement() {
        let cell = StatsCell::new();
        cell.dec_in_flight();
        assert_eq!(cell.in_flight.load(Ordering::Relaxed), 0);
    }
}
