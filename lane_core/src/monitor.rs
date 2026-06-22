//! Per-actor runtime stats and handle-duration monitoring.
//!
//! # Locking discipline
//!
//! `cells` and `post_mortem` are guarded by a `RwLock<HashMap>`.
//! The read lock is held only long enough to clone the `Arc<ActorCell>`;
//! all counter updates happen on the `Arc` afterwards — no lock held on the hot path.
//! Writes (`register`, `unregister`) take the write lock briefly and do no I/O inside it.
//!
//! # Counter limits
//!
//! All counters and millisecond fields use [`usize`]. Hot-path updates use
//! saturating arithmetic; when a value would exceed [`usize::MAX`], it is clamped
//! and a `tracing::warn!` is emitted once per overflow attempt (field + actor id).
//!
//! # Prometheus (`metrics` feature)
//!
//! Counters and histograms are pre-bound per actor at [`ActorMonitor::register`].
//! Call [`crate::metrics::render_prometheus_text`] to export Grafana-ready text.

use crate::actor::{ActorId, ExitReason};
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(feature = "metrics")]
use crate::metrics::PromActorMetrics;

static MONITOR: Lazy<ActorMonitor> = Lazy::new(ActorMonitor::new);

/// Grafana / Prometheus labels for an actor (set via [`crate::config::ActorConfig::monitor_meta`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ActorMeta {
    pub name: Option<String>,
    pub actor_type: Option<String>,
    pub supervisor_id: Option<ActorId>,
    pub node: Option<String>,
    pub service: Option<String>,
}

impl ActorMeta {
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn with_actor_type(mut self, actor_type: impl Into<String>) -> Self {
        self.actor_type = Some(actor_type.into());
        self
    }

    pub fn with_supervisor(mut self, id: ActorId) -> Self {
        self.supervisor_id = Some(id);
        self
    }
}

/// Clamp a `Duration` to milliseconds that fit in [`usize`].
#[inline(always)]
fn duration_ms(d: Duration) -> usize {
    usize::try_from(d.as_millis()).unwrap_or(usize::MAX)
}

/// Saturating add to `counter`; warn when `prev + delta` would overflow [`usize`].
fn fetch_add_saturating(
    cell: &ActorCell,
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
                #[cfg(feature = "metrics")]
                crate::metrics::record_counter_saturated(field, &cell.meta);
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

fn inc_counter(cell: &ActorCell, counter: &AtomicUsize, field: &'static str, id: ActorId) {
    fetch_add_saturating(cell, counter, 1, field, id);
}

/// Snapshot of one actor's runtime counters (deadlock / slow-handle detection).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorStats {
    pub actor_id: ActorId,
    pub meta: ActorMeta,
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
    /// Approximate messages waiting in the actor mailbox.
    pub mailbox_depth: usize,
    pub mailbox_capacity: usize,
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

    fn snapshot(&self, id: ActorId, meta: &ActorMeta) -> ActorStats {
        let messages_handled = self.messages_handled.load(Ordering::Relaxed);
        let total_handle_ms = self.total_handle_ms.load(Ordering::Relaxed);
        let mean_handle_ms = total_handle_ms.checked_div(messages_handled).unwrap_or(0);
        ActorStats {
            actor_id: id,
            meta: meta.clone(),
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
            mailbox_depth: 0,
            mailbox_capacity: 0,
        }
    }

    fn dec_in_flight(&self) {
        self.in_flight
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            })
            .ok();
    }
}

struct ActorCell {
    stats: StatsCell,
    meta: ActorMeta,
    registered_at: Instant,
    last_handle_unix_ms: AtomicU64,
    mailbox_capacity: usize,
    mailbox_queued: AtomicUsize,
    #[cfg(feature = "metrics")]
    prom: Option<PromActorMetrics>,
}

/// Process-global actor monitor (stats for every registered actor).
#[derive(Clone)]
pub struct ActorMonitor {
    cells: Arc<RwLock<HashMap<ActorId, Arc<ActorCell>>>>,
    post_mortem: Arc<RwLock<HashMap<ActorId, ActorStats>>>,
}

impl ActorCell {
    fn cell_snapshot(&self, id: ActorId) -> ActorStats {
        let mut snapshot = self.stats.snapshot(id, &self.meta);
        snapshot.mailbox_depth = self.mailbox_queued.load(Ordering::Relaxed);
        snapshot.mailbox_capacity = self.mailbox_capacity;
        snapshot
    }
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

    pub fn register(&self, id: ActorId, meta: ActorMeta, mailbox_capacity: usize) {
        self.post_mortem.write().unwrap().remove(&id);
        #[cfg(feature = "metrics")]
        let prom = crate::metrics::bind_actor_metrics(&meta, mailbox_capacity);
        #[cfg(not(feature = "metrics"))]
        let _ = (&meta, mailbox_capacity);
        let cell = Arc::new(ActorCell {
            stats: StatsCell::new(),
            meta,
            registered_at: Instant::now(),
            last_handle_unix_ms: AtomicU64::new(0),
            mailbox_capacity,
            mailbox_queued: AtomicUsize::new(0),
            #[cfg(feature = "metrics")]
            prom,
        });
        self.cells.write().unwrap().insert(id, cell);
    }

    pub fn unregister(&self, id: ActorId, reason: Option<&ExitReason>) {
        if let Some(cell) = self.cells.write().unwrap().remove(&id) {
            #[cfg(feature = "metrics")]
            if let (Some(prom), Some(reason)) = (&cell.prom, reason) {
                crate::metrics::record_exit(prom, reason);
            }
            let mut snapshot = cell.cell_snapshot(id);
            snapshot.in_flight = 0;
            snapshot.mailbox_depth = 0;
            self.post_mortem.write().unwrap().insert(id, snapshot);
        }
    }

    pub fn snapshot_and_unregister(&self, id: ActorId) -> Option<ActorStats> {
        self.post_mortem
            .write()
            .unwrap()
            .remove(&id)
            .or_else(|| {
                self.cells.write().unwrap().remove(&id).map(|cell| {
                    let mut snapshot = cell.cell_snapshot(id);
                    snapshot.in_flight = 0;
                    snapshot.mailbox_depth = 0;
                    snapshot
                })
            })
    }

    pub fn purge(&self, id: ActorId) {
        self.post_mortem.write().unwrap().remove(&id);
    }

    pub fn get(&self, id: ActorId) -> Option<ActorStats> {
        if let Some(cell) = self.cells.read().unwrap().get(&id).cloned() {
            return Some(cell.cell_snapshot(id));
        }
        self.post_mortem.read().unwrap().get(&id).cloned()
    }

    pub fn all(&self) -> Vec<ActorStats> {
        let mut out: Vec<_> = self
            .cells
            .read()
            .unwrap()
            .iter()
            .map(|(&id, cell)| cell.cell_snapshot(id))
            .collect();
        out.sort_by_key(|s| s.actor_id.0);
        out
    }

    /// Increment approximate mailbox queue depth (called after a successful enqueue).
    pub(crate) fn record_mailbox_enqueue(&self, id: ActorId) {
        if let Some(cell) = self.cell(id) {
            cell.mailbox_queued.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Decrement approximate mailbox queue depth (called when the actor dequeues).
    pub(crate) fn record_mailbox_dequeue(&self, id: ActorId) {
        if let Some(cell) = self.cell(id) {
            cell.mailbox_queued
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    Some(v.saturating_sub(1))
                })
                .ok();
        }
    }

    /// Record a rejected mailbox send (full channel or actor exited).
    pub(crate) fn record_mailbox_send_rejected(&self, id: ActorId) {
        if let Some(cell) = self.cell(id) {
            #[cfg(feature = "metrics")]
            if let Some(prom) = &cell.prom {
                prom.mailbox_send_rejected.inc();
            }
            let _ = cell;
        }
    }

    /// Refresh Prometheus gauges from live actor cells (call before scrape).
    #[cfg(feature = "metrics")]
    pub fn sync_prometheus_gauges(&self) {
        let cells: Vec<_> = self.cells.read().unwrap().values().cloned().collect();
        for cell in cells {
            let Some(prom) = &cell.prom else { continue };
            let stats = cell.cell_snapshot(ActorId(0));
            crate::metrics::sync_scrape_gauges(
                cell.registered_at,
                cell.last_handle_unix_ms.load(Ordering::Relaxed),
                prom,
                stats.in_flight,
                stats.last_handle_ms,
                stats.max_handle_ms,
                stats.mailbox_depth,
            );
        }
    }

    fn cell(&self, id: ActorId) -> Option<Arc<ActorCell>> {
        self.cells.read().unwrap().get(&id).cloned()
    }

    fn sync_in_flight_gauge(cell: &ActorCell) {
        #[cfg(feature = "metrics")]
        if let Some(prom) = &cell.prom {
            prom.in_flight
                .set(cell.stats.in_flight.load(Ordering::Relaxed) as f64);
        }
    }

    fn touch_last_handle(cell: &ActorCell) {
        if let Ok(ms) = unix_now_ms() {
            cell.last_handle_unix_ms.store(ms, Ordering::Relaxed);
        }
    }

    pub(crate) fn begin_handle(&self, id: ActorId, mailbox_wait: Duration) {
        let Some(cell) = self.cell(id) else { return };
        inc_counter(&cell, &cell.stats.in_flight, "in_flight", id);
        Self::sync_in_flight_gauge(&cell);
        #[cfg(feature = "metrics")]
        if let Some(prom) = &cell.prom {
            prom.mailbox_wait.observe(mailbox_wait.as_secs_f64());
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
        cell.stats.dec_in_flight();
        inc_counter(&cell, &cell.stats.messages_handled, "messages_handled", id);
        cell.stats.last_handle_ms.store(ms, Ordering::Relaxed);
        fetch_add_saturating(&cell, &cell.stats.total_handle_ms, ms, "total_handle_ms", id);
        Self::touch_last_handle(&cell);

        loop {
            let prev = cell.stats.max_handle_ms.load(Ordering::Relaxed);
            if ms <= prev {
                break;
            }
            if cell
                .stats
                .max_handle_ms
                .compare_exchange_weak(prev, ms, Ordering::Relaxed, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }

        let mut slow = false;
        if let Some(threshold) = slow_threshold {
            if elapsed > threshold {
                slow = true;
                inc_counter(&cell, &cell.stats.slow_handles, "slow_handles", id);
                tracing::warn!(
                    %id,
                    handle_ms = ms,
                    threshold_ms = threshold.as_millis(),
                    "actor handle exceeded slow threshold"
                );
            }
        }

        #[cfg(feature = "metrics")]
        if let Some(prom) = &cell.prom {
            prom.messages_handled.inc();
            if slow {
                prom.slow_handles.inc();
            }
            crate::metrics::observe_handle_duration(prom, elapsed);
            prom.last_handle_seconds.set(elapsed.as_secs_f64());
            let max_ms = cell.stats.max_handle_ms.load(Ordering::Relaxed);
            prom.max_handle_seconds.set(max_ms as f64 / 1000.0);
        }
        Self::sync_in_flight_gauge(&cell);
    }

    pub(crate) fn record_error(&self, id: ActorId) {
        if let Some(cell) = self.cell(id) {
            inc_counter(&cell, &cell.stats.handle_errors, "handle_errors", id);
            cell.stats.dec_in_flight();
            #[cfg(feature = "metrics")]
            if let Some(prom) = &cell.prom {
                prom.handle_errors.inc();
            }
            Self::sync_in_flight_gauge(&cell);
        }
    }

    pub(crate) fn record_panic(&self, id: ActorId) {
        if let Some(cell) = self.cell(id) {
            inc_counter(&cell, &cell.stats.panics, "panics", id);
            cell.stats.dec_in_flight();
            #[cfg(feature = "metrics")]
            if let Some(prom) = &cell.prom {
                prom.panics.inc();
            }
            Self::sync_in_flight_gauge(&cell);
        }
    }

    pub(crate) fn record_timeout(&self, id: ActorId, elapsed: Duration) {
        if let Some(cell) = self.cell(id) {
            inc_counter(&cell, &cell.stats.handle_timeouts, "handle_timeouts", id);
            inc_counter(&cell, &cell.stats.messages_handled, "messages_handled", id);
            cell.stats.dec_in_flight();
            let ms = duration_ms(elapsed);
            cell.stats.last_handle_ms.store(ms, Ordering::Relaxed);
            fetch_add_saturating(&cell, &cell.stats.total_handle_ms, ms, "total_handle_ms", id);
            Self::touch_last_handle(&cell);
            loop {
                let prev = cell.stats.max_handle_ms.load(Ordering::Relaxed);
                if ms <= prev {
                    break;
                }
                if cell
                    .stats
                    .max_handle_ms
                    .compare_exchange_weak(prev, ms, Ordering::Relaxed, Ordering::Acquire)
                    .is_ok()
                {
                    break;
                }
            }
            #[cfg(feature = "metrics")]
            if let Some(prom) = &cell.prom {
                prom.handle_timeouts.inc();
                prom.messages_handled.inc();
                crate::metrics::observe_handle_duration(prom, elapsed);
                prom.last_handle_seconds.set(elapsed.as_secs_f64());
            }
            Self::sync_in_flight_gauge(&cell);
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

fn unix_now_ms() -> Result<u64, std::time::SystemTimeError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_saturates_at_usize_max() {
        let cell = Arc::new(ActorCell {
            stats: StatsCell::new(),
            meta: ActorMeta::default(),
            registered_at: Instant::now(),
            last_handle_unix_ms: AtomicU64::new(0),
            mailbox_capacity: 64,
            mailbox_queued: AtomicUsize::new(0),
            #[cfg(feature = "metrics")]
            prom: None,
        });
        let counter = &cell.stats.messages_handled;
        let id = ActorId(42);
        counter.store(usize::MAX - 1, Ordering::Relaxed);
        inc_counter(&cell, counter, "messages_handled", id);
        assert_eq!(counter.load(Ordering::Relaxed), usize::MAX);
    }

    #[test]
    fn snapshot_mean_handle_ms_zero_when_no_messages() {
        let cell = StatsCell::new();
        let stats = cell.snapshot(ActorId(1), &ActorMeta::default());
        assert_eq!(stats.mean_handle_ms, 0);
        assert_eq!(stats.messages_handled, 0);
    }

    #[test]
    fn in_flight_never_wraps_on_decrement() {
        let cell = StatsCell::new();
        cell.dec_in_flight();
        assert_eq!(cell.in_flight.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn mailbox_depth_tracks_enqueue_dequeue() {
        let mon = ActorMonitor::new();
        let id = ActorId(5);
        mon.register(id, ActorMeta::default(), 8);
        mon.record_mailbox_enqueue(id);
        mon.record_mailbox_enqueue(id);
        assert_eq!(mon.get(id).expect("stats").mailbox_depth, 2);
        mon.record_mailbox_dequeue(id);
        assert_eq!(mon.get(id).expect("stats").mailbox_depth, 1);
    }

    #[test]
    fn register_and_get_round_trip() {
        let mon = ActorMonitor::new();
        let id = ActorId(99);
        mon.register(
            id,
            ActorMeta::default().with_name("worker"),
            32,
        );
        let stats = mon.get(id).expect("stats");
        assert_eq!(stats.meta.name.as_deref(), Some("worker"));
    }
}
