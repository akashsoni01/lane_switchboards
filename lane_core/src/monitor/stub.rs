//! No-op [`ActorMonitor`] when the `monitor` feature is disabled (zero hot-path cost).

use crate::actor::{ActorId, ExitReason};

use super::{ActorMeta, ActorStats};

/// Inert monitor — all methods are empty and inlined away on the actor hot path.
#[allow(dead_code)]
pub struct ActorMonitor;

#[allow(dead_code)]
impl ActorMonitor {
    #[inline(always)]
    pub fn new() -> Self {
        Self
    }

    #[inline(always)]
    pub fn global() -> &'static Self {
        static MONITOR: ActorMonitor = ActorMonitor;
        &MONITOR
    }

    #[inline(always)]
    pub fn register(&self, _id: ActorId, _meta: ActorMeta, _mailbox_capacity: usize) {}

    #[inline(always)]
    pub fn unregister(&self, _id: ActorId, _reason: Option<&ExitReason>) {}

    #[inline(always)]
    pub fn snapshot_and_unregister(&self, _id: ActorId) -> Option<ActorStats> {
        None
    }

    #[inline(always)]
    pub fn purge(&self, _id: ActorId) {}

    #[inline(always)]
    pub fn get(&self, _id: ActorId) -> Option<ActorStats> {
        None
    }

    #[inline(always)]
    pub fn all(&self) -> Vec<ActorStats> {
        Vec::new()
    }

    #[inline(always)]
    pub(crate) fn record_mailbox_enqueue(&self, _id: ActorId) {}

    #[inline(always)]
    pub(crate) fn record_mailbox_dequeue(&self, _id: ActorId) {}

    #[inline(always)]
    pub(crate) fn record_mailbox_send_rejected(&self, _id: ActorId) {}

    #[inline(always)]
    pub(crate) fn record_mailbox_send_blocked(&self, _id: ActorId) {}

    #[inline(always)]
    pub(crate) fn begin_handle(&self, _id: ActorId, _mailbox_wait: std::time::Duration) {}

    #[inline(always)]
    pub(crate) fn finish_handle(
        &self,
        _id: ActorId,
        _elapsed: std::time::Duration,
        _slow_threshold: Option<std::time::Duration>,
    ) {
    }

    #[inline(always)]
    pub(crate) fn record_error(&self, _id: ActorId) {}

    #[inline(always)]
    pub(crate) fn record_panic(&self, _id: ActorId) {}

    #[inline(always)]
    pub(crate) fn record_timeout(&self, _id: ActorId, _elapsed: std::time::Duration) {}
}

impl Default for ActorMonitor {
    fn default() -> Self {
        Self::new()
    }
}
