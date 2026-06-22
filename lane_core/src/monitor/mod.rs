//! Per-actor runtime stats (`monitor` feature) and type definitions always available.
//!
//! Enable in-process counters with `features = ["monitor"]`. Add `metrics` for Prometheus export.
//! With neither feature, the actor hot path has **no** monitor overhead (see `actor` module).

use crate::actor::ActorId;

#[cfg(feature = "monitor")]
mod live;
#[cfg(not(feature = "monitor"))]
mod stub;

#[cfg(feature = "monitor")]
pub use live::ActorMonitor;
#[cfg(not(feature = "monitor"))]
pub use stub::ActorMonitor;

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
