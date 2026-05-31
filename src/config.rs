//! Runtime tuning — mailbox capacities for tokio mpsc channels.

/// Actor mailbox sizing ([`crate::actor::spawn`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActorConfig {
    pub mailbox_capacity: usize,
}

impl Default for ActorConfig {
    fn default() -> Self {
        Self {
            mailbox_capacity: 64,
        }
    }
}

/// Distributed TCP bridge sizing ([`crate::distributed::serve_actor`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DistributedConfig {
    pub bridge_capacity: usize,
}

impl Default for DistributedConfig {
    fn default() -> Self {
        Self {
            bridge_capacity: 32,
        }
    }
}
