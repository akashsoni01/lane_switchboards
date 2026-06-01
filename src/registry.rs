//! Global actor index: control routing and supervisor notification channels.

use crate::actor::{ActorId, ControlMsg};
use dashmap::DashMap;
use once_cell::sync::Lazy;
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::supervisor::RestartSignal;

type ControlSender = mpsc::Sender<ControlMsg>;

static ACTOR_CONTROL_SENDERS: Lazy<DashMap<ActorId, ControlSender>> = Lazy::new(DashMap::new);
static SUPERVISOR_CHANNELS: Lazy<DashMap<ActorId, mpsc::Sender<RestartSignal>>> =
    Lazy::new(DashMap::new);

/// Register an actor's control channel for cross-type link / linked-exit routing.
pub(crate) fn register_actor(id: ActorId, tx: ControlSender) {
    ACTOR_CONTROL_SENDERS.insert(id, tx);
}

/// Register a supervisor notification channel for a child actor id.
pub fn register_supervisor(child_id: ActorId, tx: mpsc::Sender<RestartSignal>) {
    SUPERVISOR_CHANNELS.insert(child_id, tx);
}

pub fn unregister_actor(id: ActorId) {
    ACTOR_CONTROL_SENDERS.remove(&id);
    SUPERVISOR_CHANNELS.remove(&id);
}

pub(crate) fn get_control_sender(id: ActorId) -> Option<ControlSender> {
    ACTOR_CONTROL_SENDERS.get(&id).map(|e| e.value().clone())
}

pub fn get_supervisor_sender(id: ActorId) -> Option<mpsc::Sender<RestartSignal>> {
    SUPERVISOR_CHANNELS.get(&id).map(|e| e.value().clone())
}

/// Shared registry handle (for tests); maps are process-global.
#[derive(Clone, Default)]
pub struct Registry;

impl Registry {
    pub fn global() -> Arc<Self> {
        Arc::new(Self)
    }

    pub fn actor_count(&self) -> usize {
        ACTOR_CONTROL_SENDERS.len()
    }
}
