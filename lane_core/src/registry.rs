//! Global actor index: control routing and supervisor notification channels.
//!
//! # Locking discipline
//!
//! All lookups clone the sender under a brief read lock; no I/O or await
//! is performed while the lock is held.  Writes (`register`, `unregister`) take
//! the write lock and are infrequent (once per actor lifetime).

use crate::actor::{ActorId, ControlMsg};
use crate::supervisor::RestartSignal;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::RwLock;
use tokio::sync::mpsc;

type ControlSender = mpsc::Sender<ControlMsg>;

struct ActorEntry {
    control: ControlSender,
    supervisor: Option<mpsc::Sender<RestartSignal>>,
}

static ACTORS: Lazy<RwLock<HashMap<ActorId, ActorEntry>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// Register control routing and optional supervisor notification for one actor instance.
pub(crate) fn register_actor(
    id: ActorId,
    control: ControlSender,
    supervisor: Option<mpsc::Sender<RestartSignal>>,
) {
    ACTORS
        .write()
        .unwrap()
        .insert(id, ActorEntry { control, supervisor });
}

pub fn unregister_actor(id: ActorId) {
    ACTORS.write().unwrap().remove(&id);
}

pub(crate) fn get_control_sender(id: ActorId) -> Option<ControlSender> {
    ACTORS.read().unwrap().get(&id).map(|e| e.control.clone())
}

pub fn get_supervisor_sender(id: ActorId) -> Option<mpsc::Sender<RestartSignal>> {
    ACTORS
        .read()
        .unwrap()
        .get(&id)
        .and_then(|e| e.supervisor.clone())
}

pub fn actor_count() -> usize {
    ACTORS.read().unwrap().len()
}

pub fn registered_ids() -> Vec<ActorId> {
    ACTORS.read().unwrap().keys().copied().collect()
}
