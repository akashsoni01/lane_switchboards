//! Global actor index: control routing and supervisor notification channels.

use crate::actor::{ActorId, ControlMsg};
use crate::supervisor::RestartSignal;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use tokio::sync::mpsc;

type ControlSender = mpsc::Sender<ControlMsg>;

struct ActorEntry {
    control: ControlSender,
    supervisor: Option<mpsc::Sender<RestartSignal>>,
}

// ActorId values are sequential u64s — ahash (DashMap default) distributes these well.
static ACTORS: Lazy<DashMap<ActorId, ActorEntry>> = Lazy::new(DashMap::new);

/// Register control routing and optional supervisor notification for one actor instance.
///
/// Clones inside [`get_control_sender`] / [`get_supervisor_sender`] hold the shard lock only
/// for the duration of the closure — do not await or block there.
pub(crate) fn register_actor(
    id: ActorId,
    control: ControlSender,
    supervisor: Option<mpsc::Sender<RestartSignal>>,
) {
    ACTORS.insert(id, ActorEntry { control, supervisor });
}

pub fn unregister_actor(id: ActorId) {
    ACTORS.remove(&id);
}

pub(crate) fn get_control_sender(id: ActorId) -> Option<ControlSender> {
    ACTORS.get(&id).map(|e| e.control.clone())
}

pub fn get_supervisor_sender(id: ActorId) -> Option<mpsc::Sender<RestartSignal>> {
    ACTORS.get(&id).and_then(|e| e.supervisor.clone())
}

pub fn actor_count() -> usize {
    ACTORS.len()
}

pub fn registered_ids() -> Vec<ActorId> {
    ACTORS.iter().map(|e| *e.key()).collect()
}
