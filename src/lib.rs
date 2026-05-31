//! Lane Switchboards — OTP-style actor runtime.
//!
//! - [`actor`]: actors, linking, monitoring, hot code upgrade
//! - [`supervisor`]: OneForOne / OneForAll / RestForOne restart strategies,
//!   [`ChildRegistry`] / [`ChildSlot`] for stable child refs after restart
//! - [`registry`]: global actor index
//! - [`distributed`]: TCP-framed remote messaging
//!
//! Ractor-based HTTP client: see the `gateway` example (`examples/gateway/`).

pub mod actor;
pub mod distributed;
pub mod registry;
pub mod supervisor;

pub use actor::{
    spawn, Actor, ActorId, ActorProcessingErr, ActorRef, DynActor, Envelope, ExitReason,
};
pub use distributed::{Frame, Node, RemoteActorRef, RemoteMessage};
pub use supervisor::{
    child_spec, spawn_child_spec, supervise_actor, ChildRegistry, ChildSlot, ChildSpec,
    IntensityAction, RestartStrategy, Supervisor, SupervisorConfig, SupervisorHandle,
};
