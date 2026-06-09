//! `lane_core` — OTP actor primitives for the lane_switchboards runtime.
//!
//! | Module | Responsibility |
//! |--------|----------------|
//! | [`actor`] | `Actor` trait, `ActorRef`, spawn, link, monitor, hot upgrade |
//! | [`config`] | `ActorConfig`, `DistributedConfig`, `DedicatedRuntime` |
//! | [`monitor`] | `ActorMonitor`, `ActorStats` — per-actor runtime counters |
//! | [`registry`] | Process-global control-channel and supervisor-channel index |
//! | [`supervisor`] | OTP restart strategies, `ChildRegistry`, `ChildSlot` |

pub mod actor;
pub mod config;
pub mod monitor;
pub mod registry;
pub mod supervisor;
