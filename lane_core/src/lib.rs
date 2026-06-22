//! `lane_core` — OTP actor primitives for the lane_switchboards runtime.
//!
//! | Module | Responsibility |
//! |--------|----------------|
//! | [`actor`] | `Actor` trait, `ActorRef`, spawn, link, monitor, hot upgrade |
//! | [`config`] | `ActorConfig`, `DistributedConfig`, `DedicatedRuntime` |
//! | [`monitor`] | `ActorMonitor`, `ActorStats`, `ActorMeta` — per-actor runtime counters |
//! | [`metrics`] | Prometheus export (`metrics` feature) |

pub mod actor;
pub mod config;
pub mod monitor;
pub mod registry;
pub mod supervisor;

#[cfg(feature = "metrics")]
pub mod metrics;

pub use monitor::{ActorMeta, ActorMonitor, ActorStats};

#[cfg(feature = "metrics")]
pub use metrics::{
    exit_reason_label, init_metrics, record_consistency_operation, record_mesh_dispatch,
    record_remote_ack_timeout, record_remote_send, render_prometheus_text, restart_strategy_label,
    serve_metrics_http, set_child_generation, sync_storage_stats, ConsistencyOpSnapshot,
    MetricsConfig, StorageMetricsSnapshot,
};
