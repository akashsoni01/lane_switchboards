//! Cassandra-style tunable consistency levels for mesh routing.
//!
//! These types describe how many replica acknowledgements are required for
//! writes and reads. The mesh uses them to fan out to replicas and wait for
//! quorum before returning.
//!
//! # Example
//!
//! ```
//! use lane_switchboards::{ConsistencyConfig, WriteConsistency, ReadConsistency};
//!
//! let cfg = ConsistencyConfig {
//!     write_cl: WriteConsistency::Quorum,
//!     read_cl: ReadConsistency::Quorum,
//!     ..Default::default()
//! };
//! ```

use serde::{Deserialize, Serialize};
use std::fmt;
#[cfg(feature = "metrics")]
use std::sync::Arc;
use std::time::Duration;

/// Write-side consistency level (W).
///
/// Used by [`crate::ServiceMesh::invoke_consistent`] to decide how many replica
/// acks are required before a write returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WriteConsistency {
    /// Fire-and-forget to any replica; hinted handoff counts as success.
    /// Example: `write_cl = WriteConsistency::Any` — no ack wait.
    Any,
    /// Wait for one replica to acknowledge. Applies to all single-replica writes.
    One,
    /// Wait for two replicas to acknowledge. Requires `rf >= 2`.
    Two,
    /// Wait for three replicas to acknowledge. Requires `rf >= 3`.
    Three,
    /// Wait for one replica in the local datacenter (`LOCAL_*` levels).
    LocalOne,
    /// Wait for a quorum of replicas in the local datacenter.
    LocalQuorum,
    /// Wait for a quorum of replicas cluster-wide.
    Quorum,
    /// Wait for a quorum in **each** datacenter; requires [`ConsistencyConfig::dc_rfs`].
    EachQuorum,
    /// Wait for all replicas to acknowledge. Requires every replica to respond.
    All,
    /// Linearizable write via Paxos (cluster-wide). Delegates to the Paxos write path.
    Serial,
}

/// Read-side consistency level (R).
///
/// Used by [`crate::ServiceMesh::read_consistent`] and
/// [`crate::ServiceMesh::read_serial_value`]. [`ReadConsistency::Serial`] and
/// [`ReadConsistency::LocalSerial`] use the Paxos prepare path instead of simple quorum acks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ReadConsistency {
    /// Read from one replica.
    One,
    /// Read from two replicas. Requires `rf >= 2`.
    Two,
    /// Read from three replicas. Requires `rf >= 3`.
    Three,
    /// Read from one replica in the local datacenter.
    LocalOne,
    /// Read from a quorum of replicas in the local datacenter.
    LocalQuorum,
    /// Read from a quorum of replicas cluster-wide.
    Quorum,
    /// Linearizable read via Paxos (cluster-wide). Use with [`crate::paxos::PaxosProposer::read`].
    Serial,
    /// Linearizable read via Paxos (local datacenter only).
    LocalSerial,
    /// Read from all replicas.
    All,
}

/// Errors raised when consistency requirements cannot be met.
///
/// Returned by [`crate::ServiceMesh::invoke_consistent`],
/// [`crate::ServiceMesh::read_consistent`], and quorum helper functions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsistencyError {
    /// Fewer replicas are reachable than the level requires.
    NotEnoughReplicas {
        required: usize,
        available: usize,
    },
    /// Not enough replicas acknowledged within the timeout.
    ///
    /// For [`WriteConsistency::EachQuorum`], `dc` names the datacenter that failed.
    NotEnoughAcks {
        required: usize,
        received: usize,
        /// Set for [`WriteConsistency::EachQuorum`] DC-level failures.
        dc: Option<String>,
    },
    /// Paxos prepare/propose rounds exhausted due to contention
    /// ([`ReadConsistency::Serial`] / [`ReadConsistency::LocalSerial`]).
    PaxosContention { rounds: usize },
    /// Operation timed out waiting for acknowledgements.
    Timeout { after: Duration },
}

impl fmt::Display for ConsistencyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotEnoughReplicas {
                required,
                available,
            } => write!(
                f,
                "not enough replicas: required {required}, available {available}"
            ),
            Self::NotEnoughAcks {
                required,
                received,
                dc,
            } => match dc {
                Some(dc) => write!(
                    f,
                    "not enough acks in datacenter {dc}: required {required}, received {received}"
                ),
                None => write!(
                    f,
                    "not enough acks: required {required}, received {received}"
                ),
            },
            Self::PaxosContention { rounds } => {
                write!(f, "paxos contention after {rounds} rounds")
            }
            Self::Timeout { after } => {
                write!(f, "consistency operation timed out after {after:?}")
            }
        }
    }
}

impl std::error::Error for ConsistencyError {}

impl From<ConsistencyError> for std::io::Error {
    fn from(err: ConsistencyError) -> Self {
        Self::other(err)
    }
}

/// Snapshot of a completed consistency operation (opt-in via the `metrics` feature).
///
/// Emitted through [`ConsistencyConfig::on_metrics`] after each
/// [`crate::ServiceMesh::invoke_consistent`] or [`crate::ServiceMesh::read_consistent`] call.
#[cfg(feature = "metrics")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsistencyMetrics {
    /// Acknowledgements required for the configured level.
    pub acks_required: usize,
    /// Acknowledgements actually received before return.
    pub acks_received: usize,
    /// Replica endpoints contacted (fan-out size).
    pub replicas_contacted: usize,
    /// Wall-clock duration of the operation in milliseconds.
    pub duration_ms: u64,
    /// Debug representation of the write or read consistency level used.
    pub consistency_level: String,
    /// Service name passed to the mesh invoke/read call.
    pub service: String,
    /// Whether the operation returned `Ok(())`.
    pub succeeded: bool,
}

/// Returns the quorum size for a replication factor: `floor(rf / 2) + 1`.
///
/// Used by [`WriteConsistency::Quorum`], [`WriteConsistency::LocalQuorum`],
/// [`ReadConsistency::Quorum`], and [`WriteConsistency::EachQuorum`] per-DC math.
///
/// # Example
///
/// ```
/// use lane_switchboards::quorum_for;
/// assert_eq!(quorum_for(3), 2);
/// assert_eq!(quorum_for(2), 2);
/// ```
pub fn quorum_for(rf: usize) -> usize {
    rf / 2 + 1
}

/// Per-datacenter quorum requirements for [`WriteConsistency::EachQuorum`].
///
/// Returns one quorum size per entry in `dc_rfs`. Used internally by
/// [`crate::ServiceMesh::invoke_consistent`] when `write_cl = EachQuorum`.
pub fn each_quorum_acks_required(dc_rfs: &[usize]) -> Result<Vec<usize>, ConsistencyError> {
    if dc_rfs.is_empty() {
        return Err(ConsistencyError::NotEnoughReplicas {
            required: 1,
            available: 0,
        });
    }
    let mut per_dc = Vec::with_capacity(dc_rfs.len());
    for &dc_rf in dc_rfs {
        let required = quorum_for(dc_rf);
        if dc_rf < required {
            return Err(ConsistencyError::NotEnoughReplicas {
                required,
                available: dc_rf,
            });
        }
        per_dc.push(required);
    }
    Ok(per_dc)
}

/// Number of write acknowledgements required for `cl` given replication factors.
///
/// For [`WriteConsistency::EachQuorum`], pass `dc_rfs`; the return value is the
/// sum of per-DC quorum requirements (Phase 6 uses per-DC values separately via
/// [`each_quorum_acks_required`]).
///
/// # Example
///
/// ```
/// use lane_switchboards::{write_acks_required, WriteConsistency};
/// assert_eq!(write_acks_required(WriteConsistency::Quorum, 3, 3, None).unwrap(), 2);
/// ```
pub fn write_acks_required(
    cl: WriteConsistency,
    rf: usize,
    local_rf: usize,
    dc_rfs: Option<&[usize]>,
) -> Result<usize, ConsistencyError> {
    let required = match cl {
        WriteConsistency::Any | WriteConsistency::One | WriteConsistency::LocalOne => 1,
        WriteConsistency::Two => 2,
        WriteConsistency::Three => 3,
        WriteConsistency::LocalQuorum => quorum_for(local_rf),
        WriteConsistency::Quorum | WriteConsistency::Serial => quorum_for(rf),
        WriteConsistency::EachQuorum => {
            return each_quorum_acks_required(dc_rfs.unwrap_or(&[]))
                .map(|v| v.iter().sum());
        }
        WriteConsistency::All => rf,
    };

    let available = match cl {
        WriteConsistency::LocalOne | WriteConsistency::LocalQuorum => local_rf,
        WriteConsistency::EachQuorum => unreachable!(),
        _ => rf,
    };

    if available < required {
        return Err(ConsistencyError::NotEnoughReplicas {
            required,
            available,
        });
    }
    Ok(required)
}

/// Number of read acknowledgements required for `cl` given replication factors.
///
/// [`ReadConsistency::Serial`] and [`ReadConsistency::LocalSerial`] return the
/// Paxos quorum size; use [`is_paxos_read`] to select the Paxos code path.
///
/// # Example
///
/// ```
/// use lane_switchboards::{read_acks_required, ReadConsistency};
/// assert_eq!(read_acks_required(ReadConsistency::Quorum, 3, 3).unwrap(), 2);
/// ```
pub fn read_acks_required(
    cl: ReadConsistency,
    rf: usize,
    local_rf: usize,
) -> Result<usize, ConsistencyError> {
    let required = match cl {
        ReadConsistency::One | ReadConsistency::LocalOne => 1,
        ReadConsistency::Two => 2,
        ReadConsistency::Three => 3,
        ReadConsistency::LocalQuorum => quorum_for(local_rf),
        ReadConsistency::Quorum => quorum_for(rf),
        ReadConsistency::Serial => quorum_for(rf),
        ReadConsistency::LocalSerial => quorum_for(local_rf),
        ReadConsistency::All => rf,
    };

    let available = match cl {
        ReadConsistency::LocalOne | ReadConsistency::LocalQuorum | ReadConsistency::LocalSerial => {
            local_rf
        }
        ReadConsistency::Serial => rf,
        _ => rf,
    };

    if available < required {
        return Err(ConsistencyError::NotEnoughReplicas {
            required,
            available,
        });
    }
    Ok(required)
}

/// Returns `true` for [`ReadConsistency::Serial`] and [`ReadConsistency::LocalSerial`].
///
/// When true, [`crate::ServiceMesh::read_consistent`] delegates to Paxos instead of quorum acks.
pub fn is_paxos_read(cl: ReadConsistency) -> bool {
    matches!(
        cl,
        ReadConsistency::Serial | ReadConsistency::LocalSerial
    )
}

/// Returns `true` for write levels scoped to the local datacenter.
///
/// Applies to [`WriteConsistency::LocalOne`] and [`WriteConsistency::LocalQuorum`].
pub fn is_local_only(cl: WriteConsistency) -> bool {
    matches!(
        cl,
        WriteConsistency::LocalOne | WriteConsistency::LocalQuorum
    )
}

/// Returns `true` for read levels scoped to the local datacenter.
///
/// Applies to [`ReadConsistency::LocalOne`], [`ReadConsistency::LocalQuorum`], and
/// [`ReadConsistency::LocalSerial`].
pub fn is_local_only_read(cl: ReadConsistency) -> bool {
    matches!(
        cl,
        ReadConsistency::LocalOne | ReadConsistency::LocalQuorum | ReadConsistency::LocalSerial
    )
}

/// Replication and consistency defaults for mesh operations.
///
/// Configure once on [`crate::ServiceMesh::with_consistency`] or override per service via
/// [`crate::ServiceMesh::set_service_consistency`].
///
/// # Example
///
/// ```
/// use lane_switchboards::{ConsistencyConfig, WriteConsistency, ReadConsistency};
///
/// let cfg = ConsistencyConfig {
///     rf: 3,
///     write_cl: WriteConsistency::LocalQuorum,
///     read_cl: ReadConsistency::LocalQuorum,
///     ..Default::default()
/// };
/// ```
#[derive(Clone)]
pub struct ConsistencyConfig {
    /// Cluster-wide replication factor (`N` in the W + R > N formula).
    pub rf: usize,
    /// Replication factor in the local datacenter (for `LOCAL_*` levels).
    pub local_rf: usize,
    /// Per-datacenter replication factors (for [`WriteConsistency::EachQuorum`]).
    pub dc_rfs: Vec<usize>,
    /// Datacenter names parallel to [`Self::dc_rfs`] (for [`WriteConsistency::EachQuorum`]).
    pub dc_names: Vec<String>,
    /// Name of this node's datacenter (for `LOCAL_*` levels and untagged replicas).
    pub local_dc: String,
    /// Default write consistency for [`crate::ServiceMesh::invoke_consistent`].
    pub write_cl: WriteConsistency,
    /// Default read consistency for [`crate::ServiceMesh::read_consistent`].
    pub read_cl: ReadConsistency,
    /// Maximum time to wait for required acks on a single invoke/read.
    pub ack_timeout: Duration,
    /// Optional callback invoked after each consistency operation when the `metrics` feature is enabled.
    #[cfg(feature = "metrics")]
    pub on_metrics: Option<Arc<dyn Fn(ConsistencyMetrics) + Send + Sync>>,
}

impl fmt::Debug for ConsistencyConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut dbg = f.debug_struct("ConsistencyConfig");
        dbg.field("rf", &self.rf)
            .field("local_rf", &self.local_rf)
            .field("dc_rfs", &self.dc_rfs)
            .field("dc_names", &self.dc_names)
            .field("local_dc", &self.local_dc)
            .field("write_cl", &self.write_cl)
            .field("read_cl", &self.read_cl)
            .field("ack_timeout", &self.ack_timeout);
        #[cfg(feature = "metrics")]
        dbg.field(
            "on_metrics",
            &self
                .on_metrics
                .as_ref()
                .map(|_| "<callback>"),
        );
        dbg.finish()
    }
}

impl PartialEq for ConsistencyConfig {
    fn eq(&self, other: &Self) -> bool {
        self.rf == other.rf
            && self.local_rf == other.local_rf
            && self.dc_rfs == other.dc_rfs
            && self.dc_names == other.dc_names
            && self.local_dc == other.local_dc
            && self.write_cl == other.write_cl
            && self.read_cl == other.read_cl
            && self.ack_timeout == other.ack_timeout
    }
}

impl Eq for ConsistencyConfig {}

impl Default for ConsistencyConfig {
    fn default() -> Self {
        Self {
            rf: 3,
            local_rf: 3,
            dc_rfs: vec![3],
            dc_names: vec!["local".into()],
            local_dc: "local".into(),
            write_cl: WriteConsistency::LocalQuorum,
            read_cl: ReadConsistency::LocalQuorum,
            ack_timeout: Duration::from_secs(5),
            #[cfg(feature = "metrics")]
            on_metrics: None,
        }
    }
}

#[cfg(feature = "metrics")]
pub(crate) fn emit_metrics(
    config: &ConsistencyConfig,
    service: &str,
    consistency_level: impl fmt::Debug,
    acks_required: usize,
    acks_received: usize,
    replicas_contacted: usize,
    duration: Duration,
    succeeded: bool,
) {
    if let Some(cb) = &config.on_metrics {
        cb(ConsistencyMetrics {
            acks_required,
            acks_received,
            replicas_contacted,
            duration_ms: duration.as_millis() as u64,
            consistency_level: format!("{consistency_level:?}"),
            service: service.to_string(),
            succeeded,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quorum_for_values() {
        assert_eq!(quorum_for(1), 1);
        assert_eq!(quorum_for(2), 2);
        assert_eq!(quorum_for(3), 2);
        assert_eq!(quorum_for(5), 3);
    }

    #[test]
    fn write_acks_all_levels() {
        assert_eq!(
            write_acks_required(WriteConsistency::Any, 3, 3, None).unwrap(),
            1
        );
        assert_eq!(
            write_acks_required(WriteConsistency::One, 3, 3, None).unwrap(),
            1
        );
        assert_eq!(
            write_acks_required(WriteConsistency::Two, 3, 3, None).unwrap(),
            2
        );
        assert_eq!(
            write_acks_required(WriteConsistency::Three, 3, 3, None).unwrap(),
            3
        );
        assert_eq!(
            write_acks_required(WriteConsistency::LocalOne, 3, 3, None).unwrap(),
            1
        );
        assert_eq!(
            write_acks_required(WriteConsistency::LocalQuorum, 3, 3, None).unwrap(),
            2
        );
        assert_eq!(
            write_acks_required(WriteConsistency::Quorum, 3, 3, None).unwrap(),
            2
        );
        assert_eq!(
            write_acks_required(WriteConsistency::All, 3, 3, None).unwrap(),
            3
        );
        assert_eq!(
            write_acks_required(WriteConsistency::Serial, 3, 3, None).unwrap(),
            2
        );
        assert_eq!(
            write_acks_required(WriteConsistency::EachQuorum, 3, 3, Some(&[3, 3])).unwrap(),
            4
        );
    }

    #[test]
    fn write_acks_not_enough_replicas() {
        assert!(matches!(
            write_acks_required(WriteConsistency::Two, 1, 1, None),
            Err(ConsistencyError::NotEnoughReplicas {
                required: 2,
                available: 1
            })
        ));
        assert!(matches!(
            write_acks_required(WriteConsistency::LocalQuorum, 3, 0, None),
            Err(ConsistencyError::NotEnoughReplicas {
                required: 1,
                available: 0
            })
        ));
        assert!(matches!(
            write_acks_required(WriteConsistency::EachQuorum, 3, 3, Some(&[0])),
            Err(ConsistencyError::NotEnoughReplicas { .. })
        ));
        assert!(matches!(
            write_acks_required(WriteConsistency::EachQuorum, 3, 3, None),
            Err(ConsistencyError::NotEnoughReplicas { .. })
        ));
    }

    #[test]
    fn read_acks_all_levels() {
        assert_eq!(read_acks_required(ReadConsistency::One, 3, 3).unwrap(), 1);
        assert_eq!(read_acks_required(ReadConsistency::Two, 3, 3).unwrap(), 2);
        assert_eq!(read_acks_required(ReadConsistency::Three, 3, 3).unwrap(), 3);
        assert_eq!(
            read_acks_required(ReadConsistency::LocalOne, 3, 3).unwrap(),
            1
        );
        assert_eq!(
            read_acks_required(ReadConsistency::LocalQuorum, 3, 3).unwrap(),
            2
        );
        assert_eq!(read_acks_required(ReadConsistency::Quorum, 3, 3).unwrap(), 2);
        assert_eq!(read_acks_required(ReadConsistency::Serial, 3, 3).unwrap(), 2);
        assert_eq!(
            read_acks_required(ReadConsistency::LocalSerial, 3, 3).unwrap(),
            2
        );
        assert_eq!(read_acks_required(ReadConsistency::All, 3, 3).unwrap(), 3);
    }

    #[test]
    fn read_acks_not_enough_replicas() {
        assert!(matches!(
            read_acks_required(ReadConsistency::Two, 1, 1),
            Err(ConsistencyError::NotEnoughReplicas {
                required: 2,
                available: 1
            })
        ));
        assert!(matches!(
            read_acks_required(ReadConsistency::LocalSerial, 3, 0),
            Err(ConsistencyError::NotEnoughReplicas {
                required: 1,
                available: 0
            })
        ));
    }

    #[test]
    fn is_paxos_read_flag() {
        assert!(!is_paxos_read(ReadConsistency::Quorum));
        assert!(is_paxos_read(ReadConsistency::Serial));
        assert!(is_paxos_read(ReadConsistency::LocalSerial));
    }

    #[test]
    fn is_local_only_flags() {
        assert!(is_local_only(WriteConsistency::LocalOne));
        assert!(is_local_only(WriteConsistency::LocalQuorum));
        assert!(!is_local_only(WriteConsistency::Quorum));

        assert!(is_local_only_read(ReadConsistency::LocalOne));
        assert!(is_local_only_read(ReadConsistency::LocalQuorum));
        assert!(is_local_only_read(ReadConsistency::LocalSerial));
        assert!(!is_local_only_read(ReadConsistency::Serial));
    }

    #[test]
    fn consistency_config_default() {
        let cfg = ConsistencyConfig::default();
        assert_eq!(cfg.rf, 3);
        assert_eq!(cfg.local_rf, 3);
        assert_eq!(cfg.write_cl, WriteConsistency::LocalQuorum);
        assert_eq!(cfg.read_cl, ReadConsistency::LocalQuorum);
        assert_eq!(cfg.ack_timeout, Duration::from_secs(5));
    }

    #[test]
    fn consistency_error_display_and_io() {
        let err = ConsistencyError::NotEnoughAcks {
            required: 2,
            received: 1,
            dc: Some("east".into()),
        };
        assert!(err.to_string().contains("east"));
        let io: std::io::Error = err.into();
        assert_eq!(io.kind(), std::io::ErrorKind::Other);
    }

    #[test]
    fn each_quorum_per_dc() {
        assert_eq!(
            each_quorum_acks_required(&[2, 2]).unwrap(),
            vec![2, 2]
        );
        assert_eq!(
            each_quorum_acks_required(&[3, 3]).unwrap(),
            vec![2, 2]
        );
    }
}
