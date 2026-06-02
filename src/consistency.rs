//! Cassandra-style tunable consistency levels for mesh routing.
//!
//! These types describe how many replica acknowledgements are required for
//! writes and reads. The mesh uses them to fan out to replicas and wait for
//! quorum before returning.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::Duration;

/// Write-side consistency level (W).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WriteConsistency {
    /// Fire-and-forget to any replica; hinted handoff counts as success.
    Any,
    /// Wait for one replica to acknowledge.
    One,
    /// Wait for two replicas to acknowledge.
    Two,
    /// Wait for three replicas to acknowledge.
    Three,
    /// Wait for one replica in the local datacenter.
    LocalOne,
    /// Wait for a quorum of replicas in the local datacenter.
    LocalQuorum,
    /// Wait for a quorum of replicas cluster-wide.
    Quorum,
    /// Wait for a quorum in **each** datacenter (requires `dc_rfs`).
    EachQuorum,
    /// Wait for all replicas to acknowledge.
    All,
}

/// Read-side consistency level (R).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ReadConsistency {
    /// Read from one replica.
    One,
    /// Read from two replicas.
    Two,
    /// Read from three replicas.
    Three,
    /// Read from one replica in the local datacenter.
    LocalOne,
    /// Read from a quorum of replicas in the local datacenter.
    LocalQuorum,
    /// Read from a quorum of replicas cluster-wide.
    Quorum,
    /// Linearizable read via Paxos (cluster-wide).
    Serial,
    /// Linearizable read via Paxos (local datacenter only).
    LocalSerial,
    /// Read from all replicas.
    All,
}

/// Errors raised when consistency requirements cannot be met.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsistencyError {
    /// Fewer replicas are reachable than the level requires.
    NotEnoughReplicas {
        required: usize,
        available: usize,
    },
    /// Not enough replicas acknowledged within the timeout.
    NotEnoughAcks {
        required: usize,
        received: usize,
        /// Set for [`WriteConsistency::EachQuorum`] DC-level failures.
        dc: Option<String>,
    },
    /// Paxos prepare/propose rounds exhausted due to contention.
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

/// Returns the quorum size for a replication factor: `floor(rf / 2) + 1`.
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
/// sum of per-DC quorum requirements (Phase 6 uses per-DC values separately).
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
        WriteConsistency::Quorum => quorum_for(rf),
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
pub fn is_paxos_read(cl: ReadConsistency) -> bool {
    matches!(
        cl,
        ReadConsistency::Serial | ReadConsistency::LocalSerial
    )
}

/// Returns `true` for write levels scoped to the local datacenter.
pub fn is_local_only(cl: WriteConsistency) -> bool {
    matches!(
        cl,
        WriteConsistency::LocalOne | WriteConsistency::LocalQuorum
    )
}

/// Returns `true` for read levels scoped to the local datacenter.
pub fn is_local_only_read(cl: ReadConsistency) -> bool {
    matches!(
        cl,
        ReadConsistency::LocalOne | ReadConsistency::LocalQuorum | ReadConsistency::LocalSerial
    )
}

/// Replication and consistency defaults for mesh operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsistencyConfig {
    /// Cluster-wide replication factor.
    pub rf: usize,
    /// Replication factor in the local datacenter.
    pub local_rf: usize,
    /// Per-datacenter replication factors (for [`WriteConsistency::EachQuorum`]).
    pub dc_rfs: Vec<usize>,
    /// Datacenter names parallel to [`Self::dc_rfs`] (for [`WriteConsistency::EachQuorum`]).
    pub dc_names: Vec<String>,
    /// Name of this node's datacenter (for LOCAL_* levels).
    pub local_dc: String,
    pub write_cl: WriteConsistency,
    pub read_cl: ReadConsistency,
    pub ack_timeout: Duration,
}

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
        }
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
