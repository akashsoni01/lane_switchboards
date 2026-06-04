//! TCP service mesh: register microservice instances, discover by name, route via hash ring.

use crate::consistency::{
    each_quorum_acks_required, is_local_only, is_local_only_read, is_paxos_read,
    read_acks_required, write_acks_required, ConsistencyConfig, ConsistencyError,
    ReadConsistency, WriteConsistency,
};
use crate::config::DistributedConfig;
use crate::distributed::{
    serve_actor, Cluster, ClusterMember, NodeHandle, RemoteActorRef, RemoteMessage,
};
#[cfg(feature = "tls")]
use crate::distributed::serve_actor_tls_on_runtime;
use crate::hash_ring::HashRing;
use crate::paxos::{PaxosProposer, PaxosReplica};
use crate::stream::TlsConnector;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast, RwLock};

/// Records must be renewed via `Register` within this window or the registry evicts them.
pub const DEFAULT_RECORD_TTL: Duration = Duration::from_secs(30);
pub(crate) const EVICTION_INTERVAL: Duration = Duration::from_secs(5);
const WATCH_BROADCAST_CAPACITY: usize = 256;

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn record_expired(record: &ServiceRecord, now: u64, ttl: Duration) -> bool {
    let age = now.saturating_sub(record.registered_at);
    age > ttl.as_secs()
}

/// A running microservice instance (data-plane endpoint).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServiceRecord {
    pub service: String,
    pub instance_id: String,
    pub address: String,
    /// TCP frame target on the remote node — unique per instance (typically `instance_id`).
    pub target: String,
    /// Datacenter tag; `None` is treated as local everywhere.
    #[serde(default)]
    pub dc: Option<String>,
    /// Unix seconds when this record was last registered or renewed.
    #[serde(default)]
    pub registered_at: u64,
}

impl ServiceRecord {
    pub fn member(&self) -> ClusterMember {
        let mut member = ClusterMember::new(&self.instance_id, &self.address, &self.target);
        member.dc = self.dc.clone();
        member
    }
}

/// Per-service routing table (hash ring over instances).
struct ServiceRoute<M: RemoteMessage> {
    cluster: Cluster<M>,
    consistency: Option<ConsistencyConfig>,
}

/// In-process mesh control plane + data-plane router.
pub struct ServiceMesh<M: RemoteMessage> {
    routes: HashMap<String, ServiceRoute<M>>,
    consistency: ConsistencyConfig,
    tls_connector: Option<Arc<TlsConnector>>,
}

impl<M: RemoteMessage> Default for ServiceMesh<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M: RemoteMessage> ServiceMesh<M> {
    pub fn new() -> Self {
        Self::with_consistency(ConsistencyConfig::default())
    }

    /// Mesh with custom replication and consistency defaults for [`Self::invoke_consistent`].
    pub fn with_consistency(consistency: ConsistencyConfig) -> Self {
        Self {
            routes: HashMap::new(),
            consistency,
            tls_connector: None,
        }
    }

    /// TLS connector for outbound [`RemoteActorRef`] connections (`feature = "tls"`).
    #[cfg(feature = "tls")]
    pub fn set_tls_connector(&mut self, connector: Option<Arc<TlsConnector>>) {
        self.tls_connector = connector.clone();
        for route in self.routes.values_mut() {
            route.cluster.set_tls_connector(connector.clone());
        }
    }

    pub fn consistency(&self) -> &ConsistencyConfig {
        &self.consistency
    }

    pub fn set_consistency(&mut self, config: ConsistencyConfig) {
        self.consistency = config;
    }

    /// Per-service override for [`Self::invoke_consistent`] / [`Self::read_consistent`].
    pub fn set_service_consistency(&mut self, service: &str, config: ConsistencyConfig) {
        if let Some(route) = self.routes.get_mut(service) {
            route.consistency = Some(config);
        }
    }

    fn config_for(&self, service: &str) -> &ConsistencyConfig {
        self.routes
            .get(service)
            .and_then(|r| r.consistency.as_ref())
            .unwrap_or(&self.consistency)
    }

    pub fn services(&self) -> Vec<String> {
        let mut names: Vec<_> = self.routes.keys().cloned().collect();
        names.sort();
        names
    }

    pub fn instance_count(&self, service: &str) -> usize {
        self.routes
            .get(service)
            .map(|r| r.cluster.len())
            .unwrap_or(0)
    }

    pub fn records(&self, service: &str) -> Vec<ServiceRecord> {
        self.routes
            .get(service)
            .map(|route| {
                route
                    .cluster
                    .members()
                    .iter()
                    .map(|m| ServiceRecord {
                        service: service.to_string(),
                        instance_id: m.name.clone(),
                        address: m.node_addr.clone(),
                        target: m.target.clone(),
                        dc: m.dc.clone(),
                        registered_at: 0,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn ring(&self, service: &str) -> Option<&HashRing> {
        self.routes.get(service).map(|r| r.cluster.ring())
    }

    /// Register or upsert an instance under `record.service`.
    ///
    /// Returns the previous record when upserting the same `instance_id` (address/target may
    /// have changed). Callers holding a stale [`RemoteActorRef`] should refresh via
    /// [`Self::ref_for_key`] after an upsert.
    pub fn register(&mut self, record: ServiceRecord) -> Option<ServiceRecord> {
        let service = record.service.clone();
        let tls = self.tls_connector.clone();
        let route = self
            .routes
            .entry(service.clone())
            .or_insert_with(|| {
                let mut cluster = Cluster::new();
                cluster.set_tls_connector(tls.clone());
                ServiceRoute {
                    cluster,
                    consistency: None,
                }
            });
        let displaced = if route
            .cluster
            .members()
            .iter()
            .any(|m| m.name == record.instance_id)
        {
            route.cluster.leave(&record.instance_id).map(|member| ServiceRecord {
                service: service.clone(),
                instance_id: member.name,
                address: member.node_addr,
                target: member.target,
                dc: member.dc,
                registered_at: 0,
            })
        } else {
            None
        };
        route.cluster.join(record.member());
        displaced
    }

    pub fn deregister(&mut self, service: &str, instance_id: &str) -> Option<ServiceRecord> {
        let route = self.routes.get_mut(service)?;
        let member = route.cluster.leave(instance_id)?;
        if route.cluster.is_empty() {
            self.routes.remove(service);
        }
        Some(ServiceRecord {
            service: service.to_string(),
            instance_id: member.name,
            address: member.node_addr,
            target: member.target,
            dc: member.dc,
            registered_at: 0,
        })
    }

    /// Replace mesh state from a registry snapshot without clearing all routes first.
    pub fn apply_snapshot_diff(&mut self, records: Vec<ServiceRecord>) {
        let incoming: HashMap<(String, String), ServiceRecord> = records
            .into_iter()
            .map(|r| ((r.service.clone(), r.instance_id.clone()), r))
            .collect();

        let services: Vec<String> = self.routes.keys().cloned().collect();
        for service in services {
            let stale_ids: Vec<String> = self
                .routes
                .get(&service)
                .map(|route| {
                    route
                        .cluster
                        .members()
                        .iter()
                        .filter(|m| {
                            !incoming.contains_key(&(service.clone(), m.name.clone()))
                        })
                        .map(|m| m.name.clone())
                        .collect()
                })
                .unwrap_or_default();

            if let Some(route) = self.routes.get_mut(&service) {
                for id in stale_ids {
                    route.cluster.leave(&id);
                }
                if route.cluster.is_empty() {
                    self.routes.remove(&service);
                }
            }
        }

        for record in incoming.into_values() {
            self.register(record);
        }
    }

    pub fn apply_snapshot(&mut self, records: impl IntoIterator<Item = ServiceRecord>) {
        self.apply_snapshot_diff(records.into_iter().collect());
    }

    pub async fn invoke<T: Hash>(&self, service: &str, key: &T, msg: M) -> std::io::Result<()> {
        self.route(service)?
            .cluster
            .send_by_key(key, msg)
            .await
    }

    /// Write with Cassandra-style consistency: fan out to replicas and wait for W acks.
    ///
    /// Uses the mesh or per-service [`ConsistencyConfig::write_cl`]. For fire-and-forget
    /// routing use [`Self::invoke`] instead.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use lane_switchboards::{ConsistencyConfig, ServiceMesh, WriteConsistency};
    /// # async fn example(mesh: &ServiceMesh<()>, key: &str) -> Result<(), lane_switchboards::ConsistencyError> {
    /// mesh.invoke_consistent("orders", &key, ()).await
    /// # }
    /// ```
    pub async fn invoke_consistent<T: Hash>(
        &self,
        service: &str,
        key: &T,
        msg: M,
    ) -> Result<(), ConsistencyError>
    where
        M: Clone,
    {
        let config = self.config_for(service);
        let start = std::time::Instant::now();
        let span = tracing::info_span!(
            "mesh.invoke",
            service = service,
            consistency_level = ?config.write_cl,
            replicas_contacted = tracing::field::Empty,
            acks_received = tracing::field::Empty,
            latency_ms = tracing::field::Empty,
        );
        let _guard = span.enter();

        let cluster = match self.route(service) {
            Ok(route) => &route.cluster,
            Err(e) => {
                let result = Err(consistency_from_io(e));
                finish_consistency_op(
                    config,
                    service,
                    config.write_cl,
                    0,
                    0,
                    0,
                    start,
                    &span,
                    config.rf,
                    &result,
                );
                return result;
            }
        };

        let (result, stats, required) = if config.write_cl == WriteConsistency::Any {
            let result = cluster
                .send_by_key(key, msg)
                .await
                .map_err(consistency_from_io);
            (
                result,
                QuorumOutcome {
                    acks_received: 0,
                    replicas_contacted: 1,
                    acks_required: 0,
                },
                0usize,
            )
        } else if config.write_cl == WriteConsistency::EachQuorum {
            fan_out_each_quorum(cluster, key, config, msg).await
        } else {
            let required = write_acks_required(
                config.write_cl,
                config.rf,
                config.local_rf,
                Some(&config.dc_rfs),
            );
            match required {
                Ok(required) => {
                    let replicas: Vec<_> = if is_local_only(config.write_cl) {
                        cluster.local_replicas_for_key(key, config.rf, &config.local_dc)
                    } else {
                        cluster.replicas_for_key(key, config.rf)
                    };

                    if replicas.len() < required {
                        (
                            Err(ConsistencyError::NotEnoughReplicas {
                                required,
                                available: replicas.len(),
                            }),
                            QuorumOutcome {
                                acks_received: 0,
                                replicas_contacted: replicas.len(),
                                acks_required: required,
                            },
                            required,
                        )
                    } else {
                        let (result, stats) =
                            fan_out_quorum(&replicas, msg, required, config.ack_timeout, None).await;
                        (result, stats, required)
                    }
                }
                Err(e) => (
                    Err(e),
                    QuorumOutcome {
                        acks_received: 0,
                        replicas_contacted: 0,
                        acks_required: 0,
                    },
                    0,
                ),
            }
        };

        finish_consistency_op(
            config,
            service,
            config.write_cl,
            required,
            stats.acks_received,
            stats.replicas_contacted,
            start,
            &span,
            config.rf,
            &result,
        );
        result
    }

    /// Read-side consistency: wait for R replica acks of receipt (not response data).
    ///
    /// For [`ReadConsistency::Serial`] / [`ReadConsistency::LocalSerial`], runs a Paxos
    /// prepare round instead (see [`Self::read_serial_value`]).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use lane_switchboards::ServiceMesh;
    /// # async fn example(mesh: &ServiceMesh<()>, key: &str) -> Result<(), lane_switchboards::ConsistencyError> {
    /// mesh.read_consistent("orders", &key, ()).await
    /// # }
    /// ```
    pub async fn read_consistent<T: Hash + std::fmt::Debug>(
        &self,
        service: &str,
        key: &T,
        msg: M,
    ) -> Result<(), ConsistencyError>
    where
        M: Clone,
    {
        let config = self.config_for(service);
        let start = std::time::Instant::now();
        let span = tracing::info_span!(
            "mesh.read",
            service = service,
            consistency_level = ?config.read_cl,
            replicas_contacted = tracing::field::Empty,
            acks_received = tracing::field::Empty,
            latency_ms = tracing::field::Empty,
        );
        let _guard = span.enter();

        let (result, stats, required) = if is_paxos_read(config.read_cl) {
            match self.read_serial_value(service, key).await {
                Ok(_) => {
                    let quorum = read_acks_required(config.read_cl, config.rf, config.local_rf)
                        .unwrap_or(0);
                    (
                        Ok(()),
                        QuorumOutcome {
                            acks_received: quorum,
                            replicas_contacted: quorum,
                            acks_required: quorum,
                        },
                        quorum,
                    )
                }
                Err(e) => {
                    let quorum = read_acks_required(config.read_cl, config.rf, config.local_rf)
                        .unwrap_or(0);
                    (
                        Err(e),
                        QuorumOutcome {
                            acks_received: 0,
                            replicas_contacted: quorum,
                            acks_required: quorum,
                        },
                        quorum,
                    )
                }
            }
        } else {
            match self.read_quorum(service, key, msg, config).await {
                Ok(stats) => {
                    let required = stats.acks_required;
                    (Ok(()), stats, required)
                }
                Err(e) => {
                    let stats = quorum_stats_from_error(&e);
                    let required = read_acks_required(config.read_cl, config.rf, config.local_rf)
                        .unwrap_or(0);
                    (Err(e), stats, required)
                }
            }
        };

        finish_consistency_op(
            config,
            service,
            config.read_cl,
            required,
            stats.acks_received,
            stats.replicas_contacted,
            start,
            &span,
            config.rf,
            &result,
        );
        result
    }

    /// Linearizable Paxos read returning the highest accepted value for `key`.
    ///
    /// Applies to [`ReadConsistency::Serial`] and [`ReadConsistency::LocalSerial`].
    /// Returns `Ok(None)` when no value has been accepted yet.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use lane_switchboards::ServiceMesh;
    /// # async fn example(mesh: &ServiceMesh<()>, key: &str) -> Result<Option<Vec<u8>>, lane_switchboards::ConsistencyError> {
    /// mesh.read_serial_value("orders", &key).await
    /// # }
    /// ```
    pub async fn read_serial_value<T: Hash + std::fmt::Debug>(
        &self,
        service: &str,
        key: &T,
    ) -> Result<Option<Vec<u8>>, ConsistencyError> {
        let config = self.config_for(service);
        if !is_paxos_read(config.read_cl) {
            return Err(ConsistencyError::NotEnoughAcks {
                required: 1,
                received: 0,
                dc: None,
            });
        }

        let cluster = &self.route(service).map_err(consistency_from_io)?.cluster;
        let quorum = read_acks_required(config.read_cl, config.rf, config.local_rf)?;
        let paxos_key = format!("{key:?}");

        let replicas: Vec<PaxosReplica> = if config.read_cl == ReadConsistency::LocalSerial {
            cluster
                .local_replicas_for_key(key, config.rf, &config.local_dc)
                .iter()
                .map(|r| PaxosReplica {
                    node_addr: r.node_addr.clone(),
                    target: crate::paxos::paxos_target(service),
                })
                .collect()
        } else {
            cluster
                .all_replicas_for_key(key)
                .iter()
                .map(|r| PaxosReplica {
                    node_addr: r.node_addr.clone(),
                    target: crate::paxos::paxos_target(service),
                })
                .collect()
        };

        if replicas.len() < quorum {
            return Err(ConsistencyError::NotEnoughReplicas {
                required: quorum,
                available: replicas.len(),
            });
        }

        PaxosProposer::new(DistributedConfig::default())
            .read(&paxos_key, &replicas, quorum, config.ack_timeout)
            .await
    }

    async fn read_quorum<T: Hash>(
        &self,
        service: &str,
        key: &T,
        msg: M,
        config: &ConsistencyConfig,
    ) -> Result<QuorumOutcome, ConsistencyError>
    where
        M: Clone,
    {
        let cluster = &self.route(service).map_err(consistency_from_io)?.cluster;
        let required = read_acks_required(config.read_cl, config.rf, config.local_rf)?;

        let replicas: Vec<_> = if is_local_only_read(config.read_cl) {
            cluster.local_replicas_for_key(key, config.rf, &config.local_dc)
        } else {
            cluster.replicas_for_key(key, config.rf)
        };

        if replicas.len() < required {
            return Err(ConsistencyError::NotEnoughReplicas {
                required,
                available: replicas.len(),
            });
        }

        let (result, stats) =
            fan_out_quorum(&replicas, msg, required, config.ack_timeout, None).await;
        result.map(|()| QuorumOutcome {
            acks_required: required,
            ..stats
        })
    }

    pub async fn invoke_all(&self, service: &str, msg: M) -> Vec<(String, std::io::Result<()>)>
    where
        M: Clone,
    {
        match self.routes.get(service) {
            Some(route) => route.cluster.send_all(msg).await,
            None => Vec::new(),
        }
    }

    pub async fn invoke_any(&self, service: &str, msg: M) -> std::io::Result<()> {
        self.route(service)?.cluster.send_round_robin(msg).await
    }

    pub fn ref_for_key<T: Hash>(
        &self,
        service: &str,
        key: &T,
    ) -> Option<&RemoteActorRef<M>> {
        self.routes.get(service)?.cluster.ref_for_key(key)
    }

    fn route(&self, service: &str) -> std::io::Result<&ServiceRoute<M>> {
        self.routes.get(service).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("unknown service: {service}"),
            )
        })
    }
}

fn consistency_from_io(_err: std::io::Error) -> ConsistencyError {
    ConsistencyError::NotEnoughAcks {
        required: 1,
        received: 0,
        dc: None,
    }
}

/// Ack counts collected during a quorum fan-out.
struct QuorumOutcome {
    acks_received: usize,
    replicas_contacted: usize,
    acks_required: usize,
}

impl QuorumOutcome {
    fn new(replicas_contacted: usize) -> Self {
        Self {
            acks_received: 0,
            replicas_contacted,
            acks_required: 0,
        }
    }
}

fn quorum_stats_from_error(err: &ConsistencyError) -> QuorumOutcome {
    match err {
        ConsistencyError::NotEnoughAcks { received, .. } => QuorumOutcome {
            acks_received: *received,
            replicas_contacted: 0,
            acks_required: 0,
        },
        _ => QuorumOutcome::new(0),
    }
}

#[allow(clippy::too_many_arguments)]
fn finish_consistency_op(
    config: &ConsistencyConfig,
    service: &str,
    consistency_level: impl std::fmt::Debug,
    acks_required: usize,
    acks_received: usize,
    replicas_contacted: usize,
    start: std::time::Instant,
    span: &tracing::Span,
    rf: usize,
    result: &Result<(), ConsistencyError>,
) {
    let latency = start.elapsed();
    span.record("replicas_contacted", replicas_contacted);
    span.record("acks_received", acks_received);
    span.record("latency_ms", latency.as_millis() as u64);

    if result.is_ok() && acks_received < rf && acks_required > 0 {
        tracing::warn!(
            service,
            ?consistency_level,
            acks_received,
            rf,
            "consistency operation succeeded with fewer acks than replication factor (degraded)"
        );
    }

    #[cfg(feature = "metrics")]
    crate::consistency::emit_metrics(
        config,
        service,
        consistency_level,
        acks_required,
        acks_received,
        replicas_contacted,
        latency,
        result.is_ok(),
    );
    #[cfg(not(feature = "metrics"))]
    let _ = config;
}

async fn fan_out_quorum<M>(
    replicas: &[&RemoteActorRef<M>],
    msg: M,
    required: usize,
    timeout: Duration,
    dc: Option<String>,
) -> (Result<(), ConsistencyError>, QuorumOutcome)
where
    M: RemoteMessage + Clone,
{
    let replicas_contacted = replicas.len();
    let mut join_set = tokio::task::JoinSet::new();
    for replica in replicas {
        let replica = (*replica).clone();
        let msg = msg.clone();
        join_set.spawn(async move { replica.send_with_ack(msg, timeout).await });
    }

    let count = tokio::time::timeout(timeout, async {
        let mut acks = 0usize;
        while acks < required {
            match join_set.join_next().await {
                Some(Ok(Ok(()))) => acks += 1,
                Some(Ok(Err(_))) | Some(Err(_)) => {}
                None => break,
            }
        }
        acks
    })
    .await;

    join_set.abort_all();

    let stats = QuorumOutcome {
        acks_received: count.as_ref().copied().unwrap_or(0),
        replicas_contacted,
        acks_required: required,
    };

    let result = match count {
        Ok(acks) if acks >= required => Ok(()),
        Ok(acks) => Err(ConsistencyError::NotEnoughAcks {
            required,
            received: acks,
            dc,
        }),
        Err(_) => Err(ConsistencyError::Timeout { after: timeout }),
    };

    (result, stats)
}

async fn fan_out_each_quorum<M, T>(
    cluster: &Cluster<M>,
    key: &T,
    config: &ConsistencyConfig,
    msg: M,
) -> (Result<(), ConsistencyError>, QuorumOutcome, usize)
where
    M: RemoteMessage + Clone,
    T: Hash,
{
    let per_dc_required = match each_quorum_acks_required(&config.dc_rfs) {
        Ok(v) => v,
        Err(e) => {
            return (
                Err(e),
                QuorumOutcome::new(0),
                0,
            );
        }
    };
    let dc_names: Vec<String> = if config.dc_names.is_empty() {
        cluster.datacenters(&config.local_dc)
    } else {
        config.dc_names.clone()
    };

    if dc_names.len() != config.dc_rfs.len() || dc_names.len() != per_dc_required.len() {
        return (
            Err(ConsistencyError::NotEnoughReplicas {
                required: config.dc_rfs.len(),
                available: dc_names.len(),
            }),
            QuorumOutcome::new(0),
            0,
        );
    }

    let total_required: usize = per_dc_required.iter().sum();
    let mut total_replicas = 0usize;
    let mut total_acks = 0usize;

    let mut join_set = tokio::task::JoinSet::new();
    for ((dc, &dc_rf), &required) in dc_names
        .iter()
        .zip(config.dc_rfs.iter())
        .zip(per_dc_required.iter())
    {
        let replicas: Vec<RemoteActorRef<M>> = cluster
            .dc_replicas_for_key(key, dc, &config.local_dc, dc_rf)
            .into_iter()
            .cloned()
            .collect();

        total_replicas += replicas.len();

        if replicas.len() < required {
            return (
                Err(ConsistencyError::NotEnoughReplicas {
                    required,
                    available: replicas.len(),
                }),
                QuorumOutcome {
                    acks_received: 0,
                    replicas_contacted: total_replicas,
                    acks_required: total_required,
                },
                total_required,
            );
        }

        let dc = dc.clone();
        let msg = msg.clone();
        let timeout = config.ack_timeout;
        let refs = replicas;
        join_set.spawn(async move {
            let slice: Vec<&RemoteActorRef<M>> = refs.iter().collect();
            fan_out_quorum(&slice, msg, required, timeout, Some(dc)).await
        });
    }

    let outcome = tokio::time::timeout(config.ack_timeout, async {
        let mut first_err = None;
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok((Ok(()), stats)) => {
                    total_acks += stats.acks_received;
                }
                Ok((Err(e), stats)) => {
                    total_acks += stats.acks_received;
                    first_err.get_or_insert(e);
                }
                Err(_) => {
                    first_err.get_or_insert(ConsistencyError::NotEnoughAcks {
                        required: 1,
                        received: 0,
                        dc: None,
                    });
                }
            }
        }
        first_err
    })
    .await;

    join_set.abort_all();

    let stats = QuorumOutcome {
        acks_received: total_acks,
        replicas_contacted: total_replicas,
        acks_required: total_required,
    };

    let result = match outcome {
        Ok(None) => Ok(()),
        Ok(Some(e)) => Err(e),
        Err(_) => Err(ConsistencyError::Timeout {
            after: config.ack_timeout,
        }),
    };

    (result, stats, total_required)
}

/// Handle returned when a microservice instance is bound to TCP.
pub struct MicroserviceHandle<M: RemoteMessage> {
    pub record: ServiceRecord,
    _node: NodeHandle<M>,
}

impl<M: RemoteMessage> MicroserviceHandle<M> {
    pub fn service(&self) -> &str {
        &self.record.service
    }

    pub fn instance_id(&self) -> &str {
        &self.record.instance_id
    }

    pub fn address(&self) -> &str {
        &self.record.address
    }
}

/// Bind a microservice actor on TCP. Frame `target` is the unique `instance_id`.
pub async fn serve_microservice<M, A>(
    service: impl Into<String>,
    instance_id: impl Into<String>,
    bind_addr: impl Into<String>,
    actor: A,
) -> std::io::Result<MicroserviceHandle<M>>
where
    M: RemoteMessage,
    A: crate::actor::Actor<M> + Send + Sync + 'static,
{
    let service = service.into();
    let instance_id = instance_id.into();
    let target = instance_id.clone();
    let node = serve_actor(&instance_id, bind_addr, &target, actor).await?;
    Ok(MicroserviceHandle {
        record: ServiceRecord {
            service,
            instance_id,
            address: node.address().to_string(),
            target,
            dc: None,
            registered_at: 0,
        },
        _node: node,
    })
}

/// Bind a microservice with TLS on the data plane (`feature = "tls"`).
#[cfg(feature = "tls")]
pub async fn serve_microservice_tls<M, A>(
    service: impl Into<String>,
    instance_id: impl Into<String>,
    bind_addr: impl Into<String>,
    actor: A,
    tls: Arc<TlsAcceptor>,
) -> std::io::Result<MicroserviceHandle<M>>
where
    M: RemoteMessage,
    A: crate::actor::Actor<M> + Send + Sync + 'static,
{
    use crate::config::{ActorConfig, DistributedConfig};
    use tokio::runtime::Handle;

    let service = service.into();
    let instance_id = instance_id.into();
    let target = instance_id.clone();
    let node = serve_actor_tls_on_runtime(
        &Handle::current(),
        &instance_id,
        bind_addr,
        &target,
        actor,
        &DistributedConfig::default(),
        &ActorConfig::default(),
        tls,
    )
    .await?;
    Ok(MicroserviceHandle {
        record: ServiceRecord {
            service,
            instance_id,
            address: node.address().to_string(),
            target,
            dc: None,
            registered_at: 0,
        },
        _node: node,
    })
}

fn registered_event(record: ServiceRecord) -> crate::proto::control::ServiceEvent {
    crate::proto::control::ServiceEvent {
        kind: crate::proto::control::service_event::Kind::Registered as i32,
        record: Some(record.into()),
    }
}

fn deregistered_event(record: ServiceRecord) -> crate::proto::control::ServiceEvent {
    crate::proto::control::ServiceEvent {
        kind: crate::proto::control::service_event::Kind::Deregistered as i32,
        record: Some(record.into()),
    }
}

// --- Legacy TCP control types (removed from wire in Phase 6; kept for test fixtures) ---

/// Control-plane message (length-prefixed JSON over TCP).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MeshControlMsg {
    Register(ServiceRecord),
    Deregister { service: String, instance_id: String },
    List,
    ListReply(Vec<ServiceRecord>),
    Ping,
    Pong,
}

/// Shared mesh registry backing the gRPC control plane.
#[derive(Clone)]
pub struct MeshRegistry {
    records: Arc<RwLock<HashMap<(String, String), ServiceRecord>>>,
    ttl: Duration,
    events: broadcast::Sender<crate::proto::control::ServiceEvent>,
}

impl Default for MeshRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl MeshRegistry {
    pub fn new() -> Self {
        Self::with_ttl(DEFAULT_RECORD_TTL)
    }

    pub fn with_ttl(ttl: Duration) -> Self {
        let (events, _) = broadcast::channel(WATCH_BROADCAST_CAPACITY);
        Self {
            records: Arc::new(RwLock::new(HashMap::new())),
            ttl,
            events,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<crate::proto::control::ServiceEvent> {
        self.events.subscribe()
    }

    fn emit(&self, event: crate::proto::control::ServiceEvent) {
        let _ = self.events.send(event);
    }

    pub async fn register(&self, mut record: ServiceRecord) {
        record.registered_at = unix_now();
        let key = (record.service.clone(), record.instance_id.clone());
        self.records.write().await.insert(key, record.clone());
        self.emit(registered_event(record));
    }

    pub async fn deregister(&self, service: &str, instance_id: &str) -> Option<ServiceRecord> {
        let removed = self
            .records
            .write()
            .await
            .remove(&(service.to_string(), instance_id.to_string()));
        if let Some(record) = &removed {
            self.emit(deregistered_event(record.clone()));
        }
        removed
    }

    pub async fn list(&self) -> Vec<ServiceRecord> {
        let now = unix_now();
        self.records
            .read()
            .await
            .values()
            .filter(|r| !record_expired(r, now, self.ttl))
            .cloned()
            .collect()
    }

    pub async fn evict_expired(&self) {
        let now = unix_now();
        self.records
            .write()
            .await
            .retain(|_, r| !record_expired(r, now, self.ttl));
    }

    pub async fn apply_to_mesh<M: RemoteMessage>(&self, mesh: &mut ServiceMesh<M>) {
        mesh.apply_snapshot_diff(self.list().await);
    }
}

pub use crate::mesh_registry_grpc::{
    MeshRegistryClient, MeshRegistryHandle, MeshRegistryServer, PendingMeshRegistryClient,
};

/// Sidecar router: local mesh view + optional sync from gRPC registry.
pub struct MeshRouter<M: RemoteMessage> {
    pub mesh: ServiceMesh<M>,
    client: Option<PendingMeshRegistryClient>,
}

impl<M: RemoteMessage> MeshRouter<M> {
    pub fn local() -> Self {
        Self {
            mesh: ServiceMesh::new(),
            client: None,
        }
    }

    /// Sidecar router with a custom [`ConsistencyConfig`] (no registry client).
    pub fn with_consistency(config: ConsistencyConfig) -> Self {
        Self {
            mesh: ServiceMesh::with_consistency(config),
            client: None,
        }
    }

    pub fn with_registry(registry_addr: impl Into<String>) -> Self {
        Self {
            mesh: ServiceMesh::new(),
            client: Some(PendingMeshRegistryClient::new(registry_addr)),
        }
    }

    pub fn with_registry_client(client: MeshRegistryClient) -> Self {
        Self {
            mesh: ServiceMesh::new(),
            client: Some(PendingMeshRegistryClient::from_connected(client)),
        }
    }

    pub fn with_registry_and_consistency(
        registry_addr: impl Into<String>,
        config: ConsistencyConfig,
    ) -> Self {
        Self {
            mesh: ServiceMesh::with_consistency(config),
            client: Some(PendingMeshRegistryClient::new(registry_addr)),
        }
    }

    pub fn registry_client(&mut self) -> Option<&mut PendingMeshRegistryClient> {
        self.client.as_mut()
    }

    pub async fn sync(&mut self) -> Result<(), tonic::Status> {
        if let Some(client) = &mut self.client {
            client.sync_mesh(&mut self.mesh).await?;
        }
        Ok(())
    }

    pub async fn invoke<T: Hash>(&self, service: &str, key: &T, msg: M) -> std::io::Result<()> {
        self.mesh.invoke(service, key, msg).await
    }

    pub async fn invoke_consistent<T: Hash>(
        &self,
        service: &str,
        key: &T,
        msg: M,
    ) -> Result<(), ConsistencyError>
    where
        M: Clone,
    {
        self.mesh.invoke_consistent(service, key, msg).await
    }

    /// Read with configured [`ReadConsistency`] (including Paxos for `SERIAL` levels).
    pub async fn read_consistent<T: Hash + std::fmt::Debug>(
        &self,
        service: &str,
        key: &T,
        msg: M,
    ) -> Result<(), ConsistencyError>
    where
        M: Clone,
    {
        self.mesh.read_consistent(service, key, msg).await
    }

    pub async fn read_serial_value<T: Hash + std::fmt::Debug>(
        &self,
        service: &str,
        key: &T,
    ) -> Result<Option<Vec<u8>>, ConsistencyError> {
        self.mesh.read_serial_value(service, key).await
    }

    pub async fn invoke_all(&self, service: &str, msg: M) -> Vec<(String, std::io::Result<()>)>
    where
        M: Clone,
    {
        self.mesh.invoke_all(service, msg).await
    }
}

/// Register instance locally and with the remote gRPC registry (if provided).
pub async fn join_mesh<M: RemoteMessage>(
    mesh: &mut ServiceMesh<M>,
    registry_client: Option<&mut MeshRegistryClient>,
    handle: &MicroserviceHandle<M>,
) -> Result<(), tonic::Status> {
    mesh.register(handle.record.clone());
    if let Some(client) = registry_client {
        client.register(handle.record.clone()).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::{Actor, ActorProcessingErr};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Ping(String);

    struct Echo;

    #[async_trait::async_trait]
    impl Actor<Ping> for Echo {
        async fn handle(&mut self, msg: Ping) -> Result<(), ActorProcessingErr> {
            let _ = msg;
            Ok(())
        }
    }

    #[tokio::test]
    async fn mesh_routes_by_service_name() {
        let a = serve_microservice("orders", "orders-1", "127.0.0.1:0", Echo)
            .await
            .expect("a");
        let b = serve_microservice("orders", "orders-2", "127.0.0.1:0", Echo)
            .await
            .expect("b");

        let mut mesh = ServiceMesh::new();
        mesh.register(a.record.clone());
        mesh.register(b.record.clone());

        assert_eq!(mesh.instance_count("orders"), 2);
        assert_eq!(a.record.target, "orders-1");
        assert_eq!(b.record.target, "orders-2");
        mesh.invoke("orders", &42u64, Ping("x".into()))
            .await
            .expect("invoke");
    }

    #[tokio::test]
    async fn registry_grpc_watch_stream() {
        use crate::proto::control::service_event::Kind;
        use futures_util::StreamExt;

        let server = MeshRegistryHandle::bind("127.0.0.1:0")
            .await
            .expect("registry");
        let mut client = MeshRegistryClient::connect(&server.address)
            .await
            .expect("connect");
        let mut watch = client.watch().await.expect("watch");

        let record = ServiceRecord {
            service: "watch-svc".into(),
            instance_id: "w-1".into(),
            address: "127.0.0.1:1".into(),
            target: "w-1".into(),
            dc: None,
            registered_at: 0,
        };

        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            client.register(record).await.expect("register");
        });

        let event = tokio::time::timeout(std::time::Duration::from_secs(2), watch.next())
            .await
            .expect("timeout")
            .expect("stream")
            .expect("event");
        assert_eq!(event.kind, Kind::Registered as i32);
        assert_eq!(
            event.record.as_ref().expect("record").instance_id,
            "w-1"
        );
    }

    #[tokio::test]
    async fn registry_control_plane() {
        let server = MeshRegistryHandle::bind("127.0.0.1:0")
            .await
            .expect("registry");
        let record = ServiceRecord {
            service: "inventory".into(),
            instance_id: "inv-1".into(),
            address: "127.0.0.1:9999".into(),
            target: "inv-1".into(),
            dc: None,
            registered_at: 0,
        };
        let mut client = MeshRegistryClient::connect(&server.address)
            .await
            .expect("connect");
        client.register(record.clone()).await.expect("register");
        let list = client.list().await.expect("list");
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].service, record.service);
        assert_eq!(list[0].instance_id, record.instance_id);
        assert!(list[0].registered_at > 0);
    }

    #[tokio::test]
    async fn apply_snapshot_diff_keeps_routes_during_sync() {
        let a = serve_microservice("orders", "orders-1", "127.0.0.1:0", Echo)
            .await
            .expect("a");
        let mut mesh: ServiceMesh<Ping> = ServiceMesh::new();
        mesh.register(a.record.clone());
        assert_eq!(mesh.instance_count("orders"), 1);

        mesh.apply_snapshot_diff(vec![a.record.clone()]);
        assert_eq!(mesh.instance_count("orders"), 1);
    }

    #[tokio::test]
    async fn invoke_consistent_quorum_survives_one_replica_loss() {
        let h1 = serve_microservice("echo", "echo-1", "127.0.0.1:0", Echo)
            .await
            .expect("h1");
        let h2 = serve_microservice("echo", "echo-2", "127.0.0.1:0", Echo)
            .await
            .expect("h2");
        let h3 = serve_microservice("echo", "echo-3", "127.0.0.1:0", Echo)
            .await
            .expect("h3");

        let config = ConsistencyConfig {
            rf: 3,
            local_rf: 3,
            write_cl: WriteConsistency::Quorum,
            ack_timeout: Duration::from_secs(2),
            ..ConsistencyConfig::default()
        };
        let mut mesh = ServiceMesh::with_consistency(config);
        mesh.register(h1.record.clone());
        mesh.register(h2.record.clone());
        mesh.register(h3.record.clone());

        let key = "k";
        mesh.invoke_consistent("echo", &key, Ping("all".into()))
            .await
            .expect("quorum with 3 nodes");

        drop(h3);
        mesh.deregister("echo", "echo-3");
        mesh.invoke_consistent("echo", &key, Ping("two".into()))
            .await
            .expect("quorum with 2 of 3");

        drop(h2);
        mesh.deregister("echo", "echo-2");
        let err = mesh
            .invoke_consistent("echo", &key, Ping("one".into()))
            .await
            .expect_err("only one replica left");
        assert!(matches!(
            err,
            ConsistencyError::NotEnoughReplicas { required: 2, available: 1 }
                | ConsistencyError::NotEnoughAcks { required: 2, received: 1, .. }
        ));
    }

    fn record_with_dc(mut record: ServiceRecord, dc: &str) -> ServiceRecord {
        record.dc = Some(dc.into());
        record
    }

    #[tokio::test]
    async fn invoke_each_quorum_requires_quorum_in_every_dc() {
        let east1 = serve_microservice("orders", "east-1", "127.0.0.1:0", Echo)
            .await
            .expect("east1");
        let east2 = serve_microservice("orders", "east-2", "127.0.0.1:0", Echo)
            .await
            .expect("east2");
        let west1 = serve_microservice("orders", "west-1", "127.0.0.1:0", Echo)
            .await
            .expect("west1");
        let west2 = serve_microservice("orders", "west-2", "127.0.0.1:0", Echo)
            .await
            .expect("west2");

        let config = ConsistencyConfig {
            rf: 4,
            local_rf: 2,
            dc_rfs: vec![2, 2],
            dc_names: vec!["east".into(), "west".into()],
            write_cl: WriteConsistency::EachQuorum,
            ack_timeout: Duration::from_millis(500),
            ..ConsistencyConfig::default()
        };
        let mut mesh = ServiceMesh::with_consistency(config);
        mesh.register(record_with_dc(east1.record.clone(), "east"));
        mesh.register(record_with_dc(east2.record.clone(), "east"));
        mesh.register(record_with_dc(west1.record.clone(), "west"));
        mesh.register(record_with_dc(west2.record.clone(), "west"));

        let key = "order-99";
        mesh.invoke_consistent("orders", &key, Ping("write".into()))
            .await
            .expect("each quorum all alive");

        let east2_record = east2.record.clone();
        drop(east2);
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut dead_east = east2_record;
        dead_east.address = "127.0.0.1:1".into();
        mesh.register(dead_east);

        let err = mesh
            .invoke_consistent("orders", &key, Ping("degraded".into()))
            .await
            .expect_err("east lost a replica");
        match err {
            ConsistencyError::NotEnoughAcks {
                dc: Some(dc),
                required: 2,
                ..
            } => assert_eq!(dc, "east"),
            ConsistencyError::NotEnoughReplicas {
                required: 2,
                available: 1,
            } => {}
            ConsistencyError::Timeout { .. } => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[cfg(feature = "metrics")]
    #[tokio::test]
    async fn invoke_consistent_emits_metrics_callback() {
        use std::sync::{Arc, Mutex};

        let captured = Arc::new(Mutex::new(Vec::new()));
        let captured_cb = Arc::clone(&captured);
        let config = ConsistencyConfig {
            rf: 1,
            local_rf: 1,
            write_cl: WriteConsistency::One,
            ack_timeout: Duration::from_secs(2),
            on_metrics: Some(Arc::new(move |m| captured_cb.lock().unwrap().push(m))),
            ..ConsistencyConfig::default()
        };

        let h = serve_microservice("metrics-echo", "m-1", "127.0.0.1:0", Echo)
            .await
            .expect("echo");
        let mut mesh = ServiceMesh::with_consistency(config);
        mesh.register(h.record.clone());

        mesh.invoke_consistent("metrics-echo", &"k", Ping("x".into()))
            .await
            .expect("invoke");

        let events = captured.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert!(events[0].succeeded);
        assert_eq!(events[0].service, "metrics-echo");
        assert_eq!(events[0].acks_required, 1);
    }
}
