//! TCP service mesh: register microservice instances, discover by name, route via hash ring.

use crate::consistency::{
    each_quorum_acks_required, is_local_only, is_local_only_read, is_paxos_read,
    read_acks_required, write_acks_required, ConsistencyConfig, ConsistencyError,
    ReadConsistency, WriteConsistency,
};
use crate::config::DistributedConfig;
use crate::distributed::{
    serve_actor, serve_actor_tls_on_runtime, Cluster, ClusterMember, NodeHandle, RemoteActorRef,
    RemoteMessage, TlsAcceptor,
};
use crate::hash_ring::HashRing;
use crate::paxos::{PaxosProposer, PaxosReplica};
use crate::tls::{self, MaybeTlsStream};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_rustls::TlsConnector;

/// Max inbound control-plane frame size (registration/list replies are small JSON).
const MAX_CONTROL_FRAME: u32 = 64 * 1024;
const CONTROL_READ_TIMEOUT: Duration = Duration::from_secs(30);
/// Records must be renewed via `Register` within this window or the registry evicts them.
pub const DEFAULT_RECORD_TTL: Duration = Duration::from_secs(30);
const EVICTION_INTERVAL: Duration = Duration::from_secs(5);

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

    pub fn with_consistency(consistency: ConsistencyConfig) -> Self {
        Self {
            routes: HashMap::new(),
            consistency,
        }
    }

    pub fn consistency(&self) -> &ConsistencyConfig {
        &self.consistency
    }

    pub fn set_consistency(&mut self, config: ConsistencyConfig) {
        self.consistency = config;
    }

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
        let route = self
            .routes
            .entry(service.clone())
            .or_insert_with(|| ServiceRoute {
                cluster: Cluster::new(),
                consistency: None,
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
        let cluster = &self.route(service).map_err(consistency_from_io)?.cluster;

        if config.write_cl == WriteConsistency::Any {
            cluster
                .send_by_key(key, msg)
                .await
                .map_err(consistency_from_io)?;
            return Ok(());
        }

        if config.write_cl == WriteConsistency::EachQuorum {
            return fan_out_each_quorum(cluster, key, config, msg).await;
        }

        let required = write_acks_required(
            config.write_cl,
            config.rf,
            config.local_rf,
            Some(&config.dc_rfs),
        )?;

        let replicas: Vec<_> = if is_local_only(config.write_cl) {
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

        fan_out_quorum(&replicas, msg, required, config.ack_timeout, None).await
    }

    /// Read-side consistency: wait for R replica acks of receipt (not response data).
    ///
    /// For [`ReadConsistency::Serial`] / [`ReadConsistency::LocalSerial`], runs a Paxos
    /// prepare round instead (see [`Self::read_serial_value`]).
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
        if is_paxos_read(config.read_cl) {
            self.read_serial_value(service, key).await.map(|_| ())
        } else {
            self.read_quorum(service, key, msg, config).await
        }
    }

    /// Linearizable Paxos read returning the highest accepted value for `key`.
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
    ) -> Result<(), ConsistencyError>
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

        fan_out_quorum(&replicas, msg, required, config.ack_timeout, None).await
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

async fn fan_out_quorum<M>(
    replicas: &[&RemoteActorRef<M>],
    msg: M,
    required: usize,
    timeout: Duration,
    dc: Option<String>,
) -> Result<(), ConsistencyError>
where
    M: RemoteMessage + Clone,
{
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

    match count {
        Ok(acks) if acks >= required => Ok(()),
        Ok(acks) => Err(ConsistencyError::NotEnoughAcks {
            required,
            received: acks,
            dc,
        }),
        Err(_) => Err(ConsistencyError::Timeout { after: timeout }),
    }
}

async fn fan_out_each_quorum<M, T>(
    cluster: &Cluster<M>,
    key: &T,
    config: &ConsistencyConfig,
    msg: M,
) -> Result<(), ConsistencyError>
where
    M: RemoteMessage + Clone,
    T: Hash,
{
    let per_dc_required = each_quorum_acks_required(&config.dc_rfs)?;
    let dc_names: Vec<String> = if config.dc_names.is_empty() {
        cluster.datacenters(&config.local_dc)
    } else {
        config.dc_names.clone()
    };

    if dc_names.len() != config.dc_rfs.len() || dc_names.len() != per_dc_required.len() {
        return Err(ConsistencyError::NotEnoughReplicas {
            required: config.dc_rfs.len(),
            available: dc_names.len(),
        });
    }

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

        if replicas.len() < required {
            return Err(ConsistencyError::NotEnoughReplicas {
                required,
                available: replicas.len(),
            });
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
                Ok(Err(e)) => {
                    first_err.get_or_insert(e);
                }
                Ok(Ok(())) => {}
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

    match outcome {
        Ok(None) => Ok(()),
        Ok(Some(e)) => Err(e),
        Err(_) => Err(ConsistencyError::Timeout {
            after: config.ack_timeout,
        }),
    }
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

/// Bind a microservice with TLS on the data plane.
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

// --- TCP control plane (discovery) ---

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

async fn write_control<S: AsyncWrite + Unpin>(
    stream: &mut S,
    msg: &MeshControlMsg,
) -> std::io::Result<()> {
    let body = serde_json::to_vec(msg).map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, e)
    })?;
    if body.len() > MAX_CONTROL_FRAME as usize {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("control frame too large: {} bytes", body.len()),
        ));
    }
    stream.write_u32_le(body.len() as u32).await?;
    stream.write_all(&body).await?;
    stream.flush().await?;
    Ok(())
}

async fn read_control_len<S: AsyncRead + Unpin>(stream: &mut S) -> std::io::Result<Option<u32>> {
    match tokio::time::timeout(CONTROL_READ_TIMEOUT, stream.read_u32_le()).await {
        Ok(Ok(0)) => Ok(None),
        Ok(Ok(n)) => {
            if n > MAX_CONTROL_FRAME {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("control frame too large: {n} bytes"),
                ));
            }
            Ok(Some(n))
        }
        Ok(Err(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "control read timed out",
        )),
    }
}

async fn read_control<S: AsyncRead + Unpin>(stream: &mut S) -> std::io::Result<Option<MeshControlMsg>> {
    let Some(len) = read_control_len(stream).await? else {
        return Ok(None);
    };
    let mut buf = vec![0u8; len as usize];
    tokio::time::timeout(CONTROL_READ_TIMEOUT, stream.read_exact(&mut buf))
        .await
        .map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::TimedOut, "control body read timed out")
        })??;
    let msg = serde_json::from_slice(&buf).map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, e)
    })?;
    Ok(Some(msg))
}

/// Shared mesh registry backing the TCP control plane.
#[derive(Clone)]
pub struct MeshRegistry {
    records: Arc<RwLock<HashMap<(String, String), ServiceRecord>>>,
    ttl: Duration,
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
        Self {
            records: Arc::new(RwLock::new(HashMap::new())),
            ttl,
        }
    }

    pub async fn register(&self, mut record: ServiceRecord) {
        record.registered_at = unix_now();
        let key = (record.service.clone(), record.instance_id.clone());
        self.records.write().await.insert(key, record);
    }

    pub async fn deregister(&self, service: &str, instance_id: &str) -> Option<ServiceRecord> {
        self.records
            .write()
            .await
            .remove(&(service.to_string(), instance_id.to_string()))
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

/// TCP mesh registry (control plane). Microservices register here for discovery.
pub struct MeshRegistryServer {
    pub address: String,
    registry: MeshRegistry,
    _task: JoinHandle<()>,
    _eviction: JoinHandle<()>,
}

impl MeshRegistryServer {
    pub async fn bind(addr: impl Into<String>) -> std::io::Result<Self> {
        Self::bind_on_runtime(addr, None).await
    }

    /// Bind the control plane with TLS.
    pub async fn bind_tls(
        addr: impl Into<String>,
        tls: Arc<TlsAcceptor>,
    ) -> std::io::Result<Self> {
        Self::bind_on_runtime(addr, Some(tls)).await
    }

    async fn bind_on_runtime(
        addr: impl Into<String>,
        tls: Option<Arc<TlsAcceptor>>,
    ) -> std::io::Result<Self> {
        let registry = MeshRegistry::new();
        let listener = TcpListener::bind(addr.into()).await?;
        let address = listener.local_addr()?.to_string();
        let reg = registry.clone();
        let acceptor = tls;
        let tls_enabled = acceptor.is_some();

        let task = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, peer)) => {
                        let reg = reg.clone();
                        let acceptor = acceptor.clone();
                        tokio::spawn(async move {
                            match tls::accept(stream, acceptor.as_deref()).await {
                                Ok(stream) => {
                                    if let Err(e) = handle_registry_conn(stream, reg).await {
                                        tracing::warn!(%peer, error = %e, "mesh registry connection error");
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!(%peer, error = %e, "mesh registry TLS accept failed");
                                }
                            }
                        });
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "mesh registry accept failed");
                        break;
                    }
                }
            }
        });

        let reg_evict = registry.clone();
        let eviction = tokio::spawn(async move {
            let mut interval = tokio::time::interval(EVICTION_INTERVAL);
            loop {
                interval.tick().await;
                reg_evict.evict_expired().await;
            }
        });

        tracing::info!(%address, tls = tls_enabled, "mesh registry listening");
        Ok(Self {
            address,
            registry,
            _task: task,
            _eviction: eviction,
        })
    }

    pub fn registry(&self) -> &MeshRegistry {
        &self.registry
    }
}

async fn handle_registry_conn(
    mut stream: MaybeTlsStream,
    registry: MeshRegistry,
) -> std::io::Result<()> {
    loop {
        let msg = match tokio::time::timeout(CONTROL_READ_TIMEOUT, read_control(&mut stream)).await
        {
            Ok(Ok(None)) => break,
            Ok(Ok(Some(msg))) => msg,
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "control read timed out",
                ))
            }
        };

        match msg {
            MeshControlMsg::Register(record) => {
                tracing::info!(
                    service = %record.service,
                    instance = %record.instance_id,
                    address = %record.address,
                    "mesh register"
                );
                registry.register(record).await;
                write_control(&mut stream, &MeshControlMsg::Pong).await?;
            }
            MeshControlMsg::Deregister { service, instance_id } => {
                registry.deregister(&service, &instance_id).await;
                write_control(&mut stream, &MeshControlMsg::Pong).await?;
            }
            MeshControlMsg::List => {
                let list = registry.list().await;
                write_control(&mut stream, &MeshControlMsg::ListReply(list)).await?;
            }
            MeshControlMsg::Ping => {
                write_control(&mut stream, &MeshControlMsg::Pong).await?;
            }
            MeshControlMsg::Pong | MeshControlMsg::ListReply(_) => {}
        }
    }
    Ok(())
}

/// Client for the TCP mesh registry (discovery) with a persistent connection.
pub struct MeshRegistryClient {
    registry_addr: String,
    stream: Option<MaybeTlsStream>,
    tls: Option<Arc<TlsConnector>>,
}

impl MeshRegistryClient {
    pub fn new(registry_addr: impl Into<String>) -> Self {
        Self {
            registry_addr: registry_addr.into(),
            stream: None,
            tls: None,
        }
    }

    pub fn with_tls(registry_addr: impl Into<String>, tls: Arc<TlsConnector>) -> Self {
        Self {
            registry_addr: registry_addr.into(),
            stream: None,
            tls: Some(tls),
        }
    }

    fn invalidate(&mut self) {
        self.stream = None;
    }

    async fn conn(&mut self) -> std::io::Result<&mut MaybeTlsStream> {
        if self.stream.is_none() {
            self.stream = Some(tls::connect(&self.registry_addr, self.tls.as_deref()).await?);
        }
        Ok(self.stream.as_mut().unwrap())
    }

    async fn request(&mut self, msg: &MeshControlMsg) -> std::io::Result<MeshControlMsg> {
        let result = async {
            let stream = self.conn().await?;
            write_control(stream, msg).await?;
            read_control(stream)
                .await?
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "registry closed connection",
                    )
                })
        }
        .await;

        if result.is_err() {
            self.invalidate();
        }
        result
    }

    pub async fn register(&mut self, record: ServiceRecord) -> std::io::Result<()> {
        match self.request(&MeshControlMsg::Register(record)).await? {
            MeshControlMsg::Pong => Ok(()),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unexpected registry reply",
            )),
        }
    }

    /// Renew a lease by re-registering the same record (resets registry TTL).
    pub async fn renew(&mut self, record: ServiceRecord) -> std::io::Result<()> {
        self.register(record).await
    }

    pub async fn list(&mut self) -> std::io::Result<Vec<ServiceRecord>> {
        match self.request(&MeshControlMsg::List).await? {
            MeshControlMsg::ListReply(list) => Ok(list),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unexpected registry reply",
            )),
        }
    }

    pub async fn sync_mesh<M: RemoteMessage>(
        &mut self,
        mesh: &mut ServiceMesh<M>,
    ) -> std::io::Result<()> {
        mesh.apply_snapshot_diff(self.list().await?);
        Ok(())
    }
}

/// Sidecar router: local mesh view + optional sync from TCP registry.
pub struct MeshRouter<M: RemoteMessage> {
    pub mesh: ServiceMesh<M>,
    client: Option<MeshRegistryClient>,
}

impl<M: RemoteMessage> MeshRouter<M> {
    pub fn local() -> Self {
        Self {
            mesh: ServiceMesh::new(),
            client: None,
        }
    }

    pub fn with_consistency(config: ConsistencyConfig) -> Self {
        Self {
            mesh: ServiceMesh::with_consistency(config),
            client: None,
        }
    }

    pub fn with_registry(registry_addr: impl Into<String>) -> Self {
        Self {
            mesh: ServiceMesh::new(),
            client: Some(MeshRegistryClient::new(registry_addr)),
        }
    }

    pub fn with_registry_tls(registry_addr: impl Into<String>, tls: Arc<TlsConnector>) -> Self {
        Self {
            mesh: ServiceMesh::new(),
            client: Some(MeshRegistryClient::with_tls(registry_addr, tls)),
        }
    }

    pub fn with_registry_and_consistency(
        registry_addr: impl Into<String>,
        config: ConsistencyConfig,
    ) -> Self {
        Self {
            mesh: ServiceMesh::with_consistency(config),
            client: Some(MeshRegistryClient::new(registry_addr)),
        }
    }

    pub fn registry_client(&mut self) -> Option<&mut MeshRegistryClient> {
        self.client.as_mut()
    }

    pub async fn sync(&mut self) -> std::io::Result<()> {
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

/// Register instance locally and with the remote TCP registry (if provided).
pub async fn join_mesh<M: RemoteMessage>(
    mesh: &mut ServiceMesh<M>,
    registry_addr: Option<&str>,
    handle: &MicroserviceHandle<M>,
) -> std::io::Result<()> {
    mesh.register(handle.record.clone());
    if let Some(addr) = registry_addr {
        MeshRegistryClient::new(addr)
            .register(handle.record.clone())
            .await?;
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
    async fn registry_control_plane() {
        let server = MeshRegistryServer::bind("127.0.0.1:0")
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
        let mut client = MeshRegistryClient::new(&server.address);
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
}
