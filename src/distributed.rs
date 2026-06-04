//! Distributed actors over gRPC with protobuf payloads.

use crate::actor::{spawn_on_runtime, Actor, ActorProcessingErr, ActorRef};
use crate::config::{spawn_on, ActorConfig, DistributedConfig};
use crate::consistency::ConsistencyError;
use crate::distributed_grpc::{ActorMessagingService, DispatchTarget};
use crate::hash_ring::{HashRing, RingNode};
use crate::proto::data::{DeliverReply, DeliverRequest};
pub use crate::stream::{TlsAcceptor, TlsConnector};
use futures_util::StreamExt;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

/// Messages that can traverse the gRPC data plane (protobuf-encoded).
pub trait RemoteMessage:
    prost::Message + Default + Send + Sync + Clone + std::fmt::Debug + 'static
{
}

impl<T> RemoteMessage for T where
    T: prost::Message + Default + Send + Sync + Clone + std::fmt::Debug + 'static
{
}

/// HTTP(S) endpoint for [`RemoteActorRef`] / tonic.
pub fn grpc_data_endpoint(addr: &str, tls: bool) -> String {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else if tls {
        format!("https://{addr}")
    } else {
        format!("http://{addr}")
    }
}

fn transport_to_io(e: tonic::transport::Error) -> std::io::Error {
    std::io::Error::other(e)
}

fn actor_messaging_channel(
    addr: &str,
    tls: Option<&crate::config::TlsConfig>,
) -> Result<Channel, std::io::Error> {
    let use_tls = tls.is_some();
    let uri = grpc_data_endpoint(addr, use_tls);
    let endpoint = tonic::transport::Endpoint::from_shared(uri).map_err(transport_to_io)?;
    let domain = crate::grpc_tls::tls_domain_from_addr(addr);
    let endpoint = crate::grpc_tls::apply_client_tls(endpoint, tls, domain)?;
    Ok(endpoint.connect_lazy())
}

type PendingAckMap = HashMap<u64, oneshot::Sender<Result<(), ConsistencyError>>>;

struct BidiStreamState {
    request_tx: mpsc::Sender<DeliverRequest>,
    pending_acks: Arc<Mutex<PendingAckMap>>,
    _reply_task: JoinHandle<()>,
}

/// Reference to an actor on a remote node (persistent gRPC channel + bidi stream).
pub struct RemoteActorRef<M: RemoteMessage> {
    pub node_addr: String,
    target: String,
    client: Arc<
        Mutex<
            crate::proto::data::actor_messaging_client::ActorMessagingClient<Channel>,
        >,
    >,
    stream: Arc<Mutex<Option<BidiStreamState>>>,
    frame_counter: Arc<AtomicU64>,
    ack_timeout: Duration,
    _marker: PhantomData<M>,
}

impl<M: RemoteMessage> Clone for RemoteActorRef<M> {
    fn clone(&self) -> Self {
        Self {
            node_addr: self.node_addr.clone(),
            target: self.target.clone(),
            client: self.client.clone(),
            stream: self.stream.clone(),
            frame_counter: self.frame_counter.clone(),
            ack_timeout: self.ack_timeout,
            _marker: PhantomData,
        }
    }
}

impl<M: RemoteMessage> RemoteActorRef<M> {
    pub fn new(node_addr: impl Into<String>, target: impl Into<String>) -> Self {
        Self::connect(node_addr, target, &DistributedConfig::default())
    }

    pub fn connect(
        node_addr: impl Into<String>,
        target: impl Into<String>,
        config: &DistributedConfig,
    ) -> Self {
        let node_addr = node_addr.into();
        let target = target.into();
        let channel = actor_messaging_channel(&node_addr, config.tls.as_ref())
            .expect("valid grpc data endpoint");
        let client =
            crate::proto::data::actor_messaging_client::ActorMessagingClient::new(channel);
        Self {
            node_addr,
            target,
            client: Arc::new(Mutex::new(client)),
            stream: Arc::new(Mutex::new(None)),
            frame_counter: Arc::new(AtomicU64::new(1)),
            ack_timeout: config.ack_timeout,
            _marker: PhantomData,
        }
    }

    pub fn with_config(
        node_addr: impl Into<String>,
        target: impl Into<String>,
        config: &DistributedConfig,
    ) -> Self {
        Self::connect(node_addr, target, config)
    }

    pub fn target(&self) -> &str {
        &self.target
    }

    async fn ensure_stream(&self) -> Result<(), std::io::Error> {
        let mut guard = self.stream.lock().await;
        if guard.is_some() {
            return Ok(());
        }

        let mut client = self.client.lock().await;
        let (req_tx, req_rx) = mpsc::channel(64);
        let outbound = ReceiverStream::new(req_rx);
        let response = client
            .deliver(outbound)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::NotConnected, e))?;
        let mut reply_stream = response.into_inner();
        let pending = Arc::new(Mutex::new(HashMap::<
            u64,
            oneshot::Sender<Result<(), ConsistencyError>>,
        >::new()));

        let pending_c = pending.clone();
        let reply_task = tokio::spawn(async move {
            while let Some(item) = reply_stream.next().await {
                let reply: DeliverReply = match item {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let mut map = pending_c.lock().await;
                if let Some(tx) = map.remove(&reply.frame_id) {
                    let result = if reply.ok {
                        Ok(())
                    } else {
                        Err(ConsistencyError::NotEnoughAcks {
                            required: 1,
                            received: 0,
                            dc: None,
                        })
                    };
                    let _ = tx.send(result);
                }
            }
        });

        *guard = Some(BidiStreamState {
            request_tx: req_tx,
            pending_acks: pending,
            _reply_task: reply_task,
        });
        Ok(())
    }

    async fn invalidate_stream(&self) {
        self.stream.lock().await.take();
    }

    fn encode_payload(&self, msg: &M) -> Result<Vec<u8>, std::io::Error> {
        let mut buf = Vec::new();
        msg.encode(&mut buf)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Ok(buf)
    }

    pub async fn send(&self, msg: M) -> std::io::Result<()> {
        let frame_id = self.frame_counter.fetch_add(1, Ordering::Relaxed);
        let payload = self.encode_payload(&msg)?;
        let request = DeliverRequest {
            frame_id,
            target: self.target.clone(),
            payload,
            expect_ack: false,
        };

        for attempt in 0..2 {
            if self.ensure_stream().await.is_err() && attempt == 0 {
                self.invalidate_stream().await;
                continue;
            }
            let tx = {
                let guard = self.stream.lock().await;
                guard.as_ref().map(|s| s.request_tx.clone())
            };
            let Some(tx) = tx else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    "deliver stream unavailable",
                ));
            };
            match tx.send(request.clone()).await {
                Ok(()) => return Ok(()),
                Err(_) if attempt == 0 => {
                    self.invalidate_stream().await;
                    continue;
                }
                Err(_) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::NotConnected,
                        "deliver stream closed",
                    ));
                }
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::NotConnected,
            "deliver send failed",
        ))
    }

    /// Send with acknowledgement; waits up to `timeout` for the remote node to confirm dispatch.
    pub async fn send_with_ack(
        &self,
        msg: M,
        timeout: Duration,
    ) -> Result<(), ConsistencyError> {
        let frame_id = self.frame_counter.fetch_add(1, Ordering::Relaxed);
        let payload = self
            .encode_payload(&msg)
            .map_err(|_| ConsistencyError::NotEnoughAcks {
                required: 1,
                received: 0,
                dc: None,
            })?;
        let request = DeliverRequest {
            frame_id,
            target: self.target.clone(),
            payload,
            expect_ack: true,
        };

        for attempt in 0..2 {
            self.ensure_stream().await.map_err(|_| {
                ConsistencyError::NotEnoughAcks {
                    required: 1,
                    received: 0,
                    dc: None,
                }
            })?;

            let (ack_tx, ack_rx) = oneshot::channel();
            let sent = {
                let guard = self.stream.lock().await;
                let Some(state) = guard.as_ref() else {
                    self.invalidate_stream().await;
                    if attempt == 0 {
                        continue;
                    }
                    return Err(ConsistencyError::NotEnoughAcks {
                        required: 1,
                        received: 0,
                        dc: None,
                    });
                };
                state
                    .pending_acks
                    .lock()
                    .await
                    .insert(frame_id, ack_tx);
                state.request_tx.send(request.clone()).await.is_ok()
            };

            if !sent {
                self.invalidate_stream().await;
                if attempt == 0 {
                    continue;
                }
                return Err(ConsistencyError::NotEnoughAcks {
                    required: 1,
                    received: 0,
                    dc: None,
                });
            }

            return match tokio::time::timeout(timeout, ack_rx).await {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => Err(ConsistencyError::NotEnoughAcks {
                    required: 1,
                    received: 0,
                    dc: None,
                }),
                Err(_) => Err(ConsistencyError::Timeout { after: timeout }),
            };
        }

        Err(ConsistencyError::NotEnoughAcks {
            required: 1,
            received: 0,
            dc: None,
        })
    }
}

/// Local node: binds gRPC and dispatches incoming deliveries to registered actors.
pub struct Node<M: RemoteMessage> {
    name: String,
    bind_addr: String,
    dispatch: Arc<Mutex<HashMap<String, DispatchTarget<M>>>>,
    active_streams: Arc<AtomicUsize>,
    _listener: JoinHandle<()>,
}

impl<M: RemoteMessage> Node<M> {
    pub async fn bind(name: impl Into<String>, addr: impl Into<String>) -> std::io::Result<Self> {
        Self::bind_on_current_runtime(name, addr, &DistributedConfig::default()).await
    }

    pub async fn bind_on_current_runtime(
        name: impl Into<String>,
        addr: impl Into<String>,
        config: &DistributedConfig,
    ) -> std::io::Result<Self> {
        Self::bind_on_runtime(&Handle::current(), name, addr, config).await
    }

    pub async fn bind_on_runtime(
        runtime: &Handle,
        name: impl Into<String>,
        addr: impl Into<String>,
        config: &DistributedConfig,
    ) -> std::io::Result<Self> {
        let name = name.into();
        let bind_addr = addr.into();
        let listener = TcpListener::bind(&bind_addr).await?;
        let actual_addr = listener.local_addr()?;
        tracing::info!(
            node = %name,
            %actual_addr,
            tls = config.tls.is_some(),
            "distributed node listening"
        );

        let dispatch = Arc::new(Mutex::new(HashMap::<String, DispatchTarget<M>>::new()));
        let active_streams = Arc::new(AtomicUsize::new(0));
        let svc = ActorMessagingService::new(dispatch.clone(), active_streams.clone());
        let grpc =
            crate::proto::data::actor_messaging_server::ActorMessagingServer::new(svc);

        #[cfg(feature = "tls")]
        let server_tls = config.tls.clone();
        let listener_task = spawn_on(Some(runtime), async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            // TODO: expose concurrency_limit_per_connection
            #[cfg(feature = "tls")]
            let tls_ref = server_tls.as_ref();
            #[cfg(not(feature = "tls"))]
            let tls_ref: Option<&crate::config::TlsConfig> = None;
            if let Err(e) = crate::grpc_tls::apply_server_tls(
                tonic::transport::Server::builder(),
                tls_ref,
            )
            .add_service(grpc)
            .serve_with_incoming(incoming)
            .await
            {
                tracing::error!(error = %e, "gRPC node server exited");
            }
        });

        Ok(Self {
            name,
            bind_addr: actual_addr.to_string(),
            dispatch,
            active_streams,
            _listener: listener_task,
        })
    }

    /// Active bidirectional gRPC deliver streams on this node.
    pub fn connected_channels(&self) -> usize {
        self.active_streams.load(Ordering::Relaxed)
    }

    pub fn address(&self) -> &str {
        &self.bind_addr
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub async fn register_actor(&self, target: impl Into<String>, actor: ActorRef<M>) {
        self.dispatch
            .lock()
            .await
            .insert(target.into(), DispatchTarget::Actor(actor));
    }

    /// Register a raw mailbox sender (tests / custom wiring).
    pub async fn register(&self, target: impl Into<String>, tx: mpsc::Sender<M>) {
        self.dispatch
            .lock()
            .await
            .insert(target.into(), DispatchTarget::Mailbox(tx));
    }

    pub async fn unregister(&self, target: &str) {
        self.dispatch.lock().await.remove(target);
    }
}

/// A node in a cluster roster (name + gRPC address + deliver target).
#[derive(Debug, Clone)]
pub struct ClusterMember {
    pub name: String,
    pub node_addr: String,
    pub target: String,
    /// Datacenter tag; `None` is treated as the local DC.
    pub dc: Option<String>,
}

impl ClusterMember {
    pub fn new(
        name: impl Into<String>,
        node_addr: impl Into<String>,
        target: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            node_addr: node_addr.into(),
            target: target.into(),
            dc: None,
        }
    }

    pub fn with_dc(mut self, dc: impl Into<String>) -> Self {
        self.dc = Some(dc.into());
        self
    }

    pub fn remote_ref<M: RemoteMessage>(&self) -> RemoteActorRef<M> {
        RemoteActorRef::new(&self.node_addr, &self.target)
    }

    pub fn remote_ref_with<M: RemoteMessage>(
        &self,
        config: &DistributedConfig,
    ) -> RemoteActorRef<M> {
        RemoteActorRef::with_config(&self.node_addr, &self.target, config)
    }

    pub fn ring_node(&self) -> RingNode {
        RingNode::from_socket_addr(&self.name, &self.node_addr)
    }
}

/// Roster of remote actors — join nodes as they come online, dispatch by hash ring or round-robin.
pub struct Cluster<M: RemoteMessage> {
    members: Vec<ClusterMember>,
    refs: Vec<RemoteActorRef<M>>,
    refs_by_id: HashMap<String, usize>,
    ring: HashRing,
    next: AtomicUsize,
    pub(crate) distributed_config: DistributedConfig,
}

impl<M: RemoteMessage> Default for Cluster<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M: RemoteMessage> Cluster<M> {
    pub fn new() -> Self {
        Self::with_virtual_nodes(150)
    }

    pub fn with_virtual_nodes(virtual_nodes: u32) -> Self {
        Self {
            members: Vec::new(),
            refs: Vec::new(),
            refs_by_id: HashMap::new(),
            ring: HashRing::new(virtual_nodes),
            next: AtomicUsize::new(0),
            distributed_config: DistributedConfig::default(),
        }
    }

    pub fn set_distributed_config(&mut self, config: DistributedConfig) {
        self.distributed_config = config;
    }

    pub fn len(&self) -> usize {
        self.refs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.refs.is_empty()
    }

    pub fn members(&self) -> &[ClusterMember] {
        &self.members
    }

    pub fn ring(&self) -> &HashRing {
        &self.ring
    }

    /// Add a remote node to the roster and hash ring (existing nodes keep running).
    pub fn join(&mut self, member: ClusterMember) {
        let idx = self.refs.len();
        self.ring.add_node(member.ring_node());
        self.refs_by_id.insert(member.name.clone(), idx);
        self.refs
            .push(member.remote_ref_with(&self.distributed_config));
        self.members.push(member);
    }

    /// Remove a node from the roster and hash ring.
    pub fn leave(&mut self, node_id: &str) -> Option<ClusterMember> {
        self.ring.remove_node(node_id)?;
        let idx = self.refs_by_id.remove(node_id)?;
        self.refs.swap_remove(idx);
        let member = self.members.swap_remove(idx);
        if idx < self.members.len() {
            self.refs_by_id
                .insert(self.members[idx].name.clone(), idx);
        }
        Some(member)
    }

    /// Lookup cluster member for a key via the hash ring.
    pub fn member_for_key<T: Hash>(&self, key: &T) -> Option<&ClusterMember> {
        let node = self.ring.get_node(key)?;
        self.members.iter().find(|m| m.name == node.id)
    }

    /// Lookup remote ref for a key via the hash ring.
    pub fn ref_for_key<T: Hash>(&self, key: &T) -> Option<&RemoteActorRef<M>> {
        let node = self.ring.get_node(key)?;
        self.refs_by_id.get(&node.id).map(|&i| &self.refs[i])
    }

    /// Send to the node selected by consistent hash of `key`.
    pub async fn send_by_key<T: Hash>(&self, key: &T, msg: M) -> std::io::Result<()> {
        match self.ref_for_key(key) {
            Some(remote) => remote.send(msg).await,
            None => Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "cluster has no members",
            )),
        }
    }

    /// Round-robin index for the next send (does not advance on error).
    ///
    /// Uses `fetch_add % len`; u64 wraparound skew is negligible in practice.
    pub fn next(&self) -> std::io::Result<&RemoteActorRef<M>> {
        if self.refs.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "cluster has no members",
            ));
        }
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.refs.len();
        Ok(&self.refs[idx])
    }

    pub async fn send_round_robin(&self, msg: M) -> std::io::Result<()> {
        self.next()?.send(msg).await
    }

    /// Send to every member; returns the first error after attempting all nodes.
    pub async fn broadcast(&self, msg: M) -> std::io::Result<()>
    where
        M: Clone,
    {
        let mut first_err = None;
        for (_, result) in self.send_all(msg).await {
            if let Err(e) = result {
                first_err.get_or_insert(e);
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Send to every member; returns per-node results (continues after individual failures).
    pub async fn send_all(&self, msg: M) -> Vec<(String, std::io::Result<()>)>
    where
        M: Clone,
    {
        let mut results = Vec::with_capacity(self.members.len());
        for (member, remote) in self.members.iter().zip(self.refs.iter()) {
            let result = remote.send(msg.clone()).await;
            results.push((member.name.clone(), result));
        }
        results
    }

    /// Send to a subset of members by node name.
    pub async fn send_to(&self, names: &[&str], msg: M) -> Vec<(String, std::io::Result<()>)>
    where
        M: Clone,
    {
        let mut results = Vec::new();
        for name in names {
            if let Some(&idx) = self.refs_by_id.get(*name) {
                let result = self.refs[idx].send(msg.clone()).await;
                results.push(((*name).to_string(), result));
            }
        }
        results
    }

    /// Send to the primary node for `key` plus the next `count - 1` nodes on the hash ring.
    pub async fn send_replicas<T: Hash>(&self, key: &T, count: usize, msg: M) -> Vec<(String, std::io::Result<()>)>
    where
        M: Clone,
    {
        let mut results = Vec::new();
        for node in self.ring.get_nodes(key, count) {
            if let Some(&idx) = self.refs_by_id.get(&node.id) {
                let result = self.refs[idx].send(msg.clone()).await;
                results.push((node.id.clone(), result));
            }
        }
        results
    }

    pub fn ref_by_name(&self, name: &str) -> Option<&RemoteActorRef<M>> {
        self.refs_by_id.get(name).map(|&idx| &self.refs[idx])
    }

    /// Replica refs for `key` walking the hash ring (up to `count`).
    pub fn replicas_for_key<T: Hash>(&self, key: &T, count: usize) -> Vec<&RemoteActorRef<M>> {
        self.ring
            .get_nodes(key, count)
            .into_iter()
            .filter_map(|node| self.refs_by_id.get(&node.id).map(|&i| &self.refs[i]))
            .collect()
    }

    /// All replica refs for `key` (every cluster member on the ring).
    pub fn all_replicas_for_key<T: Hash>(&self, key: &T) -> Vec<&RemoteActorRef<M>> {
        self.replicas_for_key(key, self.refs.len())
    }

    /// Replica refs for `key` limited to the local datacenter.
    ///
    /// Members with `dc = None` are treated as local. `local_dc` names the caller's DC.
    pub fn local_replicas_for_key<T: Hash>(
        &self,
        key: &T,
        count: usize,
        local_dc: &str,
    ) -> Vec<&RemoteActorRef<M>> {
        self.ring
            .get_nodes(key, self.refs.len())
            .into_iter()
            .filter_map(|node| {
                let idx = *self.refs_by_id.get(&node.id)?;
                if member_is_local_dc(&self.members[idx], local_dc) {
                    Some(&self.refs[idx])
                } else {
                    None
                }
            })
            .take(count)
            .collect()
    }

    /// Remote refs for members tagged with datacenter `dc`.
    pub fn dc_members(&self, dc: &str) -> Vec<&RemoteActorRef<M>> {
        self.members
            .iter()
            .enumerate()
            .filter(|(_, m)| m.dc.as_deref() == Some(dc))
            .map(|(i, _)| &self.refs[i])
            .collect()
    }

    /// Hash-ring replica refs for `key` limited to datacenter `dc`.
    ///
    /// Members with `dc = None` are treated as belonging to `local_dc`.
    /// When the ring yields fewer than `count` nodes, remaining slots are filled
    /// from [`Self::dc_members`] / untagged local members.
    pub fn dc_replicas_for_key<T: Hash>(
        &self,
        key: &T,
        dc: &str,
        local_dc: &str,
        count: usize,
    ) -> Vec<&RemoteActorRef<M>> {
        let mut out = Vec::new();
        let mut seen = HashSet::new();

        for node in self.ring.get_nodes(key, self.refs.len()) {
            if out.len() >= count {
                break;
            }
            let Some(&idx) = self.refs_by_id.get(&node.id) else {
                continue;
            };
            if member_effective_dc(&self.members[idx], local_dc) != dc {
                continue;
            }
            if seen.insert(idx) {
                out.push(&self.refs[idx]);
            }
        }

        if out.len() < count {
            for (idx, member) in self.members.iter().enumerate() {
                if out.len() >= count {
                    break;
                }
                let in_dc = match &member.dc {
                    Some(name) => name == dc,
                    None if dc == local_dc => true,
                    None => false,
                };
                if in_dc && seen.insert(idx) {
                    out.push(&self.refs[idx]);
                }
            }
        }

        out
    }

    /// Distinct datacenter names in this cluster (`dc = None` → `local_dc`).
    pub fn datacenters(&self, local_dc: &str) -> Vec<String> {
        let mut dcs: Vec<String> = self
            .members
            .iter()
            .map(|m| member_effective_dc(m, local_dc).to_string())
            .collect();
        dcs.sort();
        dcs.dedup();
        dcs
    }
}

fn member_effective_dc<'a>(member: &'a ClusterMember, local_dc: &'a str) -> &'a str {
    match &member.dc {
        None => local_dc,
        Some(dc) => dc.as_str(),
    }
}

fn member_is_local_dc(member: &ClusterMember, local_dc: &str) -> bool {
    match &member.dc {
        None => true,
        Some(dc) => dc == local_dc,
    }
}

/// Local gRPC node serving one actor target — use `member()` to join a [`Cluster`].
pub struct NodeHandle<M: RemoteMessage> {
    pub member: ClusterMember,
    _node: Node<M>,
}

impl<M: RemoteMessage> NodeHandle<M> {
    pub fn address(&self) -> &str {
        &self.member.node_addr
    }

    pub fn name(&self) -> &str {
        &self.member.name
    }
}

/// Bind a gRPC node and serve actor deliveries on the current runtime.
pub async fn serve_actor<M, A>(
    node_name: impl Into<String>,
    bind_addr: impl Into<String>,
    target: impl Into<String>,
    actor: A,
) -> std::io::Result<NodeHandle<M>>
where
    M: RemoteMessage,
    A: Actor<M> + Send + Sync + 'static,
{
    serve_actor_on_runtime(
        &Handle::current(),
        node_name,
        bind_addr,
        target,
        actor,
        &DistributedConfig::default(),
        &ActorConfig::default(),
    )
    .await
}

/// Bind a gRPC node on the current runtime with explicit channel sizing.
pub async fn serve_actor_on_current_runtime<M, A>(
    node_name: impl Into<String>,
    bind_addr: impl Into<String>,
    target: impl Into<String>,
    actor: A,
    distributed: &DistributedConfig,
    actor_config: &ActorConfig,
) -> std::io::Result<NodeHandle<M>>
where
    M: RemoteMessage,
    A: Actor<M> + Send + Sync + 'static,
{
    serve_actor_on_runtime(
        &Handle::current(),
        node_name,
        bind_addr,
        target,
        actor,
        distributed,
        actor_config,
    )
    .await
}

/// Bind a gRPC node on a dedicated runtime.
pub async fn serve_actor_on_runtime<M, A>(
    runtime: &Handle,
    node_name: impl Into<String>,
    bind_addr: impl Into<String>,
    target: impl Into<String>,
    actor: A,
    distributed: &DistributedConfig,
    actor_config: &ActorConfig,
) -> std::io::Result<NodeHandle<M>>
where
    M: RemoteMessage,
    A: Actor<M> + Send + Sync + 'static,
{
    let node_name = node_name.into();
    let target = target.into();
    let actor_config = *actor_config;

    let (actor_ref, _actor_join) = spawn_on_runtime(runtime, actor, None, &actor_config)
        .await
        .map_err(|e: ActorProcessingErr| {
            std::io::Error::other(format!("failed to spawn bridged actor: {e}"))
        })?;

    let node = Node::<M>::bind_on_runtime(runtime, &node_name, bind_addr, distributed).await?;
    let address = node.address().to_string();

    node.register_actor(&target, actor_ref).await;

    Ok(NodeHandle {
        member: ClusterMember {
            name: node_name,
            node_addr: address,
            target,
            dc: None,
        },
        _node: node,
    })
}

#[cfg(test)]
mod ack_tests {
    use super::*;
    use crate::actor::{spawn, Actor, ActorProcessingErr};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    #[derive(Clone, PartialEq, prost::Message)]
    struct RemotePing {
        #[prost(uint64, tag = "1")]
        n: u64,
    }

    struct PingCounter(Arc<AtomicU64>);

    #[async_trait::async_trait]
    impl Actor<RemotePing> for PingCounter {
        async fn handle(&mut self, msg: RemotePing) -> Result<(), ActorProcessingErr> {
            self.0.fetch_add(1, Ordering::Relaxed);
            let _ = msg;
            Ok(())
        }
    }

    #[tokio::test]
    async fn data_plane_grpc_send_burst_and_ack() {
        let counter = Arc::new(AtomicU64::new(0));
        let handle = serve_actor(
            "echo",
            "127.0.0.1:0",
            "echo",
            PingCounter(counter.clone()),
        )
        .await
        .expect("serve");

        let remote = RemoteActorRef::<RemotePing>::new(&handle.member.node_addr, "echo");
        for i in 0..100u64 {
            remote
                .send(RemotePing { n: i })
                .await
                .expect("send");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(counter.load(Ordering::Relaxed), 100);

        remote
            .send_with_ack(RemotePing { n: 999 }, Duration::from_secs(2))
            .await
            .expect("ack");

        let bad = RemoteActorRef::<RemotePing>::new(&handle.member.node_addr, "missing");
        let err = bad
            .send_with_ack(RemotePing { n: 1 }, Duration::from_millis(500))
            .await
            .expect_err("dead target");
        assert!(
            matches!(err, ConsistencyError::NotEnoughAcks { .. })
                || matches!(err, ConsistencyError::Timeout { .. })
        );
    }

    #[tokio::test]
    async fn send_with_ack_round_trip() {
        let counter = Arc::new(AtomicU64::new(0));
        let handle = serve_actor(
            "ack-node",
            "127.0.0.1:0",
            "worker",
            PingCounter(counter.clone()),
        )
        .await
        .expect("serve");

        let remote = RemoteActorRef::<RemotePing>::new(&handle.member.node_addr, "worker");
        remote
            .send_with_ack(RemotePing { n: 1 }, Duration::from_secs(2))
            .await
            .expect("ack");

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn send_with_ack_times_out_when_mailbox_full() {
        let node = Node::<RemotePing>::bind("blocked", "127.0.0.1:0")
            .await
            .expect("bind");
        let addr = node.address().to_string();

        // Capacity 1, no consumer — second ack'd deliver blocks until mailbox drains.
        let (tx, _rx) = mpsc::channel(1);
        node.register("worker", tx).await;

        let remote = RemoteActorRef::<RemotePing>::new(&addr, "worker");
        remote
            .send_with_ack(RemotePing { n: 1 }, Duration::from_secs(2))
            .await
            .expect("first ack");

        let err = remote
            .send_with_ack(RemotePing { n: 2 }, Duration::from_millis(200))
            .await
            .expect_err("timeout");
        assert!(matches!(err, ConsistencyError::Timeout { .. }));
    }

    #[tokio::test]
    async fn fire_and_forget_send_without_ack() {
        let counter = Arc::new(AtomicU64::new(0));
        let (actor_ref, _join) = spawn(PingCounter(counter.clone()), None)
            .await
            .expect("spawn");

        let node = Node::<RemotePing>::bind("ff", "127.0.0.1:0")
            .await
            .expect("bind");
        let addr = node.address().to_string();
        let (tx, mut rx) = mpsc::channel(8);
        node.register("worker", tx).await;

        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                let _ = actor_ref.send(msg).await;
            }
        });

        let remote = RemoteActorRef::<RemotePing>::new(&addr, "worker");
        remote.send(RemotePing { n: 7 }).await.expect("send");

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn replicas_for_key_and_local_dc_filter() {
        let a = serve_actor(
            "a",
            "127.0.0.1:0",
            "worker",
            PingCounter(Arc::new(AtomicU64::new(0))),
        )
        .await
        .expect("a");
        let b = serve_actor(
            "b",
            "127.0.0.1:0",
            "worker",
            PingCounter(Arc::new(AtomicU64::new(0))),
        )
        .await
        .expect("b");
        let c = serve_actor(
            "c",
            "127.0.0.1:0",
            "worker",
            PingCounter(Arc::new(AtomicU64::new(0))),
        )
        .await
        .expect("c");

        let mut cluster = Cluster::<RemotePing>::new();
        cluster.join(a.member.clone());
        cluster.join(
            ClusterMember::new("b-west", &b.member.node_addr, "worker").with_dc("west"),
        );
        cluster.join(
            ClusterMember::new("c-east", &c.member.node_addr, "worker").with_dc("east"),
        );

        let key = "user-123";
        assert_eq!(cluster.replicas_for_key(&key, 2).len(), 2);
        assert_eq!(cluster.all_replicas_for_key(&key).len(), 3);

        let local = cluster.local_replicas_for_key(&key, 2, "east");
        assert!(local.len() <= 2);
        for r in local {
            assert!(r.target() == "worker");
        }

        // None dc counts as local
        let local_default = cluster.local_replicas_for_key(&key, 3, "any");
        assert!(!local_default.is_empty());
    }

    #[tokio::test]
    async fn dc_members_groups_by_tag() {
        let a = serve_actor(
            "a",
            "127.0.0.1:0",
            "worker",
            PingCounter(Arc::new(AtomicU64::new(0))),
        )
        .await
        .expect("a");
        let b = serve_actor(
            "b",
            "127.0.0.1:0",
            "worker",
            PingCounter(Arc::new(AtomicU64::new(0))),
        )
        .await
        .expect("b");

        let mut cluster = Cluster::<RemotePing>::new();
        cluster.join(a.member.clone());
        cluster.join(
            ClusterMember::new("b-west", &b.member.node_addr, "worker").with_dc("west"),
        );

        assert_eq!(cluster.dc_members("west").len(), 1);
        assert_eq!(cluster.datacenters("local"), vec!["local", "west"]);
    }

    #[tokio::test]
    async fn dc_replicas_for_key_includes_all_dc_members() {
        let mut records = Vec::new();
        for (id, dc) in [
            ("east-1", "east"),
            ("east-2", "east"),
            ("west-1", "west"),
            ("west-2", "west"),
        ] {
            let h = serve_actor(
                id,
                "127.0.0.1:0",
                "worker",
                PingCounter(Arc::new(AtomicU64::new(0))),
            )
            .await
            .expect("serve");
            let mut member = h.member.clone();
            member.dc = Some(dc.into());
            records.push((member, dc));
        }

        let mut cluster = Cluster::<RemotePing>::new();
        for (member, _) in &records {
            cluster.join(member.clone());
        }

        let key = "order-99";
        assert_eq!(
            cluster.dc_replicas_for_key(&key, "east", "local", 2).len(),
            2
        );
        assert_eq!(
            cluster.dc_replicas_for_key(&key, "west", "local", 2).len(),
            2
        );
    }
}
