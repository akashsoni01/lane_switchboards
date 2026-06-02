//! Distributed actors over TCP with length-prefixed JSON frames.

use crate::actor::{spawn_on_runtime, Actor, ActorProcessingErr};
use crate::config::{spawn_on, ActorConfig, DistributedConfig};
use crate::hash_ring::{HashRing, RingNode};
use crate::tls::{self, MaybeTlsStream};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio::task::JoinHandle;
use tokio_rustls::TlsConnector;
pub use tokio_rustls::TlsAcceptor;

/// Messages that can traverse the network layer.
pub trait RemoteMessage:
    Serialize + DeserializeOwned + Send + Sync + Clone + std::fmt::Debug + 'static
{
}

impl<T> RemoteMessage for T where
    T: Serialize + DeserializeOwned + Send + Sync + Clone + std::fmt::Debug + 'static
{
}

/// Wire frame: route by actor name on the remote node.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Frame {
    pub target: String,
    pub payload: serde_json::Value,
}

#[derive(Serialize)]
struct FrameOut<'a, M>
where
    M: Serialize,
{
    target: &'a str,
    payload: &'a M,
}

async fn write_frame<M, S: AsyncWrite + Unpin>(
    stream: &mut S,
    target: &str,
    msg: &M,
    max_frame_bytes: u32,
) -> std::io::Result<()>
where
    M: RemoteMessage,
{
    let body = serde_json::to_vec(&FrameOut { target, payload: msg }).map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, e)
    })?;
    if body.len() > max_frame_bytes as usize {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("frame too large: {} bytes", body.len()),
        ));
    }
    stream.write_u32_le(body.len() as u32).await?;
    stream.write_all(&body).await?;
    stream.flush().await?;
    Ok(())
}

async fn remote_write_loop<M: RemoteMessage>(
    node_addr: String,
    target: String,
    mut rx: mpsc::Receiver<M>,
    config: DistributedConfig,
    tls: Option<Arc<TlsConnector>>,
) {
    const RECONNECT_DELAY: Duration = Duration::from_millis(500);

    loop {
        let mut stream = loop {
            match tls::connect(&node_addr, tls.as_deref()).await {
                Ok(s) => break s,
                Err(e) => {
                    tracing::warn!(%node_addr, error = %e, "remote write reconnect failed");
                    tokio::time::sleep(RECONNECT_DELAY).await;
                    if rx.is_closed() {
                        return;
                    }
                }
            }
        };

        loop {
            let msg = match rx.recv().await {
                Some(m) => m,
                None => return,
            };
            if write_frame(&mut stream, &target, &msg, config.max_frame_bytes)
                .await
                .is_err()
            {
                tracing::warn!(%node_addr, "remote write failed — reconnecting");
                break;
            }
        }
    }
}

struct PeerWriter<M: RemoteMessage> {
    tx: mpsc::Sender<M>,
    _task: JoinHandle<()>,
}

/// Reference to an actor on a remote node (persistent write channel per ref).
#[derive(Clone)]
pub struct RemoteActorRef<M: RemoteMessage> {
    pub node_addr: String,
    target: String,
    writer: Arc<PeerWriter<M>>,
}

impl<M: RemoteMessage> RemoteActorRef<M> {
    pub fn new(node_addr: impl Into<String>, target: impl Into<String>) -> Self {
        Self::with_config(node_addr, target, &DistributedConfig::default(), None)
    }

    pub fn with_tls(
        node_addr: impl Into<String>,
        target: impl Into<String>,
        config: &DistributedConfig,
        tls: Arc<TlsConnector>,
    ) -> Self {
        Self::with_config(node_addr, target, config, Some(tls))
    }

    pub fn with_config(
        node_addr: impl Into<String>,
        target: impl Into<String>,
        config: &DistributedConfig,
        tls: Option<Arc<TlsConnector>>,
    ) -> Self {
        let node_addr = node_addr.into();
        let target = target.into();
        let (tx, rx) = mpsc::channel(config.remote_send_capacity.max(1));
        let loop_addr = node_addr.clone();
        let loop_target = target.clone();
        let loop_config = *config;
        let loop_tls = tls.clone();
        let task = tokio::spawn(remote_write_loop(
            loop_addr,
            loop_target,
            rx,
            loop_config,
            loop_tls,
        ));
        Self {
            node_addr,
            target,
            writer: Arc::new(PeerWriter { tx, _task: task }),
        }
    }

    pub async fn send(&self, msg: M) -> std::io::Result<()> {
        self.writer
            .tx
            .send(msg)
            .await
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    "remote peer write channel closed",
                )
            })
    }
}

/// Local node: binds TCP and dispatches incoming frames to registered actors.
pub struct Node<M: RemoteMessage> {
    name: String,
    bind_addr: String,
    dispatch: Arc<Mutex<HashMap<String, mpsc::Sender<M>>>>,
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
        Self::bind_on_runtime(&Handle::current(), name, addr, config, None).await
    }

    /// Bind with TLS (server certificate required).
    pub async fn bind_tls_on_runtime(
        runtime: &Handle,
        name: impl Into<String>,
        addr: impl Into<String>,
        config: &DistributedConfig,
        tls: Arc<TlsAcceptor>,
    ) -> std::io::Result<Self> {
        Self::bind_on_runtime(runtime, name, addr, config, Some(tls)).await
    }

    pub async fn bind_on_runtime(
        runtime: &Handle,
        name: impl Into<String>,
        addr: impl Into<String>,
        config: &DistributedConfig,
        tls: Option<Arc<TlsAcceptor>>,
    ) -> std::io::Result<Self> {
        let name = name.into();
        let bind_addr = addr.into();
        let listener = TcpListener::bind(&bind_addr).await?;
        let actual_addr = listener.local_addr()?;
        tracing::info!(
            node = %name,
            %actual_addr,
            tls = tls.is_some(),
            "distributed node listening"
        );

        let dispatch = Arc::new(Mutex::new(HashMap::<String, mpsc::Sender<M>>::new()));
        let dispatch_c = dispatch.clone();
        let in_flight = Arc::new(Semaphore::new(config.max_in_flight.max(1)));
        let conn_config = *config;
        let runtime = runtime.clone();
        let tls_acceptor = tls;

        let listener_task = spawn_on(Some(&runtime), {
            let runtime = runtime.clone();
            async move {
                loop {
                    match listener.accept().await {
                        Ok((stream, peer)) => {
                            let table = dispatch_c.clone();
                            let in_flight = in_flight.clone();
                            let conn_runtime = runtime.clone();
                            let acceptor = tls_acceptor.clone();
                            spawn_on(Some(&runtime), async move {
                                match tls::accept(stream, acceptor.as_deref()).await {
                                    Ok(stream) => {
                                        if let Err(e) = handle_conn(
                                            stream,
                                            table,
                                            in_flight,
                                            conn_runtime,
                                            conn_config,
                                        )
                                        .await
                                        {
                                            tracing::warn!(%peer, error = %e, "connection handler error");
                                        }
                                    }
                                    Err(e) => {
                                        tracing::warn!(%peer, error = %e, "TLS accept failed");
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "accept failed");
                            break;
                        }
                    }
                }
            }
        });

        Ok(Self {
            name,
            bind_addr: actual_addr.to_string(),
            dispatch,
            _listener: listener_task,
        })
    }

    pub fn address(&self) -> &str {
        &self.bind_addr
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub async fn register(&self, target: impl Into<String>, tx: mpsc::Sender<M>) {
        self.dispatch.lock().await.insert(target.into(), tx);
    }

    pub async fn unregister(&self, target: &str) {
        self.dispatch.lock().await.remove(target);
    }
}

async fn read_u32_le_timeout<S: AsyncRead + Unpin>(
    stream: &mut S,
    config: &DistributedConfig,
) -> std::io::Result<Option<u32>> {
    match tokio::time::timeout(config.read_timeout, stream.read_u32_le()).await {
        Ok(Ok(0)) => Ok(None),
        Ok(Ok(n)) => {
            if n > config.max_frame_bytes {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("frame too large: {n} bytes"),
                ));
            }
            Ok(Some(n))
        }
        Ok(Err(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "frame read timed out",
        )),
    }
}

async fn handle_conn<M: RemoteMessage>(
    mut stream: MaybeTlsStream,
    dispatch: Arc<Mutex<HashMap<String, mpsc::Sender<M>>>>,
    in_flight: Arc<Semaphore>,
    runtime: Handle,
    config: DistributedConfig,
) -> std::io::Result<()> {
    loop {
        let Some(len) = read_u32_le_timeout(&mut stream, &config).await? else {
            break;
        };

        let mut buf = vec![0u8; len as usize];
        tokio::time::timeout(config.read_timeout, stream.read_exact(&mut buf))
            .await
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::TimedOut, "frame body read timed out")
            })??;

        let frame: Frame = serde_json::from_slice(&buf).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, e)
        })?;

        let msg: M = serde_json::from_value(frame.payload).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, e)
        })?;

        let permit = match in_flight.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => break,
        };

        let table = dispatch.clone();
        let target = frame.target;
        spawn_on(Some(&runtime), async move {
            let _permit = permit;
            let tx = {
                let table = table.lock().await;
                table.get(&target).cloned()
            };
            if let Some(tx) = tx {
                let _ = tx.send(msg).await;
            } else {
                tracing::warn!(%target, "no local actor for frame target");
            }
        });
    }
    Ok(())
}

/// A node in a cluster roster (name + TCP address + frame target).
#[derive(Debug, Clone)]
pub struct ClusterMember {
    pub name: String,
    pub node_addr: String,
    pub target: String,
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
        }
    }

    pub fn remote_ref<M: RemoteMessage>(&self) -> RemoteActorRef<M> {
        RemoteActorRef::new(&self.node_addr, &self.target)
    }

    pub fn remote_ref_with<M: RemoteMessage>(
        &self,
        config: &DistributedConfig,
        tls: Option<Arc<TlsConnector>>,
    ) -> RemoteActorRef<M> {
        RemoteActorRef::with_config(&self.node_addr, &self.target, config, tls)
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
    distributed_config: DistributedConfig,
    tls: Option<Arc<TlsConnector>>,
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
            tls: None,
        }
    }

    /// TLS connector used when joining members (`RemoteActorRef` outbound).
    pub fn set_tls_connector(&mut self, connector: Option<Arc<TlsConnector>>) {
        self.tls = connector;
    }

    pub fn tls_connector(&self) -> Option<&Arc<TlsConnector>> {
        self.tls.as_ref()
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
        self.refs.push(member.remote_ref_with(
            &self.distributed_config,
            self.tls.clone(),
        ));
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
}

/// Local TCP node serving one actor target — use `member()` to join a [`Cluster`].
pub struct NodeHandle<M: RemoteMessage> {
    pub member: ClusterMember,
    _node: Node<M>,
    _bridge: JoinHandle<()>,
}

impl<M: RemoteMessage> NodeHandle<M> {
    pub fn address(&self) -> &str {
        &self.member.node_addr
    }

    pub fn name(&self) -> &str {
        &self.member.name
    }
}

/// Bind a TCP node and bridge incoming frames to a local actor on the current runtime.
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
        None,
    )
    .await
}

/// Bind a TCP node and bridge on the current runtime with explicit channel sizing.
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
        None,
    )
    .await
}

/// Bind a TCP node with TLS and bridge on a dedicated runtime.
pub async fn serve_actor_tls_on_runtime<M, A>(
    runtime: &Handle,
    node_name: impl Into<String>,
    bind_addr: impl Into<String>,
    target: impl Into<String>,
    actor: A,
    distributed: &DistributedConfig,
    actor_config: &ActorConfig,
    tls: Arc<TlsAcceptor>,
) -> std::io::Result<NodeHandle<M>>
where
    M: RemoteMessage,
    A: Actor<M> + Send + Sync + 'static,
{
    serve_actor_on_runtime(
        runtime,
        node_name,
        bind_addr,
        target,
        actor,
        distributed,
        actor_config,
        Some(tls),
    )
    .await
}

/// Bind a TCP node and bridge on a dedicated runtime with load limits.
pub async fn serve_actor_on_runtime<M, A>(
    runtime: &Handle,
    node_name: impl Into<String>,
    bind_addr: impl Into<String>,
    target: impl Into<String>,
    actor: A,
    distributed: &DistributedConfig,
    actor_config: &ActorConfig,
    tls: Option<Arc<TlsAcceptor>>,
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
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("failed to spawn bridged actor: {e}"),
            )
        })?;

    let node =
        Node::<M>::bind_on_runtime(runtime, &node_name, bind_addr, distributed, tls).await?;
    let address = node.address().to_string();

    let (tx, mut rx) = mpsc::channel(distributed.bridge_capacity);
    node.register(&target, tx).await;

    let bridge = spawn_on(Some(runtime), async move {
        while let Some(msg) = rx.recv().await {
            if actor_ref.send(msg).await.is_err() {
                tracing::warn!("serve_actor bridge: actor mailbox closed");
                break;
            }
        }
    });

    Ok(NodeHandle {
        member: ClusterMember {
            name: node_name,
            node_addr: address,
            target,
        },
        _node: node,
        _bridge: bridge,
    })
}
