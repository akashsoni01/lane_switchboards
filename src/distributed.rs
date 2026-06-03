//! Distributed actors over TCP with length-prefixed JSON frames.

use crate::actor::{spawn_on_runtime, Actor, ActorProcessingErr};
use crate::config::{spawn_on, ActorConfig, DistributedConfig};
use crate::consistency::ConsistencyError;
use crate::hash_ring::{HashRing, RingNode};
use crate::stream::{self, MaybeTlsStream};
pub use crate::stream::{TlsAcceptor, TlsConnector};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot, Mutex, Semaphore};
use tokio::task::JoinHandle;
static FRAME_ID: AtomicU64 = AtomicU64::new(1);

fn next_frame_id() -> u64 {
    FRAME_ID.fetch_add(1, Ordering::Relaxed)
}

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
    /// Unique id for correlating [`AckFrame`] responses.
    #[serde(default)]
    pub frame_id: u64,
    /// When true, the receiver writes an [`AckFrame`] after dispatch.
    #[serde(default)]
    pub expect_ack: bool,
}

/// Acknowledgement sent back when [`Frame::expect_ack`] is true.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct AckFrame {
    pub frame_id: u64,
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Optional RPC payload (used by Paxos acceptors).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

/// Inbound Paxos RPC dispatched outside the typed actor mailbox.
pub struct PaxosRpc {
    pub payload: serde_json::Value,
    pub reply: oneshot::Sender<Result<serde_json::Value, String>>,
}

/// Returns true when `target` is a Paxos acceptor route (`__paxos__*`).
pub fn is_paxos_target(target: &str) -> bool {
    target.starts_with("__paxos__")
}

/// Length-prefixed JSON request/response for Paxos (same framing as data-plane frames).
pub async fn paxos_request(
    node_addr: &str,
    target: &str,
    payload: serde_json::Value,
    timeout: Duration,
    config: &DistributedConfig,
    tls: Option<&TlsConnector>,
) -> Result<serde_json::Value, ConsistencyError> {
    let mut stream = tokio::time::timeout(timeout, stream::connect(node_addr, tls))
        .await
        .map_err(|_| ConsistencyError::Timeout { after: timeout })?
        .map_err(|_| ConsistencyError::NotEnoughAcks {
            required: 1,
            received: 0,
            dc: None,
        })?;

    let frame_id = next_frame_id();
    let frame = Frame {
        target: target.to_string(),
        payload,
        frame_id,
        expect_ack: true,
    };
    let body = serde_json::to_vec(&frame).map_err(|_| ConsistencyError::NotEnoughAcks {
        required: 1,
        received: 0,
        dc: None,
    })?;
    tokio::time::timeout(timeout, write_length_prefixed_json(&mut stream, &body, config.max_frame_bytes))
        .await
        .map_err(|_| ConsistencyError::Timeout { after: timeout })?
        .map_err(|_| ConsistencyError::NotEnoughAcks {
            required: 1,
            received: 0,
            dc: None,
        })?;

    let ack = tokio::time::timeout(timeout, read_ack_frame(&mut stream, config))
        .await
        .map_err(|_| ConsistencyError::Timeout { after: timeout })?
        .map_err(|_| ConsistencyError::NotEnoughAcks {
            required: 1,
            received: 0,
            dc: None,
        })?;

    if ack.frame_id != frame_id || !ack.ok {
        return Err(ConsistencyError::NotEnoughAcks {
            required: 1,
            received: 0,
            dc: None,
        });
    }
    ack.data.ok_or(ConsistencyError::NotEnoughAcks {
        required: 1,
        received: 0,
        dc: None,
    })
}

#[derive(Serialize)]
struct FrameOut<'a, M>
where
    M: Serialize,
{
    target: &'a str,
    payload: &'a M,
    frame_id: u64,
    expect_ack: bool,
}

async fn write_length_prefixed_json<S: AsyncWrite + Unpin>(
    stream: &mut S,
    body: &[u8],
    max_frame_bytes: u32,
) -> std::io::Result<()> {
    if body.len() > max_frame_bytes as usize {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("frame too large: {} bytes", body.len()),
        ));
    }
    stream.write_u32_le(body.len() as u32).await?;
    stream.write_all(body).await?;
    stream.flush().await?;
    Ok(())
}

async fn read_length_prefixed_json<S: AsyncRead + Unpin>(
    stream: &mut S,
    config: &DistributedConfig,
) -> std::io::Result<Option<Vec<u8>>> {
    let Some(len) = read_u32_le_timeout(stream, config).await? else {
        return Ok(None);
    };
    let mut buf = vec![0u8; len as usize];
    tokio::time::timeout(config.read_timeout, stream.read_exact(&mut buf))
        .await
        .map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::TimedOut, "frame body read timed out")
        })??;
    Ok(Some(buf))
}

async fn write_frame<M, S: AsyncWrite + Unpin>(
    stream: &mut S,
    target: &str,
    msg: &M,
    frame_id: u64,
    expect_ack: bool,
    max_frame_bytes: u32,
) -> std::io::Result<()>
where
    M: RemoteMessage,
{
    let body = serde_json::to_vec(&FrameOut {
        target,
        payload: msg,
        frame_id,
        expect_ack,
    })
    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    write_length_prefixed_json(stream, &body, max_frame_bytes).await
}

async fn write_ack_frame<S: AsyncWrite + Unpin>(
    stream: &mut S,
    ack: &AckFrame,
    max_frame_bytes: u32,
) -> std::io::Result<()> {
    let body = serde_json::to_vec(ack)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    write_length_prefixed_json(stream, &body, max_frame_bytes).await
}

async fn read_ack_frame<S: AsyncRead + Unpin>(
    stream: &mut S,
    config: &DistributedConfig,
) -> std::io::Result<AckFrame> {
    let body = read_length_prefixed_json(stream, config)
        .await?
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "missing ack"))?;
    serde_json::from_slice(&body).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

struct OutboundMsg<M: RemoteMessage> {
    msg: M,
    frame_id: u64,
    expect_ack: bool,
    ack_tx: Option<oneshot::Sender<Result<(), ConsistencyError>>>,
}

async fn remote_write_loop<M: RemoteMessage>(
    node_addr: String,
    target: String,
    mut rx: mpsc::Receiver<OutboundMsg<M>>,
    config: DistributedConfig,
    tls: Option<Arc<TlsConnector>>,
) {
    const RECONNECT_BASE: Duration = Duration::from_millis(100);
    const RECONNECT_MAX: Duration = Duration::from_secs(30);
    let mut backoff = RECONNECT_BASE;

    loop {
        let mut stream = loop {
            match stream::connect(&node_addr, tls.as_deref()).await {
                Ok(s) => {
                    backoff = RECONNECT_BASE;
                    break s;
                }
                Err(e) => {
                    tracing::warn!(%node_addr, error = %e, "remote write reconnect failed");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(RECONNECT_MAX);
                    if rx.is_closed() {
                        return;
                    }
                }
            }
        };

        loop {
            let outbound = match rx.recv().await {
                Some(m) => m,
                None => return,
            };

            let write_result = write_frame(
                &mut stream,
                &target,
                &outbound.msg,
                outbound.frame_id,
                outbound.expect_ack,
                config.max_frame_bytes,
            )
            .await;

            if write_result.is_err() {
                tracing::warn!(%node_addr, "remote write failed — reconnecting");
                if let Some(ack_tx) = outbound.ack_tx {
                    let _ = ack_tx.send(Err(ConsistencyError::NotEnoughAcks {
                        required: 1,
                        received: 0,
                        dc: None,
                    }));
                }
                break;
            }

            if outbound.expect_ack {
                match read_ack_frame(&mut stream, &config).await {
                    Ok(ack) if ack.frame_id == outbound.frame_id && ack.ok => {
                        if let Some(ack_tx) = outbound.ack_tx {
                            let _ = ack_tx.send(Ok(()));
                        }
                    }
                    Ok(ack) if ack.frame_id == outbound.frame_id => {
                        if let Some(ack_tx) = outbound.ack_tx {
                            let _ = ack_tx.send(Err(ConsistencyError::NotEnoughAcks {
                                required: 1,
                                received: 0,
                                dc: None,
                            }));
                        }
                    }
                    Ok(_) => {
                        if let Some(ack_tx) = outbound.ack_tx {
                            let _ = ack_tx.send(Err(ConsistencyError::NotEnoughAcks {
                                required: 1,
                                received: 0,
                                dc: None,
                            }));
                        }
                    }
                    Err(e) => {
                        tracing::warn!(%node_addr, error = %e, "remote ack read failed");
                        if let Some(ack_tx) = outbound.ack_tx {
                            let _ = ack_tx.send(Err(ConsistencyError::NotEnoughAcks {
                                required: 1,
                                received: 0,
                                dc: None,
                            }));
                        }
                        break;
                    }
                }
            }
        }
    }
}

struct PeerWriter<M: RemoteMessage> {
    tx: mpsc::Sender<OutboundMsg<M>>,
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

    /// Outbound TLS (requires `feature = "tls"` and a real [`TlsConnector`]).
    #[cfg(feature = "tls")]
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

    pub fn target(&self) -> &str {
        &self.target
    }

    pub async fn send(&self, msg: M) -> std::io::Result<()> {
        self.writer
            .tx
            .send(OutboundMsg {
                msg,
                frame_id: 0,
                expect_ack: false,
                ack_tx: None,
            })
            .await
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    "remote peer write channel closed",
                )
            })
    }

    /// Send with acknowledgement; waits up to `timeout` for the remote node to confirm dispatch.
    ///
    /// Used by quorum write/read paths ([`WriteConsistency::Quorum`], etc.).
    /// Fire-and-forget callers should use [`Self::send`] instead.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use lane_switchboards::RemoteActorRef;
    /// # use std::time::Duration;
    /// # async fn example(r: RemoteActorRef<()>) -> Result<(), lane_switchboards::ConsistencyError> {
    /// r.send_with_ack((), Duration::from_secs(1)).await
    /// # }
    /// ```
    pub async fn send_with_ack(
        &self,
        msg: M,
        timeout: Duration,
    ) -> Result<(), ConsistencyError> {
        let frame_id = next_frame_id();
        let (ack_tx, ack_rx) = oneshot::channel();
        self.writer
            .tx
            .send(OutboundMsg {
                msg,
                frame_id,
                expect_ack: true,
                ack_tx: Some(ack_tx),
            })
            .await
            .map_err(|_| ConsistencyError::NotEnoughAcks {
                required: 1,
                received: 0,
                dc: None,
            })?;

        match tokio::time::timeout(timeout, ack_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(ConsistencyError::NotEnoughAcks {
                required: 1,
                received: 0,
                dc: None,
            }),
            Err(_) => Err(ConsistencyError::Timeout { after: timeout }),
        }
    }
}

/// Local node: binds TCP and dispatches incoming frames to registered actors.
pub struct Node<M: RemoteMessage> {
    name: String,
    bind_addr: String,
    dispatch: Arc<Mutex<HashMap<String, mpsc::Sender<M>>>>,
    paxos: Arc<Mutex<HashMap<String, mpsc::Sender<PaxosRpc>>>>,
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

    /// Bind with TLS (server certificate required; `feature = "tls"`).
    #[cfg(feature = "tls")]
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
        let paxos = Arc::new(Mutex::new(HashMap::<String, mpsc::Sender<PaxosRpc>>::new()));
        let dispatch_c = dispatch.clone();
        let paxos_c = paxos.clone();
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
                            let paxos_table = paxos_c.clone();
                            let in_flight = in_flight.clone();
                            let conn_runtime = runtime.clone();
                            let acceptor = tls_acceptor.clone();
                            spawn_on(Some(&runtime), async move {
                                match stream::accept(stream, acceptor.as_deref()).await {
                                    Ok(stream) => {
                                        if let Err(e) = handle_conn(
                                            stream,
                                            table,
                                            paxos_table,
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
            paxos,
            _listener: listener_task,
        })
    }

    pub async fn register_paxos(&self, target: impl Into<String>, tx: mpsc::Sender<PaxosRpc>) {
        self.paxos.lock().await.insert(target.into(), tx);
    }

    pub async fn unregister_paxos(&self, target: &str) {
        self.paxos.lock().await.remove(target);
    }

    pub(crate) fn paxos_dispatch(&self) -> Arc<Mutex<HashMap<String, mpsc::Sender<PaxosRpc>>>> {
        self.paxos.clone()
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
    stream: MaybeTlsStream,
    dispatch: Arc<Mutex<HashMap<String, mpsc::Sender<M>>>>,
    paxos: Arc<Mutex<HashMap<String, mpsc::Sender<PaxosRpc>>>>,
    in_flight: Arc<Semaphore>,
    runtime: Handle,
    config: DistributedConfig,
) -> std::io::Result<()> {
    let (mut reader, mut writer) = tokio::io::split(stream);

    loop {
        let Some(body) = read_length_prefixed_json(&mut reader, &config).await? else {
            break;
        };

        let frame: Frame = serde_json::from_slice(&body).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, e)
        })?;

        if frame.expect_ack && is_paxos_target(&frame.target) {
            let target = frame.target.clone();
            let (reply_tx, reply_rx) = oneshot::channel();
            let rpc = PaxosRpc {
                payload: frame.payload,
                reply: reply_tx,
            };
            let send_result = {
                let tx = paxos.lock().await.get(&target).cloned();
                if let Some(tx) = tx {
                    tx.send(rpc).await.map_err(|_| "paxos handler closed".to_string())
                } else {
                    Err(format!("no paxos handler for target {target}"))
                }
            };

            let ack = if send_result.is_ok() {
                match tokio::time::timeout(config.read_timeout, reply_rx).await {
                    Ok(Ok(Ok(data))) => AckFrame {
                        frame_id: frame.frame_id,
                        ok: true,
                        error: None,
                        data: Some(data),
                    },
                    Ok(Ok(Err(e))) => AckFrame {
                        frame_id: frame.frame_id,
                        ok: false,
                        error: Some(e),
                        data: None,
                    },
                    Ok(Err(_)) | Err(_) => AckFrame {
                        frame_id: frame.frame_id,
                        ok: false,
                        error: Some("paxos handler dropped reply".into()),
                        data: None,
                    },
                }
            } else {
                AckFrame {
                    frame_id: frame.frame_id,
                    ok: false,
                    error: send_result.err(),
                    data: None,
                }
            };
            write_ack_frame(&mut writer, &ack, config.max_frame_bytes).await?;
            continue;
        }

        let msg: M = serde_json::from_value(frame.payload).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, e)
        })?;

        if frame.expect_ack {
            let target = frame.target.clone();
            let tx = {
                let table = dispatch.lock().await;
                table.get(&target).cloned()
            };
            let send_result = if let Some(tx) = tx {
                tx.send(msg).await.map(|_| ()).map_err(|_| {
                    "actor mailbox closed".to_string()
                })
            } else {
                tracing::warn!(%target, "no local actor for frame target");
                Err(format!("no local actor for frame target {target}"))
            };

            let ack = AckFrame {
                frame_id: frame.frame_id,
                ok: send_result.is_ok(),
                error: send_result.err(),
                data: None,
            };
            write_ack_frame(&mut writer, &ack, config.max_frame_bytes).await?;
        } else {
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
    }
    Ok(())
}

/// A node in a cluster roster (name + TCP address + frame target).
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

/// Bind a TCP node with TLS and bridge on a dedicated runtime (`feature = "tls"`).
#[cfg(feature = "tls")]
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
            dc: None,
        },
        _node: node,
        _bridge: bridge,
    })
}

#[cfg(test)]
mod ack_tests {
    use super::*;
    use crate::actor::{spawn, Actor, ActorProcessingErr};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct RemotePing(u64);

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
            .send_with_ack(RemotePing(1), Duration::from_secs(2))
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

        // Capacity 1, no consumer — second ack'd send blocks in handle_conn.
        let (tx, _rx) = mpsc::channel(1);
        node.register("worker", tx).await;

        let remote = RemoteActorRef::<RemotePing>::new(&addr, "worker");
        remote
            .send_with_ack(RemotePing(1), Duration::from_secs(2))
            .await
            .expect("first ack");

        let err = remote
            .send_with_ack(RemotePing(2), Duration::from_millis(200))
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
        remote.send(RemotePing(7)).await.expect("send");

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
