//! Distributed actors over TCP with length-prefixed JSON frames.

use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};

/// Messages that can traverse the network layer.
pub trait RemoteMessage:
    Serialize + DeserializeOwned + Send + Clone + std::fmt::Debug + 'static
{
}

impl<T> RemoteMessage for T where
    T: Serialize + DeserializeOwned + Send + Clone + std::fmt::Debug + 'static
{
}

/// Wire frame: route by actor name on the remote node.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Frame {
    pub target: String,
    pub payload: serde_json::Value,
}

/// Local node: binds TCP and dispatches incoming frames to registered actors.
pub struct Node<M: RemoteMessage> {
    name: String,
    bind_addr: String,
    dispatch: Arc<Mutex<HashMap<String, mpsc::Sender<M>>>>,
    _listener: tokio::task::JoinHandle<()>,
}

impl<M: RemoteMessage> Node<M> {
    pub async fn bind(name: impl Into<String>, addr: impl Into<String>) -> std::io::Result<Self> {
        let name = name.into();
        let bind_addr = addr.into();
        let listener = TcpListener::bind(&bind_addr).await?;
        let actual_addr = listener.local_addr()?;
        tracing::info!(node = %name, %actual_addr, "distributed node listening");

        let dispatch = Arc::new(Mutex::new(HashMap::<String, mpsc::Sender<M>>::new()));
        let dispatch_c = dispatch.clone();

        let listener_task = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, peer)) => {
                        let table = dispatch_c.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_conn(stream, table).await {
                                tracing::warn!(%peer, error = %e, "connection handler error");
                            }
                        });
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "accept failed");
                        break;
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

    pub async fn register(&self, target: impl Into<String>, tx: mpsc::Sender<M>) {
        self.dispatch.lock().await.insert(target.into(), tx);
    }

    pub async fn unregister(&self, target: &str) {
        self.dispatch.lock().await.remove(target);
    }
}

async fn handle_conn<M: RemoteMessage>(
    mut stream: TcpStream,
    dispatch: Arc<Mutex<HashMap<String, mpsc::Sender<M>>>>,
) -> std::io::Result<()> {
    loop {
        let len = match stream.read_u32_le().await {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        };

        let mut buf = vec![0u8; len as usize];
        stream.read_exact(&mut buf).await?;

        let frame: Frame = serde_json::from_slice(&buf).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, e)
        })?;

        let msg: M = serde_json::from_value(frame.payload).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, e)
        })?;

        let table = dispatch.lock().await;
        if let Some(tx) = table.get(&frame.target) {
            let _ = tx.send(msg).await;
        } else {
            tracing::warn!(target = %frame.target, "no local actor for frame target");
        }
    }
    Ok(())
}

/// Reference to an actor on a remote node (new TCP connection per send).
#[derive(Clone)]
pub struct RemoteActorRef<M: RemoteMessage> {
    pub node_addr: String,
    pub target: String,
    _marker: std::marker::PhantomData<M>,
}

impl<M: RemoteMessage> RemoteActorRef<M> {
    pub fn new(node_addr: impl Into<String>, target: impl Into<String>) -> Self {
        Self {
            node_addr: node_addr.into(),
            target: target.into(),
            _marker: std::marker::PhantomData,
        }
    }

    pub async fn send(&self, msg: M) -> std::io::Result<()> {
        let frame = Frame {
            target: self.target.clone(),
            payload: serde_json::to_value(&msg).map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, e)
            })?,
        };

        let body = serde_json::to_vec(&frame).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, e)
        })?;

        let mut stream = TcpStream::connect(&self.node_addr).await?;
        stream.write_u32_le(body.len() as u32).await?;
        stream.write_all(&body).await?;
        stream.flush().await?;
        Ok(())
    }
}
