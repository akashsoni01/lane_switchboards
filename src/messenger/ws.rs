//! WebSocket transport for browser clients (`feature = "ws"`).
//!
//! Each WebSocket **binary** message carries one complete messenger frame
//! (`| ver | type | len | payload |`). Text frames are ignored. The accept
//! loop upgrades any HTTP connection (path-agnostic) and bridges to an
//! internal TCP gateway that owns protocol state.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::BytesMut;
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::codec::{Decoder, Encoder, Framed};
use tracing::{debug, info, warn};

use super::auth::Authenticator;
use super::codec::{FrameCodec, Packet, HEADER_LEN, PROTOCOL_VERSION};
use super::server::{MessengerServer, ServerConfig};
use super::MessengerError;

/// Bind a WebSocket listener. Protocol state lives on an internal TCP
/// gateway; each upgraded socket is bridged frame-for-frame.
pub async fn bind_ws(
    addr: &str,
    auth: Arc<dyn Authenticator>,
    cfg: ServerConfig,
) -> Result<(SocketAddr, WsServerHandle), MessengerError> {
    let engine = MessengerServer::bind("127.0.0.1:0", auth, cfg).await?;
    let internal = engine.local_addr().to_string();
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    let accept = tokio::spawn(async move {
        let _engine = engine;
        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    let internal = internal.clone();
                    tokio::spawn(async move {
                        if let Err(e) = bridge_ws_to_tcp(stream, peer, &internal).await {
                            debug!(%peer, error = %e, "ws session ended");
                        }
                    });
                }
                Err(e) => {
                    warn!(error = %e, "ws accept failed");
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            }
        }
    });
    info!(%bound, "messenger websocket gateway listening");
    Ok((bound, WsServerHandle { accept }))
}

/// Handle returned by [`bind_ws`]; dropping aborts the accept loop.
pub struct WsServerHandle {
    accept: tokio::task::JoinHandle<()>,
}

impl Drop for WsServerHandle {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

async fn bridge_ws_to_tcp(
    stream: TcpStream,
    peer: SocketAddr,
    internal_addr: &str,
) -> Result<(), MessengerError> {
    let ws = tokio_tungstenite::accept_async(stream)
        .await
        .map_err(|e| MessengerError::Io(std::io::Error::other(e.to_string())))?;
    debug!(%peer, "websocket upgraded");

    let tcp = TcpStream::connect(internal_addr).await?;
    tcp.set_nodelay(true).ok();
    let mut framed = Framed::new(tcp, FrameCodec::default());
    let (mut ws_sink, mut ws_stream) = ws.split();

    loop {
        tokio::select! {
            msg = ws_stream.next() => {
                match msg {
                    None => return Ok(()),
                    Some(Err(e)) => {
                        return Err(MessengerError::Io(std::io::Error::other(e.to_string())));
                    }
                    Some(Ok(Message::Binary(data))) => {
                        let pkt = decode_frame(&data)?;
                        framed.send(pkt).await?;
                    }
                    Some(Ok(Message::Close(_))) => return Ok(()),
                    Some(Ok(Message::Ping(p))) => {
                        ws_sink
                            .send(Message::Pong(p))
                            .await
                            .map_err(|e| MessengerError::Io(std::io::Error::other(e.to_string())))?;
                    }
                    Some(Ok(_)) => {}
                }
            }
            out = framed.next() => {
                match out {
                    None => return Ok(()),
                    Some(Err(e)) => return Err(e),
                    Some(Ok(pkt)) => {
                        let bytes = encode_frame(&pkt)?;
                        ws_sink
                            .send(Message::Binary(bytes))
                            .await
                            .map_err(|e| MessengerError::Io(std::io::Error::other(e.to_string())))?;
                    }
                }
            }
        }
    }
}

/// Encode a [`Packet`] to raw frame bytes (for WS clients / tests).
pub fn encode_frame(pkt: &Packet) -> Result<Vec<u8>, MessengerError> {
    let mut codec = FrameCodec::default();
    let mut buf = BytesMut::new();
    codec.encode(pkt.clone(), &mut buf)?;
    Ok(buf.to_vec())
}

/// Decode one complete frame from a WS binary payload.
pub fn decode_frame(data: &[u8]) -> Result<Packet, MessengerError> {
    if data.len() < HEADER_LEN {
        return Err(MessengerError::Protocol("frame too short".into()));
    }
    if data[0] != PROTOCOL_VERSION {
        return Err(MessengerError::UnsupportedVersion(data[0]));
    }
    let mut codec = FrameCodec::default();
    let mut buf = BytesMut::from(data);
    codec
        .decode(&mut buf)?
        .ok_or_else(|| MessengerError::Protocol("incomplete frame".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messenger::wire;

    #[test]
    fn encode_decode_round_trip() {
        let pkt = Packet::Ping(wire::Ping { seq: 42 });
        let bytes = encode_frame(&pkt).unwrap();
        let decoded = decode_frame(&bytes).unwrap();
        assert_eq!(decoded, pkt);
    }
}
