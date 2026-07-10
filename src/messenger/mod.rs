//! FunXMPP-style compact binary messaging layer (WhatsApp-like protocol).
//!
//! Architecture:
//!
//! ```text
//! Clients (TCP, length-prefixed binary frames)
//!    │
//!    ▼
//! Gateway (accept loop)  ──►  Session task per connection
//!    │                              │
//!    ▼                              ▼
//! Presence registry  ◄──────  Message router
//!    │                              │
//!    ▼                        ┌─────┴─────┐
//! Online delivery             ▼           ▼
//!                        Offline inbox  Media store (bulk data: PDFs etc.)
//! ```
//!
//! - Wire format: `docs/messenger/01_wire_protocol.md`
//! - Bulk data (PDF/file) transfer: `docs/messenger/02_bulk_data.md`
//! - Runnable demo: `examples/messenger_demo.rs`

pub mod auth;
pub mod client;
pub mod codec;
mod cluster;
pub mod discovery;
pub mod e2ee;
pub mod inbox_storage;
mod journal;
mod key_store;
#[cfg(feature = "metrics")]
pub mod metrics;
pub mod server;
#[cfg(feature = "ws")]
pub mod ws;

pub use crate::proto::messenger as wire;
pub use auth::{Authenticator, HmacAuthenticator};
pub use client::{DownloadedMedia, LoginOutcome, MessengerClient};
pub use codec::{FrameCodec, Packet, PacketType, PROTOCOL_VERSION};
pub use discovery::{spawn_mesh_discovery, MeshDiscoveryConfig, MESSENGER_SERVICE};
pub use e2ee::{E2eeDevice, E2eeError, PeerKeyBundle};
pub use inbox_storage::InboxStorage;
pub use server::{ClusterConfig, MessengerServer, PeerAddr, ServerConfig};
#[cfg(feature = "ws")]
pub use ws::{bind_ws, decode_frame, encode_frame, WsServerHandle};

/// Errors surfaced by the messenger layer.
#[derive(Debug, thiserror::Error)]
pub enum MessengerError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol violation: {0}")]
    Protocol(String),
    #[error("frame too large: {size} bytes (max {max})")]
    FrameTooLarge { size: usize, max: usize },
    #[error("unknown packet type: {0:#04x}")]
    UnknownPacketType(u8),
    #[error("unsupported protocol version: {0}")]
    UnsupportedVersion(u8),
    #[error("decode error: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("authentication failed")]
    AuthFailed,
    #[error("connection closed")]
    Closed,
    #[error("timeout waiting for {0}")]
    Timeout(&'static str),
    #[error("media transfer failed: {0}")]
    Media(String),
    #[error("e2ee error: {0}")]
    E2ee(#[from] e2ee::E2eeError),
}
