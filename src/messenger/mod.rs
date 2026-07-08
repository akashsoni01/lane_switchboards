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
pub mod server;

pub use crate::proto::messenger as wire;
pub use auth::{Authenticator, HmacAuthenticator};
pub use client::MessengerClient;
pub use codec::{FrameCodec, Packet, PacketType, PROTOCOL_VERSION};
pub use server::{ClusterConfig, MessengerServer, PeerAddr, ServerConfig};

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
}
