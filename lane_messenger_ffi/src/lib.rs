//! Stable FFI for the FunXMPP messenger client.
//!
//! Hosts (Swift / Kotlin / Flutter / C) talk to opaque session and E2EE
//! handles. Networking, framing, and crypto stay in Rust.
//!
//! See `docs/client-ffi/` and `todo_client_ffi.md`.

#![deny(unsafe_op_in_unsafe_fn)]

mod error;
mod events;
mod e2ee_handle;
mod runtime;
mod session;

#[cfg(feature = "c-api")]
pub mod c_api;

pub use error::{FfiError, FfiErrorCode};
pub use events::{
    ChatMessageEvent, DeliveredAckEvent, DownloadedMediaInfo, GroupAckSummaryEvent,
    GroupEventInfo, GroupMessageEvent, KeyBundleInfo, LaneEvent, LoginAckEvent, MediaAckEvent,
    MediaChunkEvent, MediaStartEvent, PresenceEvent, ProtocolErrorEvent, ReadAckEvent,
    ServerAckEvent, SyncCompleteEvent,
};
pub use e2ee_handle::E2eeHandle;
pub use session::{ConnectOptions, SessionHandle};

/// Crate / library semver (tracks `lane_switchboards`).
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Wire protocol version (must match server).
pub const PROTOCOL_VERSION: u8 = 1;
/// Default max frame size (256 KiB).
pub const DEFAULT_MAX_FRAME: u32 = 256 * 1024;
/// Media chunk size (64 KiB).
pub const MEDIA_CHUNK_SIZE: u32 = 64 * 1024;
/// Default max media blob (64 MiB).
pub const DEFAULT_MAX_MEDIA: u64 = 64 * 1024 * 1024;
/// Suggested client ping interval.
pub const PING_INTERVAL_SECS: u64 = 30;

/// Library version string (NUL-terminated for C via [`c_api::lane_version`]).
pub fn version() -> &'static str {
    VERSION
}
