//! UniFFI-facing API (feature `uniffi`). Wraps [`crate::SessionHandle`] / [`crate::E2eeHandle`].
//!
//! Types and free functions here are re-exported at the crate root so UDL scaffolding
//! (which discards its stub bodies) can call them.

use std::sync::Arc;

use crate::error::{FfiError, FfiErrorCode};
use crate::events::LaneEvent;
use crate::session::{ConnectOptions, Transport};
use crate::{E2eeHandle, SessionHandle};

/// UniFFI error surface (maps from [`FfiError`]).
#[derive(Debug, thiserror::Error)]
pub enum LaneError {
    #[error("{message}")]
    Io { message: String },
    #[error("{message}")]
    Protocol { message: String },
    #[error("authentication failed")]
    AuthFailed,
    #[error("connection closed")]
    Closed,
    #[error("{message}")]
    Timeout { message: String },
    #[error("{message}")]
    Media { message: String },
    #[error("{message}")]
    E2ee { message: String },
    #[error("{message}")]
    InvalidArgument { message: String },
    #[error("not connected")]
    NotConnected,
    #[error("{message}")]
    Internal { message: String },
}

impl From<FfiError> for LaneError {
    fn from(e: FfiError) -> Self {
        let message = e.detail();
        match e.code() {
            FfiErrorCode::Io => LaneError::Io { message },
            FfiErrorCode::Protocol => LaneError::Protocol { message },
            FfiErrorCode::AuthFailed => LaneError::AuthFailed,
            FfiErrorCode::Closed => LaneError::Closed,
            FfiErrorCode::Timeout => LaneError::Timeout { message },
            FfiErrorCode::Media => LaneError::Media { message },
            FfiErrorCode::E2ee => LaneError::E2ee { message },
            FfiErrorCode::InvalidArgument => LaneError::InvalidArgument { message },
            FfiErrorCode::NotConnected => LaneError::NotConnected,
            FfiErrorCode::Internal
            | FfiErrorCode::FrameTooLarge
            | FfiErrorCode::UnknownPacketType
            | FfiErrorCode::UnsupportedVersion
            | FfiErrorCode::Decode
            | FfiErrorCode::Ok => LaneError::Internal { message },
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConnectConfig {
    pub host: String,
    pub port: u16,
    pub use_tls: bool,
    pub user_id: String,
    pub device_id: String,
    pub auth_token: String,
    pub client_version: String,
    pub resume_after_seq: u64,
    pub ping_interval_secs: u64,
    pub auto_reconnect: bool,
    pub max_reconnect_attempts: u32,
    pub use_websocket: bool,
    pub ws_url: String,
}

impl From<ConnectConfig> for ConnectOptions {
    fn from(c: ConnectConfig) -> Self {
        ConnectOptions {
            host: c.host,
            port: c.port,
            use_tls: c.use_tls,
            user_id: c.user_id,
            device_id: c.device_id,
            auth_token: c.auth_token,
            client_version: if c.client_version.is_empty() {
                format!("uniffi-{}", crate::VERSION)
            } else {
                c.client_version
            },
            resume_after_seq: c.resume_after_seq,
            ping_interval_secs: c.ping_interval_secs,
            auto_reconnect: c.auto_reconnect,
            max_reconnect_attempts: c.max_reconnect_attempts,
            transport: if c.use_websocket {
                Transport::WebSocket
            } else {
                Transport::Tcp
            },
            ws_url: c.ws_url,
            ca_pem_path: None,
        }
    }
}

pub struct LaneSession {
    inner: SessionHandle,
}

impl LaneSession {
    pub fn new(config: ConnectConfig) -> Result<Self, LaneError> {
        let inner = SessionHandle::connect(config.into())?;
        Ok(Self { inner })
    }

    pub fn ping(&self) -> Result<(), LaneError> {
        self.inner.ping().map_err(Into::into)
    }

    pub fn send_chat(
        &self,
        to_user: String,
        message_id: String,
        body: Vec<u8>,
    ) -> Result<u64, LaneError> {
        self.inner
            .send_chat(&to_user, &message_id, &body)
            .map_err(Into::into)
    }

    pub fn ack_delivered(&self, message_id: String) -> Result<(), LaneError> {
        self.inner.ack_delivered(&message_id).map_err(Into::into)
    }

    pub fn ack_read(&self, message_id: String) -> Result<(), LaneError> {
        self.inner.ack_read(&message_id).map_err(Into::into)
    }

    pub fn create_group(&self, group_id: String) -> Result<u64, LaneError> {
        self.inner.create_group(&group_id).map_err(Into::into)
    }

    pub fn add_member(&self, group_id: String, user: String) -> Result<u64, LaneError> {
        self.inner.add_member(&group_id, &user).map_err(Into::into)
    }

    pub fn send_group(
        &self,
        group_id: String,
        message_id: String,
        body: Vec<u8>,
    ) -> Result<(), LaneError> {
        self.inner
            .send_group(&group_id, &message_id, &body)
            .map_err(Into::into)
    }

    pub fn set_resume_seq(&self, seq: u64) -> Result<(), LaneError> {
        self.inner.set_resume_seq(seq).map_err(Into::into)
    }

    pub fn resume_seq(&self) -> u64 {
        self.inner.resume_seq()
    }

    pub fn poll_event_json(&self, timeout_ms: u64) -> Option<String> {
        self.inner.poll_event(timeout_ms).map(event_json)
    }

    pub fn close(&self) -> Result<(), LaneError> {
        self.inner.close().map_err(Into::into)
    }

    pub(crate) fn handle(&self) -> &SessionHandle {
        &self.inner
    }
}

pub struct LaneE2ee {
    inner: E2eeHandle,
}

impl LaneE2ee {
    pub fn new() -> Self {
        Self {
            inner: E2eeHandle::generate(),
        }
    }

    pub fn identity_key(&self) -> String {
        self.inner.identity_key()
    }

    pub fn publish(
        &self,
        session: Arc<LaneSession>,
        device_id: String,
        otk_count: u32,
    ) -> Result<(), LaneError> {
        self.inner
            .publish(session.handle(), &device_id, otk_count)
            .map_err(Into::into)
    }

    pub fn send_encrypted_chat(
        &self,
        session: Arc<LaneSession>,
        to_user: String,
        message_id: String,
        plaintext: Vec<u8>,
    ) -> Result<u64, LaneError> {
        self.inner
            .send_encrypted_chat(session.handle(), &to_user, &message_id, &plaintext)
            .map_err(Into::into)
    }

    pub fn decrypt_chat(&self, from_user: String, body: Vec<u8>) -> Result<Vec<u8>, LaneError> {
        self.inner
            .decrypt_chat(&from_user, &body)
            .map_err(Into::into)
    }

    pub fn create_group_session(&self, group_id: String) -> String {
        self.inner.create_group_session(&group_id)
    }

    pub fn distribute_group_key(
        &self,
        session: Arc<LaneSession>,
        group_id: String,
        members: Vec<String>,
    ) -> Result<(), LaneError> {
        self.inner
            .distribute_group_key(session.handle(), &group_id, &members)
            .map_err(Into::into)
    }

    pub fn send_encrypted_group(
        &self,
        session: Arc<LaneSession>,
        group_id: String,
        message_id: String,
        plaintext: Vec<u8>,
    ) -> Result<(), LaneError> {
        self.inner
            .send_encrypted_group(session.handle(), &group_id, &message_id, &plaintext)
            .map_err(Into::into)
    }

    pub fn decrypt_group(&self, group_id: String, body: Vec<u8>) -> Result<Vec<u8>, LaneError> {
        self.inner
            .decrypt_group(&group_id, &body)
            .map_err(Into::into)
    }

    pub fn export_pickle(&self, passphrase: String) -> Vec<u8> {
        self.inner.export_pickle(&passphrase)
    }
}

impl Default for LaneE2ee {
    fn default() -> Self {
        Self::new()
    }
}

pub fn version() -> String {
    crate::VERSION.to_string()
}

pub fn protocol_version() -> u8 {
    crate::PROTOCOL_VERSION
}

pub fn safety_number(local_identity_b64: String, remote_identity_b64: String) -> String {
    E2eeHandle::safety_number(&local_identity_b64, &remote_identity_b64)
}

pub fn e2ee_import_pickle(
    pickle: Vec<u8>,
    passphrase: String,
) -> Result<Arc<LaneE2ee>, LaneError> {
    let inner = E2eeHandle::import_pickle(&pickle, &passphrase).map_err(LaneError::from)?;
    Ok(Arc::new(LaneE2ee { inner }))
}

fn event_json(ev: LaneEvent) -> String {
    // Keep in sync with c_api::event_to_json shape (type + key fields).
    match ev {
        LaneEvent::LoginAck(a) => format!(
            r#"{{"type":"LoginAck","session_id":"{}","ok":{}}}"#,
            a.session_id, a.ok
        ),
        LaneEvent::SyncComplete(s) => format!(
            r#"{{"type":"SyncComplete","latest_seq":{}}}"#,
            s.latest_seq
        ),
        LaneEvent::ChatMessage(m) => format!(
            r#"{{"type":"ChatMessage","message_id":"{}","from_user":"{}","seq":{}}}"#,
            m.message_id, m.from_user, m.seq
        ),
        LaneEvent::ServerAck(a) => format!(
            r#"{{"type":"ServerAck","message_id":"{}","seq":{}}}"#,
            a.message_id, a.seq
        ),
        LaneEvent::DeliveredAck(a) => format!(
            r#"{{"type":"DeliveredAck","message_id":"{}"}}"#,
            a.message_id
        ),
        LaneEvent::ReadAck(a) => format!(r#"{{"type":"ReadAck","message_id":"{}"}}"#, a.message_id),
        LaneEvent::GroupMessage(m) => format!(
            r#"{{"type":"GroupMessage","message_id":"{}","group_id":"{}"}}"#,
            m.message_id, m.group_id
        ),
        LaneEvent::GroupAckSummary(s) => format!(
            r#"{{"type":"GroupAckSummary","message_id":"{}","member_count":{}}}"#,
            s.message_id, s.member_count
        ),
        LaneEvent::Presence(p) => {
            format!(r#"{{"type":"Presence","user_id":"{}","kind":{}}}"#, p.user_id, p.kind)
        }
        LaneEvent::ReplacedByNewSession => r#"{"type":"ReplacedByNewSession"}"#.into(),
        LaneEvent::Disconnected { reason } => {
            format!(r#"{{"type":"Disconnected","reason":"{reason}"}}"#)
        }
        other => format!(r#"{{"type":"Other","debug":"{other:?}"}}"#),
    }
}
