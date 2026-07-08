//! Reference client for the messenger binary protocol.
//!
//! Used by integration tests and `examples/messenger_demo.rs`. It speaks the
//! full protocol: login, heartbeats, chat with the ack ladder, offline sync,
//! chunked media upload/download, and groups.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use sha2::{Digest, Sha256};
use tokio_util::codec::Framed;

use crate::stream::{self, MaybeTlsStream, TlsConnector};

use super::codec::{FrameCodec, Packet};
use super::wire;
use super::MessengerError;

const RECV_TIMEOUT: Duration = Duration::from_secs(10);

fn unix_millis() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

/// Result of a successful login.
#[derive(Debug)]
pub struct LoginOutcome {
    /// Server-assigned session id.
    pub session_id: String,
    /// Messages replayed from the offline inbox before `SyncComplete`.
    pub replayed: Vec<Packet>,
    /// Highest inbox seq after sync.
    pub latest_seq: u64,
}

/// A fully downloaded media blob.
#[derive(Debug)]
pub struct DownloadedMedia {
    pub file_name: String,
    pub mime_type: String,
    pub sha256: String,
    pub data: Vec<u8>,
}

/// Blocking-style protocol client. Reads are pull-based via [`recv`]; server
/// pushes (chat, presence, acks) queue in the socket until consumed.
///
/// [`recv`]: MessengerClient::recv
pub struct MessengerClient {
    framed: Framed<MaybeTlsStream, FrameCodec>,
    user_id: String,
    ping_seq: u64,
}

impl MessengerClient {
    /// Connect over plain TCP and authenticate; replays pending messages.
    pub async fn connect(
        addr: &str,
        user_id: &str,
        device_id: &str,
        auth_token: &str,
        resume_after_seq: u64,
    ) -> Result<(Self, LoginOutcome), MessengerError> {
        Self::connect_tls(addr, None, user_id, device_id, auth_token, resume_after_seq).await
    }

    /// Connect with an optional TLS connector (`feature = "tls"`). The server
    /// name for certificate validation is the host portion of `addr`.
    pub async fn connect_tls(
        addr: &str,
        tls: Option<&TlsConnector>,
        user_id: &str,
        device_id: &str,
        auth_token: &str,
        resume_after_seq: u64,
    ) -> Result<(Self, LoginOutcome), MessengerError> {
        let socket = stream::connect(addr, tls).await?;
        let mut framed = Framed::new(socket, FrameCodec::default());

        framed
            .send(Packet::Login(wire::Login {
                user_id: user_id.into(),
                device_id: device_id.into(),
                auth_token: auth_token.into(),
                client_version: env!("CARGO_PKG_VERSION").into(),
                resume_after_seq,
            }))
            .await?;

        let ack = match Self::next(&mut framed).await? {
            Packet::LoginAck(a) if a.ok => a,
            Packet::LoginAck(a) => return Err(MessengerError::Protocol(a.error)),
            Packet::Error(e) => return Err(MessengerError::Protocol(e.detail)),
            other => {
                return Err(MessengerError::Protocol(format!(
                    "expected LoginAck, got {:?}",
                    other.packet_type()
                )))
            }
        };

        // Drain replay until SyncComplete.
        let mut replayed = Vec::new();
        let latest_seq = loop {
            match Self::next(&mut framed).await? {
                Packet::SyncComplete(s) => break s.latest_seq,
                pkt => replayed.push(pkt),
            }
        };

        Ok((
            Self { framed, user_id: user_id.into(), ping_seq: 0 },
            LoginOutcome { session_id: ack.session_id, replayed, latest_seq },
        ))
    }

    async fn next(
        framed: &mut Framed<MaybeTlsStream, FrameCodec>,
    ) -> Result<Packet, MessengerError> {
        match tokio::time::timeout(RECV_TIMEOUT, framed.next()).await {
            Err(_) => Err(MessengerError::Timeout("server frame")),
            Ok(None) => Err(MessengerError::Closed),
            Ok(Some(r)) => r,
        }
    }

    /// Receive the next server frame (chat, presence, acks, …).
    pub async fn recv(&mut self) -> Result<Packet, MessengerError> {
        Self::next(&mut self.framed).await
    }

    /// Receive frames until `pred` matches, returning the matching packet.
    /// Non-matching frames are discarded (fine for tests; a real client
    /// would dispatch them).
    pub async fn recv_until(
        &mut self,
        mut pred: impl FnMut(&Packet) -> bool,
    ) -> Result<Packet, MessengerError> {
        loop {
            let pkt = self.recv().await?;
            if pred(&pkt) {
                return Ok(pkt);
            }
        }
    }

    /// Heartbeat round-trip.
    pub async fn ping(&mut self) -> Result<(), MessengerError> {
        self.ping_seq += 1;
        let seq = self.ping_seq;
        self.framed.send(Packet::Ping(wire::Ping { seq })).await?;
        match self.recv_until(|p| matches!(p, Packet::Pong(_))).await? {
            Packet::Pong(p) if p.seq == seq => Ok(()),
            _ => Err(MessengerError::Protocol("pong seq mismatch".into())),
        }
    }

    /// Send a 1:1 message and wait for the `ServerAck` (single tick).
    /// Returns the recipient-inbox seq the server assigned.
    pub async fn send_chat(
        &mut self,
        to_user: &str,
        message_id: &str,
        body: &[u8],
    ) -> Result<u64, MessengerError> {
        self.send_chat_with_media(to_user, message_id, body, "").await
    }

    /// Send a 1:1 message referencing an uploaded media blob.
    pub async fn send_chat_with_media(
        &mut self,
        to_user: &str,
        message_id: &str,
        body: &[u8],
        media_id: &str,
    ) -> Result<u64, MessengerError> {
        self.framed
            .send(Packet::ChatMessage(wire::ChatMessage {
                message_id: message_id.into(),
                from_user: self.user_id.clone(),
                to_user: to_user.into(),
                body: body.to_vec(),
                sent_at: unix_millis(),
                seq: 0,
                media_id: media_id.into(),
            }))
            .await?;
        match self
            .recv_until(|p| matches!(p, Packet::ServerAck(a) if a.message_id == message_id))
            .await?
        {
            Packet::ServerAck(a) => Ok(a.seq),
            _ => unreachable!("recv_until guarantees ServerAck"),
        }
    }

    /// Acknowledge delivery of a received message (double tick).
    pub async fn ack_delivered(&mut self, message_id: &str) -> Result<(), MessengerError> {
        self.framed
            .send(Packet::DeliveredAck(wire::DeliveredAck {
                message_id: message_id.into(),
                from_user: self.user_id.clone(),
            }))
            .await?;
        Ok(())
    }

    /// Acknowledge reading a message (blue tick).
    pub async fn ack_read(&mut self, message_id: &str) -> Result<(), MessengerError> {
        self.framed
            .send(Packet::ReadAck(wire::ReadAck {
                message_id: message_id.into(),
                from_user: self.user_id.clone(),
            }))
            .await?;
        Ok(())
    }

    /// Upload a blob (e.g. a PDF) in 64 KiB chunks. Waits for the final
    /// verified `MediaAck`. Returns total bytes stored on the server.
    pub async fn upload_media(
        &mut self,
        media_id: &str,
        file_name: &str,
        mime_type: &str,
        data: &[u8],
    ) -> Result<u64, MessengerError> {
        let digest = {
            use std::fmt::Write;
            let d = Sha256::digest(data);
            let mut s = String::with_capacity(64);
            for b in d {
                let _ = write!(s, "{b:02x}");
            }
            s
        };
        self.framed
            .send(Packet::MediaStart(wire::MediaStart {
                media_id: media_id.into(),
                file_name: file_name.into(),
                mime_type: mime_type.into(),
                total_size: data.len() as u64,
                sha256: digest,
            }))
            .await?;
        // Initial credit ack.
        match self.recv_until(|p| matches!(p, Packet::MediaAck(a) if a.media_id == media_id)).await? {
            Packet::MediaAck(a) if a.ok => {}
            Packet::MediaAck(a) => return Err(MessengerError::Media(a.error)),
            _ => unreachable!(),
        }

        const CHUNK: usize = 64 * 1024;
        let mut offset = 0usize;
        loop {
            let end = (offset + CHUNK).min(data.len());
            let last = end == data.len();
            self.framed
                .send(Packet::MediaChunk(wire::MediaChunk {
                    media_id: media_id.into(),
                    offset: offset as u64,
                    data: data[offset..end].to_vec(),
                    last,
                }))
                .await?;
            let ack = match self
                .recv_until(|p| matches!(p, Packet::MediaAck(a) if a.media_id == media_id))
                .await?
            {
                Packet::MediaAck(a) => a,
                _ => unreachable!(),
            };
            if !ack.ok {
                return Err(MessengerError::Media(ack.error));
            }
            if last {
                if !ack.complete {
                    return Err(MessengerError::Media("server did not finalize upload".into()));
                }
                return Ok(ack.received_bytes);
            }
            offset = end;
        }
    }

    /// Download a stored blob, verifying its sha256 digest locally.
    pub async fn fetch_media(&mut self, media_id: &str) -> Result<DownloadedMedia, MessengerError> {
        self.framed
            .send(Packet::MediaFetch(wire::MediaFetch {
                media_id: media_id.into(),
                from_offset: 0,
            }))
            .await?;
        let start = match self
            .recv_until(|p| {
                matches!(p, Packet::MediaStart(s) if s.media_id == media_id)
                    || matches!(p, Packet::Error(_))
            })
            .await?
        {
            Packet::MediaStart(s) => s,
            Packet::Error(e) => return Err(MessengerError::Media(e.detail)),
            _ => unreachable!(),
        };
        let mut data = Vec::with_capacity(start.total_size as usize);
        loop {
            match self
                .recv_until(|p| matches!(p, Packet::MediaChunk(c) if c.media_id == media_id))
                .await?
            {
                Packet::MediaChunk(c) => {
                    data.extend_from_slice(&c.data);
                    if c.last {
                        break;
                    }
                }
                _ => unreachable!(),
            }
        }
        if !start.sha256.is_empty() {
            use std::fmt::Write;
            let d = Sha256::digest(&data);
            let mut got = String::with_capacity(64);
            for b in d {
                let _ = write!(got, "{b:02x}");
            }
            if !got.eq_ignore_ascii_case(&start.sha256) {
                return Err(MessengerError::Media("downloaded blob failed sha256 check".into()));
            }
        }
        Ok(DownloadedMedia {
            file_name: start.file_name,
            mime_type: start.mime_type,
            sha256: start.sha256,
            data,
        })
    }

    /// Create a group with the caller as sole member/admin.
    pub async fn create_group(&mut self, group_id: &str) -> Result<u64, MessengerError> {
        self.group_event(group_id, wire::GroupOp::Create, "").await
    }

    /// Add a member (caller must be admin).
    pub async fn add_member(&mut self, group_id: &str, user: &str) -> Result<u64, MessengerError> {
        self.group_event(group_id, wire::GroupOp::AddMember, user).await
    }

    async fn group_event(
        &mut self,
        group_id: &str,
        op: wire::GroupOp,
        subject: &str,
    ) -> Result<u64, MessengerError> {
        self.framed
            .send(Packet::GroupEvent(wire::GroupEvent {
                group_id: group_id.into(),
                op: op as i32,
                actor_user: self.user_id.clone(),
                subject_user: subject.into(),
                version: 0,
                to_user: String::new(),
            }))
            .await?;
        match self
            .recv_until(|p| {
                matches!(p, Packet::GroupEvent(e) if e.group_id == group_id)
                    || matches!(p, Packet::Error(_))
            })
            .await?
        {
            Packet::GroupEvent(e) => Ok(e.version),
            Packet::Error(e) => Err(MessengerError::Protocol(e.detail)),
            _ => unreachable!(),
        }
    }

    /// Send a group message; waits for `ServerAck`.
    pub async fn send_group(
        &mut self,
        group_id: &str,
        message_id: &str,
        body: &[u8],
    ) -> Result<(), MessengerError> {
        self.framed
            .send(Packet::GroupMessage(wire::GroupMessage {
                message_id: message_id.into(),
                from_user: self.user_id.clone(),
                group_id: group_id.into(),
                body: body.to_vec(),
                sent_at: unix_millis(),
                media_id: String::new(),
                seq: 0,
                to_user: String::new(),
            }))
            .await?;
        match self
            .recv_until(|p| {
                matches!(p, Packet::ServerAck(a) if a.message_id == message_id)
                    || matches!(p, Packet::Error(_))
            })
            .await?
        {
            Packet::Error(e) => Err(MessengerError::Protocol(e.detail)),
            _ => Ok(()),
        }
    }

    /// Close the connection gracefully.
    pub async fn close(mut self) -> Result<(), MessengerError> {
        self.framed.close().await
    }
}
