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
use super::e2ee::{E2eeDevice, PeerKeyBundle};
use super::wire;
use super::MessengerError;

#[cfg(feature = "ws")]
use super::ws::{decode_frame, encode_frame};
#[cfg(feature = "ws")]
use tokio_tungstenite::tungstenite::Message;
#[cfg(feature = "ws")]
use tokio_tungstenite::WebSocketStream;

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

enum ClientIo {
    Tcp(Framed<MaybeTlsStream, FrameCodec>),
    #[cfg(feature = "ws")]
    Ws(WebSocketStream<MaybeTlsStream>),
}

impl ClientIo {
    async fn send(&mut self, packet: Packet) -> Result<(), MessengerError> {
        match self {
            ClientIo::Tcp(framed) => framed.send(packet).await,
            #[cfg(feature = "ws")]
            ClientIo::Ws(ws) => {
                let bytes = encode_frame(&packet)?;
                ws.send(Message::Binary(bytes.into()))
                    .await
                    .map_err(|e| MessengerError::Io(std::io::Error::other(e.to_string())))
            }
        }
    }

    async fn recv(&mut self) -> Result<Packet, MessengerError> {
        match self {
            ClientIo::Tcp(framed) => match framed.next().await {
                None => Err(MessengerError::Closed),
                Some(r) => r,
            },
            #[cfg(feature = "ws")]
            ClientIo::Ws(ws) => loop {
                match ws.next().await {
                    None => return Err(MessengerError::Closed),
                    Some(Err(e)) => {
                        return Err(MessengerError::Io(std::io::Error::other(e.to_string())));
                    }
                    Some(Ok(Message::Binary(data))) => return decode_frame(&data),
                    Some(Ok(Message::Close(_))) => return Err(MessengerError::Closed),
                    Some(Ok(Message::Ping(p))) => {
                        ws.send(Message::Pong(p))
                            .await
                            .map_err(|e| MessengerError::Io(std::io::Error::other(e.to_string())))?;
                    }
                    Some(Ok(_)) => continue,
                }
            },
        }
    }

    async fn close(&mut self) -> Result<(), MessengerError> {
        match self {
            ClientIo::Tcp(framed) => framed.close().await,
            #[cfg(feature = "ws")]
            ClientIo::Ws(ws) => {
                ws.close(None)
                    .await
                    .map_err(|e| MessengerError::Io(std::io::Error::other(e.to_string())))
            }
        }
    }
}

/// Blocking-style protocol client. Reads are pull-based via [`recv`]; server
/// pushes (chat, presence, acks) queue in the socket until consumed.
///
/// [`recv`]: MessengerClient::recv
pub struct MessengerClient {
    io: ClientIo,
    user_id: String,
    ping_seq: u64,
}

impl MessengerClient {
    /// Authenticated user id for this connection.
    pub fn user_id(&self) -> &str {
        &self.user_id
    }

    /// Send a raw packet without waiting for a reply (FFI / advanced hosts).
    pub async fn send_packet(&mut self, packet: Packet) -> Result<(), MessengerError> {
        self.io.send(packet).await
    }

    /// Allocate the next ping sequence number (does not send).
    pub fn alloc_ping_seq(&mut self) -> u64 {
        self.ping_seq += 1;
        self.ping_seq
    }

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
        let io = ClientIo::Tcp(Framed::new(socket, FrameCodec::default()));
        Self::login(io, user_id, device_id, auth_token, resume_after_seq).await
    }

    /// Connect over WebSocket (`feature = "ws"`). `url` is e.g. `ws://127.0.0.1:9001/`
    /// or `wss://host/messenger`. Each WS binary message is one full frame.
    #[cfg(feature = "ws")]
    pub async fn connect_ws(
        url: &str,
        tls: Option<&TlsConnector>,
        user_id: &str,
        device_id: &str,
        auth_token: &str,
        resume_after_seq: u64,
    ) -> Result<(Self, LoginOutcome), MessengerError> {
        let parsed = url::Url::parse(url)
            .map_err(|e| MessengerError::Protocol(format!("bad ws url: {e}")))?;
        let host = parsed.host_str().unwrap_or("127.0.0.1");
        let port = parsed.port_or_known_default().unwrap_or(80);
        let addr = format!("{host}:{port}");
        let use_tls = parsed.scheme() == "wss" || tls.is_some();
        let socket = if use_tls {
            stream::connect(&addr, tls).await?
        } else {
            stream::connect(&addr, None).await?
        };
        let (ws, _) = tokio_tungstenite::client_async(url, socket)
            .await
            .map_err(|e| MessengerError::Io(std::io::Error::other(e.to_string())))?;
        Self::login(
            ClientIo::Ws(ws),
            user_id,
            device_id,
            auth_token,
            resume_after_seq,
        )
        .await
    }

    async fn login(
        mut io: ClientIo,
        user_id: &str,
        device_id: &str,
        auth_token: &str,
        resume_after_seq: u64,
    ) -> Result<(Self, LoginOutcome), MessengerError> {
        io.send(Packet::Login(wire::Login {
            user_id: user_id.into(),
            device_id: device_id.into(),
            auth_token: auth_token.into(),
            client_version: env!("CARGO_PKG_VERSION").into(),
            resume_after_seq,
        }))
        .await?;

        let ack = match Self::next_io(&mut io).await? {
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

        let mut replayed = Vec::new();
        let latest_seq = loop {
            match Self::next_io(&mut io).await? {
                Packet::SyncComplete(s) => break s.latest_seq,
                pkt => replayed.push(pkt),
            }
        };

        Ok((
            Self {
                io,
                user_id: user_id.into(),
                ping_seq: 0,
            },
            LoginOutcome {
                session_id: ack.session_id,
                replayed,
                latest_seq,
            },
        ))
    }

    async fn next_io(io: &mut ClientIo) -> Result<Packet, MessengerError> {
        match tokio::time::timeout(RECV_TIMEOUT, io.recv()).await {
            Err(_) => Err(MessengerError::Timeout("server frame")),
            Ok(r) => r,
        }
    }

    /// Receive the next server frame (chat, presence, acks, …).
    pub async fn recv(&mut self) -> Result<Packet, MessengerError> {
        Self::next_io(&mut self.io).await
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
        self.io.send(Packet::Ping(wire::Ping { seq })).await?;
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
        self.io.send(Packet::ChatMessage(wire::ChatMessage {
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

    /// Like [`send_chat`](Self::send_chat) but retries until `ServerAck` or
    /// `max_attempts` is exhausted. Safe to retry the same `message_id` — the
    /// server dedups and re-acks duplicates.
    pub async fn send_chat_with_retry(
        &mut self,
        to_user: &str,
        message_id: &str,
        body: &[u8],
        max_attempts: u32,
    ) -> Result<u64, MessengerError> {
        self.send_chat_with_media_retry(to_user, message_id, body, "", max_attempts)
            .await
    }

    /// Like [`send_chat_with_media`](Self::send_chat_with_media) with exponential
    /// backoff on transient errors (`Timeout`, `Closed`, `Io`).
    pub async fn send_chat_with_media_retry(
        &mut self,
        to_user: &str,
        message_id: &str,
        body: &[u8],
        media_id: &str,
        max_attempts: u32,
    ) -> Result<u64, MessengerError> {
        let attempts = max_attempts.max(1);
        let mut backoff = Duration::from_millis(50);
        for attempt in 0..attempts {
            match self
                .send_chat_with_media(to_user, message_id, body, media_id)
                .await
            {
                Ok(seq) => return Ok(seq),
                Err(e) if attempt + 1 < attempts && Self::send_error_retryable(&e) => {
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                }
                Err(e) => return Err(e),
            }
        }
        unreachable!("loop returns on last attempt")
    }

    fn send_error_retryable(err: &MessengerError) -> bool {
        matches!(
            err,
            MessengerError::Timeout(_) | MessengerError::Closed | MessengerError::Io(_)
        )
    }

    /// Acknowledge delivery of a received message (double tick).
    pub async fn ack_delivered(&mut self, message_id: &str) -> Result<(), MessengerError> {
        self.io.send(Packet::DeliveredAck(wire::DeliveredAck {
                message_id: message_id.into(),
                from_user: self.user_id.clone(),
            }))
            .await?;
        Ok(())
    }

    /// Acknowledge reading a message (blue tick).
    pub async fn ack_read(&mut self, message_id: &str) -> Result<(), MessengerError> {
        self.io.send(Packet::ReadAck(wire::ReadAck {
                message_id: message_id.into(),
                from_user: self.user_id.clone(),
            }))
            .await?;
        Ok(())
    }

    /// Replace this user's presence contact roster. Until called, presence is
    /// broadcast to all online users (legacy). After subscribe, only listed
    /// contacts exchange presence with this user.
    pub async fn subscribe_presence(
        &mut self,
        contact_ids: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<(), MessengerError> {
        self.io.send(Packet::SubscribePresence(wire::SubscribePresence {
                user_id: self.user_id.clone(),
                contact_ids: contact_ids.into_iter().map(Into::into).collect(),
            }))
            .await?;
        Ok(())
    }

    /// Send a client-initiated presence update (e.g. Available / Unavailable).
    pub async fn framed_send_presence(
        &mut self,
        kind: wire::PresenceKind,
    ) -> Result<(), MessengerError> {
        self.io.send(Packet::Presence(wire::Presence {
                user_id: self.user_id.clone(),
                kind: kind as i32,
                last_seen: 0,
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
        self.io.send(Packet::MediaStart(wire::MediaStart {
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
            self.io.send(Packet::MediaChunk(wire::MediaChunk {
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
        self.io.send(Packet::MediaFetch(wire::MediaFetch {
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

    /// Remove a member (caller must be admin).
    pub async fn remove_member(
        &mut self,
        group_id: &str,
        user: &str,
    ) -> Result<u64, MessengerError> {
        self.group_event(group_id, wire::GroupOp::RemoveMember, user).await
    }

    /// Leave a group (caller is the subject).
    pub async fn leave_group(&mut self, group_id: &str) -> Result<u64, MessengerError> {
        self.group_event(group_id, wire::GroupOp::Leave, "").await
    }

    async fn group_event(
        &mut self,
        group_id: &str,
        op: wire::GroupOp,
        subject: &str,
    ) -> Result<u64, MessengerError> {
        self.io.send(Packet::GroupEvent(wire::GroupEvent {
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
        self.io.send(Packet::GroupMessage(wire::GroupMessage {
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
        self.io.close().await
    }

    // ---- E2EE (Phase 10) ----------------------------------------------------

    /// Publish this device's identity + one-time prekeys to the key directory.
    pub async fn publish_keys(
        &mut self,
        device_id: &str,
        identity_key: &str,
        one_time_keys: Vec<String>,
    ) -> Result<(), MessengerError> {
        self.io.send(Packet::PublishKeys(wire::PublishKeys {
                user_id: self.user_id.clone(),
                device_id: device_id.into(),
                identity_key: identity_key.into(),
                one_time_keys,
            }))
            .await?;
        Ok(())
    }

    /// Request a peer's prekey bundle from the key directory.
    pub async fn fetch_key_bundle(&mut self, user_id: &str) -> Result<wire::KeyBundle, MessengerError> {
        self.fetch_key_bundle_for_device(user_id, "").await
    }

    /// Request a specific device's prekey bundle (`device_id` empty = primary).
    pub async fn fetch_key_bundle_for_device(
        &mut self,
        user_id: &str,
        device_id: &str,
    ) -> Result<wire::KeyBundle, MessengerError> {
        let for_user = self.user_id.clone();
        self.io.send(Packet::FetchKeys(wire::FetchKeys {
                user_id: user_id.into(),
                for_user: for_user.clone(),
                device_id: device_id.into(),
            }))
            .await?;
        match self
            .recv_until(|p| {
                matches!(p, Packet::KeyBundle(b) if b.for_user == for_user || b.for_user.is_empty())
            })
            .await?
        {
            Packet::KeyBundle(b) => Ok(b),
            _ => unreachable!(),
        }
    }

    /// Revoke this device's published keys on the server.
    pub async fn remove_device_keys(&mut self, device_id: &str) -> Result<(), MessengerError> {
        self.io.send(Packet::RemoveDeviceKeys(wire::RemoveDeviceKeys {
                user_id: self.user_id.clone(),
                device_id: device_id.into(),
            }))
            .await?;
        Ok(())
    }

    /// Publish keys from an [`E2eeDevice`] and mark them published locally.
    pub async fn publish_e2ee_device(
        &mut self,
        device_id: &str,
        e2ee: &mut E2eeDevice,
        one_time_count: usize,
    ) -> Result<(), MessengerError> {
        let otks = e2ee.generate_one_time_keys(one_time_count);
        self.publish_keys(device_id, &e2ee.identity_key_base64(), otks)
            .await?;
        e2ee.mark_keys_published();
        Ok(())
    }

    /// Fetch a peer bundle and establish an outbound Olm session.
    pub async fn establish_e2ee_session(
        &mut self,
        peer: &str,
        e2ee: &mut E2eeDevice,
    ) -> Result<(), MessengerError> {
        let bundle = self.fetch_key_bundle(peer).await?;
        e2ee.establish_outbound(peer, &PeerKeyBundle::from_wire(&bundle)?)?;
        Ok(())
    }

    /// Send an E2EE 1:1 message; `ChatMessage.body` carries an encrypted payload.
    pub async fn send_encrypted_chat(
        &mut self,
        e2ee: &mut E2eeDevice,
        to_user: &str,
        message_id: &str,
        plaintext: &[u8],
    ) -> Result<u64, MessengerError> {
        let body = e2ee.encrypt_for_peer(to_user, plaintext)?;
        self.send_chat_with_media(to_user, message_id, &body, "").await
    }

    /// Decrypt an incoming `ChatMessage` body from `from_user`.
    pub fn decrypt_chat(
        e2ee: &mut E2eeDevice,
        from_user: &str,
        body: &[u8],
    ) -> Result<Vec<u8>, MessengerError> {
        Ok(e2ee.decrypt_from_sender(from_user, body)?)
    }

    /// Create a Megolm sender session for `group_id` (returns session id).
    pub fn create_group_e2ee_session(e2ee: &mut E2eeDevice, group_id: &str) -> String {
        e2ee.create_group_sender_session(group_id)
    }

    /// Distribute the group sender key to each member via Olm-encrypted 1:1 chat.
    pub async fn distribute_group_session_key(
        &mut self,
        e2ee: &mut E2eeDevice,
        group_id: &str,
        members: &[&str],
    ) -> Result<(), MessengerError> {
        let share = e2ee.group_session_key_share(group_id)?;
        for member in members {
            if *member == self.user_id {
                continue;
            }
            if !e2ee.has_olm_session(member) {
                let bundle = self.fetch_key_bundle(member).await?;
                e2ee.establish_outbound(member, &PeerKeyBundle::from_wire(&bundle)?)?;
            }
            let body = e2ee.encrypt_for_peer(member, &share)?;
            let msg_id = format!("gsk-{group_id}-{member}");
            self.send_chat_with_media(member, &msg_id, &body, "").await?;
        }
        Ok(())
    }

    /// Send an E2EE group message; `GroupMessage.body` carries Megolm ciphertext.
    pub async fn send_encrypted_group(
        &mut self,
        e2ee: &mut E2eeDevice,
        group_id: &str,
        message_id: &str,
        plaintext: &[u8],
    ) -> Result<(), MessengerError> {
        let body = e2ee.encrypt_group_message(group_id, plaintext)?;
        self.send_group(group_id, message_id, &body).await
    }

    /// Decrypt a `GroupMessage` body (Megolm).
    pub fn decrypt_group(
        e2ee: &mut E2eeDevice,
        group_id: &str,
        body: &[u8],
    ) -> Result<Vec<u8>, MessengerError> {
        Ok(e2ee.decrypt_group_message(group_id, body)?)
    }

    /// Handle an incoming 1:1 message that may carry a group session key share.
    pub fn try_import_group_key_from_chat(
        e2ee: &mut E2eeDevice,
        from_user: &str,
        body: &[u8],
    ) -> Result<bool, MessengerError> {
        Ok(e2ee.try_import_group_key_share(from_user, body)?)
    }
}
