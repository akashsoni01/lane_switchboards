//! Opaque session handle: Tokio actor + event queue.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use lane_switchboards::messenger::{
    wire, E2eeDevice, MessengerClient, MessengerError, Packet, PeerKeyBundle,
};
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};

use crate::error::FfiError;
use crate::events::*;
use crate::runtime;

static NEXT_SESSION_ID: AtomicU64 = AtomicU64::new(1);

/// Connect / login options (Phase F2).
#[derive(Debug, Clone)]
pub struct ConnectOptions {
    pub host: String,
    pub port: u16,
    pub use_tls: bool,
    pub user_id: String,
    pub device_id: String,
    pub auth_token: String,
    pub client_version: String,
    pub resume_after_seq: u64,
    /// Auto-ping interval; `0` disables.
    pub ping_interval_secs: u64,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 9000,
            use_tls: false,
            user_id: String::new(),
            device_id: String::new(),
            auth_token: String::new(),
            client_version: format!("ffi-{}", crate::VERSION),
            resume_after_seq: 0,
            ping_interval_secs: crate::PING_INTERVAL_SECS,
        }
    }
}

enum Cmd {
    Ping {
        reply: oneshot::Sender<Result<(), FfiError>>,
    },
    SendChat {
        to_user: String,
        message_id: String,
        body: Vec<u8>,
        media_id: String,
        reply: oneshot::Sender<Result<u64, FfiError>>,
    },
    AckDelivered {
        message_id: String,
        reply: oneshot::Sender<Result<(), FfiError>>,
    },
    AckRead {
        message_id: String,
        reply: oneshot::Sender<Result<(), FfiError>>,
    },
    SubscribePresence {
        contact_ids: Vec<String>,
        reply: oneshot::Sender<Result<(), FfiError>>,
    },
    SendPresence {
        kind: i32,
        reply: oneshot::Sender<Result<(), FfiError>>,
    },
    CreateGroup {
        group_id: String,
        reply: oneshot::Sender<Result<u64, FfiError>>,
    },
    AddMember {
        group_id: String,
        user: String,
        reply: oneshot::Sender<Result<u64, FfiError>>,
    },
    RemoveMember {
        group_id: String,
        user: String,
        reply: oneshot::Sender<Result<u64, FfiError>>,
    },
    LeaveGroup {
        group_id: String,
        reply: oneshot::Sender<Result<u64, FfiError>>,
    },
    SendGroup {
        group_id: String,
        message_id: String,
        body: Vec<u8>,
        reply: oneshot::Sender<Result<(), FfiError>>,
    },
    UploadMedia {
        media_id: String,
        file_name: String,
        mime_type: String,
        data: Vec<u8>,
        reply: oneshot::Sender<Result<u64, FfiError>>,
    },
    FetchMedia {
        media_id: String,
        reply: oneshot::Sender<Result<DownloadedMediaInfo, FfiError>>,
    },
    PublishE2ee {
        device_id: String,
        identity_key: String,
        one_time_keys: Vec<String>,
        reply: oneshot::Sender<Result<(), FfiError>>,
    },
    FetchKeyBundle {
        user_id: String,
        device_id: String,
        reply: oneshot::Sender<Result<KeyBundleInfo, FfiError>>,
    },
    RemoveDeviceKeys {
        device_id: String,
        reply: oneshot::Sender<Result<(), FfiError>>,
    },
    SendEncryptedChat {
        to_user: String,
        message_id: String,
        ciphertext: Vec<u8>,
        reply: oneshot::Sender<Result<u64, FfiError>>,
    },
    SendEncryptedGroup {
        group_id: String,
        message_id: String,
        ciphertext: Vec<u8>,
        reply: oneshot::Sender<Result<(), FfiError>>,
    },
    Close {
        reply: oneshot::Sender<Result<(), FfiError>>,
    },
}

enum Waiter {
    Pong {
        seq: u64,
        reply: oneshot::Sender<Result<(), FfiError>>,
    },
    ServerAck {
        message_id: String,
        reply: oneshot::Sender<Result<u64, FfiError>>,
    },
    GroupEvent {
        group_id: String,
        reply: oneshot::Sender<Result<u64, FfiError>>,
    },
    GroupServerAck {
        message_id: String,
        reply: oneshot::Sender<Result<(), FfiError>>,
    },
    KeyBundle {
        user_id: String,
        reply: oneshot::Sender<Result<KeyBundleInfo, FfiError>>,
    },
    /// Upload: phased credit → chunks → final ack.
    MediaUpload {
        media_id: String,
        remaining: Vec<u8>,
        offset: u64,
        reply: oneshot::Sender<Result<u64, FfiError>>,
        /// false = waiting for initial credit after MediaStart; true = waiting chunk/final ack
        after_credit: bool,
    },
    /// Download: accumulate MediaStart + MediaChunks until last=true.
    MediaDownload {
        media_id: String,
        meta: Option<(String, String, String)>,
        chunks: Vec<u8>,
        reply: oneshot::Sender<Result<DownloadedMediaInfo, FfiError>>,
    },
}

struct ActorState {
    client: MessengerClient,
    events: mpsc::UnboundedSender<LaneEvent>,
    waiters: Vec<Waiter>,
    ping_interval: Duration,
    /// Packets to send after processing an inbound frame (media chunk pipeline).
    outbox: Vec<Packet>,
}

/// Opaque session: commands go to a background actor; events are polled.
pub struct SessionHandle {
    id: u64,
    cmd_tx: mpsc::UnboundedSender<Cmd>,
    events: Arc<Mutex<mpsc::UnboundedReceiver<LaneEvent>>>,
    closed: Arc<Mutex<bool>>,
}

impl SessionHandle {
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Connect, login, emit sync events, start reader + optional auto-ping.
    pub fn connect(opts: ConnectOptions) -> Result<Self, FfiError> {
        if opts.user_id.is_empty() || opts.device_id.is_empty() {
            return Err(FfiError::InvalidArgument(
                "user_id and device_id required".into(),
            ));
        }
        runtime::block_on(Self::connect_async(opts))
    }

    async fn connect_async(opts: ConnectOptions) -> Result<Self, FfiError> {
        let addr = format!("{}:{}", opts.host, opts.port);
        let (client, outcome) = if opts.use_tls {
            #[cfg(feature = "tls")]
            {
                let cfg = lane_switchboards::tls::client_config_from_pem(
                    None::<&str>,
                    None::<&str>,
                    None::<&str>,
                )
                    .map_err(|e| FfiError::Messenger(MessengerError::Io(e)))?;
                let connector = lane_switchboards::tls::build_connector(cfg);
                MessengerClient::connect_tls(
                    &addr,
                    Some(&connector),
                    &opts.user_id,
                    &opts.device_id,
                    &opts.auth_token,
                    opts.resume_after_seq,
                )
                .await?
            }
            #[cfg(not(feature = "tls"))]
            {
                return Err(FfiError::InvalidArgument(
                    "use_tls=true requires the tls feature".into(),
                ));
            }
        } else {
            MessengerClient::connect(
                &addr,
                &opts.user_id,
                &opts.device_id,
                &opts.auth_token,
                opts.resume_after_seq,
            )
            .await?
        };

        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let _ = event_tx.send(LaneEvent::LoginAck(LoginAckEvent {
            session_id: outcome.session_id.clone(),
            pending_messages: outcome.replayed.len() as u32,
            ok: true,
            error: String::new(),
        }));
        let replay_count = outcome.replayed.len() as u32;
        for pkt in outcome.replayed {
            if let Some(ev) = packet_to_event(&pkt) {
                let _ = event_tx.send(LaneEvent::SyncMessage(Box::new(ev)));
            }
        }
        let _ = event_tx.send(LaneEvent::SyncComplete(SyncCompleteEvent {
            delivered: replay_count,
            latest_seq: outcome.latest_seq,
        }));

        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let ping_interval = if opts.ping_interval_secs == 0 {
            Duration::ZERO
        } else {
            Duration::from_secs(opts.ping_interval_secs)
        };

        runtime::runtime().spawn(session_actor(ActorState {
            client,
            events: event_tx,
            waiters: Vec::new(),
            ping_interval,
            outbox: Vec::new(),
        }, cmd_rx));

        Ok(Self {
            id: NEXT_SESSION_ID.fetch_add(1, Ordering::Relaxed),
            cmd_tx,
            events: Arc::new(Mutex::new(event_rx)),
            closed: Arc::new(Mutex::new(false)),
        })
    }

    /// Poll next event; `timeout_ms == 0` returns immediately if empty.
    pub fn poll_event(&self, timeout_ms: u64) -> Option<LaneEvent> {
        let mut rx = self.events.lock();
        if let Ok(ev) = rx.try_recv() {
            return Some(ev);
        }
        if timeout_ms == 0 {
            return None;
        }
        drop(rx);
        let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms);
        loop {
            {
                let mut rx = self.events.lock();
                if let Ok(ev) = rx.try_recv() {
                    return Some(ev);
                }
            }
            if std::time::Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// Drain all currently queued events (non-blocking).
    pub fn drain_events(&self) -> Vec<LaneEvent> {
        let mut out = Vec::new();
        let mut rx = self.events.lock();
        while let Ok(ev) = rx.try_recv() {
            out.push(ev);
        }
        out
    }

    fn call<T>(&self, build: impl FnOnce(oneshot::Sender<Result<T, FfiError>>) -> Cmd) -> Result<T, FfiError> {
        if *self.closed.lock() {
            return Err(FfiError::NotConnected);
        }
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(build(tx))
            .map_err(|_| FfiError::NotConnected)?;
        runtime::block_on(rx).map_err(|_| FfiError::Internal("session actor gone".into()))?
    }

    pub fn ping(&self) -> Result<(), FfiError> {
        self.call(|reply| Cmd::Ping { reply })
    }

    pub fn send_chat(
        &self,
        to_user: &str,
        message_id: &str,
        body: &[u8],
    ) -> Result<u64, FfiError> {
        self.send_chat_with_media(to_user, message_id, body, "")
    }

    pub fn send_chat_with_media(
        &self,
        to_user: &str,
        message_id: &str,
        body: &[u8],
        media_id: &str,
    ) -> Result<u64, FfiError> {
        self.call(|reply| Cmd::SendChat {
            to_user: to_user.into(),
            message_id: message_id.into(),
            body: body.to_vec(),
            media_id: media_id.into(),
            reply,
        })
    }

    pub fn send_chat_with_retry(
        &self,
        to_user: &str,
        message_id: &str,
        body: &[u8],
        max_attempts: u32,
    ) -> Result<u64, FfiError> {
        let attempts = max_attempts.max(1);
        let mut last = FfiError::Internal("no attempts".into());
        for attempt in 0..attempts {
            match self.send_chat(to_user, message_id, body) {
                Ok(seq) => return Ok(seq),
                Err(e) if attempt + 1 < attempts && is_retryable(&e) => {
                    last = e;
                    std::thread::sleep(Duration::from_millis(50u64 << attempt.min(6)));
                }
                Err(e) => return Err(e),
            }
        }
        Err(last)
    }

    pub fn ack_delivered(&self, message_id: &str) -> Result<(), FfiError> {
        self.call(|reply| Cmd::AckDelivered {
            message_id: message_id.into(),
            reply,
        })
    }

    pub fn ack_read(&self, message_id: &str) -> Result<(), FfiError> {
        self.call(|reply| Cmd::AckRead {
            message_id: message_id.into(),
            reply,
        })
    }

    pub fn subscribe_presence(&self, contact_ids: &[String]) -> Result<(), FfiError> {
        self.call(|reply| Cmd::SubscribePresence {
            contact_ids: contact_ids.to_vec(),
            reply,
        })
    }

    pub fn send_presence(&self, kind: i32) -> Result<(), FfiError> {
        self.call(|reply| Cmd::SendPresence { kind, reply })
    }

    pub fn create_group(&self, group_id: &str) -> Result<u64, FfiError> {
        self.call(|reply| Cmd::CreateGroup {
            group_id: group_id.into(),
            reply,
        })
    }

    pub fn add_member(&self, group_id: &str, user: &str) -> Result<u64, FfiError> {
        self.call(|reply| Cmd::AddMember {
            group_id: group_id.into(),
            user: user.into(),
            reply,
        })
    }

    pub fn remove_member(&self, group_id: &str, user: &str) -> Result<u64, FfiError> {
        self.call(|reply| Cmd::RemoveMember {
            group_id: group_id.into(),
            user: user.into(),
            reply,
        })
    }

    pub fn leave_group(&self, group_id: &str) -> Result<u64, FfiError> {
        self.call(|reply| Cmd::LeaveGroup {
            group_id: group_id.into(),
            reply,
        })
    }

    pub fn send_group(
        &self,
        group_id: &str,
        message_id: &str,
        body: &[u8],
    ) -> Result<(), FfiError> {
        self.call(|reply| Cmd::SendGroup {
            group_id: group_id.into(),
            message_id: message_id.into(),
            body: body.to_vec(),
            reply,
        })
    }

    pub fn upload_media(
        &self,
        media_id: &str,
        file_name: &str,
        mime_type: &str,
        data: &[u8],
    ) -> Result<u64, FfiError> {
        self.call(|reply| Cmd::UploadMedia {
            media_id: media_id.into(),
            file_name: file_name.into(),
            mime_type: mime_type.into(),
            data: data.to_vec(),
            reply,
        })
    }

    pub fn fetch_media(&self, media_id: &str) -> Result<DownloadedMediaInfo, FfiError> {
        self.call(|reply| Cmd::FetchMedia {
            media_id: media_id.into(),
            reply,
        })
    }

    pub fn publish_e2ee_keys(
        &self,
        device_id: &str,
        identity_key: &str,
        one_time_keys: &[String],
    ) -> Result<(), FfiError> {
        self.call(|reply| Cmd::PublishE2ee {
            device_id: device_id.into(),
            identity_key: identity_key.into(),
            one_time_keys: one_time_keys.to_vec(),
            reply,
        })
    }

    pub fn fetch_key_bundle(
        &self,
        user_id: &str,
        device_id: &str,
    ) -> Result<KeyBundleInfo, FfiError> {
        self.call(|reply| Cmd::FetchKeyBundle {
            user_id: user_id.into(),
            device_id: device_id.into(),
            reply,
        })
    }

    pub fn remove_device_keys(&self, device_id: &str) -> Result<(), FfiError> {
        self.call(|reply| Cmd::RemoveDeviceKeys {
            device_id: device_id.into(),
            reply,
        })
    }

    /// Send already-encrypted chat body (from [`crate::E2eeHandle`]).
    pub fn send_encrypted_chat_body(
        &self,
        to_user: &str,
        message_id: &str,
        ciphertext: &[u8],
    ) -> Result<u64, FfiError> {
        self.call(|reply| Cmd::SendEncryptedChat {
            to_user: to_user.into(),
            message_id: message_id.into(),
            ciphertext: ciphertext.to_vec(),
            reply,
        })
    }

    pub fn send_encrypted_group_body(
        &self,
        group_id: &str,
        message_id: &str,
        ciphertext: &[u8],
    ) -> Result<(), FfiError> {
        self.call(|reply| Cmd::SendEncryptedGroup {
            group_id: group_id.into(),
            message_id: message_id.into(),
            ciphertext: ciphertext.to_vec(),
            reply,
        })
    }

    /// Establish Olm with peer using a fetched bundle, then return ciphertext helper via device.
    pub fn establish_and_encrypt(
        &self,
        device: &mut E2eeDevice,
        peer_user: &str,
        bundle: &KeyBundleInfo,
        plaintext: &[u8],
    ) -> Result<Vec<u8>, FfiError> {
        let peer = PeerKeyBundle {
            identity_key: bundle.identity_key.clone(),
            one_time_key: bundle.one_time_key.clone(),
        };
        if !device.has_olm_session(peer_user) {
            device
                .establish_outbound(peer_user, &peer)
                .map_err(|e| FfiError::E2ee(e.to_string()))?;
        }
        device
            .encrypt_for_peer(peer_user, plaintext)
            .map_err(|e| FfiError::E2ee(e.to_string()))
    }

    pub fn close(&self) -> Result<(), FfiError> {
        let r = self.call(|reply| Cmd::Close { reply });
        *self.closed.lock() = true;
        r
    }
}

fn is_retryable(e: &FfiError) -> bool {
    matches!(
        e.code(),
        crate::error::FfiErrorCode::Timeout
            | crate::error::FfiErrorCode::Closed
            | crate::error::FfiErrorCode::Io
    )
}

fn unix_millis() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn packet_to_event(pkt: &Packet) -> Option<LaneEvent> {
    match pkt {
        Packet::ChatMessage(m) => Some(LaneEvent::ChatMessage(ChatMessageEvent {
            message_id: m.message_id.clone(),
            from_user: m.from_user.clone(),
            to_user: m.to_user.clone(),
            body: m.body.clone(),
            sent_at: m.sent_at,
            seq: m.seq,
            media_id: m.media_id.clone(),
        })),
        Packet::ServerAck(a) => Some(LaneEvent::ServerAck(ServerAckEvent {
            message_id: a.message_id.clone(),
            seq: a.seq,
        })),
        Packet::DeliveredAck(a) => Some(LaneEvent::DeliveredAck(DeliveredAckEvent {
            message_id: a.message_id.clone(),
            from_user: a.from_user.clone(),
        })),
        Packet::ReadAck(a) => Some(LaneEvent::ReadAck(ReadAckEvent {
            message_id: a.message_id.clone(),
            from_user: a.from_user.clone(),
        })),
        Packet::GroupAckSummary(s) => Some(LaneEvent::GroupAckSummary(GroupAckSummaryEvent {
            message_id: s.message_id.clone(),
            group_id: s.group_id.clone(),
            delivered_by: s.delivered_by.clone(),
            read_by: s.read_by.clone(),
            member_count: s.member_count,
        })),
        Packet::Presence(p) => Some(LaneEvent::Presence(PresenceEvent {
            user_id: p.user_id.clone(),
            kind: p.kind,
            last_seen: p.last_seen,
        })),
        Packet::GroupMessage(m) => Some(LaneEvent::GroupMessage(GroupMessageEvent {
            message_id: m.message_id.clone(),
            from_user: m.from_user.clone(),
            group_id: m.group_id.clone(),
            body: m.body.clone(),
            sent_at: m.sent_at,
            media_id: m.media_id.clone(),
            seq: m.seq,
            to_user: m.to_user.clone(),
        })),
        Packet::GroupEvent(e) => Some(LaneEvent::GroupEvent(GroupEventInfo {
            group_id: e.group_id.clone(),
            op: e.op,
            actor_user: e.actor_user.clone(),
            subject_user: e.subject_user.clone(),
            version: e.version,
            to_user: e.to_user.clone(),
        })),
        Packet::MediaStart(m) => Some(LaneEvent::MediaStart(MediaStartEvent {
            media_id: m.media_id.clone(),
            file_name: m.file_name.clone(),
            mime_type: m.mime_type.clone(),
            total_size: m.total_size,
            sha256: m.sha256.clone(),
        })),
        Packet::MediaChunk(c) => Some(LaneEvent::MediaChunk(MediaChunkEvent {
            media_id: c.media_id.clone(),
            offset: c.offset,
            data: c.data.clone(),
            last: c.last,
        })),
        Packet::MediaAck(a) => Some(LaneEvent::MediaAck(MediaAckEvent {
            media_id: a.media_id.clone(),
            ok: a.ok,
            complete: a.complete,
            received_bytes: a.received_bytes,
            error: a.error.clone(),
        })),
        Packet::KeyBundle(k) => Some(LaneEvent::KeyBundle(KeyBundleInfo {
            user_id: k.user_id.clone(),
            device_id: k.device_id.clone(),
            identity_key: k.identity_key.clone(),
            one_time_key: k.one_time_key.clone(),
            found: k.found,
            device_ids: k.device_ids.clone(),
        })),
        Packet::Error(e) => {
            if e.code == wire::ErrorCode::ReplacedByNewSession as i32 {
                Some(LaneEvent::ReplacedByNewSession)
            } else {
                Some(LaneEvent::ProtocolError(ProtocolErrorEvent {
                    code: e.code,
                    detail: e.detail.clone(),
                }))
            }
        }
        Packet::Pong(p) => Some(LaneEvent::Pong { seq: p.seq }),
        // Peer / login / sync / subscribe / fetch — not host events
        _ => None,
    }
}

async fn session_actor(mut state: ActorState, mut cmd_rx: mpsc::UnboundedReceiver<Cmd>) {
    let mut ping_tick = if state.ping_interval.is_zero() {
        None
    } else {
        Some(tokio::time::interval(state.ping_interval))
    };

    loop {
        while let Some(pkt) = state.outbox.pop() {
            if let Err(e) = state.client.send_packet(pkt).await {
                let reason = e.to_string();
                fail_all_waiters(&mut state, FfiError::Messenger(e));
                let _ = state.events.send(LaneEvent::Disconnected { reason });
                return;
            }
        }

        tokio::select! {
            biased;
            cmd = cmd_rx.recv() => {
                let Some(cmd) = cmd else { break; };
                if handle_cmd(&mut state, cmd).await {
                    break;
                }
            }
            pkt = state.client.recv() => {
                match pkt {
                    Ok(p) => dispatch_packet(&mut state, p),
                    Err(e) => {
                        let reason = e.to_string();
                        fail_all_waiters(&mut state, FfiError::Messenger(e));
                        let _ = state.events.send(LaneEvent::Disconnected { reason });
                        break;
                    }
                }
            }
            _ = async {
                if let Some(ref mut t) = ping_tick {
                    t.tick().await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                let seq = state.client.alloc_ping_seq();
                if let Err(e) = state.client.send_packet(Packet::Ping(wire::Ping { seq })).await {
                    let _ = state.events.send(LaneEvent::Disconnected { reason: e.to_string() });
                    break;
                }
            }
        }
    }
}

fn fail_all_waiters(state: &mut ActorState, err: FfiError) {
    for w in state.waiters.drain(..) {
        match w {
            Waiter::Pong { reply, .. } => {
                let _ = reply.send(Err(clone_err(&err)));
            }
            Waiter::ServerAck { reply, .. } => {
                let _ = reply.send(Err(clone_err(&err)));
            }
            Waiter::GroupEvent { reply, .. } => {
                let _ = reply.send(Err(clone_err(&err)));
            }
            Waiter::GroupServerAck { reply, .. } => {
                let _ = reply.send(Err(clone_err(&err)));
            }
            Waiter::KeyBundle { reply, .. } => {
                let _ = reply.send(Err(clone_err(&err)));
            }
            Waiter::MediaUpload { reply, .. } => {
                let _ = reply.send(Err(clone_err(&err)));
            }
            Waiter::MediaDownload { reply, .. } => {
                let _ = reply.send(Err(clone_err(&err)));
            }
        }
    }
}

fn clone_err(e: &FfiError) -> FfiError {
    FfiError::Internal(e.detail())
}

fn dispatch_packet(state: &mut ActorState, pkt: Packet) {
    // Try waiters first for request/response.
    if try_complete_waiter(state, &pkt) {
        // Still emit host-visible events for acks / chat / etc.
    }
    match &pkt {
        Packet::Pong(_) => return, // consumed by waiter or ignored for auto-ping
        Packet::Error(e) if e.code == wire::ErrorCode::ReplacedByNewSession as i32 => {
            let _ = state.events.send(LaneEvent::ReplacedByNewSession);
            return;
        }
        _ => {}
    }
    if let Some(ev) = packet_to_event(&pkt) {
        match &ev {
            LaneEvent::Pong { .. } => {}
            _ => {
                let _ = state.events.send(ev);
            }
        }
    }
}

fn try_complete_waiter(state: &mut ActorState, pkt: &Packet) -> bool {
    let mut idx = None;
    for (i, w) in state.waiters.iter().enumerate() {
        let matched = match (w, pkt) {
            (Waiter::Pong { seq, .. }, Packet::Pong(p)) => p.seq == *seq,
            (Waiter::ServerAck { message_id, .. }, Packet::ServerAck(a)) => {
                a.message_id == *message_id
            }
            (Waiter::GroupServerAck { message_id, .. }, Packet::ServerAck(a)) => {
                a.message_id == *message_id
            }
            (Waiter::GroupEvent { group_id, .. }, Packet::GroupEvent(e)) => {
                e.group_id == *group_id
            }
            (Waiter::KeyBundle { user_id, .. }, Packet::KeyBundle(k)) => k.user_id == *user_id,
            (Waiter::MediaUpload { media_id, .. }, Packet::MediaAck(a)) => {
                a.media_id == *media_id
            }
            (Waiter::MediaDownload { media_id, .. }, Packet::MediaStart(s)) => {
                s.media_id == *media_id
            }
            (Waiter::MediaDownload { media_id, .. }, Packet::MediaChunk(c)) => {
                c.media_id == *media_id
            }
            (Waiter::MediaDownload { .. }, Packet::Error(_)) => true,
            (Waiter::ServerAck { .. }, Packet::Error(_)) => true,
            (Waiter::GroupServerAck { .. }, Packet::Error(_)) => true,
            (Waiter::GroupEvent { .. }, Packet::Error(_)) => true,
            (Waiter::MediaUpload { .. }, Packet::Error(_)) => true,
            _ => false,
        };
        if matched {
            idx = Some(i);
            break;
        }
    }
    let Some(i) = idx else {
        return false;
    };

    if matches!(state.waiters[i], Waiter::MediaDownload { .. }) {
        return complete_media_download(state, i, pkt);
    }
    if matches!(state.waiters[i], Waiter::MediaUpload { .. }) {
        // Handled async below — mark and process in place.
        return complete_media_upload_ack(state, i, pkt);
    }

    let w = state.waiters.remove(i);
    match (w, pkt) {
        (Waiter::Pong { reply, .. }, Packet::Pong(_)) => {
            let _ = reply.send(Ok(()));
        }
        (Waiter::ServerAck { reply, .. }, Packet::ServerAck(a)) => {
            let _ = reply.send(Ok(a.seq));
        }
        (Waiter::GroupServerAck { reply, .. }, Packet::ServerAck(_)) => {
            let _ = reply.send(Ok(()));
        }
        (Waiter::GroupEvent { reply, .. }, Packet::GroupEvent(e)) => {
            let _ = reply.send(Ok(e.version));
        }
        (Waiter::KeyBundle { reply, .. }, Packet::KeyBundle(k)) => {
            let _ = reply.send(Ok(KeyBundleInfo {
                user_id: k.user_id.clone(),
                device_id: k.device_id.clone(),
                identity_key: k.identity_key.clone(),
                one_time_key: k.one_time_key.clone(),
                found: k.found,
                device_ids: k.device_ids.clone(),
            }));
        }
        (w, Packet::Error(e)) => {
            let err = FfiError::Messenger(MessengerError::Protocol(e.detail.clone()));
            match w {
                Waiter::Pong { reply, .. } => {
                    let _ = reply.send(Err(err));
                }
                Waiter::ServerAck { reply, .. } => {
                    let _ = reply.send(Err(err));
                }
                Waiter::GroupEvent { reply, .. } => {
                    let _ = reply.send(Err(err));
                }
                Waiter::GroupServerAck { reply, .. } => {
                    let _ = reply.send(Err(err));
                }
                Waiter::KeyBundle { reply, .. } => {
                    let _ = reply.send(Err(err));
                }
                Waiter::MediaUpload { reply, .. } => {
                    let _ = reply.send(Err(err));
                }
                Waiter::MediaDownload { reply, .. } => {
                    let _ = reply.send(Err(err));
                }
            }
        }
        _ => {}
    }
    true
}

fn complete_media_download(state: &mut ActorState, i: usize, pkt: &Packet) -> bool {
    match pkt {
        Packet::MediaStart(s) => {
            if let Waiter::MediaDownload { meta, .. } = &mut state.waiters[i] {
                *meta = Some((s.file_name.clone(), s.mime_type.clone(), s.sha256.clone()));
            }
            true
        }
        Packet::MediaChunk(c) => {
            let finish = c.last;
            if let Waiter::MediaDownload { chunks, .. } = &mut state.waiters[i] {
                chunks.extend_from_slice(&c.data);
            }
            if finish {
                if let Waiter::MediaDownload {
                    meta,
                    chunks,
                    reply,
                    ..
                } = state.waiters.remove(i)
                {
                    let (file_name, mime_type, sha256) = meta.unwrap_or_default();
                    if !sha256.is_empty() {
                        use sha2::{Digest, Sha256};
                        let got = format!("{:x}", Sha256::digest(&chunks));
                        if !got.eq_ignore_ascii_case(&sha256) {
                            let _ = reply.send(Err(FfiError::Messenger(MessengerError::Media(
                                "downloaded blob failed sha256 check".into(),
                            ))));
                            return true;
                        }
                    }
                    let _ = reply.send(Ok(DownloadedMediaInfo {
                        file_name,
                        mime_type,
                        sha256,
                        data: chunks,
                    }));
                }
            }
            true
        }
        Packet::Error(e) => {
            if let Waiter::MediaDownload { reply, .. } = state.waiters.remove(i) {
                let _ = reply.send(Err(FfiError::Messenger(MessengerError::Protocol(
                    e.detail.clone(),
                ))));
            }
            true
        }
        _ => false,
    }
}

fn complete_media_upload_ack(state: &mut ActorState, i: usize, pkt: &Packet) -> bool {
    match pkt {
        Packet::Error(e) => {
            if let Waiter::MediaUpload { reply, .. } = state.waiters.remove(i) {
                let _ = reply.send(Err(FfiError::Messenger(MessengerError::Protocol(
                    e.detail.clone(),
                ))));
            }
            true
        }
        Packet::MediaAck(a) => {
            if !a.ok {
                if let Waiter::MediaUpload { reply, .. } = state.waiters.remove(i) {
                    let _ = reply.send(Err(FfiError::Messenger(MessengerError::Media(
                        a.error.clone(),
                    ))));
                }
                return true;
            }
            if a.complete {
                if let Waiter::MediaUpload { reply, .. } = state.waiters.remove(i) {
                    let _ = reply.send(Ok(a.received_bytes));
                }
                return true;
            }
            // Credit or per-chunk ack: queue next chunk if any remain.
            let chunk_size = crate::MEDIA_CHUNK_SIZE as usize;
            let next = {
                let Waiter::MediaUpload {
                    media_id,
                    remaining,
                    offset,
                    after_credit,
                    ..
                } = &mut state.waiters[i]
                else {
                    return true;
                };
                *after_credit = true;
                if remaining.is_empty() {
                    None
                } else {
                    let end = chunk_size.min(remaining.len());
                    let data = remaining.drain(..end).collect::<Vec<_>>();
                    let last = remaining.is_empty();
                    let off = *offset;
                    *offset += data.len() as u64;
                    Some(Packet::MediaChunk(wire::MediaChunk {
                        media_id: media_id.clone(),
                        offset: off,
                        data,
                        last,
                    }))
                }
            };
            if let Some(pkt) = next {
                state.outbox.push(pkt);
            }
            true
        }
        _ => false,
    }
}

async fn handle_cmd(state: &mut ActorState, cmd: Cmd) -> bool {
    match cmd {
        Cmd::Ping { reply } => {
            let seq = state.client.alloc_ping_seq();
            match state
                .client
                .send_packet(Packet::Ping(wire::Ping { seq }))
                .await
            {
                Ok(()) => state.waiters.push(Waiter::Pong { seq, reply }),
                Err(e) => {
                    let _ = reply.send(Err(e.into()));
                }
            }
            false
        }
        Cmd::SendChat {
            to_user,
            message_id,
            body,
            media_id,
            reply,
        } => {
            let user = state.client.user_id().to_string();
            match state
                .client
                .send_packet(Packet::ChatMessage(wire::ChatMessage {
                    message_id: message_id.clone(),
                    from_user: user,
                    to_user,
                    body,
                    sent_at: unix_millis(),
                    seq: 0,
                    media_id,
                }))
                .await
            {
                Ok(()) => state.waiters.push(Waiter::ServerAck { message_id, reply }),
                Err(e) => {
                    let _ = reply.send(Err(e.into()));
                }
            }
            false
        }
        Cmd::AckDelivered { message_id, reply } => {
            let user = state.client.user_id().to_string();
            let r = state
                .client
                .send_packet(Packet::DeliveredAck(wire::DeliveredAck {
                    message_id,
                    from_user: user,
                }))
                .await
                .map_err(Into::into);
            let _ = reply.send(r);
            false
        }
        Cmd::AckRead { message_id, reply } => {
            let user = state.client.user_id().to_string();
            let r = state
                .client
                .send_packet(Packet::ReadAck(wire::ReadAck {
                    message_id,
                    from_user: user,
                }))
                .await
                .map_err(Into::into);
            let _ = reply.send(r);
            false
        }
        Cmd::SubscribePresence {
            contact_ids,
            reply,
        } => {
            let user = state.client.user_id().to_string();
            let r = state
                .client
                .send_packet(Packet::SubscribePresence(wire::SubscribePresence {
                    user_id: user,
                    contact_ids,
                }))
                .await
                .map_err(Into::into);
            let _ = reply.send(r);
            false
        }
        Cmd::SendPresence { kind, reply } => {
            let user = state.client.user_id().to_string();
            let r = state
                .client
                .send_packet(Packet::Presence(wire::Presence {
                    user_id: user,
                    kind,
                    last_seen: unix_millis(),
                }))
                .await
                .map_err(Into::into);
            let _ = reply.send(r);
            false
        }
        Cmd::CreateGroup { group_id, reply } => {
            send_group_op(state, group_id, wire::GroupOp::Create, "", reply).await;
            false
        }
        Cmd::AddMember {
            group_id,
            user,
            reply,
        } => {
            send_group_op(state, group_id, wire::GroupOp::AddMember, &user, reply).await;
            false
        }
        Cmd::RemoveMember {
            group_id,
            user,
            reply,
        } => {
            send_group_op(state, group_id, wire::GroupOp::RemoveMember, &user, reply).await;
            false
        }
        Cmd::LeaveGroup { group_id, reply } => {
            send_group_op(state, group_id, wire::GroupOp::Leave, "", reply).await;
            false
        }
        Cmd::SendGroup {
            group_id,
            message_id,
            body,
            reply,
        } => {
            let user = state.client.user_id().to_string();
            match state
                .client
                .send_packet(Packet::GroupMessage(wire::GroupMessage {
                    message_id: message_id.clone(),
                    from_user: user,
                    group_id,
                    body,
                    sent_at: unix_millis(),
                    media_id: String::new(),
                    seq: 0,
                    to_user: String::new(),
                }))
                .await
            {
                Ok(()) => state
                    .waiters
                    .push(Waiter::GroupServerAck { message_id, reply }),
                Err(e) => {
                    let _ = reply.send(Err(e.into()));
                }
            }
            false
        }
        Cmd::UploadMedia {
            media_id,
            file_name,
            mime_type,
            data,
            reply,
        } => {
            upload_media_cmd(state, media_id, file_name, mime_type, data, reply).await;
            false
        }
        Cmd::FetchMedia { media_id, reply } => {
            match state
                .client
                .send_packet(Packet::MediaFetch(wire::MediaFetch {
                    media_id: media_id.clone(),
                    from_offset: 0,
                }))
                .await
            {
                Ok(()) => state.waiters.push(Waiter::MediaDownload {
                    media_id,
                    meta: None,
                    chunks: Vec::new(),
                    reply,
                }),
                Err(e) => {
                    let _ = reply.send(Err(e.into()));
                }
            }
            false
        }
        Cmd::PublishE2ee {
            device_id,
            identity_key,
            one_time_keys,
            reply,
        } => {
            let user = state.client.user_id().to_string();
            let r = state
                .client
                .send_packet(Packet::PublishKeys(wire::PublishKeys {
                    user_id: user,
                    device_id,
                    identity_key,
                    one_time_keys,
                }))
                .await
                .map_err(Into::into);
            let _ = reply.send(r);
            false
        }
        Cmd::FetchKeyBundle {
            user_id,
            device_id,
            reply,
        } => {
            let for_user = state.client.user_id().to_string();
            match state
                .client
                .send_packet(Packet::FetchKeys(wire::FetchKeys {
                    user_id: user_id.clone(),
                    device_id,
                    for_user,
                }))
                .await
            {
                Ok(()) => state.waiters.push(Waiter::KeyBundle { user_id, reply }),
                Err(e) => {
                    let _ = reply.send(Err(e.into()));
                }
            }
            false
        }
        Cmd::RemoveDeviceKeys { device_id, reply } => {
            let user = state.client.user_id().to_string();
            let r = state
                .client
                .send_packet(Packet::RemoveDeviceKeys(wire::RemoveDeviceKeys {
                    user_id: user,
                    device_id,
                }))
                .await
                .map_err(Into::into);
            let _ = reply.send(r);
            false
        }
        Cmd::SendEncryptedChat {
            to_user,
            message_id,
            ciphertext,
            reply,
        } => {
            let user = state.client.user_id().to_string();
            match state
                .client
                .send_packet(Packet::ChatMessage(wire::ChatMessage {
                    message_id: message_id.clone(),
                    from_user: user,
                    to_user,
                    body: ciphertext,
                    sent_at: unix_millis(),
                    seq: 0,
                    media_id: String::new(),
                }))
                .await
            {
                Ok(()) => state.waiters.push(Waiter::ServerAck { message_id, reply }),
                Err(e) => {
                    let _ = reply.send(Err(e.into()));
                }
            }
            false
        }
        Cmd::SendEncryptedGroup {
            group_id,
            message_id,
            ciphertext,
            reply,
        } => {
            let user = state.client.user_id().to_string();
            match state
                .client
                .send_packet(Packet::GroupMessage(wire::GroupMessage {
                    message_id: message_id.clone(),
                    from_user: user,
                    group_id,
                    body: ciphertext,
                    sent_at: unix_millis(),
                    media_id: String::new(),
                    seq: 0,
                    to_user: String::new(),
                }))
                .await
            {
                Ok(()) => state
                    .waiters
                    .push(Waiter::GroupServerAck { message_id, reply }),
                Err(e) => {
                    let _ = reply.send(Err(e.into()));
                }
            }
            false
        }
        Cmd::Close { reply } => {
            // Dropping the client closes the socket.
            let _ = reply.send(Ok(()));
            true
        }
    }
}

async fn send_group_op(
    state: &mut ActorState,
    group_id: String,
    op: wire::GroupOp,
    subject: &str,
    reply: oneshot::Sender<Result<u64, FfiError>>,
) {
    let user = state.client.user_id().to_string();
    match state
        .client
        .send_packet(Packet::GroupEvent(wire::GroupEvent {
            group_id: group_id.clone(),
            op: op as i32,
            actor_user: user,
            subject_user: subject.into(),
            version: 0,
            to_user: String::new(),
        }))
        .await
    {
        Ok(()) => state.waiters.push(Waiter::GroupEvent { group_id, reply }),
        Err(e) => {
            let _ = reply.send(Err(e.into()));
        }
    }
}

async fn upload_media_cmd(
    state: &mut ActorState,
    media_id: String,
    file_name: String,
    mime_type: String,
    data: Vec<u8>,
    reply: oneshot::Sender<Result<u64, FfiError>>,
) {
    use sha2::{Digest, Sha256};
    let sha = format!("{:x}", Sha256::digest(&data));
    let start = wire::MediaStart {
        media_id: media_id.clone(),
        file_name,
        mime_type,
        total_size: data.len() as u64,
        sha256: sha,
    };
    if let Err(e) = state.client.send_packet(Packet::MediaStart(start)).await {
        let _ = reply.send(Err(e.into()));
        return;
    }
    state.waiters.push(Waiter::MediaUpload {
        media_id,
        remaining: data,
        offset: 0,
        reply,
        after_credit: false,
    });
}
