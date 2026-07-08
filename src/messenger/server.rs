//! Messenger gateway server.
//!
//! One [`MessengerServer`] binds a TCP listener; every accepted socket becomes
//! a session task speaking the compact binary protocol ([`super::codec`]).
//!
//! Responsibilities implemented here (single node):
//! - login + HMAC auth with a pre-auth deadline
//! - session registry with same-device replacement
//! - presence (available / unavailable / last-seen) with contact broadcast
//! - message routing: online delivery or offline inbox with per-user seq
//! - ack ladder: `ServerAck` (persisted) → `DeliveredAck` → `ReadAck`
//! - dedup by `message_id` (at-least-once from clients, exactly-once effect)
//! - offline replay on login (`resume_after_seq`) ending in `SyncComplete`
//! - ping/pong heartbeats + idle timeout sweep
//! - chunked media (bulk data e.g. PDF) upload / fetch with sha256 verify
//! - groups: membership with versioning + fan-out through the same router

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use sha2::{Digest, Sha256};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio_util::codec::Framed;
use tracing::{debug, info, warn};

use super::codec::{FrameCodec, Packet};
use super::wire;
use super::{Authenticator, MessengerError};

/// Tunables for a gateway node. All limits have safe defaults.
#[derive(Clone)]
pub struct ServerConfig {
    /// Max payload bytes per frame.
    pub max_frame: usize,
    /// A connection must complete login within this window.
    pub login_deadline: Duration,
    /// Sessions silent for longer than this are closed (2 missed ping
    /// intervals at a 30 s client cadence by default).
    pub idle_timeout: Duration,
    /// Outbound frames buffered per session before the session is dropped as
    /// a slow consumer.
    pub session_buffer: usize,
    /// Max pending offline messages retained per user (oldest dropped).
    pub max_inbox: usize,
    /// Max media blob size in bytes.
    pub max_media_bytes: u64,
    /// Max members per group.
    pub max_group_members: usize,
    /// Max new connections accepted per source IP per minute (0 = unlimited).
    pub max_conns_per_ip_per_min: u32,
    /// Base delay for exponential login-failure backoff (doubles per
    /// consecutive failure for the same user, capped at `auth_backoff_max`).
    pub auth_backoff_base: Duration,
    /// Upper bound for login-failure backoff.
    pub auth_backoff_max: Duration,
    /// TCP keepalive probe interval for client sockets (`None` disables).
    /// Complements application-level Ping/Pong: keepalive detects dead NAT
    /// paths below the protocol layer.
    pub tcp_keepalive: Option<Duration>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_frame: super::codec::DEFAULT_MAX_FRAME,
            login_deadline: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(90),
            session_buffer: 256,
            max_inbox: 10_000,
            max_media_bytes: 64 * 1024 * 1024,
            max_group_members: 1024,
            max_conns_per_ip_per_min: 120,
            auth_backoff_base: Duration::from_millis(250),
            auth_backoff_max: Duration::from_secs(30),
            tcp_keepalive: Some(Duration::from_secs(60)),
        }
    }
}

fn unix_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

/// Handle to a connected device session: outbound frame queue + liveness.
struct SessionHandle {
    session_id: u64,
    device_id: String,
    tx: mpsc::Sender<Packet>,
    last_seen: Instant,
}

/// A message persisted for (eventual) delivery to one recipient device set.
#[derive(Clone)]
struct StoredMessage {
    seq: u64,
    packet: Packet,
    delivered: bool,
}

#[derive(Default)]
struct Inbox {
    next_seq: u64,
    /// Ordered by seq. Entries stay until `DeliveredAck` (tombstoned on ack).
    pending: VecDeque<StoredMessage>,
    /// message_ids already accepted — dedup across sender retries.
    seen: HashSet<String>,
}

struct MediaBlob {
    file_name: String,
    mime_type: String,
    total_size: u64,
    sha256: String,
    data: Vec<u8>,
    complete: bool,
}

struct Group {
    members: HashSet<String>,
    admins: HashSet<String>,
    version: u64,
}

/// Shared state for one gateway node.
struct State {
    cfg: ServerConfig,
    auth: Arc<dyn Authenticator>,
    /// user_id -> live device sessions.
    sessions: RwLock<HashMap<String, Vec<SessionHandle>>>,
    /// user_id -> last seen (unix secs) for offline users.
    last_seen: RwLock<HashMap<String, u64>>,
    /// user_id -> inbox (offline store + dedup + seq).
    inboxes: Mutex<HashMap<String, Inbox>>,
    /// media_id -> blob (in-memory store; swap for StorageNode later).
    media: Mutex<HashMap<String, MediaBlob>>,
    groups: Mutex<HashMap<String, Group>>,
    session_ids: AtomicU64,
    /// source IP -> (window start, connections accepted in window).
    conn_rate: Mutex<HashMap<std::net::IpAddr, (Instant, u32)>>,
    /// user_id -> consecutive login failures (drives exponential backoff).
    auth_failures: Mutex<HashMap<String, u32>>,
}

/// A running messenger gateway. Dropping the handle aborts the accept loop.
pub struct MessengerServer {
    addr: SocketAddr,
    state: Arc<State>,
    accept_task: JoinHandle<()>,
    sweep_task: JoinHandle<()>,
}

impl Drop for MessengerServer {
    fn drop(&mut self) {
        self.accept_task.abort();
        self.sweep_task.abort();
    }
}

impl MessengerServer {
    /// Bind `addr` and start accepting client connections.
    pub async fn bind(
        addr: &str,
        auth: Arc<dyn Authenticator>,
        cfg: ServerConfig,
    ) -> Result<Self, MessengerError> {
        let listener = TcpListener::bind(addr).await?;
        let addr = listener.local_addr()?;
        let state = Arc::new(State {
            cfg,
            auth,
            sessions: RwLock::new(HashMap::new()),
            last_seen: RwLock::new(HashMap::new()),
            inboxes: Mutex::new(HashMap::new()),
            media: Mutex::new(HashMap::new()),
            groups: Mutex::new(HashMap::new()),
            session_ids: AtomicU64::new(1),
            conn_rate: Mutex::new(HashMap::new()),
            auth_failures: Mutex::new(HashMap::new()),
        });

        // Idle sweep: close sessions that missed heartbeats.
        let sweep_state = state.clone();
        let sweep_task = tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            loop {
                tick.tick().await;
                sweep_idle(&sweep_state).await;
            }
        });

        let accept_state = state.clone();
        let accept_task = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((socket, peer)) => {
                        let st = accept_state.clone();
                        if !admit_connection(&st, peer.ip()).await {
                            debug!(%peer, "connection rejected: per-IP rate limit");
                            drop(socket);
                            continue;
                        }
                        tokio::spawn(async move {
                            if let Err(e) = run_session(st, socket).await {
                                debug!(%peer, error = %e, "session ended");
                            }
                        });
                    }
                    Err(e) => {
                        warn!(error = %e, "accept failed");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        });

        info!(%addr, "messenger gateway listening");
        Ok(Self { addr, state, accept_task, sweep_task })
    }

    /// Actual bound address (useful with port 0 in tests).
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Number of pending offline messages for `user_id`.
    pub async fn pending_for(&self, user_id: &str) -> usize {
        self.state
            .inboxes
            .lock()
            .await
            .get(user_id)
            .map(|i| i.pending.iter().filter(|m| !m.delivered).count())
            .unwrap_or(0)
    }

    /// True when the user has at least one live session.
    pub async fn is_online(&self, user_id: &str) -> bool {
        self.state.sessions.read().await.get(user_id).is_some_and(|v| !v.is_empty())
    }

    /// Graceful shutdown: stop accepting new connections, then close every
    /// session. Outbound queues drain because closing the sender side lets
    /// each session loop flush already-queued frames and return.
    pub async fn shutdown(self) {
        self.accept_task.abort();
        self.sweep_task.abort();
        let drained: Vec<String> = {
            let mut sessions = self.state.sessions.write().await;
            let users: Vec<String> = sessions.keys().cloned().collect();
            // Dropping the SessionHandles drops the mpsc senders; each
            // session task observes `rx.recv() == None` and exits cleanly.
            sessions.clear();
            users
        };
        let now = unix_secs();
        let mut last_seen = self.state.last_seen.write().await;
        for user in drained {
            last_seen.insert(user, now);
        }
        info!("messenger gateway shut down");
    }
}

/// Sliding one-minute window per source IP. Returns false when the IP
/// exceeded its connection budget.
async fn admit_connection(state: &Arc<State>, ip: std::net::IpAddr) -> bool {
    let limit = state.cfg.max_conns_per_ip_per_min;
    if limit == 0 {
        return true;
    }
    let mut rate = state.conn_rate.lock().await;
    // Opportunistic cleanup keeps the map bounded without a background task.
    if rate.len() > 10_000 {
        rate.retain(|_, (start, _)| start.elapsed() < Duration::from_secs(60));
    }
    let entry = rate.entry(ip).or_insert((Instant::now(), 0));
    if entry.0.elapsed() >= Duration::from_secs(60) {
        *entry = (Instant::now(), 0);
    }
    entry.1 += 1;
    entry.1 <= limit
}

async fn sweep_idle(state: &Arc<State>) {
    let idle = state.cfg.idle_timeout;
    let mut stale: Vec<(String, u64)> = Vec::new();
    {
        let sessions = state.sessions.read().await;
        for (user, handles) in sessions.iter() {
            for h in handles {
                if h.last_seen.elapsed() > idle {
                    stale.push((user.clone(), h.session_id));
                }
            }
        }
    }
    for (user, sid) in stale {
        debug!(user, sid, "closing idle session");
        remove_session(state, &user, sid).await;
    }
}

/// Detach a session, update last-seen, broadcast Unavailable when the user's
/// last device went away.
async fn remove_session(state: &Arc<State>, user: &str, session_id: u64) {
    let now_offline = {
        let mut sessions = state.sessions.write().await;
        let Some(handles) = sessions.get_mut(user) else { return };
        handles.retain(|h| h.session_id != session_id);
        if handles.is_empty() {
            sessions.remove(user);
            true
        } else {
            false
        }
    };
    if now_offline {
        state.last_seen.write().await.insert(user.to_string(), unix_secs());
        broadcast_presence(state, user, wire::PresenceKind::Unavailable).await;
    }
}

/// Send a presence update about `user` to every online session (roster model
/// intentionally simple: all online users; contact filtering is a follow-up).
async fn broadcast_presence(state: &Arc<State>, user: &str, kind: wire::PresenceKind) {
    let last_seen = if kind == wire::PresenceKind::Unavailable { unix_secs() } else { 0 };
    let pkt = Packet::Presence(wire::Presence {
        user_id: user.to_string(),
        kind: kind as i32,
        last_seen,
    });
    let sessions = state.sessions.read().await;
    for (other, handles) in sessions.iter() {
        if other == user {
            continue;
        }
        for h in handles {
            let _ = h.tx.try_send(pkt.clone());
        }
    }
}

/// Deliver `pkt` to all live sessions of `user`. Returns true if at least one
/// session accepted it.
async fn deliver_online(state: &Arc<State>, user: &str, pkt: &Packet) -> bool {
    let sessions = state.sessions.read().await;
    let Some(handles) = sessions.get(user) else { return false };
    let mut any = false;
    for h in handles {
        if h.tx.try_send(pkt.clone()).is_ok() {
            any = true;
        }
    }
    any
}

/// Persist a message into `to_user`'s inbox (assigning seq) unless it is a
/// duplicate. Returns `Some(seq)` for new messages, `None` for duplicates.
async fn store_message(
    state: &Arc<State>,
    to_user: &str,
    message_id: &str,
    make_packet: impl FnOnce(u64) -> Packet,
) -> Option<u64> {
    let mut inboxes = state.inboxes.lock().await;
    let inbox = inboxes.entry(to_user.to_string()).or_default();
    if !inbox.seen.insert(message_id.to_string()) {
        return None; // duplicate retry — already stored
    }
    inbox.next_seq += 1;
    let seq = inbox.next_seq;
    inbox.pending.push_back(StoredMessage { seq, packet: make_packet(seq), delivered: false });
    while inbox.pending.len() > state.cfg.max_inbox {
        inbox.pending.pop_front();
    }
    Some(seq)
}

fn message_id_of(pkt: &Packet) -> Option<&str> {
    match pkt {
        Packet::ChatMessage(m) => Some(&m.message_id),
        Packet::GroupMessage(m) => Some(&m.message_id),
        _ => None,
    }
}

/// Mark a message delivered (tombstone) once the recipient acks it.
async fn mark_delivered(state: &Arc<State>, user: &str, message_id: &str) {
    let mut inboxes = state.inboxes.lock().await;
    if let Some(inbox) = inboxes.get_mut(user) {
        for m in inbox.pending.iter_mut() {
            if message_id_of(&m.packet) == Some(message_id) {
                m.delivered = true;
            }
        }
        while inbox.pending.front().is_some_and(|m| m.delivered) {
            inbox.pending.pop_front();
        }
    }
}

fn proto_error(code: wire::ErrorCode, detail: impl Into<String>) -> Packet {
    Packet::Error(wire::ProtocolError { code: code as i32, detail: detail.into() })
}

/// Enable OS-level TCP keepalive on a client socket. Best effort: failure is
/// logged and ignored because the app-level Ping/Pong still covers liveness.
fn set_tcp_keepalive(socket: &TcpStream, interval: Duration) {
    use socket2::{SockRef, TcpKeepalive};
    let ka = TcpKeepalive::new().with_time(interval).with_interval(interval);
    if let Err(e) = SockRef::from(socket).set_tcp_keepalive(&ka) {
        debug!(error = %e, "failed to set TCP keepalive");
    }
}

/// Per-connection state machine.
async fn run_session(state: Arc<State>, socket: TcpStream) -> Result<(), MessengerError> {
    socket.set_nodelay(true).ok();
    if let Some(interval) = state.cfg.tcp_keepalive {
        set_tcp_keepalive(&socket, interval);
    }
    let mut framed = Framed::new(socket, FrameCodec::with_max_frame(state.cfg.max_frame));

    // ---- Phase: AwaitingLogin -------------------------------------------
    let login = match tokio::time::timeout(state.cfg.login_deadline, framed.next()).await {
        Err(_) => {
            let _ = framed
                .send(proto_error(wire::ErrorCode::NotAuthenticated, "login deadline exceeded"))
                .await;
            return Err(MessengerError::Timeout("login"));
        }
        Ok(None) => return Err(MessengerError::Closed),
        Ok(Some(Err(e))) => return Err(e),
        Ok(Some(Ok(Packet::Login(l)))) => l,
        Ok(Some(Ok(_))) => {
            let _ = framed
                .send(proto_error(wire::ErrorCode::NotAuthenticated, "first frame must be Login"))
                .await;
            return Err(MessengerError::Protocol("first frame must be Login".into()));
        }
    };

    if login.user_id.is_empty() || login.device_id.is_empty() {
        let _ = framed
            .send(proto_error(wire::ErrorCode::AuthFailed, "missing user_id/device_id"))
            .await;
        return Err(MessengerError::AuthFailed);
    }
    if !state.auth.verify(&login.user_id, &login.device_id, &login.auth_token) {
        // Exponential per-user backoff before answering, so credential
        // guessing costs the attacker wall-clock time.
        let failures = {
            let mut fails = state.auth_failures.lock().await;
            let n = fails.entry(login.user_id.clone()).or_insert(0);
            *n = n.saturating_add(1);
            *n
        };
        let delay = state
            .cfg
            .auth_backoff_base
            .saturating_mul(1u32 << (failures - 1).min(20))
            .min(state.cfg.auth_backoff_max);
        tokio::time::sleep(delay).await;
        warn!(user = %login.user_id, failures, "auth failed");
        let _ = framed.send(proto_error(wire::ErrorCode::AuthFailed, "invalid token")).await;
        return Err(MessengerError::AuthFailed);
    }
    state.auth_failures.lock().await.remove(&login.user_id);

    let user = login.user_id.clone();
    let session_id = state.session_ids.fetch_add(1, Ordering::Relaxed);
    let (tx, mut rx) = mpsc::channel::<Packet>(state.cfg.session_buffer);

    // ---- Register session; kick same-device predecessor ------------------
    {
        let mut sessions = state.sessions.write().await;
        let handles = sessions.entry(user.clone()).or_default();
        if let Some(pos) = handles.iter().position(|h| h.device_id == login.device_id) {
            let old = handles.remove(pos);
            let _ = old
                .tx
                .try_send(proto_error(wire::ErrorCode::ReplacedByNewSession, "newer login"));
        }
        handles.push(SessionHandle {
            session_id,
            device_id: login.device_id.clone(),
            tx: tx.clone(),
            last_seen: Instant::now(),
        });
    }
    // The registry owns the only sender now; when the session is removed
    // (kick, sweep, shutdown) `rx` closes and the loop below exits.
    drop(tx);
    state.last_seen.write().await.remove(&user);

    // ---- LoginAck + offline replay ---------------------------------------
    let replay: Vec<StoredMessage> = {
        let inboxes = state.inboxes.lock().await;
        inboxes
            .get(&user)
            .map(|i| {
                i.pending
                    .iter()
                    .filter(|m| !m.delivered && m.seq > login.resume_after_seq)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    };
    framed
        .send(Packet::LoginAck(wire::LoginAck {
            ok: true,
            error: String::new(),
            session_id: format!("s-{session_id}"),
            pending_messages: replay.len() as u64,
        }))
        .await?;
    let latest_seq = replay.last().map(|m| m.seq).unwrap_or(login.resume_after_seq);
    let replayed = replay.len() as u64;
    for m in &replay {
        framed.send(m.packet.clone()).await?;
    }
    framed
        .send(Packet::SyncComplete(wire::SyncComplete { delivered: replayed, latest_seq }))
        .await?;

    info!(user, session_id, device = %login.device_id, "session authenticated");
    broadcast_presence(&state, &user, wire::PresenceKind::Available).await;

    // ---- Authenticated main loop -----------------------------------------
    let result = session_loop(&state, &user, session_id, &mut framed, &mut rx).await;

    remove_session(&state, &user, session_id).await;
    result
}

async fn session_loop(
    state: &Arc<State>,
    user: &str,
    session_id: u64,
    framed: &mut Framed<TcpStream, FrameCodec>,
    rx: &mut mpsc::Receiver<Packet>,
) -> Result<(), MessengerError> {
    // In-progress upload for this connection (media_id guard against interleaving).
    let mut upload: Option<String> = None;

    loop {
        tokio::select! {
            // Outbound: router → this client.
            out = rx.recv() => {
                let Some(pkt) = out else { return Ok(()) };
                let fatal = matches!(&pkt, Packet::Error(_));
                framed.send(pkt).await?;
                if fatal {
                    return Ok(());
                }
            }
            // Inbound: client → server.
            inbound = framed.next() => {
                let pkt = match inbound {
                    None => return Ok(()),
                    Some(Err(e)) => {
                        let _ = framed.send(proto_error(wire::ErrorCode::MalformedFrame, e.to_string())).await;
                        return Err(e);
                    }
                    Some(Ok(p)) => p,
                };
                touch_session(state, user, session_id).await;
                match pkt {
                    Packet::Ping(p) => {
                        framed.send(Packet::Pong(wire::Pong { seq: p.seq })).await?;
                    }
                    Packet::Presence(p) => {
                        // Client-initiated presence update (e.g. going invisible).
                        let kind = wire::PresenceKind::try_from(p.kind)
                            .unwrap_or(wire::PresenceKind::Available);
                        broadcast_presence(state, user, kind).await;
                    }
                    Packet::ChatMessage(mut m) => {
                        m.from_user = user.to_string(); // never trust the client field
                        handle_chat(state, framed, m).await?;
                    }
                    Packet::DeliveredAck(mut a) => {
                        a.from_user = user.to_string();
                        handle_delivered_ack(state, user, a).await;
                    }
                    Packet::ReadAck(mut a) => {
                        a.from_user = user.to_string();
                        handle_read_ack(state, a).await;
                    }
                    Packet::MediaStart(m) => {
                        handle_media_start(state, framed, &mut upload, m).await?;
                    }
                    Packet::MediaChunk(c) => {
                        handle_media_chunk(state, framed, &mut upload, c).await?;
                    }
                    Packet::MediaFetch(f) => {
                        handle_media_fetch(state, framed, f).await?;
                    }
                    Packet::GroupEvent(ev) => {
                        handle_group_event(state, framed, user, ev).await?;
                    }
                    Packet::GroupMessage(mut m) => {
                        m.from_user = user.to_string();
                        handle_group_message(state, framed, m).await?;
                    }
                    other => {
                        framed.send(proto_error(
                            wire::ErrorCode::MalformedFrame,
                            format!("unexpected client packet {:?}", other.packet_type()),
                        )).await?;
                        return Err(MessengerError::Protocol("unexpected packet".into()));
                    }
                }
            }
        }
    }
}

async fn touch_session(state: &Arc<State>, user: &str, session_id: u64) {
    let mut sessions = state.sessions.write().await;
    if let Some(handles) = sessions.get_mut(user) {
        for h in handles.iter_mut() {
            if h.session_id == session_id {
                h.last_seen = Instant::now();
            }
        }
    }
}

// ---- 1:1 chat ---------------------------------------------------------------

async fn handle_chat(
    state: &Arc<State>,
    framed: &mut Framed<TcpStream, FrameCodec>,
    m: wire::ChatMessage,
) -> Result<(), MessengerError> {
    if m.to_user.is_empty() || m.message_id.is_empty() {
        framed
            .send(proto_error(wire::ErrorCode::MalformedFrame, "missing to_user/message_id"))
            .await?;
        return Ok(());
    }
    let to_user = m.to_user.clone();
    let message_id = m.message_id.clone();

    // Persist first (ServerAck must mean "durable"), then attempt delivery.
    let seq = store_message(state, &to_user, &message_id, |seq| {
        let mut stored = m.clone();
        stored.seq = seq;
        Packet::ChatMessage(stored)
    })
    .await;

    match seq {
        None => {
            // Duplicate retry: re-ack so the sender stops retrying.
            framed.send(Packet::ServerAck(wire::ServerAck { message_id, seq: 0 })).await?;
        }
        Some(seq) => {
            framed
                .send(Packet::ServerAck(wire::ServerAck { message_id: message_id.clone(), seq }))
                .await?;
            let mut delivered = m;
            delivered.seq = seq;
            deliver_online(state, &to_user, &Packet::ChatMessage(delivered)).await;
        }
    }
    Ok(())
}

async fn handle_delivered_ack(state: &Arc<State>, acking_user: &str, a: wire::DeliveredAck) {
    mark_delivered(state, acking_user, &a.message_id).await;
    // Relay the double-tick to the original sender if online. The stored
    // packet is gone after tombstoning, so we relay by broadcast-to-sender
    // via the ack's implicit addressing: sender learns from message_id.
    let pkt = Packet::DeliveredAck(a.clone());
    let sessions = state.sessions.read().await;
    for handles in sessions.values() {
        for h in handles {
            let _ = h.tx.try_send(pkt.clone());
        }
    }
}

async fn handle_read_ack(state: &Arc<State>, a: wire::ReadAck) {
    let pkt = Packet::ReadAck(a.clone());
    let sessions = state.sessions.read().await;
    for handles in sessions.values() {
        for h in handles {
            let _ = h.tx.try_send(pkt.clone());
        }
    }
}

// ---- Media (bulk data: PDFs, images, …) --------------------------------------

async fn handle_media_start(
    state: &Arc<State>,
    framed: &mut Framed<TcpStream, FrameCodec>,
    upload: &mut Option<String>,
    m: wire::MediaStart,
) -> Result<(), MessengerError> {
    if m.media_id.is_empty() || m.total_size == 0 {
        framed
            .send(proto_error(wire::ErrorCode::MediaTransferFailed, "missing media_id/size"))
            .await?;
        return Ok(());
    }
    if m.total_size > state.cfg.max_media_bytes {
        framed
            .send(Packet::MediaAck(wire::MediaAck {
                media_id: m.media_id,
                ok: false,
                complete: false,
                received_bytes: 0,
                error: format!("blob exceeds max {} bytes", state.cfg.max_media_bytes),
            }))
            .await?;
        return Ok(());
    }
    state.media.lock().await.insert(
        m.media_id.clone(),
        MediaBlob {
            file_name: m.file_name,
            mime_type: m.mime_type,
            total_size: m.total_size,
            sha256: m.sha256,
            data: Vec::with_capacity(m.total_size.min(1 << 20) as usize),
            complete: false,
        },
    );
    *upload = Some(m.media_id.clone());
    framed
        .send(Packet::MediaAck(wire::MediaAck {
            media_id: m.media_id,
            ok: true,
            complete: false,
            received_bytes: 0,
            error: String::new(),
        }))
        .await?;
    Ok(())
}

async fn handle_media_chunk(
    state: &Arc<State>,
    framed: &mut Framed<TcpStream, FrameCodec>,
    upload: &mut Option<String>,
    c: wire::MediaChunk,
) -> Result<(), MessengerError> {
    if upload.as_deref() != Some(c.media_id.as_str()) {
        framed
            .send(proto_error(wire::ErrorCode::MediaTransferFailed, "chunk without MediaStart"))
            .await?;
        return Ok(());
    }
    let reply = {
        let mut media = state.media.lock().await;
        let Some(blob) = media.get_mut(&c.media_id) else {
            framed
                .send(proto_error(wire::ErrorCode::MediaTransferFailed, "unknown media_id"))
                .await?;
            return Ok(());
        };
        if c.offset != blob.data.len() as u64 {
            wire::MediaAck {
                media_id: c.media_id.clone(),
                ok: false,
                complete: false,
                received_bytes: blob.data.len() as u64,
                error: format!("out-of-order chunk: expected offset {}", blob.data.len()),
            }
        } else if blob.data.len() as u64 + c.data.len() as u64 > blob.total_size {
            media.remove(&c.media_id);
            *upload = None;
            wire::MediaAck {
                media_id: c.media_id.clone(),
                ok: false,
                complete: false,
                received_bytes: 0,
                error: "blob larger than declared total_size".into(),
            }
        } else {
            blob.data.extend_from_slice(&c.data);
            let received = blob.data.len() as u64;
            if c.last {
                // Verify size + integrity digest before accepting.
                let digest = hex::encode(Sha256::digest(&blob.data));
                if received != blob.total_size {
                    media.remove(&c.media_id);
                    *upload = None;
                    wire::MediaAck {
                        media_id: c.media_id.clone(),
                        ok: false,
                        complete: false,
                        received_bytes: received,
                        error: "size mismatch at final chunk".into(),
                    }
                } else if !blob.sha256.is_empty() && !digest.eq_ignore_ascii_case(&blob.sha256) {
                    media.remove(&c.media_id);
                    *upload = None;
                    wire::MediaAck {
                        media_id: c.media_id.clone(),
                        ok: false,
                        complete: false,
                        received_bytes: received,
                        error: "sha256 mismatch".into(),
                    }
                } else {
                    blob.complete = true;
                    *upload = None;
                    wire::MediaAck {
                        media_id: c.media_id.clone(),
                        ok: true,
                        complete: true,
                        received_bytes: received,
                        error: String::new(),
                    }
                }
            } else {
                wire::MediaAck {
                    media_id: c.media_id.clone(),
                    ok: true,
                    complete: false,
                    received_bytes: received,
                    error: String::new(),
                }
            }
        }
    };
    framed.send(Packet::MediaAck(reply)).await?;
    Ok(())
}

async fn handle_media_fetch(
    state: &Arc<State>,
    framed: &mut Framed<TcpStream, FrameCodec>,
    f: wire::MediaFetch,
) -> Result<(), MessengerError> {
    // Copy out under the lock, stream without holding it.
    let blob = {
        let media = state.media.lock().await;
        match media.get(&f.media_id) {
            Some(b) if b.complete => Some((
                b.file_name.clone(),
                b.mime_type.clone(),
                b.total_size,
                b.sha256.clone(),
                b.data.clone(),
            )),
            _ => None,
        }
    };
    let Some((file_name, mime_type, total_size, sha256, data)) = blob else {
        framed
            .send(proto_error(wire::ErrorCode::MediaTransferFailed, "unknown or incomplete media"))
            .await?;
        return Ok(());
    };
    framed
        .send(Packet::MediaStart(wire::MediaStart {
            media_id: f.media_id.clone(),
            file_name,
            mime_type,
            total_size,
            sha256,
        }))
        .await?;
    const CHUNK: usize = 64 * 1024;
    let start = (f.from_offset as usize).min(data.len());
    let mut offset = start;
    while offset < data.len() {
        let end = (offset + CHUNK).min(data.len());
        framed
            .send(Packet::MediaChunk(wire::MediaChunk {
                media_id: f.media_id.clone(),
                offset: offset as u64,
                data: data[offset..end].to_vec(),
                last: end == data.len(),
            }))
            .await?;
        offset = end;
    }
    if data.is_empty() || start >= data.len() {
        framed
            .send(Packet::MediaChunk(wire::MediaChunk {
                media_id: f.media_id,
                offset: data.len() as u64,
                data: Vec::new(),
                last: true,
            }))
            .await?;
    }
    Ok(())
}

// ---- Groups -------------------------------------------------------------------

async fn handle_group_event(
    state: &Arc<State>,
    framed: &mut Framed<TcpStream, FrameCodec>,
    user: &str,
    mut ev: wire::GroupEvent,
) -> Result<(), MessengerError> {
    ev.actor_user = user.to_string();
    let op = wire::GroupOp::try_from(ev.op).unwrap_or(wire::GroupOp::Unspecified);
    let outcome: Result<(u64, Vec<String>), String> = {
        let mut groups = state.groups.lock().await;
        match op {
            wire::GroupOp::Create => {
                if groups.contains_key(&ev.group_id) {
                    Err("group already exists".into())
                } else {
                    let mut members = HashSet::new();
                    members.insert(user.to_string());
                    let mut admins = HashSet::new();
                    admins.insert(user.to_string());
                    groups.insert(ev.group_id.clone(), Group { members, admins, version: 1 });
                    Ok((1, vec![user.to_string()]))
                }
            }
            wire::GroupOp::AddMember | wire::GroupOp::RemoveMember => {
                match groups.get_mut(&ev.group_id) {
                    None => Err("unknown group".into()),
                    Some(g) if !g.admins.contains(user) => Err("not an admin".into()),
                    Some(g) => {
                        if op == wire::GroupOp::AddMember {
                            if g.members.len() >= state.cfg.max_group_members {
                                Err("group full".into())
                            } else {
                                g.members.insert(ev.subject_user.clone());
                                g.version += 1;
                                Ok((g.version, g.members.iter().cloned().collect()))
                            }
                        } else {
                            g.members.remove(&ev.subject_user);
                            g.admins.remove(&ev.subject_user);
                            g.version += 1;
                            Ok((g.version, g.members.iter().cloned().collect()))
                        }
                    }
                }
            }
            wire::GroupOp::Leave => match groups.get_mut(&ev.group_id) {
                None => Err("unknown group".into()),
                Some(g) => {
                    g.members.remove(user);
                    g.admins.remove(user);
                    g.version += 1;
                    Ok((g.version, g.members.iter().cloned().collect()))
                }
            },
            wire::GroupOp::Unspecified => Err("unspecified group op".into()),
        }
    };

    match outcome {
        Err(detail) => {
            framed.send(proto_error(wire::ErrorCode::MalformedFrame, detail)).await?;
        }
        Ok((version, members)) => {
            ev.version = version;
            let pkt = Packet::GroupEvent(ev.clone());
            framed.send(pkt.clone()).await?;
            for member in members {
                if member != user {
                    deliver_online(state, &member, &pkt).await;
                }
            }
        }
    }
    Ok(())
}

async fn handle_group_message(
    state: &Arc<State>,
    framed: &mut Framed<TcpStream, FrameCodec>,
    m: wire::GroupMessage,
) -> Result<(), MessengerError> {
    if m.group_id.is_empty() || m.message_id.is_empty() {
        framed
            .send(proto_error(wire::ErrorCode::MalformedFrame, "missing group_id/message_id"))
            .await?;
        return Ok(());
    }
    // Snapshot membership at a consistent version.
    let members: Option<Vec<String>> = {
        let groups = state.groups.lock().await;
        groups.get(&m.group_id).and_then(|g| {
            g.members
                .contains(&m.from_user)
                .then(|| g.members.iter().cloned().collect())
        })
    };
    let Some(members) = members else {
        framed
            .send(proto_error(wire::ErrorCode::UnknownRecipient, "not a member or unknown group"))
            .await?;
        return Ok(());
    };

    framed
        .send(Packet::ServerAck(wire::ServerAck { message_id: m.message_id.clone(), seq: 0 }))
        .await?;

    for member in members {
        if member == m.from_user {
            continue;
        }
        // Per-member dedup key so one group message maps to one inbox entry each.
        let dedup = format!("{}:{}", m.message_id, member);
        let msg = m.clone();
        let stored = store_message(state, &member, &dedup, move |seq| {
            let mut g = msg;
            g.seq = seq;
            Packet::GroupMessage(g)
        })
        .await;
        if let Some(seq) = stored {
            let mut out = m.clone();
            out.seq = seq;
            deliver_online(state, &member, &Packet::GroupMessage(out)).await;
        }
    }
    Ok(())
}

// Minimal hex helper (avoids new dependency for one call site).
mod hex {
    pub fn encode(bytes: impl AsRef<[u8]>) -> String {
        use std::fmt::Write;
        let bytes = bytes.as_ref();
        let mut s = String::with_capacity(bytes.len() * 2);
        for b in bytes {
            let _ = write!(s, "{b:02x}");
        }
        s
    }
}
