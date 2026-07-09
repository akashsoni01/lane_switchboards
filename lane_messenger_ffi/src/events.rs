//! Host-facing event payloads (push / poll).

/// Events delivered to the host via poll or callback.
#[derive(Debug, Clone)]
pub enum LaneEvent {
    LoginAck(LoginAckEvent),
    SyncMessage(Box<LaneEvent>),
    SyncComplete(SyncCompleteEvent),
    ChatMessage(ChatMessageEvent),
    ServerAck(ServerAckEvent),
    DeliveredAck(DeliveredAckEvent),
    ReadAck(ReadAckEvent),
    GroupAckSummary(GroupAckSummaryEvent),
    Presence(PresenceEvent),
    GroupMessage(GroupMessageEvent),
    GroupEvent(GroupEventInfo),
    MediaStart(MediaStartEvent),
    MediaChunk(MediaChunkEvent),
    MediaAck(MediaAckEvent),
    KeyBundle(KeyBundleInfo),
    ProtocolError(ProtocolErrorEvent),
    Disconnected { reason: String },
    ReplacedByNewSession,
    /// Internal: ignored by hosts (pong consumed by ping).
    #[allow(dead_code)]
    Pong { seq: u64 },
}

#[derive(Debug, Clone)]
pub struct LoginAckEvent {
    pub session_id: String,
    pub pending_messages: u32,
    pub ok: bool,
    pub error: String,
}

#[derive(Debug, Clone)]
pub struct SyncCompleteEvent {
    pub delivered: u32,
    pub latest_seq: u64,
}

#[derive(Debug, Clone)]
pub struct ChatMessageEvent {
    pub message_id: String,
    pub from_user: String,
    pub to_user: String,
    pub body: Vec<u8>,
    pub sent_at: u64,
    pub seq: u64,
    pub media_id: String,
}

#[derive(Debug, Clone)]
pub struct ServerAckEvent {
    pub message_id: String,
    pub seq: u64,
}

#[derive(Debug, Clone)]
pub struct DeliveredAckEvent {
    pub message_id: String,
    pub from_user: String,
}

#[derive(Debug, Clone)]
pub struct ReadAckEvent {
    pub message_id: String,
    pub from_user: String,
}

#[derive(Debug, Clone)]
pub struct GroupAckSummaryEvent {
    pub message_id: String,
    pub group_id: String,
    pub delivered_by: Vec<String>,
    pub read_by: Vec<String>,
    pub member_count: u32,
}

#[derive(Debug, Clone)]
pub struct PresenceEvent {
    pub user_id: String,
    /// 1=Available, 2=Unavailable, 3=LastSeen
    pub kind: i32,
    pub last_seen: u64,
}

#[derive(Debug, Clone)]
pub struct GroupMessageEvent {
    pub message_id: String,
    pub from_user: String,
    pub group_id: String,
    pub body: Vec<u8>,
    pub sent_at: u64,
    pub media_id: String,
    pub seq: u64,
    pub to_user: String,
}

#[derive(Debug, Clone)]
pub struct GroupEventInfo {
    pub group_id: String,
    pub op: i32,
    pub actor_user: String,
    pub subject_user: String,
    pub version: u64,
    pub to_user: String,
}

#[derive(Debug, Clone)]
pub struct MediaStartEvent {
    pub media_id: String,
    pub file_name: String,
    pub mime_type: String,
    pub total_size: u64,
    pub sha256: String,
}

#[derive(Debug, Clone)]
pub struct MediaChunkEvent {
    pub media_id: String,
    pub offset: u64,
    pub data: Vec<u8>,
    pub last: bool,
}

#[derive(Debug, Clone)]
pub struct MediaAckEvent {
    pub media_id: String,
    pub ok: bool,
    pub complete: bool,
    pub received_bytes: u64,
    pub error: String,
}

#[derive(Debug, Clone)]
pub struct KeyBundleInfo {
    pub user_id: String,
    pub device_id: String,
    pub identity_key: String,
    pub one_time_key: String,
    pub found: bool,
    pub device_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ProtocolErrorEvent {
    pub code: i32,
    pub detail: String,
}

#[derive(Debug, Clone)]
pub struct DownloadedMediaInfo {
    pub file_name: String,
    pub mime_type: String,
    pub sha256: String,
    pub data: Vec<u8>,
}
