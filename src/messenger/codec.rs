//! Compact binary frame codec.
//!
//! Frame layout (network byte order):
//!
//! ```text
//! +---------+--------------+-------------+------------------+
//! | u8 ver  | u8 pkt_type  | u32 length  | protobuf payload |
//! +---------+--------------+-------------+------------------+
//! ```
//!
//! `length` counts only the payload. Frames above the configured maximum are
//! rejected before any allocation to prevent memory-exhaustion attacks.

use bytes::{Buf, BufMut, BytesMut};
use prost::Message;
use tokio_util::codec::{Decoder, Encoder};

use super::wire;
use super::MessengerError;

/// Current protocol version. Clients advertising a different version in the
/// frame header are rejected with [`MessengerError::UnsupportedVersion`].
pub const PROTOCOL_VERSION: u8 = 1;

/// Frame header size: version (1) + packet type (1) + payload length (4).
pub const HEADER_LEN: usize = 6;

/// Default maximum payload size. Media chunks are 64 KiB, so 256 KiB leaves
/// generous headroom while still bounding per-connection memory.
pub const DEFAULT_MAX_FRAME: usize = 256 * 1024;

/// Wire identifiers for each packet. Append-only: never reuse a value.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketType {
    Login = 0x01,
    LoginAck = 0x02,
    Ping = 0x03,
    Pong = 0x04,
    Error = 0x0F,
    Presence = 0x10,
    ChatMessage = 0x20,
    ServerAck = 0x21,
    DeliveredAck = 0x22,
    ReadAck = 0x23,
    SyncComplete = 0x24,
    MediaStart = 0x30,
    MediaChunk = 0x31,
    MediaAck = 0x32,
    MediaFetch = 0x33,
    GroupMessage = 0x40,
    GroupEvent = 0x41,
    PeerHello = 0x50,
    PeerPresence = 0x51,
    PeerSync = 0x52,
    PeerJoin = 0x53,
    PeerLeave = 0x54,
    PeerHandoffUser = 0x55,
    PeerHandoffGroup = 0x56,
    PublishKeys = 0x60,
    FetchKeys = 0x61,
    KeyBundle = 0x62,
}

impl TryFrom<u8> for PacketType {
    type Error = MessengerError;

    fn try_from(v: u8) -> Result<Self, MessengerError> {
        Ok(match v {
            0x01 => Self::Login,
            0x02 => Self::LoginAck,
            0x03 => Self::Ping,
            0x04 => Self::Pong,
            0x0F => Self::Error,
            0x10 => Self::Presence,
            0x20 => Self::ChatMessage,
            0x21 => Self::ServerAck,
            0x22 => Self::DeliveredAck,
            0x23 => Self::ReadAck,
            0x24 => Self::SyncComplete,
            0x30 => Self::MediaStart,
            0x31 => Self::MediaChunk,
            0x32 => Self::MediaAck,
            0x33 => Self::MediaFetch,
            0x40 => Self::GroupMessage,
            0x41 => Self::GroupEvent,
            0x50 => Self::PeerHello,
            0x51 => Self::PeerPresence,
            0x52 => Self::PeerSync,
            0x53 => Self::PeerJoin,
            0x54 => Self::PeerLeave,
            0x55 => Self::PeerHandoffUser,
            0x56 => Self::PeerHandoffGroup,
            0x60 => Self::PublishKeys,
            0x61 => Self::FetchKeys,
            0x62 => Self::KeyBundle,
            other => return Err(MessengerError::UnknownPacketType(other)),
        })
    }
}

/// A fully decoded protocol packet.
#[derive(Debug, Clone, PartialEq)]
pub enum Packet {
    Login(wire::Login),
    LoginAck(wire::LoginAck),
    Ping(wire::Ping),
    Pong(wire::Pong),
    Error(wire::ProtocolError),
    Presence(wire::Presence),
    ChatMessage(wire::ChatMessage),
    ServerAck(wire::ServerAck),
    DeliveredAck(wire::DeliveredAck),
    ReadAck(wire::ReadAck),
    SyncComplete(wire::SyncComplete),
    MediaStart(wire::MediaStart),
    MediaChunk(wire::MediaChunk),
    MediaAck(wire::MediaAck),
    MediaFetch(wire::MediaFetch),
    GroupMessage(wire::GroupMessage),
    GroupEvent(wire::GroupEvent),
    PeerHello(wire::PeerHello),
    PeerPresence(wire::PeerPresence),
    PeerSync(wire::PeerSync),
    PeerJoin(wire::PeerJoin),
    PeerLeave(wire::PeerLeave),
    PeerHandoffUser(wire::PeerHandoffUser),
    PeerHandoffGroup(wire::PeerHandoffGroup),
    PublishKeys(wire::PublishKeys),
    FetchKeys(wire::FetchKeys),
    KeyBundle(wire::KeyBundle),
}

impl Packet {
    /// Wire identifier for this packet.
    pub fn packet_type(&self) -> PacketType {
        match self {
            Packet::Login(_) => PacketType::Login,
            Packet::LoginAck(_) => PacketType::LoginAck,
            Packet::Ping(_) => PacketType::Ping,
            Packet::Pong(_) => PacketType::Pong,
            Packet::Error(_) => PacketType::Error,
            Packet::Presence(_) => PacketType::Presence,
            Packet::ChatMessage(_) => PacketType::ChatMessage,
            Packet::ServerAck(_) => PacketType::ServerAck,
            Packet::DeliveredAck(_) => PacketType::DeliveredAck,
            Packet::ReadAck(_) => PacketType::ReadAck,
            Packet::SyncComplete(_) => PacketType::SyncComplete,
            Packet::MediaStart(_) => PacketType::MediaStart,
            Packet::MediaChunk(_) => PacketType::MediaChunk,
            Packet::MediaAck(_) => PacketType::MediaAck,
            Packet::MediaFetch(_) => PacketType::MediaFetch,
            Packet::GroupMessage(_) => PacketType::GroupMessage,
            Packet::GroupEvent(_) => PacketType::GroupEvent,
            Packet::PeerHello(_) => PacketType::PeerHello,
            Packet::PeerPresence(_) => PacketType::PeerPresence,
            Packet::PeerSync(_) => PacketType::PeerSync,
            Packet::PeerJoin(_) => PacketType::PeerJoin,
            Packet::PeerLeave(_) => PacketType::PeerLeave,
            Packet::PeerHandoffUser(_) => PacketType::PeerHandoffUser,
            Packet::PeerHandoffGroup(_) => PacketType::PeerHandoffGroup,
            Packet::PublishKeys(_) => PacketType::PublishKeys,
            Packet::FetchKeys(_) => PacketType::FetchKeys,
            Packet::KeyBundle(_) => PacketType::KeyBundle,
        }
    }

    fn encoded_len(&self) -> usize {
        match self {
            Packet::Login(m) => m.encoded_len(),
            Packet::LoginAck(m) => m.encoded_len(),
            Packet::Ping(m) => m.encoded_len(),
            Packet::Pong(m) => m.encoded_len(),
            Packet::Error(m) => m.encoded_len(),
            Packet::Presence(m) => m.encoded_len(),
            Packet::ChatMessage(m) => m.encoded_len(),
            Packet::ServerAck(m) => m.encoded_len(),
            Packet::DeliveredAck(m) => m.encoded_len(),
            Packet::ReadAck(m) => m.encoded_len(),
            Packet::SyncComplete(m) => m.encoded_len(),
            Packet::MediaStart(m) => m.encoded_len(),
            Packet::MediaChunk(m) => m.encoded_len(),
            Packet::MediaAck(m) => m.encoded_len(),
            Packet::MediaFetch(m) => m.encoded_len(),
            Packet::GroupMessage(m) => m.encoded_len(),
            Packet::GroupEvent(m) => m.encoded_len(),
            Packet::PeerHello(m) => m.encoded_len(),
            Packet::PeerPresence(m) => m.encoded_len(),
            Packet::PeerSync(m) => m.encoded_len(),
            Packet::PeerJoin(m) => m.encoded_len(),
            Packet::PeerLeave(m) => m.encoded_len(),
            Packet::PeerHandoffUser(m) => m.encoded_len(),
            Packet::PeerHandoffGroup(m) => m.encoded_len(),
            Packet::PublishKeys(m) => m.encoded_len(),
            Packet::FetchKeys(m) => m.encoded_len(),
            Packet::KeyBundle(m) => m.encoded_len(),
        }
    }

    fn encode_body(&self, buf: &mut BytesMut) {
        // encode() only fails when the buffer lacks capacity; BytesMut grows.
        let r = match self {
            Packet::Login(m) => m.encode(buf),
            Packet::LoginAck(m) => m.encode(buf),
            Packet::Ping(m) => m.encode(buf),
            Packet::Pong(m) => m.encode(buf),
            Packet::Error(m) => m.encode(buf),
            Packet::Presence(m) => m.encode(buf),
            Packet::ChatMessage(m) => m.encode(buf),
            Packet::ServerAck(m) => m.encode(buf),
            Packet::DeliveredAck(m) => m.encode(buf),
            Packet::ReadAck(m) => m.encode(buf),
            Packet::SyncComplete(m) => m.encode(buf),
            Packet::MediaStart(m) => m.encode(buf),
            Packet::MediaChunk(m) => m.encode(buf),
            Packet::MediaAck(m) => m.encode(buf),
            Packet::MediaFetch(m) => m.encode(buf),
            Packet::GroupMessage(m) => m.encode(buf),
            Packet::GroupEvent(m) => m.encode(buf),
            Packet::PeerHello(m) => m.encode(buf),
            Packet::PeerPresence(m) => m.encode(buf),
            Packet::PeerSync(m) => m.encode(buf),
            Packet::PeerJoin(m) => m.encode(buf),
            Packet::PeerLeave(m) => m.encode(buf),
            Packet::PeerHandoffUser(m) => m.encode(buf),
            Packet::PeerHandoffGroup(m) => m.encode(buf),
            Packet::PublishKeys(m) => m.encode(buf),
            Packet::FetchKeys(m) => m.encode(buf),
            Packet::KeyBundle(m) => m.encode(buf),
        };
        debug_assert!(r.is_ok(), "BytesMut encode cannot fail");
    }

    fn decode_body(ty: PacketType, payload: &[u8]) -> Result<Self, MessengerError> {
        Ok(match ty {
            PacketType::Login => Packet::Login(wire::Login::decode(payload)?),
            PacketType::LoginAck => Packet::LoginAck(wire::LoginAck::decode(payload)?),
            PacketType::Ping => Packet::Ping(wire::Ping::decode(payload)?),
            PacketType::Pong => Packet::Pong(wire::Pong::decode(payload)?),
            PacketType::Error => Packet::Error(wire::ProtocolError::decode(payload)?),
            PacketType::Presence => Packet::Presence(wire::Presence::decode(payload)?),
            PacketType::ChatMessage => Packet::ChatMessage(wire::ChatMessage::decode(payload)?),
            PacketType::ServerAck => Packet::ServerAck(wire::ServerAck::decode(payload)?),
            PacketType::DeliveredAck => Packet::DeliveredAck(wire::DeliveredAck::decode(payload)?),
            PacketType::ReadAck => Packet::ReadAck(wire::ReadAck::decode(payload)?),
            PacketType::SyncComplete => Packet::SyncComplete(wire::SyncComplete::decode(payload)?),
            PacketType::MediaStart => Packet::MediaStart(wire::MediaStart::decode(payload)?),
            PacketType::MediaChunk => Packet::MediaChunk(wire::MediaChunk::decode(payload)?),
            PacketType::MediaAck => Packet::MediaAck(wire::MediaAck::decode(payload)?),
            PacketType::MediaFetch => Packet::MediaFetch(wire::MediaFetch::decode(payload)?),
            PacketType::GroupMessage => Packet::GroupMessage(wire::GroupMessage::decode(payload)?),
            PacketType::GroupEvent => Packet::GroupEvent(wire::GroupEvent::decode(payload)?),
            PacketType::PeerHello => Packet::PeerHello(wire::PeerHello::decode(payload)?),
            PacketType::PeerPresence => Packet::PeerPresence(wire::PeerPresence::decode(payload)?),
            PacketType::PeerSync => Packet::PeerSync(wire::PeerSync::decode(payload)?),
            PacketType::PeerJoin => Packet::PeerJoin(wire::PeerJoin::decode(payload)?),
            PacketType::PeerLeave => Packet::PeerLeave(wire::PeerLeave::decode(payload)?),
            PacketType::PeerHandoffUser => {
                Packet::PeerHandoffUser(wire::PeerHandoffUser::decode(payload)?)
            }
            PacketType::PeerHandoffGroup => {
                Packet::PeerHandoffGroup(wire::PeerHandoffGroup::decode(payload)?)
            }
            PacketType::PublishKeys => Packet::PublishKeys(wire::PublishKeys::decode(payload)?),
            PacketType::FetchKeys => Packet::FetchKeys(wire::FetchKeys::decode(payload)?),
            PacketType::KeyBundle => Packet::KeyBundle(wire::KeyBundle::decode(payload)?),
        })
    }
}

/// Length-prefixed binary codec for [`Packet`] frames.
#[derive(Debug, Clone)]
pub struct FrameCodec {
    max_frame: usize,
}

impl Default for FrameCodec {
    fn default() -> Self {
        Self { max_frame: DEFAULT_MAX_FRAME }
    }
}

impl FrameCodec {
    /// Codec with a custom maximum payload size.
    pub fn with_max_frame(max_frame: usize) -> Self {
        Self { max_frame }
    }
}

impl Decoder for FrameCodec {
    type Item = Packet;
    type Error = MessengerError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Packet>, MessengerError> {
        if src.len() < HEADER_LEN {
            return Ok(None);
        }
        let version = src[0];
        if version != PROTOCOL_VERSION {
            return Err(MessengerError::UnsupportedVersion(version));
        }
        // Validate packet type before waiting for the body so garbage input
        // fails fast instead of stalling on a bogus length.
        let ty = PacketType::try_from(src[1])?;
        let len = u32::from_be_bytes([src[2], src[3], src[4], src[5]]) as usize;
        if len > self.max_frame {
            return Err(MessengerError::FrameTooLarge { size: len, max: self.max_frame });
        }
        if src.len() < HEADER_LEN + len {
            src.reserve(HEADER_LEN + len - src.len());
            return Ok(None);
        }
        src.advance(HEADER_LEN);
        let payload = src.split_to(len);
        Ok(Some(Packet::decode_body(ty, &payload)?))
    }
}

impl Encoder<Packet> for FrameCodec {
    type Error = MessengerError;

    fn encode(&mut self, pkt: Packet, dst: &mut BytesMut) -> Result<(), MessengerError> {
        let len = pkt.encoded_len();
        if len > self.max_frame {
            return Err(MessengerError::FrameTooLarge { size: len, max: self.max_frame });
        }
        dst.reserve(HEADER_LEN + len);
        dst.put_u8(PROTOCOL_VERSION);
        dst.put_u8(pkt.packet_type() as u8);
        dst.put_u32(len as u32);
        pkt.encode_body(dst);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(pkt: Packet) {
        let mut codec = FrameCodec::default();
        let mut buf = BytesMut::new();
        codec.encode(pkt.clone(), &mut buf).expect("encode");
        let out = codec.decode(&mut buf).expect("decode").expect("complete frame");
        assert_eq!(out, pkt);
        assert!(buf.is_empty(), "no trailing bytes");
    }

    #[test]
    fn round_trip_all_packet_types() {
        round_trip(Packet::Login(wire::Login {
            user_id: "akash".into(),
            device_id: "phone-1".into(),
            auth_token: "deadbeef".into(),
            client_version: "0.1".into(),
            resume_after_seq: 42,
        }));
        round_trip(Packet::LoginAck(wire::LoginAck {
            ok: true,
            error: String::new(),
            session_id: "s-1".into(),
            pending_messages: 3,
        }));
        round_trip(Packet::Ping(wire::Ping { seq: 7 }));
        round_trip(Packet::Pong(wire::Pong { seq: 7 }));
        round_trip(Packet::Error(wire::ProtocolError {
            code: wire::ErrorCode::AuthFailed as i32,
            detail: "bad token".into(),
        }));
        round_trip(Packet::Presence(wire::Presence {
            user_id: "john".into(),
            kind: wire::PresenceKind::Available as i32,
            last_seen: 0,
        }));
        round_trip(Packet::ChatMessage(wire::ChatMessage {
            message_id: "m-1".into(),
            from_user: "akash".into(),
            to_user: "john".into(),
            body: b"hello".to_vec(),
            sent_at: 1,
            seq: 0,
            media_id: String::new(),
        }));
        round_trip(Packet::ServerAck(wire::ServerAck {
            message_id: "m-1".into(),
            seq: 9,
            to_user: String::new(),
        }));
        round_trip(Packet::DeliveredAck(wire::DeliveredAck {
            message_id: "m-1".into(),
            from_user: "john".into(),
        }));
        round_trip(Packet::ReadAck(wire::ReadAck {
            message_id: "m-1".into(),
            from_user: "john".into(),
        }));
        round_trip(Packet::SyncComplete(wire::SyncComplete {
            delivered: 2,
            latest_seq: 10,
            user_id: String::new(),
        }));
        round_trip(Packet::MediaStart(wire::MediaStart {
            media_id: "f-1".into(),
            file_name: "report.pdf".into(),
            mime_type: "application/pdf".into(),
            total_size: 1024,
            sha256: "abc".into(),
        }));
        round_trip(Packet::MediaChunk(wire::MediaChunk {
            media_id: "f-1".into(),
            offset: 0,
            data: vec![1, 2, 3],
            last: true,
        }));
        round_trip(Packet::MediaAck(wire::MediaAck {
            media_id: "f-1".into(),
            ok: true,
            complete: true,
            received_bytes: 3,
            error: String::new(),
        }));
        round_trip(Packet::MediaFetch(wire::MediaFetch { media_id: "f-1".into(), from_offset: 0 }));
        round_trip(Packet::GroupMessage(wire::GroupMessage {
            message_id: "m-2".into(),
            from_user: "akash".into(),
            group_id: "g-1".into(),
            body: b"hi all".to_vec(),
            sent_at: 2,
            media_id: String::new(),
            seq: 0,
            to_user: String::new(),
        }));
        round_trip(Packet::GroupEvent(wire::GroupEvent {
            group_id: "g-1".into(),
            op: wire::GroupOp::AddMember as i32,
            actor_user: "akash".into(),
            subject_user: "john".into(),
            version: 2,
            to_user: String::new(),
        }));
        round_trip(Packet::PeerHello(wire::PeerHello {
            node_id: "node-1".into(),
            auth_token: "cafe".into(),
        }));
        round_trip(Packet::PeerPresence(wire::PeerPresence {
            user_id: "akash".into(),
            online: true,
            node_id: "node-1".into(),
            last_seen: 0,
        }));
        round_trip(Packet::PeerSync(wire::PeerSync { user_id: "akash".into(), after_seq: 5 }));
        round_trip(Packet::PeerJoin(wire::PeerJoin {
            node_id: "node-3".into(),
            addr: "127.0.0.1:9000".into(),
        }));
        round_trip(Packet::PeerLeave(wire::PeerLeave { node_id: "node-3".into() }));
        round_trip(Packet::PeerHandoffUser(wire::PeerHandoffUser {
            user_id: "bob".into(),
            next_seq: 5,
            seen: vec!["m-1".into()],
            chats: vec![],
            groups: vec![],
        }));
        round_trip(Packet::PeerHandoffGroup(wire::PeerHandoffGroup {
            group_id: "g-1".into(),
            members: vec!["alice".into(), "bob".into()],
            admins: vec!["alice".into()],
            version: 2,
        }));
        round_trip(Packet::PublishKeys(wire::PublishKeys {
            user_id: "akash".into(),
            device_id: "d1".into(),
            identity_key: "idk".into(),
            one_time_keys: vec!["otk1".into(), "otk2".into()],
        }));
        round_trip(Packet::FetchKeys(wire::FetchKeys {
            user_id: "john".into(),
            for_user: "akash".into(),
        }));
        round_trip(Packet::KeyBundle(wire::KeyBundle {
            user_id: "john".into(),
            device_id: "d1".into(),
            identity_key: "idk".into(),
            one_time_key: "otk".into(),
            for_user: "akash".into(),
            found: true,
        }));
    }

    #[test]
    fn partial_frame_returns_none() {
        let mut codec = FrameCodec::default();
        let mut buf = BytesMut::new();
        codec.encode(Packet::Ping(wire::Ping { seq: 1 }), &mut buf).unwrap();
        let full = buf.clone();
        for cut in 0..full.len() {
            let mut partial = BytesMut::from(&full[..cut]);
            assert!(codec.decode(&mut partial).unwrap().is_none(), "cut at {cut}");
        }
    }

    #[test]
    fn unknown_packet_type_errors() {
        let mut codec = FrameCodec::default();
        let mut buf = BytesMut::from(&[PROTOCOL_VERSION, 0xEE, 0, 0, 0, 0][..]);
        assert!(matches!(
            codec.decode(&mut buf),
            Err(MessengerError::UnknownPacketType(0xEE))
        ));
    }

    #[test]
    fn wrong_version_errors() {
        let mut codec = FrameCodec::default();
        let mut buf = BytesMut::from(&[9u8, 0x03, 0, 0, 0, 0][..]);
        assert!(matches!(
            codec.decode(&mut buf),
            Err(MessengerError::UnsupportedVersion(9))
        ));
    }

    #[test]
    fn oversized_frame_rejected_before_buffering() {
        let mut codec = FrameCodec::with_max_frame(16);
        let mut header = BytesMut::from(&[PROTOCOL_VERSION, 0x20][..]);
        header.put_u32(1_000_000);
        assert!(matches!(
            codec.decode(&mut header),
            Err(MessengerError::FrameTooLarge { .. })
        ));
    }

    #[test]
    fn garbage_payload_errors_not_panics() {
        let mut codec = FrameCodec::default();
        let mut buf = BytesMut::new();
        buf.put_u8(PROTOCOL_VERSION);
        buf.put_u8(PacketType::ChatMessage as u8);
        let garbage = [0xFFu8; 32];
        buf.put_u32(garbage.len() as u32);
        buf.extend_from_slice(&garbage);
        assert!(codec.decode(&mut buf).is_err());
    }

    #[test]
    fn pipelined_frames_decode_in_order() {
        let mut codec = FrameCodec::default();
        let mut buf = BytesMut::new();
        for seq in 0..5u64 {
            codec.encode(Packet::Ping(wire::Ping { seq }), &mut buf).unwrap();
        }
        for seq in 0..5u64 {
            match codec.decode(&mut buf).unwrap().unwrap() {
                Packet::Ping(p) => assert_eq!(p.seq, seq),
                other => panic!("unexpected {other:?}"),
            }
        }
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }
}
