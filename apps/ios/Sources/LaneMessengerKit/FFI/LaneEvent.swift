import Foundation

public enum ConnectionState: String, Sendable, Equatable {
    case disconnected
    case connecting
    case awaitingLogin
    case syncing
    case ready
    case offline
    case replaced
}

/// Parsed FFI poll JSON (see `lane_messenger_ffi` / C `event_to_json`).
public enum LaneEvent: Sendable, Equatable {
    case loginAck(sessionId: String, ok: Bool, pendingMessages: UInt32, error: String)
    case syncComplete(latestSeq: UInt64, delivered: UInt32)
    case chatMessage(
        messageId: String,
        fromUser: String,
        toUser: String,
        body: String,
        seq: UInt64,
        mediaId: String,
        sentAt: UInt64
    )
    case serverAck(messageId: String, seq: UInt64)
    case deliveredAck(messageId: String, fromUser: String)
    case readAck(messageId: String, fromUser: String)
    case groupMessage(
        messageId: String,
        fromUser: String,
        groupId: String,
        body: String,
        seq: UInt64,
        mediaId: String,
        sentAt: UInt64
    )
    case groupEvent(
        groupId: String,
        op: Int,
        actorUser: String,
        subjectUser: String,
        version: UInt64
    )
    case groupAckSummary(
        messageId: String,
        groupId: String,
        memberCount: UInt32,
        deliveredCount: UInt32,
        readCount: UInt32
    )
    case presence(userId: String, kind: Int, lastSeen: UInt64?)
    case protocolError(code: Int, detail: String)
    case replacedByNewSession
    case disconnected(reason: String)
    case other(raw: String)

    public static func parse(json: String) -> LaneEvent {
        guard let data = json.data(using: .utf8),
              let obj = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let type = obj["type"] as? String
        else {
            return .other(raw: json)
        }
        switch type {
        case "LoginAck":
            return .loginAck(
                sessionId: obj["session_id"] as? String ?? "",
                ok: obj["ok"] as? Bool ?? false,
                pendingMessages: uint32(obj["pending_messages"]),
                error: obj["error"] as? String ?? ""
            )
        case "SyncComplete":
            return .syncComplete(
                latestSeq: uint64(obj["latest_seq"]),
                delivered: uint32(obj["delivered"])
            )
        case "ChatMessage":
            let encoded = obj["body_hex"] as? String ?? obj["body_b64"] as? String ?? ""
            return .chatMessage(
                messageId: obj["message_id"] as? String ?? "",
                fromUser: obj["from_user"] as? String ?? "",
                toUser: obj["to_user"] as? String ?? "",
                body: decodeBody(encoded),
                seq: uint64(obj["seq"]),
                mediaId: obj["media_id"] as? String ?? "",
                sentAt: uint64(obj["sent_at"])
            )
        case "ServerAck":
            return .serverAck(
                messageId: obj["message_id"] as? String ?? "",
                seq: uint64(obj["seq"])
            )
        case "DeliveredAck":
            return .deliveredAck(
                messageId: obj["message_id"] as? String ?? "",
                fromUser: obj["from_user"] as? String ?? ""
            )
        case "ReadAck":
            return .readAck(
                messageId: obj["message_id"] as? String ?? "",
                fromUser: obj["from_user"] as? String ?? ""
            )
        case "GroupMessage":
            let encoded = obj["body_hex"] as? String ?? obj["body_b64"] as? String ?? ""
            return .groupMessage(
                messageId: obj["message_id"] as? String ?? "",
                fromUser: obj["from_user"] as? String ?? "",
                groupId: obj["group_id"] as? String ?? "",
                body: decodeBody(encoded),
                seq: uint64(obj["seq"]),
                mediaId: obj["media_id"] as? String ?? "",
                sentAt: uint64(obj["sent_at"])
            )
        case "GroupEvent":
            return .groupEvent(
                groupId: obj["group_id"] as? String ?? "",
                op: obj["op"] as? Int ?? (obj["op"] as? NSNumber)?.intValue ?? 0,
                actorUser: obj["actor_user"] as? String ?? "",
                subjectUser: obj["subject_user"] as? String ?? "",
                version: uint64(obj["version"])
            )
        case "GroupAckSummary":
            return .groupAckSummary(
                messageId: obj["message_id"] as? String ?? "",
                groupId: obj["group_id"] as? String ?? "",
                memberCount: uint32(obj["member_count"]),
                deliveredCount: uint32(obj["delivered_count"]),
                readCount: uint32(obj["read_count"])
            )
        case "Presence":
            let last: UInt64? = {
                if obj["last_seen"] == nil { return nil }
                return uint64(obj["last_seen"])
            }()
            return .presence(
                userId: obj["user_id"] as? String ?? "",
                kind: obj["kind"] as? Int ?? (obj["kind"] as? NSNumber)?.intValue ?? 0,
                lastSeen: last
            )
        case "ProtocolError":
            return .protocolError(
                code: obj["code"] as? Int ?? (obj["code"] as? NSNumber)?.intValue ?? 0,
                detail: obj["detail"] as? String ?? ""
            )
        case "ReplacedByNewSession":
            return .replacedByNewSession
        case "Disconnected":
            return .disconnected(reason: obj["reason"] as? String ?? "")
        default:
            return .other(raw: json)
        }
    }
}

public struct ConnectRequest: Sendable, Equatable {
    public var config: AppConfig
    public var credentials: AuthCredentials
    public var resumeAfterSeq: UInt64
    public var clientVersion: String

    public init(
        config: AppConfig,
        credentials: AuthCredentials,
        resumeAfterSeq: UInt64 = 0,
        clientVersion: String = AppConfig.clientVersion
    ) {
        self.config = config
        self.credentials = credentials
        self.resumeAfterSeq = resumeAfterSeq
        self.clientVersion = clientVersion
    }
}

/// C API field is named `body_hex` but encodes **base64** (`c_api::b64`).
private func decodeBody(_ encoded: String) -> String {
    if encoded.isEmpty { return "" }
    if let data = Data(base64Encoded: encoded), let s = String(data: data, encoding: .utf8) {
        return s
    }
    var bytes = [UInt8]()
    var idx = encoded.startIndex
    while idx < encoded.endIndex {
        let next = encoded.index(idx, offsetBy: 2, limitedBy: encoded.endIndex) ?? encoded.endIndex
        if let b = UInt8(encoded[idx..<next], radix: 16) { bytes.append(b) }
        idx = next
    }
    return String(bytes: bytes, encoding: .utf8) ?? ""
}

private func uint64(_ value: Any?) -> UInt64 {
    if let n = value as? NSNumber { return n.uint64Value }
    if let i = value as? Int { return UInt64(i) }
    if let u = value as? UInt64 { return u }
    return 0
}

private func uint32(_ value: Any?) -> UInt32 {
    if let n = value as? NSNumber { return n.uint32Value }
    if let i = value as? Int { return UInt32(i) }
    if let u = value as? UInt32 { return u }
    return 0
}
