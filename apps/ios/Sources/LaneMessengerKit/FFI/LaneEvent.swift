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
    case chatMessage(messageId: String, fromUser: String, seq: UInt64)
    case serverAck(messageId: String, seq: UInt64)
    case deliveredAck(messageId: String)
    case readAck(messageId: String)
    case groupMessage(messageId: String, groupId: String)
    case groupAckSummary(messageId: String, memberCount: UInt32)
    case presence(userId: String, kind: Int)
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
            return .chatMessage(
                messageId: obj["message_id"] as? String ?? "",
                fromUser: obj["from_user"] as? String ?? "",
                seq: uint64(obj["seq"])
            )
        case "ServerAck":
            return .serverAck(
                messageId: obj["message_id"] as? String ?? "",
                seq: uint64(obj["seq"])
            )
        case "DeliveredAck":
            return .deliveredAck(messageId: obj["message_id"] as? String ?? "")
        case "ReadAck":
            return .readAck(messageId: obj["message_id"] as? String ?? "")
        case "GroupMessage":
            return .groupMessage(
                messageId: obj["message_id"] as? String ?? "",
                groupId: obj["group_id"] as? String ?? ""
            )
        case "GroupAckSummary":
            return .groupAckSummary(
                messageId: obj["message_id"] as? String ?? "",
                memberCount: uint32(obj["member_count"])
            )
        case "Presence":
            return .presence(
                userId: obj["user_id"] as? String ?? "",
                kind: obj["kind"] as? Int ?? 0
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
