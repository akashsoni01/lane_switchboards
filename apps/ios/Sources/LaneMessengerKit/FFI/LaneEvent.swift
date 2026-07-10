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

/// Parsed FFI poll JSON (see `lane_messenger_ffi` event_json).
public enum LaneEvent: Sendable, Equatable {
    case loginAck(sessionId: String, ok: Bool)
    case syncComplete(latestSeq: UInt64)
    case chatMessage(messageId: String, fromUser: String, seq: UInt64)
    case serverAck(messageId: String, seq: UInt64)
    case deliveredAck(messageId: String)
    case readAck(messageId: String)
    case groupMessage(messageId: String, groupId: String)
    case groupAckSummary(messageId: String, memberCount: UInt32)
    case presence(userId: String, kind: Int)
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
                ok: obj["ok"] as? Bool ?? false
            )
        case "SyncComplete":
            let seq: UInt64
            if let n = obj["latest_seq"] as? NSNumber { seq = n.uint64Value }
            else if let i = obj["latest_seq"] as? Int { seq = UInt64(i) }
            else { seq = 0 }
            return .syncComplete(latestSeq: seq)
        case "ChatMessage":
            return .chatMessage(
                messageId: obj["message_id"] as? String ?? "",
                fromUser: obj["from_user"] as? String ?? "",
                seq: (obj["seq"] as? NSNumber)?.uint64Value ?? 0
            )
        case "ServerAck":
            return .serverAck(
                messageId: obj["message_id"] as? String ?? "",
                seq: (obj["seq"] as? NSNumber)?.uint64Value ?? 0
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
                memberCount: (obj["member_count"] as? NSNumber)?.uint32Value ?? 0
            )
        case "Presence":
            return .presence(
                userId: obj["user_id"] as? String ?? "",
                kind: obj["kind"] as? Int ?? 0
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
