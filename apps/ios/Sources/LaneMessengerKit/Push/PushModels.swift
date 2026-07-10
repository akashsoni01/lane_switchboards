import Foundation

/// APNs / local notification payload contract (see `docs/client-ios/09_push.md`).
public struct PushPayload: Sendable, Equatable {
    public var conversationId: String
    public var messageId: String
    public var senderId: String
    public var preview: String
    public var encrypted: Bool

    public init(
        conversationId: String,
        messageId: String = "",
        senderId: String = "",
        preview: String = "",
        encrypted: Bool = false
    ) {
        self.conversationId = conversationId
        self.messageId = messageId
        self.senderId = senderId
        self.preview = preview
        self.encrypted = encrypted
    }

    /// Parse APNs `userInfo` (or Notification Service Extension payload).
    public static func parse(userInfo: [AnyHashable: Any]) -> PushPayload? {
        let lane = userInfo["lane"] as? [String: Any] ?? userInfo as? [String: Any] ?? [:]
        let conversationId = (lane["conversation_id"] as? String)
            ?? (lane["conversationId"] as? String)
            ?? (userInfo["conversation_id"] as? String)
            ?? ""
        guard !conversationId.isEmpty else { return nil }
        let encrypted = (lane["encrypted"] as? Bool)
            ?? (lane["e2ee"] as? Bool)
            ?? false
        return PushPayload(
            conversationId: conversationId,
            messageId: (lane["message_id"] as? String) ?? (lane["messageId"] as? String) ?? "",
            senderId: (lane["sender_id"] as? String) ?? (lane["from_user"] as? String) ?? "",
            preview: (lane["preview"] as? String) ?? (lane["body"] as? String) ?? "",
            encrypted: encrypted
        )
    }

    /// Deep link: `lane://chat/<conversationId>` or `https://lane.app/chat/<id>`.
    public static func parse(url: URL) -> PushPayload? {
        let host = url.host?.lowercased() ?? ""
        let pathParts = url.path.split(separator: "/").map(String.init)
        if url.scheme?.lowercased() == "lane" {
            // lane://chat/bob  or lane:///chat/bob
            if host == "chat", let id = pathParts.first, !id.isEmpty {
                return PushPayload(conversationId: id)
            }
            if pathParts.count >= 2, pathParts[0] == "chat" {
                return PushPayload(conversationId: pathParts[1])
            }
            if host != "chat", !host.isEmpty, pathParts.isEmpty {
                // lane://bob
                return PushPayload(conversationId: host)
            }
        }
        if host.contains("lane"), pathParts.count >= 2, pathParts[0] == "chat" {
            return PushPayload(conversationId: pathParts[1])
        }
        return nil
    }

    public var deepLinkURL: URL? {
        URL(string: "lane://chat/\(conversationId)")
    }
}

/// How much message content appears on the lock screen.
public enum NotificationPreviewPolicy: String, Sendable, Equatable {
    /// Always show sender + preview text.
    case showPreview
    /// When E2EE is on (or payload marked encrypted), hide body.
    case hideBodyWhenE2EE
    /// Never show body (sender only / generic).
    case hideAlways

    public static var `default`: NotificationPreviewPolicy {
        FeatureFlags.e2eeEnabled ? .hideBodyWhenE2EE : .showPreview
    }

    public func displayTitle(for payload: PushPayload) -> String {
        if payload.senderId.isEmpty { return "Lane" }
        return payload.senderId
    }

    public func displayBody(for payload: PushPayload) -> String {
        switch self {
        case .showPreview:
            return payload.preview.isEmpty ? "New message" : payload.preview
        case .hideAlways:
            return "New message"
        case .hideBodyWhenE2EE:
            if payload.encrypted || FeatureFlags.e2eeEnabled {
                return "New message"
            }
            return payload.preview.isEmpty ? "New message" : payload.preview
        }
    }
}

/// Registers the APNs device token with the identity / push gateway (stub until server exists).
public protocol PushTokenRegistering: AnyObject, Sendable {
    func register(deviceTokenHex: String, userId: String, deviceId: String) async throws
    func unregister(userId: String, deviceId: String) async throws
}

/// In-memory registrar for tests and DEBUG without a push backend.
public final class StubPushTokenRegistrar: PushTokenRegistering, @unchecked Sendable {
    public private(set) var registeredTokens: [String] = []
    public private(set) var lastUserId: String?
    public var shouldFail = false
    public var endpointLogged: String?

    public init(endpoint: String = "https://identity.invalid/v1/push/register") {
        self.endpointLogged = endpoint
    }

    public func register(deviceTokenHex: String, userId: String, deviceId: String) async throws {
        if shouldFail { throw AppError.connection("push register failed") }
        _ = deviceId
        registeredTokens.append(deviceTokenHex)
        lastUserId = userId
        LaneLog.ui.info("stub push register token_len=\(deviceTokenHex.count, privacy: .public)")
    }

    public func unregister(userId: String, deviceId: String) async throws {
        _ = userId
        _ = deviceId
        registeredTokens.removeAll()
    }
}

/// HTTPS registrar — posts JSON when the identity service is available.
public final class HttpPushTokenRegistrar: PushTokenRegistering, @unchecked Sendable {
    public let endpoint: URL
    private let session: URLSession

    public init(endpoint: URL, session: URLSession = .shared) {
        self.endpoint = endpoint
        self.session = session
    }

    public func register(deviceTokenHex: String, userId: String, deviceId: String) async throws {
        var req = URLRequest(url: endpoint)
        req.httpMethod = "POST"
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")
        let body: [String: String] = [
            "platform": "ios",
            "token": deviceTokenHex,
            "user_id": userId,
            "device_id": deviceId,
            // Explicit: standard alert pushes only — never VoIP.
            "push_type": "alert",
        ]
        req.httpBody = try JSONSerialization.data(withJSONObject: body)
        let (_, response) = try await session.data(for: req)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            throw AppError.connection("push registration rejected")
        }
    }

    public func unregister(userId: String, deviceId: String) async throws {
        var components = URLComponents(url: endpoint, resolvingAgainstBaseURL: false)
        components?.queryItems = [
            URLQueryItem(name: "user_id", value: userId),
            URLQueryItem(name: "device_id", value: deviceId),
        ]
        guard let url = components?.url else { return }
        var req = URLRequest(url: url)
        req.httpMethod = "DELETE"
        _ = try await session.data(for: req)
    }
}

/// Presents local / remote notifications and manages the app icon badge.
public protocol NotificationPresenting: AnyObject, Sendable {
    var previewPolicy: NotificationPreviewPolicy { get set }
    func requestAuthorization() async -> Bool
    func presentLocal(payload: PushPayload) async
    func setBadge(_ count: Int) async
    func clearNotifications(forConversationId conversationId: String) async
}

/// Records presentations for unit tests (no system UI).
public final class RecordingNotificationPresenter: NotificationPresenting, @unchecked Sendable {
    public var previewPolicy: NotificationPreviewPolicy = .default
    public private(set) var presented: [PushPayload] = []
    public private(set) var badge: Int = 0
    public private(set) var cleared: [String] = []
    public var authorized = true

    public init() {}

    public func requestAuthorization() async -> Bool { authorized }

    public func presentLocal(payload: PushPayload) async {
        presented.append(payload)
    }

    public func setBadge(_ count: Int) async {
        badge = max(0, count)
    }

    public func clearNotifications(forConversationId conversationId: String) async {
        cleared.append(conversationId)
        presented.removeAll { $0.conversationId == conversationId }
    }
}

public enum PushTokenFormat {
    public static func hex(_ data: Data) -> String {
        data.map { String(format: "%02x", $0) }.joined()
    }
}

public enum UnreadBadge {
    public static func total(from conversations: [Conversation]) -> Int {
        conversations.reduce(0) { $0 + max(0, $1.unread) }
    }
}
