import Foundation

public enum MessageDirection: String, Sendable, Equatable {
    case inbound
    case outbound
    case system
}

public enum MessageStatus: String, Sendable, Equatable {
    case pending
    case sent
    case delivered
    case read
    case failed
}

/// Wire presence kinds (`docs/messenger/04_presence.md`).
public enum PresenceKind: Int, Sendable, Equatable {
    case available = 0
    case unavailable = 1
    case lastSeen = 2

    public var isOnline: Bool { self == .available }
}

/// Proto `GroupOp` values.
public enum GroupOp: Int, Sendable, Equatable {
    case unspecified = 0
    case create = 1
    case addMember = 2
    case removeMember = 3
    case leave = 4
}

public struct StoredMessage: Sendable, Equatable, Identifiable {
    public var id: String { messageId }
    public var messageId: String
    public var conversationId: String
    public var direction: MessageDirection
    public var body: String
    public var mediaId: String
    public var seq: UInt64
    public var status: MessageStatus
    public var createdAt: Date
    /// Sender user id (group threads); empty for 1:1 outbound.
    public var fromUser: String
    public var deliveredCount: UInt32
    public var readCount: UInt32
    public var memberCount: UInt32

    public init(
        messageId: String,
        conversationId: String,
        direction: MessageDirection,
        body: String,
        mediaId: String = "",
        seq: UInt64 = 0,
        status: MessageStatus = .pending,
        createdAt: Date = Date(),
        fromUser: String = "",
        deliveredCount: UInt32 = 0,
        readCount: UInt32 = 0,
        memberCount: UInt32 = 0
    ) {
        self.messageId = messageId
        self.conversationId = conversationId
        self.direction = direction
        self.body = body
        self.mediaId = mediaId
        self.seq = seq
        self.status = status
        self.createdAt = createdAt
        self.fromUser = fromUser
        self.deliveredCount = deliveredCount
        self.readCount = readCount
        self.memberCount = memberCount
    }
}

public struct Conversation: Sendable, Equatable, Identifiable {
    public var id: String
    public var title: String
    public var sortTs: Date
    public var unread: Int
    public var draft: String
    public var lastPreview: String
    public var presence: PresenceKind?
    public var lastSeen: Date?
    public var isGroup: Bool

    public init(
        id: String,
        title: String = "",
        sortTs: Date = Date(),
        unread: Int = 0,
        draft: String = "",
        lastPreview: String = "",
        presence: PresenceKind? = nil,
        lastSeen: Date? = nil,
        isGroup: Bool = false
    ) {
        self.id = id
        self.title = title.isEmpty ? id : title
        self.sortTs = sortTs
        self.unread = unread
        self.draft = draft
        self.lastPreview = lastPreview
        self.presence = presence
        self.lastSeen = lastSeen
        self.isGroup = isGroup
    }

    public static func groupConversationId(_ groupId: String) -> String {
        "group:\(groupId)"
    }

    public static func groupId(fromConversationId id: String) -> String? {
        guard id.hasPrefix("group:") else { return nil }
        return String(id.dropFirst("group:".count))
    }
}

public struct Contact: Sendable, Equatable, Identifiable {
    public var id: String { userId }
    public var userId: String
    public var displayName: String
    public var presence: PresenceKind
    public var lastSeen: Date?

    public init(
        userId: String,
        displayName: String = "",
        presence: PresenceKind = .unavailable,
        lastSeen: Date? = nil
    ) {
        self.userId = userId
        self.displayName = displayName.isEmpty ? userId : displayName
        self.presence = presence
        self.lastSeen = lastSeen
    }
}

public struct GroupMember: Sendable, Equatable, Identifiable {
    public var id: String { userId }
    public var userId: String
    public var isAdmin: Bool

    public init(userId: String, isAdmin: Bool) {
        self.userId = userId
        self.isAdmin = isAdmin
    }
}

public struct GroupInfo: Sendable, Equatable, Identifiable {
    public var id: String
    public var title: String
    public var version: UInt64
    public var members: [GroupMember]

    public init(id: String, title: String = "", version: UInt64 = 0, members: [GroupMember] = []) {
        self.id = id
        self.title = title.isEmpty ? id : title
        self.version = version
        self.members = members
    }

    public var memberCount: Int { members.count }

    public func isAdmin(_ userId: String) -> Bool {
        members.contains { $0.userId == userId && $0.isAdmin }
    }

    public func isMember(_ userId: String) -> Bool {
        members.contains { $0.userId == userId }
    }
}

public enum AppLimits {
    public static let maxGroupMembers = 1024
    /// Matches `ServerConfig.max_media_bytes` default.
    public static let maxMediaBytes = 64 * 1024 * 1024
    public static let maxMediaChunkBytes = 64 * 1024
}

/// Offline-first local store. `resume_after_seq` is the reconnect high-water mark.
public protocol LocalStore: AnyObject, Sendable {
    func resumeAfterSeq() throws -> UInt64
    func setResumeAfterSeq(_ seq: UInt64) throws
    @discardableResult
    func upsertMessage(_ message: StoredMessage) throws -> Bool
    func updateStatus(messageId: String, status: MessageStatus, seq: UInt64?) throws
    func updateGroupAckSummary(
        messageId: String,
        deliveredCount: UInt32,
        readCount: UInt32,
        memberCount: UInt32
    ) throws
    func conversations() throws -> [Conversation]
    func messages(conversationId: String, limit: Int) throws -> [StoredMessage]
    func setDraft(conversationId: String, draft: String) throws
    func markConversationRead(conversationId: String) throws
    func ensureConversation(id: String, title: String?, isGroup: Bool) throws
    func upsertContact(_ contact: Contact) throws
    func contacts() throws -> [Contact]
    func contact(userId: String) throws -> Contact?
    func upsertGroup(_ group: GroupInfo) throws
    func group(id: String) throws -> GroupInfo?
    func groups() throws -> [GroupInfo]
    /// Apply membership change only if `version` is newer than stored.
    @discardableResult
    func applyGroupEvent(
        groupId: String,
        op: GroupOp,
        actor: String,
        subject: String,
        version: UInt64
    ) throws -> Bool
    func wipeUserData() throws
}

public extension LocalStore {
    func ensureConversation(id: String, title: String?) throws {
        try ensureConversation(id: id, title: title, isGroup: Conversation.groupId(fromConversationId: id) != nil)
    }
}
