import Foundation

public enum MessageDirection: String, Sendable, Equatable {
    case inbound
    case outbound
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

    public init(
        messageId: String,
        conversationId: String,
        direction: MessageDirection,
        body: String,
        mediaId: String = "",
        seq: UInt64 = 0,
        status: MessageStatus = .pending,
        createdAt: Date = Date()
    ) {
        self.messageId = messageId
        self.conversationId = conversationId
        self.direction = direction
        self.body = body
        self.mediaId = mediaId
        self.seq = seq
        self.status = status
        self.createdAt = createdAt
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

    public init(
        id: String,
        title: String = "",
        sortTs: Date = Date(),
        unread: Int = 0,
        draft: String = "",
        lastPreview: String = "",
        presence: PresenceKind? = nil,
        lastSeen: Date? = nil
    ) {
        self.id = id
        self.title = title.isEmpty ? id : title
        self.sortTs = sortTs
        self.unread = unread
        self.draft = draft
        self.lastPreview = lastPreview
        self.presence = presence
        self.lastSeen = lastSeen
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

/// Offline-first local store. `resume_after_seq` is the reconnect high-water mark.
public protocol LocalStore: AnyObject, Sendable {
    func resumeAfterSeq() throws -> UInt64
    func setResumeAfterSeq(_ seq: UInt64) throws
    /// Upsert by `message_id` (idempotent). Advances resume seq when `seq` is higher.
    @discardableResult
    func upsertMessage(_ message: StoredMessage) throws -> Bool
    func updateStatus(messageId: String, status: MessageStatus, seq: UInt64?) throws
    func conversations() throws -> [Conversation]
    func messages(conversationId: String, limit: Int) throws -> [StoredMessage]
    func setDraft(conversationId: String, draft: String) throws
    func markConversationRead(conversationId: String) throws
    func ensureConversation(id: String, title: String?) throws
    func upsertContact(_ contact: Contact) throws
    func contacts() throws -> [Contact]
    func contact(userId: String) throws -> Contact?
    func wipeUserData() throws
}
