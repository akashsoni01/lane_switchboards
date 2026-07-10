import Foundation

/// Domain use-cases for groups. Membership is version-gated; sends are offline-first.
public struct GroupService: Sendable {
    public let store: LocalStore
    public let session: SessionActor
    public let e2ee: E2eeService?

    public init(store: LocalStore, session: SessionActor, e2ee: E2eeService? = nil) {
        self.store = store
        self.session = session
        self.e2ee = e2ee
    }

    /// Create group on the wire, then seed local membership (creator = admin).
    @discardableResult
    public func createGroup(groupId: String, title: String, creator: String) async throws -> GroupInfo {
        let gid = groupId.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !gid.isEmpty else { throw AppError.internalError("empty group id") }
        let version = try await session.createGroup(groupId: gid)
        let info = GroupInfo(
            id: gid,
            title: title.isEmpty ? gid : title,
            version: version,
            members: [GroupMember(userId: creator, isAdmin: true)]
        )
        try store.upsertGroup(info)
        let convoId = Conversation.groupConversationId(gid)
        try store.ensureConversation(id: convoId, title: info.title, isGroup: true)
        try appendSystemLine(
            groupId: gid,
            text: Self.systemText(op: .create, actor: creator, subject: creator),
            version: version
        )
        try await e2ee?.setupGroupEncryption(groupId: gid, members: [creator])
        LaneLog.groups.info("created group")
        return info
    }

    public func addMember(groupId: String, user: String, actor: String) async throws {
        guard let info = try store.group(id: groupId) else {
            throw AppError.internalError("unknown group")
        }
        guard info.memberCount < AppLimits.maxGroupMembers else {
            throw AppError.internalError("group is full (max \(AppLimits.maxGroupMembers))")
        }
        let version = try await session.addMember(groupId: groupId, user: user)
        let applied = try store.applyGroupEvent(
            groupId: groupId,
            op: .addMember,
            actor: actor,
            subject: user,
            version: version
        )
        if applied {
            try appendSystemLine(
                groupId: groupId,
                text: Self.systemText(op: .addMember, actor: actor, subject: user),
                version: version
            )
            if let info = try store.group(id: groupId) {
                let members = info.members.map(\.userId).filter { $0 != actor }
                try await e2ee?.setupGroupEncryption(groupId: groupId, members: members)
            }
        }
    }

    public func removeMember(groupId: String, user: String, actor: String) async throws {
        let version = try await session.removeMember(groupId: groupId, user: user)
        let applied = try store.applyGroupEvent(
            groupId: groupId,
            op: .removeMember,
            actor: actor,
            subject: user,
            version: version
        )
        if applied {
            try appendSystemLine(
                groupId: groupId,
                text: Self.systemText(op: .removeMember, actor: actor, subject: user),
                version: version
            )
        }
    }

    public func leave(groupId: String, user: String) async throws {
        let version = try await session.leaveGroup(groupId: groupId)
        let applied = try store.applyGroupEvent(
            groupId: groupId,
            op: .leave,
            actor: user,
            subject: user,
            version: version
        )
        if applied {
            try appendSystemLine(
                groupId: groupId,
                text: Self.systemText(op: .leave, actor: user, subject: user),
                version: version
            )
        }
    }

    @discardableResult
    public func sendText(groupId: String, body: String, fromUser: String) async throws -> StoredMessage {
        let trimmed = body.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { throw AppError.internalError("empty message") }
        if let info = try store.group(id: groupId), !info.isMember(fromUser) {
            throw AppError.protocolError(code: "authz", message: "not a member")
        }
        let convoId = Conversation.groupConversationId(groupId)
        let messageId = "ios-g-\(UUID().uuidString.lowercased())"
        let pending = StoredMessage(
            messageId: messageId,
            conversationId: convoId,
            direction: .outbound,
            body: trimmed,
            status: .pending,
            createdAt: Date(),
            fromUser: fromUser
        )
        let title = (try? store.group(id: groupId))?.title ?? groupId
        try store.ensureConversation(id: convoId, title: title, isGroup: true)
        _ = try store.upsertMessage(pending)
        try store.setDraft(conversationId: convoId, draft: "")

        do {
            let data = Data(trimmed.utf8)
            if let e2ee, await e2ee.shouldEncryptSends() {
                try await e2ee.sendGroup(groupId: groupId, messageId: messageId, plaintext: data)
            } else {
                try await session.sendGroup(groupId: groupId, messageId: messageId, body: data)
            }
            LaneLog.groups.info("sent group message")
            return pending
        } catch {
            try store.updateStatus(messageId: messageId, status: .failed, seq: nil)
            throw (error as? AppError) ?? AppError.connection(error.localizedDescription)
        }
    }

    public func retryFailed(message: StoredMessage, groupId: String) async throws {
        guard message.status == .failed, message.direction == .outbound else { return }
        try store.updateStatus(messageId: message.messageId, status: .pending, seq: nil)
        do {
            let data = Data(message.body.utf8)
            if let e2ee, await e2ee.shouldEncryptSends() {
                try await e2ee.sendGroup(groupId: groupId, messageId: message.messageId, plaintext: data)
            } else {
                try await session.sendGroup(
                    groupId: groupId,
                    messageId: message.messageId,
                    body: data
                )
            }
        } catch {
            try store.updateStatus(messageId: message.messageId, status: .failed, seq: nil)
            throw (error as? AppError) ?? AppError.connection(error.localizedDescription)
        }
    }

    /// Insert a local system line for membership changes (UI only).
    public func appendSystemLine(groupId: String, text: String, version: UInt64) throws {
        let convoId = Conversation.groupConversationId(groupId)
        let msg = StoredMessage(
            messageId: "sys-\(groupId)-\(version)-\(UUID().uuidString.prefix(8))",
            conversationId: convoId,
            direction: .system,
            body: text,
            status: .sent,
            createdAt: Date()
        )
        try store.ensureConversation(id: convoId, title: nil, isGroup: true)
        _ = try store.upsertMessage(msg)
    }

    public static func systemText(op: GroupOp, actor: String, subject: String) -> String {
        switch op {
        case .create:
            return "\(actor) created the group"
        case .addMember:
            return "\(actor) added \(subject)"
        case .removeMember:
            return "\(actor) removed \(subject)"
        case .leave:
            return "\(actor) left"
        case .unspecified:
            return "Group updated"
        }
    }
}
