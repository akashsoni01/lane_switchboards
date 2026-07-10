import Foundation

/// Domain use-cases for 1:1 chat. Always writes local DB before FFI send.
public struct ChatService: Sendable {
    public let store: LocalStore
    public let session: SessionActor

    public init(store: LocalStore, session: SessionActor) {
        self.store = store
        self.session = session
    }

    /// Insert pending outbound row, then send. Marks `failed` on transport error.
    @discardableResult
    public func sendText(to peer: String, body: String, fromUser: String) async throws -> StoredMessage {
        let trimmed = body.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { throw AppError.internalError("empty message") }
        let messageId = "ios-\(UUID().uuidString.lowercased())"
        let pending = StoredMessage(
            messageId: messageId,
            conversationId: peer,
            direction: .outbound,
            body: trimmed,
            status: .pending,
            createdAt: Date()
        )
        try store.ensureConversation(id: peer, title: peer)
        _ = try store.upsertMessage(pending)
        try store.setDraft(conversationId: peer, draft: "")

        do {
            let data = Data(trimmed.utf8)
            let seq = try await session.sendChat(to: peer, messageId: messageId, body: data)
            // ServerAck will move pending → sent; stamp seq early for resume.
            try store.updateStatus(messageId: messageId, status: .pending, seq: seq)
            LaneLog.chat.info("sent message_id length=\(messageId.count, privacy: .public) to peer")
            return pending
        } catch {
            try store.updateStatus(messageId: messageId, status: .failed, seq: nil)
            LaneLog.chat.error("send failed")
            throw (error as? AppError) ?? AppError.connection(error.localizedDescription)
        }
    }

    public func retryFailed(message: StoredMessage, to peer: String) async throws {
        guard message.status == .failed, message.direction == .outbound else { return }
        try store.updateStatus(messageId: message.messageId, status: .pending, seq: nil)
        do {
            let seq = try await session.sendChat(
                to: peer,
                messageId: message.messageId,
                body: Data(message.body.utf8)
            )
            try store.updateStatus(messageId: message.messageId, status: .pending, seq: seq)
        } catch {
            try store.updateStatus(messageId: message.messageId, status: .failed, seq: nil)
            throw (error as? AppError) ?? AppError.connection(error.localizedDescription)
        }
    }

    /// On opening a thread: ack delivered+read for inbound messages not yet read.
    public func openThread(peer: String) async throws {
        try store.markConversationRead(conversationId: peer)
        let msgs = try store.messages(conversationId: peer, limit: 500)
        for msg in msgs where msg.direction == .inbound && msg.status != .read {
            try? await session.ackDelivered(messageId: msg.messageId)
            try? await session.ackRead(messageId: msg.messageId)
            try? store.updateStatus(messageId: msg.messageId, status: .read, seq: nil)
        }
    }
}
