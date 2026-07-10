import Foundation

/// Serial media upload/download queue (one upload at a time per session rule).
public actor MediaService {
    public let store: LocalStore
    public let blobs: MediaBlobStore
    public let session: SessionActor

    public private(set) var transfers: [String: MediaTransferProgress] = [:]
    private var uploadBusy = false
    private var uploadWaiters: [CheckedContinuation<Void, Never>] = []
    private var downloadInFlight: Set<String> = []

    public init(store: LocalStore, blobs: MediaBlobStore, session: SessionActor) {
        self.store = store
        self.blobs = blobs
        self.session = session
    }

    public func progress(for mediaId: String) -> MediaTransferProgress? {
        transfers[mediaId]
    }

    /// Validate, persist locally, upload (queued), then send chat/group with `media_id`.
    @discardableResult
    public func sendAttachment(
        conversationId: String,
        data: Data,
        fileName: String,
        mimeType: String,
        caption: String,
        fromUser: String
    ) async throws -> StoredMessage {
        guard !data.isEmpty else { throw AppError.internalError("empty attachment") }
        if data.count > AppLimits.maxMediaBytes {
            throw AppError.mediaTooLarge(size: data.count, limit: AppLimits.maxMediaBytes)
        }
        let mediaId = "media-\(UUID().uuidString.lowercased())"
        let messageId = "ios-m-\(UUID().uuidString.lowercased())"
        let sha = MediaHasher.sha256Hex(data)
        _ = try blobs.write(
            mediaId: mediaId,
            fileName: fileName,
            mimeType: mimeType,
            data: data,
            expectedSha256: sha,
            complete: true
        )

        let isGroup = Conversation.groupId(fromConversationId: conversationId) != nil
        let pending = StoredMessage(
            messageId: messageId,
            conversationId: conversationId,
            direction: .outbound,
            body: caption,
            mediaId: mediaId,
            status: .pending,
            createdAt: Date(),
            fromUser: fromUser
        )
        try store.ensureConversation(id: conversationId, title: nil, isGroup: isGroup)
        _ = try store.upsertMessage(pending)

        transfers[mediaId] = MediaTransferProgress(
            mediaId: mediaId,
            kind: .upload,
            fraction: 0,
            status: .queued
        )

        do {
            try await withUploadSlot {
                transfers[mediaId] = MediaTransferProgress(
                    mediaId: mediaId,
                    kind: .upload,
                    fraction: 0.25,
                    status: .transferring
                )
                _ = try await session.uploadMedia(
                    mediaId: mediaId,
                    fileName: fileName,
                    mimeType: mimeType,
                    data: data
                )
                transfers[mediaId] = MediaTransferProgress(
                    mediaId: mediaId,
                    kind: .upload,
                    fraction: 1,
                    status: .complete
                )
            }
            let bodyData = Data(caption.utf8)
            if let groupId = Conversation.groupId(fromConversationId: conversationId) {
                try await session.sendGroup(
                    groupId: groupId,
                    messageId: messageId,
                    body: bodyData,
                    mediaId: mediaId
                )
            } else {
                let seq = try await session.sendChat(
                    to: conversationId,
                    messageId: messageId,
                    body: bodyData,
                    mediaId: mediaId
                )
                try store.updateStatus(messageId: messageId, status: .pending, seq: seq)
            }
            LaneLog.media.info("attachment sent")
            return pending
        } catch {
            try? store.updateStatus(messageId: messageId, status: .failed, seq: nil)
            transfers[mediaId] = MediaTransferProgress(
                mediaId: mediaId,
                kind: .upload,
                fraction: transfers[mediaId]?.fraction ?? 0,
                status: .failed,
                error: (error as? AppError)?.errorDescription ?? error.localizedDescription
            )
            throw (error as? AppError) ?? AppError.connection(error.localizedDescription)
        }
    }

    public func retryFailedAttachment(_ message: StoredMessage) async throws {
        guard message.status == .failed, !message.mediaId.isEmpty else { return }
        guard let data = try blobs.readData(message.mediaId),
              let meta = try blobs.meta(for: message.mediaId)
        else {
            throw AppError.mediaIncomplete("local blob missing")
        }
        try store.updateStatus(messageId: message.messageId, status: .pending, seq: nil)
        transfers[message.mediaId] = MediaTransferProgress(
            mediaId: message.mediaId,
            kind: .upload,
            fraction: 0,
            status: .queued
        )
        do {
            try await withUploadSlot {
                transfers[message.mediaId] = MediaTransferProgress(
                    mediaId: message.mediaId,
                    kind: .upload,
                    fraction: 0.25,
                    status: .transferring
                )
                _ = try await session.uploadMedia(
                    mediaId: message.mediaId,
                    fileName: meta.fileName,
                    mimeType: meta.mimeType,
                    data: data
                )
                transfers[message.mediaId] = MediaTransferProgress(
                    mediaId: message.mediaId,
                    kind: .upload,
                    fraction: 1,
                    status: .complete
                )
            }
            let bodyData = Data(message.body.utf8)
            if let groupId = Conversation.groupId(fromConversationId: message.conversationId) {
                try await session.sendGroup(
                    groupId: groupId,
                    messageId: message.messageId,
                    body: bodyData,
                    mediaId: message.mediaId
                )
            } else {
                let seq = try await session.sendChat(
                    to: message.conversationId,
                    messageId: message.messageId,
                    body: bodyData,
                    mediaId: message.mediaId
                )
                try store.updateStatus(messageId: message.messageId, status: .pending, seq: seq)
            }
        } catch {
            try store.updateStatus(messageId: message.messageId, status: .failed, seq: nil)
            throw (error as? AppError) ?? AppError.connection(error.localizedDescription)
        }
    }

    /// Fetch if not already complete locally. Never presents partial as complete.
    public func ensureDownloaded(mediaId: String) async throws -> MediaBlobMeta {
        if let meta = try blobs.meta(for: mediaId), meta.complete, blobs.hasComplete(mediaId) {
            return meta
        }
        while downloadInFlight.contains(mediaId) {
            try await Task.sleep(nanoseconds: 40_000_000)
            if let meta = try blobs.meta(for: mediaId), meta.complete {
                return meta
            }
        }
        downloadInFlight.insert(mediaId)
        defer { downloadInFlight.remove(mediaId) }

        transfers[mediaId] = MediaTransferProgress(
            mediaId: mediaId,
            kind: .download,
            fraction: 0.1,
            status: .transferring
        )
        do {
            let fetched = try await session.fetchMedia(mediaId: mediaId)
            transfers[mediaId] = MediaTransferProgress(
                mediaId: mediaId,
                kind: .download,
                fraction: 0.85,
                status: .transferring
            )
            let expected = fetched.sha256
            let meta = try blobs.write(
                mediaId: mediaId,
                fileName: fetched.fileName.isEmpty ? mediaId : fetched.fileName,
                mimeType: fetched.mimeType.isEmpty ? "application/octet-stream" : fetched.mimeType,
                data: fetched.data,
                expectedSha256: expected.isEmpty ? nil : expected,
                complete: true
            )
            transfers[mediaId] = MediaTransferProgress(
                mediaId: mediaId,
                kind: .download,
                fraction: 1,
                status: .complete
            )
            LaneLog.media.info("downloaded media")
            return meta
        } catch {
            try? blobs.markIncomplete(mediaId: mediaId)
            transfers[mediaId] = MediaTransferProgress(
                mediaId: mediaId,
                kind: .download,
                fraction: 0.1,
                status: .failed,
                error: (error as? AppError)?.errorDescription ?? error.localizedDescription
            )
            LaneLog.media.error("download failed")
            throw (error as? AppError) ?? AppError.connection(error.localizedDescription)
        }
    }

    private func withUploadSlot(_ body: () async throws -> Void) async throws {
        while uploadBusy {
            await withCheckedContinuation { (cont: CheckedContinuation<Void, Never>) in
                uploadWaiters.append(cont)
            }
        }
        uploadBusy = true
        defer {
            uploadBusy = false
            if !uploadWaiters.isEmpty {
                uploadWaiters.removeFirst().resume()
            }
        }
        try await body()
    }
}
