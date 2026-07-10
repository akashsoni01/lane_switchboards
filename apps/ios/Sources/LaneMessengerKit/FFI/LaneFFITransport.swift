import Foundation

#if canImport(LaneMessengerFFI)
import LaneMessengerFFI
#endif

/// Production transport wrapping `LaneMessengerFFI.LaneSession` (C ABI).
public final class LaneFFITransport: MessengerTransport, @unchecked Sendable {
    #if canImport(LaneMessengerFFI)
    private var session: LaneSession?
    #endif

    public init() {}

    public func connect(_ request: ConnectRequest) async throws {
        #if canImport(LaneMessengerFFI)
        let session = try LaneSession(
            host: request.config.host,
            port: request.config.port,
            useTls: request.config.useTls,
            userId: request.credentials.userId,
            deviceId: request.credentials.deviceId,
            authToken: request.credentials.authToken,
            clientVersion: request.clientVersion,
            resumeAfterSeq: request.resumeAfterSeq,
            pingIntervalSecs: request.config.pingIntervalSecs
        )
        self.session = session
        #else
        throw AppError.notConfigured
        #endif
    }

    public func ping() async throws {
        #if canImport(LaneMessengerFFI)
        guard let session else { throw AppError.connection("not connected") }
        try session.ping()
        #else
        throw AppError.notConfigured
        #endif
    }

    public func pollEvent(timeoutMs: UInt64) async -> String? {
        #if canImport(LaneMessengerFFI)
        session?.pollEvent(timeoutMs: timeoutMs)
        #else
        nil
        #endif
    }

    public func close() async throws {
        #if canImport(LaneMessengerFFI)
        try session?.close()
        session = nil
        #endif
    }

    public func sendChat(to: String, messageId: String, body: Data) async throws -> UInt64 {
        try await sendChat(to: to, messageId: messageId, body: body, mediaId: "")
    }

    public func sendChat(to: String, messageId: String, body: Data, mediaId: String) async throws -> UInt64 {
        #if canImport(LaneMessengerFFI)
        guard let session else { throw AppError.connection("not connected") }
        return try session.sendChat(to: to, messageId: messageId, body: body, mediaId: mediaId)
        #else
        throw AppError.notConfigured
        #endif
    }

    public func ackDelivered(messageId: String) async throws {
        #if canImport(LaneMessengerFFI)
        guard let session else { throw AppError.connection("not connected") }
        try session.ackDelivered(messageId: messageId)
        #else
        throw AppError.notConfigured
        #endif
    }

    public func ackRead(messageId: String) async throws {
        #if canImport(LaneMessengerFFI)
        guard let session else { throw AppError.connection("not connected") }
        try session.ackRead(messageId: messageId)
        #else
        throw AppError.notConfigured
        #endif
    }

    public func subscribePresence(contactIds: [String]) async throws {
        #if canImport(LaneMessengerFFI)
        guard let session else { throw AppError.connection("not connected") }
        try session.subscribePresence(contactIds: contactIds)
        #else
        throw AppError.notConfigured
        #endif
    }

    public func createGroup(groupId: String) async throws -> UInt64 {
        #if canImport(LaneMessengerFFI)
        guard let session else { throw AppError.connection("not connected") }
        return try session.createGroup(groupId: groupId)
        #else
        throw AppError.notConfigured
        #endif
    }

    public func addMember(groupId: String, user: String) async throws -> UInt64 {
        #if canImport(LaneMessengerFFI)
        guard let session else { throw AppError.connection("not connected") }
        return try session.addMember(groupId: groupId, user: user)
        #else
        throw AppError.notConfigured
        #endif
    }

    public func removeMember(groupId: String, user: String) async throws -> UInt64 {
        #if canImport(LaneMessengerFFI)
        guard let session else { throw AppError.connection("not connected") }
        return try session.removeMember(groupId: groupId, user: user)
        #else
        throw AppError.notConfigured
        #endif
    }

    public func leaveGroup(groupId: String) async throws -> UInt64 {
        #if canImport(LaneMessengerFFI)
        guard let session else { throw AppError.connection("not connected") }
        return try session.leaveGroup(groupId: groupId)
        #else
        throw AppError.notConfigured
        #endif
    }

    public func sendGroup(groupId: String, messageId: String, body: Data) async throws {
        try await sendGroup(groupId: groupId, messageId: messageId, body: body, mediaId: "")
    }

    public func sendGroup(groupId: String, messageId: String, body: Data, mediaId: String) async throws {
        #if canImport(LaneMessengerFFI)
        guard let session else { throw AppError.connection("not connected") }
        try session.sendGroup(groupId: groupId, messageId: messageId, body: body, mediaId: mediaId)
        #else
        throw AppError.notConfigured
        #endif
    }

    public func uploadMedia(
        mediaId: String,
        fileName: String,
        mimeType: String,
        data: Data
    ) async throws -> UInt64 {
        #if canImport(LaneMessengerFFI)
        guard let session else { throw AppError.connection("not connected") }
        return try session.uploadMedia(
            mediaId: mediaId,
            fileName: fileName,
            mimeType: mimeType,
            data: data
        )
        #else
        throw AppError.notConfigured
        #endif
    }

    public func fetchMedia(mediaId: String) async throws -> FetchedMediaBlob {
        #if canImport(LaneMessengerFFI)
        guard let session else { throw AppError.connection("not connected") }
        let m = try session.fetchMedia(mediaId: mediaId)
        return FetchedMediaBlob(
            data: m.data,
            fileName: m.fileName,
            mimeType: m.mimeType,
            sha256: m.sha256
        )
        #else
        throw AppError.notConfigured
        #endif
    }
}
