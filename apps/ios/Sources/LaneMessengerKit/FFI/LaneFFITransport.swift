import Foundation

#if canImport(LaneMessengerFFI)
import LaneMessengerFFI
#endif

/// Production transport wrapping `LaneMessengerFFI.LaneSession` (C ABI).
///
/// When the FFI module is not linked (unit tests / macOS package builds),
/// construction throws `AppError.notConfigured` — inject a mock instead.
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
}
