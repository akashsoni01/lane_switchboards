import Foundation

/// Abstraction over `LaneMessengerFFI.LaneSession` so unit tests can inject fakes.
public protocol MessengerTransport: AnyObject, Sendable {
    func connect(_ request: ConnectRequest) async throws
    func ping() async throws
    func pollEvent(timeoutMs: UInt64) async -> String?
    func close() async throws
}

/// In-memory transport for tests and UI previews (no native library required).
public final class MockMessengerTransport: MessengerTransport, @unchecked Sendable {
    public var connectError: AppError?
    public var events: [String] = []
    public var failPing = false
    public var disconnectAfterReady = false
    public private(set) var didConnect = false
    public private(set) var didClose = false
    public private(set) var connectCount = 0
    public private(set) var lastRequest: ConnectRequest?
    public private(set) var pingCount = 0

    private var eventIndex = 0
    private var emittedDisconnect = false

    public init() {}

    public func connect(_ request: ConnectRequest) async throws {
        if let connectError { throw connectError }
        lastRequest = request
        didConnect = true
        didClose = false
        connectCount += 1
        eventIndex = 0
        emittedDisconnect = false
        if events.isEmpty {
            events = [
                #"{"type":"LoginAck","session_id":"mock","pending_messages":0,"ok":true,"error":""}"#,
                #"{"type":"SyncComplete","delivered":0,"latest_seq":0}"#,
            ]
        }
    }

    public func ping() async throws {
        guard didConnect, !didClose else { throw AppError.connection("not connected") }
        if failPing { throw AppError.connection("ping failed") }
        pingCount += 1
    }

    public func pollEvent(timeoutMs: UInt64) async -> String? {
        _ = timeoutMs
        if eventIndex < events.count {
            defer { eventIndex += 1 }
            return events[eventIndex]
        }
        if disconnectAfterReady, !emittedDisconnect, didConnect, !didClose {
            emittedDisconnect = true
            return #"{"type":"Disconnected","reason":"mock drop"}"#
        }
        return nil
    }

    public func close() async throws {
        didClose = true
        didConnect = false
    }

    public func enqueueReplaced() {
        events.append(#"{"type":"ReplacedByNewSession"}"#)
    }

    public func resetEvents(_ next: [String]) {
        events = next
        eventIndex = 0
    }
}
