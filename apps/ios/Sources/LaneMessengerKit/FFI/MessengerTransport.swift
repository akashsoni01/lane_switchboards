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
    public private(set) var didConnect = false
    public private(set) var didClose = false
    public private(set) var lastRequest: ConnectRequest?

    private var eventIndex = 0

    public init() {}

    public func connect(_ request: ConnectRequest) async throws {
        if let connectError { throw connectError }
        lastRequest = request
        didConnect = true
        didClose = false
        eventIndex = 0
        // Simulate LoginAck + SyncComplete if queue empty.
        if events.isEmpty {
            events = [
                #"{"type":"LoginAck","session_id":"mock","ok":true}"#,
                #"{"type":"SyncComplete","latest_seq":0}"#,
            ]
        }
    }

    public func ping() async throws {
        guard didConnect, !didClose else { throw AppError.connection("not connected") }
    }

    public func pollEvent(timeoutMs: UInt64) async -> String? {
        _ = timeoutMs
        guard eventIndex < events.count else { return nil }
        defer { eventIndex += 1 }
        return events[eventIndex]
    }

    public func close() async throws {
        didClose = true
    }

    public func enqueueReplaced() {
        events.append(#"{"type":"ReplacedByNewSession"}"#)
    }
}
