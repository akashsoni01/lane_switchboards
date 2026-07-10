import Foundation

/// Abstraction over `LaneMessengerFFI.LaneSession` so unit tests can inject fakes.
public protocol MessengerTransport: AnyObject, Sendable {
    func connect(_ request: ConnectRequest) async throws
    func ping() async throws
    func pollEvent(timeoutMs: UInt64) async -> String?
    func close() async throws
    func sendChat(to: String, messageId: String, body: Data) async throws -> UInt64
    func ackDelivered(messageId: String) async throws
    func ackRead(messageId: String) async throws
    func subscribePresence(contactIds: [String]) async throws
}

public extension MessengerTransport {
    func subscribePresence(contactIds: [String]) async throws {
        _ = contactIds
    }
}

/// In-memory transport for tests and UI previews (no native library required).
public final class MockMessengerTransport: MessengerTransport, @unchecked Sendable {
    public var connectError: AppError?
    public var sendError: AppError?
    public var events: [String] = []
    public var failPing = false
    public var disconnectAfterReady = false
    /// When true, `sendChat` enqueues a local ServerAck (and optional echo).
    public var autoAckSends = true
    public var echoSendsToSelf = false
    public private(set) var didConnect = false
    public private(set) var didClose = false
    public private(set) var connectCount = 0
    public private(set) var lastRequest: ConnectRequest?
    public private(set) var pingCount = 0
    public private(set) var sent: [(to: String, messageId: String, body: Data)] = []
    public private(set) var deliveredAcks: [String] = []
    public private(set) var readAcks: [String] = []
    public private(set) var presenceSubscriptions: [String] = []

    private var eventIndex = 0
    private var emittedDisconnect = false
    private var seqCounter: UInt64 = 100

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

    public func sendChat(to: String, messageId: String, body: Data) async throws -> UInt64 {
        guard didConnect, !didClose else { throw AppError.connection("not connected") }
        if let sendError { throw sendError }
        sent.append((to, messageId, body))
        seqCounter += 1
        let seq = seqCounter
        if autoAckSends {
            events.append(#"{"type":"ServerAck","message_id":"\#(messageId)","seq":\#(seq)}"#)
            if echoSendsToSelf, let user = lastRequest?.credentials.userId {
                let b64 = body.base64EncodedString()
                events.append(
                    #"{"type":"ChatMessage","message_id":"\#(messageId)-echo","from_user":"\#(to)","to_user":"\#(user)","body_hex":"\#(b64)","sent_at":0,"seq":\#(seq + 1),"media_id":""}"#
                )
            }
        }
        return seq
    }

    public func ackDelivered(messageId: String) async throws {
        guard didConnect, !didClose else { throw AppError.connection("not connected") }
        deliveredAcks.append(messageId)
    }

    public func ackRead(messageId: String) async throws {
        guard didConnect, !didClose else { throw AppError.connection("not connected") }
        readAcks.append(messageId)
    }

    public func subscribePresence(contactIds: [String]) async throws {
        presenceSubscriptions = contactIds
    }

    public func enqueue(_ json: String) {
        events.append(json)
    }

    public func enqueueReplaced() {
        enqueue(#"{"type":"ReplacedByNewSession"}"#)
    }

    public func resetEvents(_ next: [String]) {
        events = next
        eventIndex = 0
    }
}
