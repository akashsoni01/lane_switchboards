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
    func createGroup(groupId: String) async throws -> UInt64
    func addMember(groupId: String, user: String) async throws -> UInt64
    func removeMember(groupId: String, user: String) async throws -> UInt64
    func leaveGroup(groupId: String) async throws -> UInt64
    func sendGroup(groupId: String, messageId: String, body: Data) async throws
}

public extension MessengerTransport {
    func subscribePresence(contactIds: [String]) async throws {
        _ = contactIds
    }

    func createGroup(groupId: String) async throws -> UInt64 {
        _ = groupId
        throw AppError.notConfigured
    }

    func addMember(groupId: String, user: String) async throws -> UInt64 {
        _ = groupId
        _ = user
        throw AppError.notConfigured
    }

    func removeMember(groupId: String, user: String) async throws -> UInt64 {
        _ = groupId
        _ = user
        throw AppError.notConfigured
    }

    func leaveGroup(groupId: String) async throws -> UInt64 {
        _ = groupId
        throw AppError.notConfigured
    }

    func sendGroup(groupId: String, messageId: String, body: Data) async throws {
        _ = groupId
        _ = messageId
        _ = body
        throw AppError.notConfigured
    }
}

/// In-memory transport for tests and UI previews (no native library required).
public final class MockMessengerTransport: MessengerTransport, @unchecked Sendable {
    public var connectError: AppError?
    public var sendError: AppError?
    public var groupError: AppError?
    public var events: [String] = []
    public var failPing = false
    public var disconnectAfterReady = false
    public var autoAckSends = true
    public var echoSendsToSelf = false
    public private(set) var didConnect = false
    public private(set) var didClose = false
    public private(set) var connectCount = 0
    public private(set) var lastRequest: ConnectRequest?
    public private(set) var pingCount = 0
    public private(set) var sent: [(to: String, messageId: String, body: Data)] = []
    public private(set) var groupSent: [(groupId: String, messageId: String, body: Data)] = []
    public private(set) var deliveredAcks: [String] = []
    public private(set) var readAcks: [String] = []
    public private(set) var presenceSubscriptions: [String] = []
    public private(set) var createdGroups: [String] = []

    private var eventIndex = 0
    private var emittedDisconnect = false
    private var seqCounter: UInt64 = 100
    private var groupVersions: [String: UInt64] = [:]
    private var groupMembers: [String: Set<String>] = [:]
    private var groupAdmins: [String: Set<String>] = [:]

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

    public func createGroup(groupId: String) async throws -> UInt64 {
        try requireReady()
        if let groupError { throw groupError }
        let me = lastRequest?.credentials.userId ?? "me"
        createdGroups.append(groupId)
        groupVersions[groupId] = 1
        groupMembers[groupId] = [me]
        groupAdmins[groupId] = [me]
        enqueueGroupEvent(groupId: groupId, op: 1, actor: me, subject: me, version: 1)
        return 1
    }

    public func addMember(groupId: String, user: String) async throws -> UInt64 {
        try requireReady()
        if let groupError { throw groupError }
        let me = lastRequest?.credentials.userId ?? "me"
        guard groupAdmins[groupId]?.contains(me) == true else {
            throw AppError.protocolError(code: "authz", message: "not admin")
        }
        let version = (groupVersions[groupId] ?? 0) + 1
        groupVersions[groupId] = version
        groupMembers[groupId, default: []].insert(user)
        enqueueGroupEvent(groupId: groupId, op: 2, actor: me, subject: user, version: version)
        return version
    }

    public func removeMember(groupId: String, user: String) async throws -> UInt64 {
        try requireReady()
        if let groupError { throw groupError }
        let me = lastRequest?.credentials.userId ?? "me"
        guard groupAdmins[groupId]?.contains(me) == true else {
            throw AppError.protocolError(code: "authz", message: "not admin")
        }
        let version = (groupVersions[groupId] ?? 0) + 1
        groupVersions[groupId] = version
        groupMembers[groupId]?.remove(user)
        groupAdmins[groupId]?.remove(user)
        enqueueGroupEvent(groupId: groupId, op: 3, actor: me, subject: user, version: version)
        return version
    }

    public func leaveGroup(groupId: String) async throws -> UInt64 {
        try requireReady()
        if let groupError { throw groupError }
        let me = lastRequest?.credentials.userId ?? "me"
        guard groupMembers[groupId]?.contains(me) == true else {
            throw AppError.protocolError(code: "authz", message: "not a member")
        }
        let version = (groupVersions[groupId] ?? 0) + 1
        groupVersions[groupId] = version
        groupMembers[groupId]?.remove(me)
        groupAdmins[groupId]?.remove(me)
        enqueueGroupEvent(groupId: groupId, op: 4, actor: me, subject: me, version: version)
        return version
    }

    public func sendGroup(groupId: String, messageId: String, body: Data) async throws {
        try requireReady()
        if let sendError { throw sendError }
        let me = lastRequest?.credentials.userId ?? "me"
        guard groupMembers[groupId]?.contains(me) == true else {
            throw AppError.protocolError(code: "authz", message: "not a member")
        }
        groupSent.append((groupId, messageId, body))
        seqCounter += 1
        let seq = seqCounter
        if autoAckSends {
            events.append(#"{"type":"ServerAck","message_id":"\#(messageId)","seq":\#(seq)}"#)
            let members = groupMembers[groupId] ?? []
            let count = UInt32(members.count)
            events.append(
                #"{"type":"GroupAckSummary","message_id":"\#(messageId)","group_id":"\#(groupId)","member_count":\#(count),"delivered_count":\#(max(0, Int(count) - 1)),"read_count":0}"#
            )
        }
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

    private func requireReady() throws {
        guard didConnect, !didClose else { throw AppError.connection("not connected") }
    }

    private func enqueueGroupEvent(
        groupId: String,
        op: Int,
        actor: String,
        subject: String,
        version: UInt64
    ) {
        events.append(
            #"{"type":"GroupEvent","group_id":"\#(groupId)","op":\#(op),"actor_user":"\#(actor)","subject_user":"\#(subject)","version":\#(version)}"#
        )
    }
}
