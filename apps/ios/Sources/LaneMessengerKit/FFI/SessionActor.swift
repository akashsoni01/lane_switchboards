import Foundation

/// Owns the messenger connection off the main actor.
///
/// Production apps inject `LaneFFITransport` (links XCFramework). Tests inject
/// `MockMessengerTransport`.
public actor SessionActor {
    public private(set) var state: ConnectionState = .disconnected
    public private(set) var lastError: AppError?
    public private(set) var pendingMessagesHint: UInt32 = 0

    private let transport: MessengerTransport
    private var pollTask: Task<Void, Never>?
    private var subscribers: [UUID: AsyncStream<LaneEvent>.Continuation] = [:]
    private var autoReconnect = true
    private var replacedLock = false

    public init(transport: MessengerTransport) {
        self.transport = transport
    }

    /// Subscribe to inbound events. Safe to call before or after `connect`.
    public func subscribeEvents() -> AsyncStream<LaneEvent> {
        let id = UUID()
        return AsyncStream { [weak self] continuation in
            let actor = self
            Task { await actor?.addSubscriber(id, continuation) }
            continuation.onTermination = { _ in
                Task { await actor?.removeSubscriber(id) }
            }
        }
    }

    private func addSubscriber(_ id: UUID, _ continuation: AsyncStream<LaneEvent>.Continuation) {
        subscribers[id] = continuation
    }

    public func connect(_ request: ConnectRequest) async throws {
        guard !replacedLock else { throw AppError.replacedByNewSession }
        lastError = nil
        state = .connecting
        LaneLog.session.info(
            "connecting host=\(request.config.host, privacy: .public) port=\(request.config.port)"
        )
        do {
            try await transport.connect(request)
            state = .awaitingLogin
            startPolling()
        } catch let err as AppError {
            state = .disconnected
            lastError = err
            throw err
        } catch {
            let mapped = AppError.connection(error.localizedDescription)
            state = .disconnected
            lastError = mapped
            throw mapped
        }
    }

    public func ping() async throws {
        try await transport.ping()
    }

    public func close() async {
        await stopPolling()
        try? await transport.close()
        state = .disconnected
        LaneLog.session.info("session closed")
    }

    /// After a kick, user must confirm before reconnect is allowed.
    public func acknowledgeReplacement() {
        replacedLock = false
        state = .disconnected
    }

    public func setAutoReconnect(_ enabled: Bool) {
        autoReconnect = enabled
    }

    private func removeSubscriber(_ id: UUID) {
        subscribers.removeValue(forKey: id)
    }

    private func startPolling() {
        pollTask?.cancel()
        pollTask = Task {
            while !Task.isCancelled {
                let json = await transport.pollEvent(timeoutMs: 200)
                if let json {
                    handleRawEvent(json)
                } else {
                    try? await Task.sleep(nanoseconds: 50_000_000)
                }
            }
        }
    }

    private func stopPolling() async {
        pollTask?.cancel()
        pollTask = nil
    }

    private func handleRawEvent(_ json: String) {
        let event = LaneEvent.parse(json: json)
        switch event {
        case .loginAck(_, let ok):
            if ok {
                state = .syncing
            } else {
                state = .disconnected
                lastError = .authFailed("LoginAck.ok=false")
                autoReconnect = false
            }
        case .syncComplete:
            state = .ready
        case .replacedByNewSession:
            state = .replaced
            replacedLock = true
            autoReconnect = false
            lastError = .replacedByNewSession
            LaneLog.session.warning("replaced by new session on this device_id")
        case .disconnected(let reason):
            state = .offline
            lastError = .connection(reason)
            _ = autoReconnect
        default:
            break
        }
        for continuation in subscribers.values {
            continuation.yield(event)
        }
    }
}
