import Foundation

/// Owns the messenger connection off the main actor.
///
/// Handles connect → LoginAck → SyncComplete → Ready, client pings, and
/// exponential-backoff reconnect (unless paused by protocol / kick).
public actor SessionActor {
    public private(set) var state: ConnectionState = .disconnected
    public private(set) var lastError: AppError?
    public private(set) var pendingMessagesHint: UInt32 = 0
    public private(set) var lastPingRttMs: UInt64?
    public private(set) var reconnectAttempt: UInt32 = 0

    private let transport: MessengerTransport
    private var policy: ReconnectPolicy
    private var pollTask: Task<Void, Never>?
    private var pingTask: Task<Void, Never>?
    private var reconnectTask: Task<Void, Never>?
    private var subscribers: [UUID: AsyncStream<LaneEvent>.Continuation] = [:]
    private var autoReconnect = true
    private var replacedLock = false
    private var pauseReconnect = false
    private var lastRequest: ConnectRequest?
    private var resumeSeqProvider: (@Sendable () async -> UInt64)?
    private var appInForeground = true

    public init(
        transport: MessengerTransport,
        policy: ReconnectPolicy = .default
    ) {
        self.transport = transport
        self.policy = policy
    }

    public func setReconnectPolicy(_ policy: ReconnectPolicy) {
        self.policy = policy
    }

    /// Called before each connect/reconnect to stamp `resume_after_seq`.
    public func setResumeSeqProvider(_ provider: (@Sendable () async -> UInt64)?) {
        resumeSeqProvider = provider
    }

    public func setAppInForeground(_ foreground: Bool) {
        let wasBackground = !appInForeground
        appInForeground = foreground
        if foreground, wasBackground, autoReconnect, !replacedLock, !pauseReconnect {
            if state == .offline || state == .disconnected {
                scheduleReconnect(reason: "foreground")
            }
        }
        if !foreground {
            // Expect socket drop in background; stop ping noise.
            pingTask?.cancel()
            pingTask = nil
        } else if state == .ready {
            startPingLoop()
        }
    }

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

    private func removeSubscriber(_ id: UUID) {
        subscribers.removeValue(forKey: id)
    }

    public func connect(_ request: ConnectRequest) async throws {
        guard !replacedLock else { throw AppError.replacedByNewSession }
        reconnectTask?.cancel()
        reconnectTask = nil
        reconnectAttempt = 0
        pauseReconnect = false
        autoReconnect = true
        try await performConnect(request)
    }

    public func ping() async throws {
        let start = Date()
        try await transport.ping()
        let ms = max(0, Date().timeIntervalSince(start) * 1000)
        lastPingRttMs = UInt64(ms.rounded())
    }

    public func sendChat(to: String, messageId: String, body: Data) async throws -> UInt64 {
        guard state == .ready || state == .syncing || state == .awaitingLogin else {
            throw AppError.connection("not ready")
        }
        return try await transport.sendChat(to: to, messageId: messageId, body: body)
    }

    public func ackDelivered(messageId: String) async throws {
        try await transport.ackDelivered(messageId: messageId)
    }

    public func ackRead(messageId: String) async throws {
        try await transport.ackRead(messageId: messageId)
    }

    public func subscribePresence(contactIds: [String]) async throws {
        try await transport.subscribePresence(contactIds: contactIds)
    }

    public func close() async {
        autoReconnect = false
        reconnectTask?.cancel()
        reconnectTask = nil
        await stopWorkers()
        try? await transport.close()
        state = .disconnected
        LaneLog.session.info("session closed")
    }

    public func acknowledgeReplacement() {
        replacedLock = false
        pauseReconnect = false
        state = .disconnected
    }

    public func setAutoReconnect(_ enabled: Bool) {
        autoReconnect = enabled
    }

    /// Clear `UNSUPPORTED_VERSION` / auth pause so user can retry after upgrade/re-login.
    public func clearReconnectPause() {
        pauseReconnect = false
    }

    private func performConnect(_ request: ConnectRequest) async throws {
        var req = request
        if let provider = resumeSeqProvider {
            req.resumeAfterSeq = await provider()
        }
        lastRequest = req
        lastError = nil
        state = .connecting
        pendingMessagesHint = 0
        LaneLog.session.info(
            "connecting host=\(req.config.host, privacy: .public) port=\(req.config.port) resume=\(req.resumeAfterSeq)"
        )
        do {
            try await transport.connect(req)
            state = .awaitingLogin
            startPolling()
            if appInForeground {
                startPingLoop()
            }
        } catch let err as AppError {
            state = .offline
            lastError = err
            scheduleReconnect(reason: "connect error")
            throw err
        } catch {
            let mapped = AppError.connection(error.localizedDescription)
            state = .offline
            lastError = mapped
            scheduleReconnect(reason: "connect error")
            throw mapped
        }
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

    private func startPingLoop() {
        pingTask?.cancel()
        let interval = lastRequest?.config.pingIntervalSecs ?? 30
        pingTask = Task {
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: interval * 1_000_000_000)
                guard !Task.isCancelled else { break }
                guard state == .ready, appInForeground else { continue }
                do {
                    try await ping()
                    #if DEBUG
                    if let rtt = lastPingRttMs {
                        LaneLog.session.debug("ping rtt=\(rtt)ms")
                    }
                    #endif
                } catch {
                    LaneLog.session.error("ping failed")
                    handleTransportLoss(reason: "ping failed")
                }
            }
        }
    }

    private func stopWorkers() async {
        pollTask?.cancel()
        pingTask?.cancel()
        pollTask = nil
        pingTask = nil
    }

    private func handleTransportLoss(reason: String) {
        guard state != .replaced else { return }
        state = .offline
        lastError = .connection(reason)
        emit(.disconnected(reason: reason))
        scheduleReconnect(reason: reason)
    }

    private func scheduleReconnect(reason: String) {
        guard autoReconnect, !replacedLock, !pauseReconnect, appInForeground else {
            LaneLog.session.info("reconnect skipped reason=\(reason, privacy: .public)")
            return
        }
        guard lastRequest != nil else { return }
        reconnectTask?.cancel()
        reconnectTask = Task {
            reconnectAttempt += 1
            let attempt = reconnectAttempt
            guard policy.shouldRetry(attempt: attempt) else {
                LaneLog.session.error("reconnect attempts exhausted")
                return
            }
            let delay = policy.delayMs(forAttempt: attempt)
            LaneLog.session.info("reconnect in \(delay)ms attempt=\(attempt)")
            try? await Task.sleep(nanoseconds: delay * 1_000_000)
            guard !Task.isCancelled else { return }
            guard autoReconnect, !replacedLock, !pauseReconnect, appInForeground else { return }
            guard var req = lastRequest else { return }
            if let provider = resumeSeqProvider {
                req.resumeAfterSeq = await provider()
            }
            await stopWorkers()
            try? await transport.close()
            do {
                try await performConnect(req)
            } catch {
                // performConnect already scheduled another attempt on failure
            }
        }
    }

    private func handleRawEvent(_ json: String) {
        let event = LaneEvent.parse(json: json)
        switch event {
        case .loginAck(_, let ok, let pending, let error):
            pendingMessagesHint = pending
            if ok {
                state = .syncing
                reconnectAttempt = 0
            } else {
                state = .disconnected
                lastError = .authFailed(error.isEmpty ? "LoginAck.ok=false" : error)
                autoReconnect = false
                pauseReconnect = true
            }
        case .syncComplete:
            state = .ready
            reconnectAttempt = 0
            if appInForeground { startPingLoop() }
        case .protocolError(let code, let detail):
            let mapped = ProtocolErrorCode(raw: code)
            lastError = .protocolError(code: String(code), message: detail.isEmpty ? mapped.userMessage : detail)
            if mapped == .replacedByNewSession {
                state = .replaced
                replacedLock = true
                autoReconnect = false
            } else if mapped.pausesReconnect {
                pauseReconnect = true
                autoReconnect = false
                state = .disconnected
            } else {
                handleTransportLoss(reason: detail.isEmpty ? mapped.userMessage : detail)
                return
            }
        case .replacedByNewSession:
            state = .replaced
            replacedLock = true
            autoReconnect = false
            pauseReconnect = true
            lastError = .replacedByNewSession
            LaneLog.session.warning("replaced by new session on this device_id")
        case .disconnected(let reason):
            handleTransportLoss(reason: reason)
            return
        default:
            break
        }
        emit(event)
    }

    private func emit(_ event: LaneEvent) {
        for continuation in subscribers.values {
            continuation.yield(event)
        }
    }
}
