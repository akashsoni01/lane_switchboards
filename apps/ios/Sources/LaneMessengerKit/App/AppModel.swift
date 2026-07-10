import Foundation
import Observation

/// Root app state: auth (I1) + session lifecycle (I2) + local store (I3).
@MainActor
@Observable
public final class AppModel {
    public enum Route: Equatable {
        case splash
        case login
        case home
    }

    public private(set) var route: Route = .splash
    public private(set) var connectionState: ConnectionState = .disconnected
    public private(set) var credentials: AuthCredentials?
    public private(set) var pendingMessagesHint: UInt32 = 0
    public private(set) var lastPingRttMs: UInt64?
    public private(set) var inbox: [Conversation] = []
    public var errorBanner: String?
    public var showReplacedAlert = false
    public var showUpgradeAlert = false
    public var isBusy = false

    public var config: AppConfig
    public let credentialsStore: CredentialStore
    public let authService: AuthService
    public let session: SessionActor
    public let store: LocalStore

    private var eventsTask: Task<Void, Never>?
    private var stateTask: Task<Void, Never>?

    public init(
        config: AppConfig = .default,
        credentialsStore: CredentialStore = CredentialStore(),
        authService: AuthService? = nil,
        transport: MessengerTransport? = nil,
        store: LocalStore? = nil
    ) {
        self.config = config
        self.credentialsStore = credentialsStore
        let storeRef = credentialsStore
        #if DEBUG
        self.authService = authService ?? DebugAuthService {
            try storeRef.deviceId()
        }
        #else
        self.authService = authService ?? HttpAuthService(
            endpoint: URL(string: "https://identity.invalid/v1/login")!,
            deviceIdProvider: { try storeRef.deviceId() }
        )
        #endif
        let transport = transport ?? MockMessengerTransport()
        self.session = SessionActor(transport: transport)
        if let store {
            self.store = store
        } else {
            self.store = (try? SQLiteLocalStore(path: SQLiteLocalStore.defaultPath()))
                ?? InMemoryLocalStore()
        }
    }

    public func bootstrap() async {
        route = .splash
        errorBanner = nil
        await configureSession()
        do {
            if let creds = try credentialsStore.load() {
                credentials = creds
                try await connect(with: creds)
                route = .home
                refreshInbox()
            } else {
                route = .login
            }
        } catch let err as AppError where err == .replacedByNewSession {
            showReplacedAlert = true
            route = .login
        } catch {
            LaneLog.ui.error("bootstrap failed: \(String(describing: error), privacy: .public)")
            errorBanner = (error as? AppError)?.errorDescription ?? "Could not restore session."
            route = .login
        }
    }

    public func signIn(userId: String, secret: String) async {
        isBusy = true
        errorBanner = nil
        defer { isBusy = false }
        await configureSession()
        do {
            let creds = try await authService.login(userId: userId, secret: secret)
            try credentialsStore.save(creds)
            credentials = creds
            try await connect(with: creds)
            route = .home
            refreshInbox()
        } catch let err as AppError {
            errorBanner = err.errorDescription
            if err == .replacedByNewSession { showReplacedAlert = true }
        } catch {
            errorBanner = error.localizedDescription
        }
    }

    public func signOut() async {
        await session.close()
        try? credentialsStore.clearSession()
        try? store.wipeUserData()
        credentials = nil
        connectionState = .disconnected
        pendingMessagesHint = 0
        inbox = []
        stopEvents()
        route = .login
    }

    public func acknowledgeDeviceReplaced() async {
        await session.acknowledgeReplacement()
        showReplacedAlert = false
        try? credentialsStore.clearSession()
        credentials = nil
        route = .login
    }

    public func handleScenePhase(_ phase: String) async {
        // "active" | "inactive" | "background"
        let foreground = phase == "active"
        await session.setAppInForeground(foreground)
        connectionState = await session.state
    }

    public func refreshInbox() {
        inbox = (try? store.conversations()) ?? []
    }

    private func configureSession() async {
        let local = store
        await session.setResumeSeqProvider {
            (try? local.resumeAfterSeq()) ?? 0
        }
        await session.setAutoReconnect(true)
    }

    private func connect(with creds: AuthCredentials) async throws {
        stopEvents()
        startEvents()
        let resume = (try? store.resumeAfterSeq()) ?? 0
        let request = ConnectRequest(
            config: config,
            credentials: creds,
            resumeAfterSeq: resume
        )
        try await session.connect(request)
        connectionState = await session.state
        pendingMessagesHint = await session.pendingMessagesHint
        for _ in 0..<50 {
            let state = await session.state
            connectionState = state
            pendingMessagesHint = await session.pendingMessagesHint
            lastPingRttMs = await session.lastPingRttMs
            if state == .ready { return }
            if state == .replaced { throw AppError.replacedByNewSession }
            if let err = await session.lastError, state == .disconnected {
                throw err
            }
            try await Task.sleep(nanoseconds: 20_000_000)
        }
        let final = await session.state
        connectionState = final
        if final != .ready, final != .syncing, final != .awaitingLogin {
            throw await session.lastError ?? AppError.connection("login timed out")
        }
    }

    private func startEvents() {
        eventsTask = Task { [weak self] in
            guard let self else { return }
            let stream = await self.session.subscribeEvents()
            for await event in stream {
                await self.handle(event)
            }
        }
        stateTask = Task { [weak self] in
            while !Task.isCancelled {
                guard let self else { return }
                self.connectionState = await self.session.state
                self.pendingMessagesHint = await self.session.pendingMessagesHint
                self.lastPingRttMs = await self.session.lastPingRttMs
                try? await Task.sleep(nanoseconds: 200_000_000)
            }
        }
    }

    private func stopEvents() {
        eventsTask?.cancel()
        stateTask?.cancel()
        eventsTask = nil
        stateTask = nil
    }

    private func handle(_ event: LaneEvent) async {
        connectionState = await session.state
        pendingMessagesHint = await session.pendingMessagesHint
        switch event {
        case .replacedByNewSession:
            showReplacedAlert = true
            errorBanner = AppError.replacedByNewSession.errorDescription
        case .loginAck(_, false, _, let error):
            errorBanner = AppError.authFailed(error).errorDescription
        case .protocolError(let code, let detail):
            let mapped = ProtocolErrorCode(raw: code)
            errorBanner = detail.isEmpty ? mapped.userMessage : detail
            if mapped == .unsupportedVersion {
                showUpgradeAlert = true
            }
            if mapped == .replacedByNewSession {
                showReplacedAlert = true
            }
        case .chatMessage(let messageId, let fromUser, let seq):
            let stored = StoredMessage(
                messageId: messageId,
                conversationId: fromUser,
                direction: .inbound,
                body: "",
                seq: seq,
                status: .delivered
            )
            _ = try? store.upsertMessage(stored)
            refreshInbox()
        case .serverAck(let messageId, let seq):
            try? store.updateStatus(messageId: messageId, status: .sent, seq: seq)
        case .deliveredAck(let messageId):
            try? store.updateStatus(messageId: messageId, status: .delivered, seq: nil)
        case .readAck(let messageId):
            try? store.updateStatus(messageId: messageId, status: .read, seq: nil)
        case .syncComplete(let latestSeq, _):
            if latestSeq > 0 {
                let current = (try? store.resumeAfterSeq()) ?? 0
                if latestSeq > current {
                    try? store.setResumeAfterSeq(latestSeq)
                }
            }
        default:
            break
        }
    }
}
