import Foundation
import Observation

/// Root app state for splash → login → signed-in shell (Phase I1).
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
    public var errorBanner: String?
    public var showReplacedAlert = false
    public var isBusy = false

    public var config: AppConfig
    public let credentialsStore: CredentialStore
    public let authService: AuthService
    public let session: SessionActor

    private var eventsTask: Task<Void, Never>?

    public init(
        config: AppConfig = .default,
        credentialsStore: CredentialStore = CredentialStore(),
        authService: AuthService? = nil,
        transport: MessengerTransport? = nil
    ) {
        self.config = config
        self.credentialsStore = credentialsStore
        let store = credentialsStore
        #if DEBUG
        self.authService = authService ?? DebugAuthService {
            try store.deviceId()
        }
        #else
        // Production must inject HttpAuthService with a real identity URL.
        self.authService = authService ?? HttpAuthService(
            endpoint: URL(string: "https://identity.invalid/v1/login")!,
            deviceIdProvider: { try store.deviceId() }
        )
        #endif
        let transport = transport ?? MockMessengerTransport()
        self.session = SessionActor(transport: transport)
    }

    /// Cold start: restore Keychain session if present.
    public func bootstrap() async {
        route = .splash
        errorBanner = nil
        do {
            if let creds = try credentialsStore.load() {
                credentials = creds
                try await connect(with: creds)
                route = .home
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
        do {
            let creds = try await authService.login(userId: userId, secret: secret)
            try credentialsStore.save(creds)
            credentials = creds
            try await connect(with: creds)
            route = .home
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
        credentials = nil
        connectionState = .disconnected
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

    private func connect(with creds: AuthCredentials) async throws {
        stopEvents()
        startEvents()
        let request = ConnectRequest(config: config, credentials: creds)
        try await session.connect(request)
        connectionState = await session.state
        // Wait briefly for LoginAck / SyncComplete in mock & real FFI.
        for _ in 0..<50 {
            let state = await session.state
            connectionState = state
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
    }

    private func stopEvents() {
        eventsTask?.cancel()
        eventsTask = nil
    }

    private func handle(_ event: LaneEvent) async {
        connectionState = await session.state
        switch event {
        case .replacedByNewSession:
            showReplacedAlert = true
            errorBanner = AppError.replacedByNewSession.errorDescription
        case .loginAck(_, false):
            errorBanner = AppError.authFailed("").errorDescription
        default:
            break
        }
    }
}
