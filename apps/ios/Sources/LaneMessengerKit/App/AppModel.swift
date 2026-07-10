import Foundation
import Observation

/// Root app state: auth, session, store, chat (I4), presence (I5).
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
    public private(set) var contacts: [Contact] = []
    public private(set) var threadMessages: [StoredMessage] = []
    public var selectedPeer: String?
    public var composeText: String = ""
    public var errorBanner: String?
    public var showReplacedAlert = false
    public var showUpgradeAlert = false
    public var showNewChat = false
    public var isBusy = false
    public var isSending = false

    public var config: AppConfig
    public let credentialsStore: CredentialStore
    public let authService: AuthService
    public let session: SessionActor
    public let store: LocalStore
    public var chat: ChatService { ChatService(store: store, session: session) }

    private var eventsTask: Task<Void, Never>?
    private var stateTask: Task<Void, Never>?
    private var draftSaveTask: Task<Void, Never>?

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
                refreshContacts()
                await seedDebugContactsIfNeeded()
                await subscribePresenceRoster()
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
            refreshContacts()
            await seedDebugContactsIfNeeded()
            await subscribePresenceRoster()
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
        contacts = []
        threadMessages = []
        selectedPeer = nil
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
        let foreground = phase == "active"
        await session.setAppInForeground(foreground)
        connectionState = await session.state
    }

    public func refreshInbox() {
        inbox = (try? store.conversations()) ?? []
    }

    public func refreshContacts() {
        contacts = (try? store.contacts()) ?? []
    }

    public func openChat(peer: String) async {
        selectedPeer = peer
        try? store.ensureConversation(id: peer, title: peer)
        if let draft = try? store.conversations().first(where: { $0.id == peer })?.draft {
            composeText = draft
        } else {
            composeText = ""
        }
        try? await chat.openThread(peer: peer)
        reloadThread()
        refreshInbox()
    }

    public func closeChat() {
        selectedPeer = nil
        composeText = ""
        threadMessages = []
    }

    public func updateDraft(_ text: String) {
        composeText = text
        guard let peer = selectedPeer else { return }
        draftSaveTask?.cancel()
        draftSaveTask = Task {
            try? await Task.sleep(nanoseconds: 300_000_000)
            guard !Task.isCancelled else { return }
            try? store.setDraft(conversationId: peer, draft: text)
        }
    }

    public func sendCurrentCompose() async {
        guard let peer = selectedPeer, let me = credentials?.userId else { return }
        let text = composeText
        guard !text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return }
        isSending = true
        defer { isSending = false }
        do {
            _ = try await chat.sendText(to: peer, body: text, fromUser: me)
            composeText = ""
            reloadThread()
            refreshInbox()
        } catch let err as AppError {
            errorBanner = err.errorDescription
            reloadThread()
        } catch {
            errorBanner = error.localizedDescription
            reloadThread()
        }
    }

    public func retryMessage(_ message: StoredMessage) async {
        guard let peer = selectedPeer else { return }
        do {
            try await chat.retryFailed(message: message, to: peer)
            reloadThread()
        } catch let err as AppError {
            errorBanner = err.errorDescription
        } catch {
            errorBanner = error.localizedDescription
        }
    }

    public func startChat(with userId: String) async {
        let peer = userId.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !peer.isEmpty else { return }
        showNewChat = false
        try? store.upsertContact(Contact(userId: peer))
        try? store.ensureConversation(id: peer, title: peer)
        refreshContacts()
        await openChat(peer: peer)
        await subscribePresenceRoster()
    }

    public func reloadThread() {
        guard let peer = selectedPeer else {
            threadMessages = []
            return
        }
        threadMessages = (try? store.messages(conversationId: peer, limit: 500)) ?? []
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
        let me = credentials?.userId ?? ""

        switch event {
        case .replacedByNewSession:
            showReplacedAlert = true
            errorBanner = AppError.replacedByNewSession.errorDescription
        case .loginAck(_, false, _, let error):
            errorBanner = AppError.authFailed(error).errorDescription
        case .protocolError(let code, let detail):
            let mapped = ProtocolErrorCode(raw: code)
            errorBanner = detail.isEmpty ? mapped.userMessage : detail
            if mapped == .unsupportedVersion { showUpgradeAlert = true }
            if mapped == .replacedByNewSession { showReplacedAlert = true }
        case .chatMessage(let messageId, let fromUser, let toUser, let body, let seq, let mediaId, let sentAt):
            let peer = fromUser == me ? toUser : fromUser
            let direction: MessageDirection = fromUser == me ? .outbound : .inbound
            let created = sentAt > 0
                ? Date(timeIntervalSince1970: TimeInterval(sentAt))
                : Date()
            let stored = StoredMessage(
                messageId: messageId,
                conversationId: peer.isEmpty ? fromUser : peer,
                direction: direction,
                body: body,
                mediaId: mediaId,
                seq: seq,
                status: direction == .inbound ? .delivered : .sent,
                createdAt: created
            )
            _ = try? store.upsertMessage(stored)
            if selectedPeer == stored.conversationId, direction == .inbound {
                try? await session.ackDelivered(messageId: messageId)
                try? await session.ackRead(messageId: messageId)
                try? store.updateStatus(messageId: messageId, status: .read, seq: nil)
                try? store.markConversationRead(conversationId: stored.conversationId)
            }
            refreshInbox()
            if selectedPeer == stored.conversationId { reloadThread() }
        case .serverAck(let messageId, let seq):
            try? store.updateStatus(messageId: messageId, status: .sent, seq: seq)
            if selectedPeer != nil { reloadThread() }
        case .deliveredAck(let messageId, _):
            try? store.updateStatus(messageId: messageId, status: .delivered, seq: nil)
            if selectedPeer != nil { reloadThread() }
        case .readAck(let messageId, _):
            try? store.updateStatus(messageId: messageId, status: .read, seq: nil)
            if selectedPeer != nil { reloadThread() }
        case .presence(let userId, let kind, let lastSeen):
            // Never invent last-seen: only store when server provided a value.
            let existing = try? store.contact(userId: userId)
            let presence = PresenceKind(rawValue: kind) ?? .unavailable
            let seen: Date? = {
                if let lastSeen, lastSeen > 0 {
                    return Date(timeIntervalSince1970: TimeInterval(lastSeen))
                }
                return existing?.lastSeen
            }()
            let contact = Contact(
                userId: userId,
                displayName: existing?.displayName ?? userId,
                presence: presence,
                lastSeen: presence == .lastSeen || presence == .unavailable ? seen : existing?.lastSeen
            )
            // Privacy: if server omitted last_seen on LAST_SEEN, keep prior only; do not invent.
            if presence == .lastSeen, lastSeen == nil || lastSeen == 0 {
                let c = Contact(
                    userId: userId,
                    displayName: contact.displayName,
                    presence: .lastSeen,
                    lastSeen: existing?.lastSeen
                )
                try? store.upsertContact(c)
            } else {
                try? store.upsertContact(contact)
            }
            refreshContacts()
            refreshInbox()
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

    private func subscribePresenceRoster() async {
        let ids = ((try? store.contacts()) ?? []).map(\.userId)
        guard !ids.isEmpty else { return }
        try? await session.subscribePresence(contactIds: ids)
    }

    private func seedDebugContactsIfNeeded() async {
        #if DEBUG
        let existing = (try? store.contacts()) ?? []
        guard existing.isEmpty, let me = credentials?.userId else { return }
        // Bootstrap peers for local gateway demos (messenger_demo uses alice/bob).
        let seeds = ["alice", "bob", "carol"].filter { $0 != me }
        for user in seeds {
            try? store.upsertContact(Contact(userId: user, presence: .unavailable))
        }
        refreshContacts()
        #endif
    }
}
