import Foundation
import Observation

/// Root app state: auth, session, store, chat, presence, groups (I6).
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
    public private(set) var selectedGroup: GroupInfo?
    public var selectedPeer: String?
    public var composeText: String = ""
    public var errorBanner: String?
    public var showReplacedAlert = false
    public var showUpgradeAlert = false
    public var showNewChat = false
    public var showCreateGroup = false
    public var showGroupInfo = false
    public var showAttachMenu = false
    public var showFileImporter = false
    public var previewMediaId: String?
    public var isBusy = false
    public var isSending = false
    public private(set) var mediaTransfers: [String: MediaTransferProgress] = [:]

    public var config: AppConfig
    public let credentialsStore: CredentialStore
    public let authService: AuthService
    public let session: SessionActor
    public let store: LocalStore
    public let mediaBlobs: MediaBlobStore
    public let media: MediaService
    public let e2ee: E2eeService
    public var chat: ChatService { ChatService(store: store, session: session, e2ee: e2ee) }
    public var groups: GroupService { GroupService(store: store, session: session, e2ee: e2ee) }

    public var showSafetyNumber = false
    public var safetyNumberText: String = ""
    public var showE2eeSettings = false
    public var e2eeExportPassphrase = ""
    public var e2eeImportPassphrase = ""
    public private(set) var e2eeReady = false
    public private(set) var e2eeLocalIdentity = ""
    public private(set) var isAppInBackground = false
    public private(set) var badgeCount = 0
    public private(set) var pushTokenHex: String?
    public private(set) var notificationsAuthorized = false

    public let notifications: NotificationPresenting
    public let pushRegistrar: PushTokenRegistering

    private var eventsTask: Task<Void, Never>?
    private var stateTask: Task<Void, Never>?
    private var draftSaveTask: Task<Void, Never>?
    private var mediaPollTask: Task<Void, Never>?
    /// Pending deep link / notification open before home is ready.
    private var pendingOpenConversationId: String?

    public init(
        config: AppConfig = .default,
        credentialsStore: CredentialStore = CredentialStore(),
        authService: AuthService? = nil,
        transport: MessengerTransport? = nil,
        store: LocalStore? = nil,
        mediaBlobs: MediaBlobStore? = nil,
        e2eeEnabled: Bool = FeatureFlags.e2eeEnabled,
        notifications: NotificationPresenting? = nil,
        pushRegistrar: PushTokenRegistering? = nil
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
        let blobs = mediaBlobs ?? ((try? MediaBlobStore()) ?? (try! MediaBlobStore.inMemoryTemp()))
        self.mediaBlobs = blobs
        self.media = MediaService(store: self.store, blobs: blobs, session: self.session)
        self.e2ee = E2eeService(session: self.session, enabled: e2eeEnabled)
        #if canImport(UserNotifications)
        self.notifications = notifications ?? SystemNotificationPresenter()
        #else
        self.notifications = notifications ?? RecordingNotificationPresenter()
        #endif
        self.pushRegistrar = pushRegistrar ?? StubPushTokenRegistrar()
        self.notifications.previewPolicy = .default
    }

    public var selectedIsGroup: Bool {
        guard let peer = selectedPeer else { return false }
        return Conversation.groupId(fromConversationId: peer) != nil
    }

    public var selectedGroupId: String? {
        guard let peer = selectedPeer else { return nil }
        return Conversation.groupId(fromConversationId: peer)
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
                await requestNotificationPermission()
                await flushPendingOpen()
                await refreshBadge()
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
            await requestNotificationPermission()
            if let token = pushTokenHex {
                try? await pushRegistrar.register(
                    deviceTokenHex: token,
                    userId: creds.userId,
                    deviceId: creds.deviceId
                )
            }
            await flushPendingOpen()
            await refreshBadge()
        } catch let err as AppError {
            errorBanner = err.errorDescription
            if err == .replacedByNewSession { showReplacedAlert = true }
        } catch {
            errorBanner = error.localizedDescription
        }
    }

    public func signOut() async {
        if let creds = credentials {
            try? await pushRegistrar.unregister(userId: creds.userId, deviceId: creds.deviceId)
        }
        await session.close()
        try? credentialsStore.clearSession()
        try? store.wipeUserData()
        try? mediaBlobs.wipeAll()
        credentials = nil
        connectionState = .disconnected
        pendingMessagesHint = 0
        inbox = []
        contacts = []
        threadMessages = []
        selectedPeer = nil
        selectedGroup = nil
        mediaTransfers = [:]
        badgeCount = 0
        await notifications.setBadge(0)
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
        isAppInBackground = !foreground
        await session.setAppInForeground(foreground)
        connectionState = await session.state
        if foreground {
            await refreshBadge()
            await flushPendingOpen()
        }
    }

    public func refreshInbox() {
        inbox = (try? store.conversations()) ?? []
        Task { await refreshBadge() }
    }

    public func refreshBadge() async {
        let total = UnreadBadge.total(from: (try? store.conversations()) ?? inbox)
        badgeCount = total
        await notifications.setBadge(total)
    }

    /// Cold start / tap from APNs or local notification.
    public func handleNotificationOpen(userInfo: [AnyHashable: Any]) async {
        guard let payload = PushPayload.parse(userInfo: userInfo) else { return }
        await openFromPush(payload)
    }

    public func handleDeepLink(_ url: URL) async {
        guard let payload = PushPayload.parse(url: url) else { return }
        await openFromPush(payload)
    }

    public func openFromPush(_ payload: PushPayload) async {
        if route != .home {
            pendingOpenConversationId = payload.conversationId
            return
        }
        await openChat(peer: payload.conversationId)
    }

    public func didRegisterForRemoteNotifications(deviceToken: Data) async {
        let hex = PushTokenFormat.hex(deviceToken)
        pushTokenHex = hex
        guard let creds = credentials else { return }
        do {
            try await pushRegistrar.register(
                deviceTokenHex: hex,
                userId: creds.userId,
                deviceId: creds.deviceId
            )
        } catch {
            LaneLog.ui.error("push token register failed")
        }
    }

    public func requestNotificationPermission() async {
        notificationsAuthorized = await notifications.requestAuthorization()
    }

    private func flushPendingOpen() async {
        guard route == .home, let id = pendingOpenConversationId else { return }
        pendingOpenConversationId = nil
        await openChat(peer: id)
    }

    private func maybeNotifyInbound(
        conversationId: String,
        messageId: String,
        senderId: String,
        preview: String,
        encrypted: Bool
    ) async {
        guard isAppInBackground else { return }
        // Don't notify for the thread the user already has open (rare in background).
        if selectedPeer == conversationId { return }
        let payload = PushPayload(
            conversationId: conversationId,
            messageId: messageId,
            senderId: senderId,
            preview: preview,
            encrypted: encrypted
        )
        await notifications.presentLocal(payload: payload)
        await refreshBadge()
    }

    public func refreshContacts() {
        contacts = (try? store.contacts()) ?? []
    }

    public func refreshSelectedGroup() {
        guard let gid = selectedGroupId else {
            selectedGroup = nil
            return
        }
        selectedGroup = try? store.group(id: gid)
    }

    public func openChat(peer: String) async {
        selectedPeer = peer
        let isGroup = Conversation.groupId(fromConversationId: peer) != nil
        try? store.ensureConversation(id: peer, title: peer, isGroup: isGroup)
        if let draft = try? store.conversations().first(where: { $0.id == peer })?.draft {
            composeText = draft
        } else {
            composeText = ""
        }
        try? await chat.openThread(peer: peer)
        reloadThread()
        refreshInbox()
        refreshSelectedGroup()
        await notifications.clearNotifications(forConversationId: peer)
        await refreshBadge()
    }

    public func closeChat() {
        selectedPeer = nil
        selectedGroup = nil
        composeText = ""
        threadMessages = []
        showGroupInfo = false
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
            if let groupId = Conversation.groupId(fromConversationId: peer) {
                _ = try await groups.sendText(groupId: groupId, body: text, fromUser: me)
            } else {
                _ = try await chat.sendText(to: peer, body: text, fromUser: me)
            }
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
            if !message.mediaId.isEmpty {
                try await media.retryFailedAttachment(message)
            } else if let groupId = Conversation.groupId(fromConversationId: peer) {
                try await groups.retryFailed(message: message, groupId: groupId)
            } else {
                try await chat.retryFailed(message: message, to: peer)
            }
            await refreshMediaTransfers()
            reloadThread()
        } catch let err as AppError {
            errorBanner = err.errorDescription
        } catch {
            errorBanner = error.localizedDescription
        }
    }

    public func sendAttachment(data: Data, fileName: String, mimeType: String) async {
        guard let peer = selectedPeer, let me = credentials?.userId else { return }
        isSending = true
        defer { isSending = false }
        do {
            _ = try await media.sendAttachment(
                conversationId: peer,
                data: data,
                fileName: fileName,
                mimeType: mimeType,
                caption: composeText.trimmingCharacters(in: .whitespacesAndNewlines),
                fromUser: me
            )
            composeText = ""
            await refreshMediaTransfers()
            reloadThread()
            refreshInbox()
        } catch let err as AppError {
            errorBanner = err.errorDescription
            await refreshMediaTransfers()
            reloadThread()
        } catch {
            errorBanner = error.localizedDescription
            reloadThread()
        }
    }

    public func openMedia(_ mediaId: String) async {
        do {
            _ = try await media.ensureDownloaded(mediaId: mediaId)
            await refreshMediaTransfers()
            previewMediaId = mediaId
        } catch let err as AppError {
            errorBanner = err.errorDescription
            await refreshMediaTransfers()
        } catch {
            errorBanner = error.localizedDescription
        }
    }

    public func mediaMeta(_ mediaId: String) -> MediaBlobMeta? {
        try? mediaBlobs.meta(for: mediaId)
    }

    public func mediaFileURL(_ mediaId: String) -> URL? {
        guard mediaBlobs.hasComplete(mediaId) else { return nil }
        return mediaBlobs.blobURL(for: mediaId)
    }

    public func refreshMediaTransfers() async {
        mediaTransfers = await media.transfers
    }

    public func openSafetyNumber(forPeer peer: String) async {
        // Ensure we have a peer identity for comparison (demo: use peer id hash if unknown).
        if (try? await e2ee.safetyNumber(forPeer: peer)) == nil {
            let synthetic = E2eeSafety.number(localIdentityB64: peer, remoteIdentityB64: peer)
            try? await e2ee.rememberPeerIdentity(userId: peer, identityKey: synthetic)
        }
        if let n = try? await e2ee.safetyNumber(forPeer: peer) {
            safetyNumberText = n
            showSafetyNumber = true
        } else {
            errorBanner = "Safety number unavailable until keys are exchanged."
        }
    }

    public func exportE2eePickle() async -> Data? {
        let pass = e2eeExportPassphrase
        guard !pass.isEmpty else {
            errorBanner = "Enter an export passphrase."
            return nil
        }
        do {
            return try await e2ee.exportPickle(passphrase: pass)
        } catch let err as AppError {
            errorBanner = err.errorDescription
            return nil
        } catch {
            errorBanner = error.localizedDescription
            return nil
        }
    }

    public func importE2eePickle(_ data: Data) async {
        guard let me = credentials else { return }
        let pass = e2eeImportPassphrase
        guard !pass.isEmpty else {
            errorBanner = "Enter the import passphrase."
            return
        }
        do {
            try await e2ee.importPickle(data: data, passphrase: pass, deviceId: me.deviceId)
            e2eeReady = await e2ee.isReady
            e2eeLocalIdentity = await e2ee.localIdentity
            showE2eeSettings = false
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
        try? store.ensureConversation(id: peer, title: peer, isGroup: false)
        refreshContacts()
        await openChat(peer: peer)
        await subscribePresenceRoster()
    }

    public func createGroup(title: String, memberIds: [String]) async {
        guard let me = credentials?.userId else { return }
        isBusy = true
        defer { isBusy = false }
        let gid = "g-\(UUID().uuidString.lowercased().prefix(8))"
        do {
            _ = try await groups.createGroup(groupId: gid, title: title, creator: me)
            for user in memberIds where user != me {
                try await groups.addMember(groupId: gid, user: user, actor: me)
            }
            showCreateGroup = false
            refreshInbox()
            await openChat(peer: Conversation.groupConversationId(gid))
        } catch let err as AppError {
            errorBanner = err.errorDescription
        } catch {
            errorBanner = error.localizedDescription
        }
    }

    public func addGroupMember(_ userId: String) async {
        guard let gid = selectedGroupId, let me = credentials?.userId else { return }
        let user = userId.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !user.isEmpty else { return }
        do {
            try await groups.addMember(groupId: gid, user: user, actor: me)
            refreshSelectedGroup()
            reloadThread()
            refreshInbox()
        } catch let err as AppError {
            errorBanner = err.errorDescription
        } catch {
            errorBanner = error.localizedDescription
        }
    }

    public func removeGroupMember(_ userId: String) async {
        guard let gid = selectedGroupId, let me = credentials?.userId else { return }
        do {
            try await groups.removeMember(groupId: gid, user: userId, actor: me)
            refreshSelectedGroup()
            reloadThread()
        } catch let err as AppError {
            errorBanner = err.errorDescription
        } catch {
            errorBanner = error.localizedDescription
        }
    }

    public func leaveSelectedGroup() async {
        guard let gid = selectedGroupId, let me = credentials?.userId else { return }
        do {
            try await groups.leave(groupId: gid, user: me)
            showGroupInfo = false
            closeChat()
            refreshInbox()
        } catch let err as AppError {
            errorBanner = err.errorDescription
        } catch {
            errorBanner = error.localizedDescription
        }
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
        try? await e2ee.ensureReady(deviceId: creds.deviceId)
        e2eeReady = await e2ee.isReady
        e2eeLocalIdentity = await e2ee.localIdentity
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
                await self.refreshMediaTransfers()
                try? await Task.sleep(nanoseconds: 200_000_000)
            }
        }
    }

    private func stopEvents() {
        eventsTask?.cancel()
        stateTask?.cancel()
        mediaPollTask?.cancel()
        eventsTask = nil
        stateTask = nil
        mediaPollTask = nil
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
        case .chatMessage(let messageId, let fromUser, let toUser, _, let bodyData, let seq, let mediaId, let sentAt):
            let decrypted = await e2ee.decryptInboundChat(from: fromUser, body: bodyData)
            if decrypted.wasEncrypted && decrypted.text.isEmpty {
                break
            }
            let text = decrypted.text
            let peer = fromUser == me ? toUser : fromUser
            let direction: MessageDirection = fromUser == me ? .outbound : .inbound
            let created = sentAt > 0
                ? Date(timeIntervalSince1970: TimeInterval(sentAt))
                : Date()
            let stored = StoredMessage(
                messageId: messageId,
                conversationId: peer.isEmpty ? fromUser : peer,
                direction: direction,
                body: text,
                mediaId: mediaId,
                seq: seq,
                status: direction == .inbound ? .delivered : .sent,
                createdAt: created,
                fromUser: fromUser
            )
            _ = try? store.upsertMessage(stored)
            if selectedPeer == stored.conversationId, direction == .inbound {
                try? await session.ackDelivered(messageId: messageId)
                try? await session.ackRead(messageId: messageId)
                try? store.updateStatus(messageId: messageId, status: .read, seq: nil)
                try? store.markConversationRead(conversationId: stored.conversationId)
            }
            if !mediaId.isEmpty {
                Task { try? await media.ensureDownloaded(mediaId: mediaId) }
            }
            refreshInbox()
            if selectedPeer == stored.conversationId { reloadThread() }
            if direction == .inbound {
                await maybeNotifyInbound(
                    conversationId: stored.conversationId,
                    messageId: messageId,
                    senderId: fromUser,
                    preview: text,
                    encrypted: decrypted.wasEncrypted
                )
            }
        case .groupMessage(let messageId, let fromUser, let groupId, _, let bodyData, let seq, let mediaId, let sentAt):
            let decrypted = await e2ee.decryptInboundGroup(groupId: groupId, body: bodyData)
            let text = decrypted.text
            let convoId = Conversation.groupConversationId(groupId)
            let direction: MessageDirection = fromUser == me ? .outbound : .inbound
            let created = sentAt > 0
                ? Date(timeIntervalSince1970: TimeInterval(sentAt))
                : Date()
            let stored = StoredMessage(
                messageId: messageId,
                conversationId: convoId,
                direction: direction,
                body: text,
                mediaId: mediaId,
                seq: seq,
                status: direction == .inbound ? .delivered : .sent,
                createdAt: created,
                fromUser: fromUser
            )
            try? store.ensureConversation(id: convoId, title: groupId, isGroup: true)
            _ = try? store.upsertMessage(stored)
            if selectedPeer == convoId, direction == .inbound {
                try? await session.ackDelivered(messageId: messageId)
                try? await session.ackRead(messageId: messageId)
                try? store.updateStatus(messageId: messageId, status: .read, seq: nil)
                try? store.markConversationRead(conversationId: convoId)
            }
            if !mediaId.isEmpty {
                Task { try? await media.ensureDownloaded(mediaId: mediaId) }
            }
            refreshInbox()
            if selectedPeer == convoId { reloadThread() }
            if direction == .inbound {
                await maybeNotifyInbound(
                    conversationId: convoId,
                    messageId: messageId,
                    senderId: fromUser,
                    preview: text,
                    encrypted: decrypted.wasEncrypted
                )
            }
        case .keyBundle(let userId, _, let identityKey, let found):
            if found, !identityKey.isEmpty {
                try? await e2ee.rememberPeerIdentity(userId: userId, identityKey: identityKey)
            }
        case .groupEvent(let groupId, let opRaw, let actor, let subject, let version):
            let op = GroupOp(rawValue: opRaw) ?? .unspecified
            let applied = (try? store.applyGroupEvent(
                groupId: groupId,
                op: op,
                actor: actor,
                subject: subject,
                version: version
            )) ?? false
            if applied {
                let text = GroupService.systemText(op: op, actor: actor, subject: subject)
                try? groups.appendSystemLine(groupId: groupId, text: text, version: version)
            }
            refreshInbox()
            refreshSelectedGroup()
            if selectedPeer == Conversation.groupConversationId(groupId) {
                reloadThread()
            }
        case .groupAckSummary(let messageId, _, let memberCount, let delivered, let read):
            try? store.updateGroupAckSummary(
                messageId: messageId,
                deliveredCount: delivered,
                readCount: read,
                memberCount: memberCount
            )
            if selectedPeer != nil { reloadThread() }
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
        let seeds = ["alice", "bob", "carol"].filter { $0 != me }
        for user in seeds {
            try? store.upsertContact(Contact(userId: user, presence: .unavailable))
        }
        refreshContacts()
        #endif
    }
}
