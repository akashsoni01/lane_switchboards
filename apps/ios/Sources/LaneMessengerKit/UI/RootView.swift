import SwiftUI

public struct RootView: View {
    @Bindable var model: AppModel
    #if os(iOS)
    @Environment(\.scenePhase) private var scenePhase
    #endif

    public init(model: AppModel) {
        self.model = model
    }

    public var body: some View {
        Group {
            switch model.route {
            case .splash:
                SplashView()
            case .login:
                LoginView(model: model)
            case .home:
                HomeShellView(model: model)
            }
        }
        .task {
            if model.route == .splash {
                await model.bootstrap()
            }
        }
        #if os(iOS)
        .onChange(of: scenePhase) { _, phase in
            Task {
                switch phase {
                case .active: await model.handleScenePhase("active")
                case .inactive: await model.handleScenePhase("inactive")
                case .background: await model.handleScenePhase("background")
                @unknown default: break
                }
            }
        }
        #endif
        .alert("Signed in elsewhere", isPresented: $model.showReplacedAlert) {
            Button("OK") { Task { await model.acknowledgeDeviceReplaced() } }
        } message: {
            Text(AppError.replacedByNewSession.errorDescription)
        }
        .alert("Update required", isPresented: $model.showUpgradeAlert) {
            Button("OK", role: .cancel) {}
        } message: {
            Text(ProtocolErrorCode.unsupportedVersion.userMessage)
        }
    }
}

struct SplashView: View {
    var body: some View {
        VStack(spacing: 16) {
            ProgressView()
            Text("Lane Messenger")
                .font(.title2.weight(.semibold))
            Text("Restoring session…")
                .foregroundStyle(.secondary)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }
}

public struct LoginView: View {
    @Bindable var model: AppModel
    @State private var userId = ""
    @State private var secret = ""

    public init(model: AppModel) {
        self.model = model
    }

    public var body: some View {
        NavigationStack {
            Form {
                Section {
                    TextField("User ID", text: $userId)
                        #if os(iOS)
                        .textInputAutocapitalization(.never)
                        .autocorrectionDisabled()
                        #endif
                    SecureField(secretPlaceholder, text: $secret)
                } header: {
                    Text("Sign in")
                } footer: {
                    Text(footerText)
                }

                if let banner = model.errorBanner {
                    Section {
                        Text(banner).foregroundStyle(.red)
                    }
                }

                Section {
                    Button {
                        Task {
                            await model.signIn(
                                userId: userId.trimmingCharacters(in: .whitespaces),
                                secret: secret
                            )
                        }
                    } label: {
                        if model.isBusy { ProgressView() } else { Text("Continue") }
                    }
                    .disabled(userId.trimmingCharacters(in: .whitespaces).isEmpty || model.isBusy)
                }

                #if DEBUG
                Section("Gateway (DEBUG)") {
                    TextField("Host", text: Binding(
                        get: { model.config.host },
                        set: { model.config.host = $0 }
                    ))
                    #if os(iOS)
                    .textInputAutocapitalization(.never)
                    #endif
                    Stepper(
                        "Port \(model.config.port)",
                        value: Binding(
                            get: { Int(model.config.port) },
                            set: { model.config.port = UInt16($0) }
                        ),
                        in: 1...65535
                    )
                    Toggle("TLS", isOn: Binding(
                        get: { model.config.useTls },
                        set: { model.config.useTls = $0 }
                    ))
                }
                #endif
            }
            .navigationTitle("Lane")
        }
    }

    private var secretPlaceholder: String {
        #if DEBUG
        "Password or pasted token (DEBUG: empty = mint)"
        #else
        "Password"
        #endif
    }

    private var footerText: String {
        #if DEBUG
        "DEBUG builds mint HMAC tokens with demo-secret when the secret field is empty."
        #else
        "Tokens are minted by the identity service."
        #endif
    }
}

struct HomeShellView: View {
    @Bindable var model: AppModel

    var body: some View {
        NavigationStack {
            InboxView(model: model)
                .navigationDestination(isPresented: Binding(
                    get: { model.selectedPeer != nil },
                    set: { if !$0 { model.closeChat() } }
                )) {
                    if let peer = model.selectedPeer {
                        ChatThreadView(model: model, peer: peer)
                    }
                }
        }
        .sheet(isPresented: $model.showNewChat) {
            NewChatSheet(model: model)
        }
        .sheet(isPresented: $model.showCreateGroup) {
            CreateGroupSheet(model: model)
        }
        .sheet(isPresented: $model.showGroupInfo) {
            GroupInfoSheet(model: model)
        }
        .sheet(isPresented: $model.showAttachMenu) {
            AttachMenuView(model: model)
        }
        #if os(iOS)
        .fullScreenCover(isPresented: Binding(
            get: { model.previewMediaId != nil },
            set: { if !$0 { model.previewMediaId = nil } }
        )) {
            MediaPreviewHost(model: model)
        }
        #else
        .sheet(isPresented: Binding(
            get: { model.previewMediaId != nil },
            set: { if !$0 { model.previewMediaId = nil } }
        )) {
            MediaPreviewHost(model: model)
        }
        #endif
        .fileImporter(
            isPresented: $model.showFileImporter,
            allowedContentTypes: [.pdf, .image, .data],
            allowsMultipleSelection: false
        ) { result in
            switch result {
            case .success(let urls):
                guard let url = urls.first else { return }
                Task {
                    let accessed = url.startAccessingSecurityScopedResource()
                    defer { if accessed { url.stopAccessingSecurityScopedResource() } }
                    guard let data = try? Data(contentsOf: url) else { return }
                    let mime: String
                    if url.pathExtension.lowercased() == "pdf" {
                        mime = "application/pdf"
                    } else if ["png"].contains(url.pathExtension.lowercased()) {
                        mime = "image/png"
                    } else if ["jpg", "jpeg"].contains(url.pathExtension.lowercased()) {
                        mime = "image/jpeg"
                    } else {
                        mime = "application/octet-stream"
                    }
                    await model.sendAttachment(
                        data: data,
                        fileName: url.lastPathComponent,
                        mimeType: mime
                    )
                }
            case .failure(let error):
                model.errorBanner = error.localizedDescription
            }
        }
    }
}

public struct InboxView: View {
    @Bindable var model: AppModel

    public init(model: AppModel) {
        self.model = model
    }

    public var body: some View {
        List {
            if let banner = model.errorBanner {
                Section {
                    Text(banner).foregroundStyle(.red).font(.footnote)
                }
            }

            Section {
                if model.inbox.isEmpty {
                    ContentUnavailableView(
                        "No chats yet",
                        systemImage: "bubble.left.and.bubble.right",
                        description: Text("Start a conversation with a contact.")
                    )
                    .listRowBackground(Color.clear)
                } else {
                    ForEach(model.inbox) { convo in
                        Button {
                            Task { await model.openChat(peer: convo.id) }
                        } label: {
                            InboxRow(conversation: convo)
                        }
                        .buttonStyle(.plain)
                    }
                }
            } header: {
                connectionHeader
            }
        }
        .navigationTitle("Chats")
        .toolbar {
            ToolbarItem(placement: .primaryAction) {
                Menu {
                    Button {
                        model.showNewChat = true
                    } label: {
                        Label("New chat", systemImage: "square.and.pencil")
                    }
                    Button {
                        model.showCreateGroup = true
                    } label: {
                        Label("New group", systemImage: "person.3")
                    }
                } label: {
                    Image(systemName: "plus")
                }
                .accessibilityLabel("New")
            }
            ToolbarItem(placement: .navigation) {
                Menu {
                    #if DEBUG
                    if let rtt = model.lastPingRttMs {
                        Text("Ping \(rtt) ms")
                    }
                    #endif
                    Button("Sign out", role: .destructive) {
                        Task { await model.signOut() }
                    }
                } label: {
                    Image(systemName: "gearshape")
                }
            }
        }
        .overlay(alignment: .top) {
            if let status = statusBanner {
                Text(status)
                    .font(.footnote.weight(.medium))
                    .padding(.horizontal, 12)
                    .padding(.vertical, 6)
                    .background(.yellow.opacity(0.92), in: Capsule())
                    .padding(.top, 8)
            }
        }
        .onAppear { model.refreshInbox() }
    }

    private var connectionHeader: some View {
        HStack {
            Text(model.credentials?.userId ?? "")
            Spacer()
            Text(model.connectionState.rawValue)
                .foregroundStyle(.secondary)
        }
        .font(.caption)
        .textCase(nil)
    }

    private var statusBanner: String? {
        switch model.connectionState {
        case .connecting, .awaitingLogin: return "Connecting…"
        case .syncing:
            return model.pendingMessagesHint > 0
                ? "Syncing \(model.pendingMessagesHint) messages…"
                : "Syncing…"
        case .offline: return "Offline — reconnecting…"
        case .replaced: return "Signed in elsewhere"
        case .disconnected: return "Disconnected"
        case .ready: return nil
        }
    }
}

struct InboxRow: View {
    let conversation: Conversation

    var body: some View {
        HStack(spacing: 12) {
            ZStack(alignment: .bottomTrailing) {
                AvatarView(title: conversation.title)
                if !conversation.isGroup {
                    PresenceDot(kind: conversation.presence)
                        .offset(x: 2, y: 2)
                }
            }
            VStack(alignment: .leading, spacing: 4) {
                HStack {
                    if conversation.isGroup {
                        Image(systemName: "person.3.fill")
                            .font(.caption2)
                            .foregroundStyle(.secondary)
                    }
                    Text(conversation.title)
                        .font(.headline)
                        .foregroundStyle(.primary)
                    Spacer()
                    Text(conversation.sortTs, style: .time)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
                HStack {
                    Text(conversation.lastPreview.isEmpty ? "No messages" : conversation.lastPreview)
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                    Spacer()
                    if conversation.unread > 0 {
                        Text("\(conversation.unread)")
                            .font(.caption2.weight(.bold))
                            .padding(.horizontal, 7)
                            .padding(.vertical, 2)
                            .background(.blue, in: Capsule())
                            .foregroundStyle(.white)
                    }
                }
            }
        }
        .padding(.vertical, 4)
        .accessibilityElement(children: .combine)
    }
}

public struct ChatThreadView: View {
    @Bindable var model: AppModel
    let peer: String

    public init(model: AppModel, peer: String) {
        self.model = model
        self.peer = peer
    }

    private var isGroup: Bool {
        Conversation.groupId(fromConversationId: peer) != nil
    }

    private var title: String {
        if isGroup {
            return model.selectedGroup?.title
                ?? model.inbox.first(where: { $0.id == peer })?.title
                ?? peer
        }
        return peer
    }

    public var body: some View {
        VStack(spacing: 0) {
            if model.connectionState != .ready {
                Text(offlineHint)
                    .font(.caption)
                    .frame(maxWidth: .infinity)
                    .padding(6)
                    .background(Color.yellow.opacity(0.3))
            }

            ScrollViewReader { proxy in
                ScrollView {
                    LazyVStack(spacing: 8) {
                        ForEach(groupedByDay, id: \.day) { group in
                            Text(group.label)
                                .font(.caption)
                                .foregroundStyle(.secondary)
                                .padding(.vertical, 4)
                            ForEach(group.messages) { message in
                                MessageBubble(
                                    message: message,
                                    showSender: isGroup && message.direction == .inbound,
                                    meta: message.mediaId.isEmpty
                                        ? nil
                                        : model.mediaMeta(message.mediaId),
                                    progress: message.mediaId.isEmpty
                                        ? nil
                                        : model.mediaTransfers[message.mediaId],
                                    completeURL: message.mediaId.isEmpty
                                        ? nil
                                        : model.mediaFileURL(message.mediaId),
                                    onRetry: { Task { await model.retryMessage(message) } },
                                    onOpenMedia: {
                                        Task { await model.openMedia(message.mediaId) }
                                    }
                                )
                                .id(message.messageId)
                            }
                        }
                    }
                    .padding(.horizontal, 12)
                    .padding(.vertical, 8)
                }
                .onChange(of: model.threadMessages.count) { _, _ in
                    if let last = model.threadMessages.last {
                        withAnimation { proxy.scrollTo(last.messageId, anchor: .bottom) }
                    }
                }
            }

            Divider()
            composer
        }
        .navigationTitle(title)
        #if os(iOS)
        .navigationBarTitleDisplayMode(.inline)
        #endif
        .toolbar {
            if isGroup {
                ToolbarItem(placement: .primaryAction) {
                    Button {
                        model.refreshSelectedGroup()
                        model.showGroupInfo = true
                    } label: {
                        Image(systemName: "info.circle")
                    }
                    .accessibilityLabel("Group info")
                }
            }
        }
        .onAppear {
            Task { await model.openChat(peer: peer) }
        }
    }

    private var offlineHint: String {
        switch model.connectionState {
        case .offline: return "You're offline. Messages will send when reconnected."
        case .connecting, .awaitingLogin, .syncing: return "Connecting…"
        default: return "Not connected"
        }
    }

    private var composer: some View {
        HStack(alignment: .bottom, spacing: 8) {
            Button {
                model.showAttachMenu = true
            } label: {
                Image(systemName: "paperclip")
            }
            .disabled(model.connectionState == .offline || model.isSending)
            .accessibilityLabel("Attach")

            TextField("Message", text: Binding(
                get: { model.composeText },
                set: { model.updateDraft($0) }
            ), axis: .vertical)
            .lineLimit(1...5)
            .textFieldStyle(.roundedBorder)

            Button {
                Task { await model.sendCurrentCompose() }
            } label: {
                if model.isSending {
                    ProgressView()
                } else {
                    Image(systemName: "arrow.up.circle.fill")
                        .font(.title2)
                }
            }
            .disabled(
                model.isSending
                    || model.composeText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
                    || model.connectionState == .offline
            )
            .accessibilityLabel("Send")
        }
        .padding(10)
    }

    private var groupedByDay: [(day: Date, label: String, messages: [StoredMessage])] {
        let cal = Calendar.current
        let grouped = Dictionary(grouping: model.threadMessages) { msg in
            cal.startOfDay(for: msg.createdAt)
        }
        return grouped.keys.sorted().map { day in
            let label: String
            if cal.isDateInToday(day) { label = "Today" }
            else if cal.isDateInYesterday(day) { label = "Yesterday" }
            else {
                label = day.formatted(date: .abbreviated, time: .omitted)
            }
            return (day, label, grouped[day] ?? [])
        }
    }
}

struct MessageBubble: View {
    let message: StoredMessage
    var showSender: Bool = false
    var meta: MediaBlobMeta? = nil
    var progress: MediaTransferProgress? = nil
    var completeURL: URL? = nil
    var onRetry: () -> Void
    var onOpenMedia: () -> Void = {}

    var body: some View {
        if message.direction == .system {
            Text(message.body)
                .font(.caption)
                .foregroundStyle(.secondary)
                .multilineTextAlignment(.center)
                .frame(maxWidth: .infinity)
                .padding(.vertical, 4)
        } else {
            HStack {
                if message.direction == .outbound { Spacer(minLength: 48) }
                VStack(alignment: message.direction == .outbound ? .trailing : .leading, spacing: 4) {
                    if showSender, !message.fromUser.isEmpty {
                        Text(message.fromUser)
                            .font(.caption2.weight(.semibold))
                            .foregroundStyle(.secondary)
                    }
                    if !message.mediaId.isEmpty {
                        MediaAttachmentView(
                            message: message,
                            meta: meta,
                            progress: progress,
                            completeURL: completeURL,
                            onOpen: onOpenMedia,
                            onRetryDownload: onOpenMedia
                        )
                    }
                    if !message.body.isEmpty {
                        Text(message.body)
                            .padding(.horizontal, 12)
                            .padding(.vertical, 8)
                            .background(
                                message.direction == .outbound
                                    ? Color.accentColor.opacity(0.18)
                                    : Color.secondary.opacity(0.12),
                                in: RoundedRectangle(cornerRadius: 16, style: .continuous)
                            )
                    } else if message.mediaId.isEmpty {
                        Text(" ")
                            .padding(.horizontal, 12)
                            .padding(.vertical, 8)
                    }
                    HStack(spacing: 4) {
                        Text(message.createdAt, style: .time)
                            .font(.caption2)
                            .foregroundStyle(.secondary)
                        MessageTicksView(
                            status: message.status,
                            isOutbound: message.direction == .outbound
                        )
                        if message.direction == .outbound, message.memberCount > 0 {
                            Text(groupAckLabel)
                                .font(.caption2)
                                .foregroundStyle(.secondary)
                        }
                        if message.status == .failed {
                            Button("Retry", action: onRetry)
                                .font(.caption2)
                        }
                    }
                }
                if message.direction == .inbound { Spacer(minLength: 48) }
            }
            .accessibilityElement(children: .combine)
        }
    }

    private var groupAckLabel: String {
        if message.readCount > 0 {
            return "read \(message.readCount)/\(message.memberCount)"
        }
        if message.deliveredCount > 0 {
            return "delivered \(message.deliveredCount)/\(message.memberCount)"
        }
        return ""
    }
}

struct NewChatSheet: View {
    @Bindable var model: AppModel
    @State private var peer = ""

    var body: some View {
        NavigationStack {
            List {
                Section("Contacts") {
                    if model.contacts.isEmpty {
                        Text("No contacts yet")
                            .foregroundStyle(.secondary)
                    } else {
                        ForEach(model.contacts) { contact in
                            Button {
                                Task { await model.startChat(with: contact.userId) }
                            } label: {
                                HStack {
                                    AvatarView(title: contact.displayName)
                                    VStack(alignment: .leading) {
                                        Text(contact.displayName)
                                        Text(contact.presence.isOnline ? "Online" : "Offline")
                                            .font(.caption)
                                            .foregroundStyle(.secondary)
                                    }
                                    Spacer()
                                    PresenceDot(kind: contact.presence)
                                }
                            }
                            .buttonStyle(.plain)
                        }
                    }
                }
                Section("User ID") {
                    TextField("user id", text: $peer)
                        #if os(iOS)
                        .textInputAutocapitalization(.never)
                        .autocorrectionDisabled()
                        #endif
                    Button("Start chat") {
                        Task { await model.startChat(with: peer) }
                    }
                    .disabled(peer.trimmingCharacters(in: .whitespaces).isEmpty)
                }
            }
            .navigationTitle("New chat")
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Close") { model.showNewChat = false }
                }
            }
            .onAppear { model.refreshContacts() }
        }
    }
}

struct CreateGroupSheet: View {
    @Bindable var model: AppModel
    @State private var title = ""
    @State private var selected: Set<String> = []

    var body: some View {
        NavigationStack {
            Form {
                Section("Group name") {
                    TextField("Title", text: $title)
                }
                Section {
                    ForEach(model.contacts) { contact in
                        Toggle(isOn: Binding(
                            get: { selected.contains(contact.userId) },
                            set: { on in
                                if on { selected.insert(contact.userId) }
                                else { selected.remove(contact.userId) }
                            }
                        )) {
                            Text(contact.displayName)
                        }
                    }
                } header: {
                    Text("Members (\(selected.count)/\(AppLimits.maxGroupMembers - 1))")
                } footer: {
                    Text("Max \(AppLimits.maxGroupMembers) members including you.")
                }
                if let banner = model.errorBanner {
                    Section { Text(banner).foregroundStyle(.red) }
                }
            }
            .navigationTitle("New group")
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Close") { model.showCreateGroup = false }
                }
                ToolbarItem(placement: .confirmationAction) {
                    Button("Create") {
                        Task {
                            await model.createGroup(
                                title: title.trimmingCharacters(in: .whitespaces),
                                memberIds: Array(selected)
                            )
                        }
                    }
                    .disabled(model.isBusy || selected.count >= AppLimits.maxGroupMembers)
                }
            }
            .onAppear { model.refreshContacts() }
        }
    }
}

struct GroupInfoSheet: View {
    @Bindable var model: AppModel
    @State private var addUser = ""

    private var me: String { model.credentials?.userId ?? "" }
    private var isAdmin: Bool { model.selectedGroup?.isAdmin(me) == true }

    var body: some View {
        NavigationStack {
            List {
                if let group = model.selectedGroup {
                    Section("Group") {
                        Text(group.title)
                        Text("\(group.memberCount) members · v\(group.version)")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                    Section("Members") {
                        ForEach(group.members) { member in
                            HStack {
                                Text(member.userId)
                                if member.isAdmin {
                                    Text("admin")
                                        .font(.caption2)
                                        .padding(.horizontal, 6)
                                        .padding(.vertical, 2)
                                        .background(Color.secondary.opacity(0.2), in: Capsule())
                                }
                                Spacer()
                                if isAdmin, member.userId != me {
                                    Button("Remove", role: .destructive) {
                                        Task { await model.removeGroupMember(member.userId) }
                                    }
                                    .font(.caption)
                                }
                            }
                        }
                    }
                    if isAdmin {
                        Section("Add member") {
                            TextField("user id", text: $addUser)
                                #if os(iOS)
                                .textInputAutocapitalization(.never)
                                .autocorrectionDisabled()
                                #endif
                            Button("Add") {
                                Task {
                                    await model.addGroupMember(addUser)
                                    addUser = ""
                                }
                            }
                            .disabled(
                                addUser.trimmingCharacters(in: .whitespaces).isEmpty
                                    || (model.selectedGroup?.memberCount ?? 0) >= AppLimits.maxGroupMembers
                            )
                        }
                    }
                    Section {
                        Button("Leave group", role: .destructive) {
                            Task { await model.leaveSelectedGroup() }
                        }
                    }
                } else {
                    Text("Group not found")
                        .foregroundStyle(.secondary)
                }
            }
            .navigationTitle("Group info")
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Done") { model.showGroupInfo = false }
                }
            }
            .onAppear { model.refreshSelectedGroup() }
        }
    }
}
