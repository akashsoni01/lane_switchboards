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
                Button {
                    model.showNewChat = true
                } label: {
                    Image(systemName: "square.and.pencil")
                }
                .accessibilityLabel("New chat")
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
                PresenceDot(kind: conversation.presence)
                    .offset(x: 2, y: 2)
            }
            VStack(alignment: .leading, spacing: 4) {
                HStack {
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
                                MessageBubble(message: message) {
                                    Task { await model.retryMessage(message) }
                                }
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
        .navigationTitle(peer)
        #if os(iOS)
        .navigationBarTitleDisplayMode(.inline)
        #endif
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
                // Attachment affordance — media lands in I7.
            } label: {
                Image(systemName: "paperclip")
            }
            .disabled(true)
            .accessibilityLabel("Attach (coming soon)")

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
    var onRetry: () -> Void

    var body: some View {
        HStack {
            if message.direction == .outbound { Spacer(minLength: 48) }
            VStack(alignment: message.direction == .outbound ? .trailing : .leading, spacing: 4) {
                Text(message.body.isEmpty ? (message.mediaId.isEmpty ? " " : "📎 Media") : message.body)
                    .padding(.horizontal, 12)
                    .padding(.vertical, 8)
                    .background(
                        message.direction == .outbound
                            ? Color.accentColor.opacity(0.18)
                            : Color.secondary.opacity(0.12),
                        in: RoundedRectangle(cornerRadius: 16, style: .continuous)
                    )
                HStack(spacing: 4) {
                    Text(message.createdAt, style: .time)
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                    MessageTicksView(
                        status: message.status,
                        isOutbound: message.direction == .outbound
                    )
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
