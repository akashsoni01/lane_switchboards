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
                HomeView(model: model)
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
        .alert(
            "Signed in elsewhere",
            isPresented: $model.showReplacedAlert
        ) {
            Button("OK") {
                Task { await model.acknowledgeDeviceReplaced() }
            }
        } message: {
            Text(AppError.replacedByNewSession.errorDescription)
        }
        .alert(
            "Update required",
            isPresented: $model.showUpgradeAlert
        ) {
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
                        Text(banner)
                            .foregroundStyle(.red)
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
                        if model.isBusy {
                            ProgressView()
                        } else {
                            Text("Continue")
                        }
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
        "DEBUG builds mint HMAC tokens with demo-secret when the secret field is empty (parity with messenger_demo)."
        #else
        "Tokens are minted by the identity service. The app never embeds the gateway HMAC secret."
        #endif
    }
}

public struct HomeView: View {
    @Bindable var model: AppModel

    public init(model: AppModel) {
        self.model = model
    }

    public var body: some View {
        NavigationStack {
            List {
                Section("Session") {
                    LabeledContent("User", value: model.credentials?.userId ?? "—")
                    LabeledContent("Device", value: shortDevice)
                    LabeledContent("State", value: model.connectionState.rawValue)
                    LabeledContent("Gateway", value: "\(model.config.host):\(model.config.port)")
                    #if DEBUG
                    if let rtt = model.lastPingRttMs {
                        LabeledContent("Ping RTT", value: "\(rtt) ms")
                    }
                    #endif
                }

                Section("Chats") {
                    if model.inbox.isEmpty {
                        Text("No conversations yet")
                            .foregroundStyle(.secondary)
                    } else {
                        ForEach(model.inbox) { convo in
                            VStack(alignment: .leading, spacing: 4) {
                                HStack {
                                    Text(convo.title).font(.headline)
                                    Spacer()
                                    if convo.unread > 0 {
                                        Text("\(convo.unread)")
                                            .font(.caption2.weight(.bold))
                                            .padding(.horizontal, 6)
                                            .padding(.vertical, 2)
                                            .background(.blue, in: Capsule())
                                            .foregroundStyle(.white)
                                    }
                                }
                                Text(convo.lastPreview.isEmpty ? " " : convo.lastPreview)
                                    .font(.subheadline)
                                    .foregroundStyle(.secondary)
                                    .lineLimit(1)
                            }
                        }
                    }
                }

                Section {
                    Button("Sign out", role: .destructive) {
                        Task { await model.signOut() }
                    }
                }
            }
            .navigationTitle("Chats")
            .overlay(alignment: .top) {
                if let banner = statusBanner {
                    Text(banner)
                        .font(.footnote.weight(.medium))
                        .padding(.horizontal, 12)
                        .padding(.vertical, 6)
                        .background(.yellow.opacity(0.9), in: Capsule())
                        .padding(.top, 8)
                }
            }
            .onAppear { model.refreshInbox() }
        }
    }

    private var shortDevice: String {
        guard let id = model.credentials?.deviceId, id.count > 8 else {
            return model.credentials?.deviceId ?? "—"
        }
        return String(id.prefix(8)) + "…"
    }

    private var statusBanner: String? {
        switch model.connectionState {
        case .connecting, .awaitingLogin:
            return "Connecting…"
        case .syncing:
            if model.pendingMessagesHint > 0 {
                return "Syncing \(model.pendingMessagesHint) messages…"
            }
            return "Syncing…"
        case .offline:
            return "Offline — reconnecting…"
        case .replaced:
            return "Signed in elsewhere"
        case .disconnected:
            return "Disconnected"
        case .ready:
            return nil
        }
    }
}
