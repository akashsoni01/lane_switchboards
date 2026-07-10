import SwiftUI

public struct RootView: View {
    @Bindable var model: AppModel

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
                            await model.signIn(userId: userId.trimmingCharacters(in: .whitespaces), secret: secret)
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
                }
                Section {
                    Button("Sign out", role: .destructive) {
                        Task { await model.signOut() }
                    }
                }
            }
            .navigationTitle("Chats")
            .overlay(alignment: .top) {
                if model.connectionState != .ready {
                    Text(statusBanner)
                        .font(.footnote.weight(.medium))
                        .padding(.horizontal, 12)
                        .padding(.vertical, 6)
                        .background(.yellow.opacity(0.9), in: Capsule())
                        .padding(.top, 8)
                }
            }
        }
    }

    private var shortDevice: String {
        guard let id = model.credentials?.deviceId, id.count > 8 else {
            return model.credentials?.deviceId ?? "—"
        }
        return String(id.prefix(8)) + "…"
    }

    private var statusBanner: String {
        switch model.connectionState {
        case .connecting, .awaitingLogin: return "Connecting…"
        case .syncing: return "Syncing…"
        case .offline: return "Offline"
        case .replaced: return "Signed in elsewhere"
        case .disconnected: return "Disconnected"
        case .ready: return ""
        }
    }
}
