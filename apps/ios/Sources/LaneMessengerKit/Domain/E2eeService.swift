import Foundation

/// Owns the local E2EE device lifecycle: generate/import, publish, encrypt paths.
public actor E2eeService {
    public private(set) var isReady = false
    public private(set) var localIdentity: String = ""
    public private(set) var enabled: Bool

    private let session: SessionActor
    private let pickleStore: E2eePickleStore
    private var backend: (any E2eeBackend)?
    private let autoPassphrase: String
    /// Optional factory for production FFI devices (injected when XCFramework is linked).
    private let backendFactory: (@Sendable (Data?) throws -> any E2eeBackend)?

    public init(
        session: SessionActor,
        pickleStore: E2eePickleStore? = nil,
        enabled: Bool = FeatureFlags.e2eeEnabled,
        autoPassphrase: String = "lane-device-local",
        backendFactory: (@Sendable (Data?) throws -> any E2eeBackend)? = nil
    ) {
        self.session = session
        if let pickleStore {
            self.pickleStore = pickleStore
        } else {
            self.pickleStore = (try? E2eePickleStore()) ?? (try! E2eePickleStore(
                fileURL: FileManager.default.temporaryDirectory
                    .appendingPathComponent("e2ee-\(UUID().uuidString).pickle")
            ))
        }
        self.enabled = enabled
        self.autoPassphrase = autoPassphrase
        self.backendFactory = backendFactory
    }

    public func setEnabled(_ on: Bool) {
        enabled = on
    }

    /// Load pickle or generate; publish one-time keys when session is ready.
    public func ensureReady(deviceId: String) async throws {
        guard enabled else {
            isReady = false
            return
        }
        if backend != nil, isReady {
            try await persistPickle()
            return
        }
        let existing = try pickleStore.load()
        if let factory = backendFactory {
            backend = try factory(existing)
        } else if let data = existing {
            backend = try MockE2eeBackend.importPickle(
                data: data,
                passphrase: autoPassphrase,
                session: session
            )
        } else {
            backend = MockE2eeBackend(session: session)
            try await persistPickle()
        }
        if existing == nil, backendFactory != nil {
            try await persistPickle()
        }
        localIdentity = try backend?.identityKey() ?? ""
        try await backend?.publish(deviceId: deviceId, otkCount: 20)
        isReady = true
        LaneLog.e2ee.info("e2ee ready")
    }

    public func shouldEncryptSends() -> Bool {
        enabled && isReady && !FeatureFlags.debugPlaintextFallback
    }

    public func sendChat(to: String, messageId: String, plaintext: Data) async throws -> UInt64 {
        guard let backend, shouldEncryptSends() else {
            return try await session.sendChat(to: to, messageId: messageId, body: plaintext)
        }
        let seq = try await backend.sendEncryptedChat(to: to, messageId: messageId, plaintext: plaintext)
        try await persistPickle()
        return seq
    }

    public func sendGroup(groupId: String, messageId: String, plaintext: Data) async throws {
        guard let backend, shouldEncryptSends() else {
            try await session.sendGroup(groupId: groupId, messageId: messageId, body: plaintext)
            return
        }
        try await backend.sendEncryptedGroup(
            groupId: groupId,
            messageId: messageId,
            plaintext: plaintext
        )
        try await persistPickle()
    }

    /// Decrypt inbound 1:1 body. Returns plaintext string; may try group-key import first.
    public func decryptInboundChat(from: String, body: Data) async -> (text: String, wasEncrypted: Bool) {
        guard enabled, let backend else {
            return (String(data: body, encoding: .utf8) ?? "", false)
        }
        if let imported = try? backend.tryImportGroupKey(from: from, body: body), imported {
            try? await persistPickle()
            return ("", true) // key share — no chat UI line needed from body
        }
        do {
            let plain = try backend.decryptChat(from: from, body: body)
            try? await persistPickle()
            return (String(data: plain, encoding: .utf8) ?? "", true)
        } catch {
            if FeatureFlags.debugPlaintextFallback {
                return (String(data: body, encoding: .utf8) ?? "", false)
            }
            // Maybe plaintext from older clients
            if let s = String(data: body, encoding: .utf8), !s.isEmpty, body.count < 8 || !isLikelyCiphertext(body) {
                return (s, false)
            }
            return ("🔒 Unable to decrypt", true)
        }
    }

    public func decryptInboundGroup(groupId: String, body: Data) async -> (text: String, wasEncrypted: Bool) {
        guard enabled, let backend else {
            return (String(data: body, encoding: .utf8) ?? "", false)
        }
        do {
            let plain = try backend.decryptGroup(groupId: groupId, body: body)
            try? await persistPickle()
            return (String(data: plain, encoding: .utf8) ?? "", true)
        } catch {
            if FeatureFlags.debugPlaintextFallback {
                return (String(data: body, encoding: .utf8) ?? "", false)
            }
            if let s = String(data: body, encoding: .utf8), !isLikelyCiphertext(body) {
                return (s, false)
            }
            return ("🔒 Unable to decrypt", true)
        }
    }

    public func setupGroupEncryption(groupId: String, members: [String]) async throws {
        guard let backend, shouldEncryptSends() else { return }
        _ = try backend.createGroupSession(groupId: groupId)
        let others = members.filter { !$0.isEmpty }
        if !others.isEmpty {
            try await backend.distributeGroupKey(groupId: groupId, members: others)
        }
        try await persistPickle()
    }

    public func safetyNumber(forPeer userId: String) throws -> String? {
        guard let remote = try pickleStore.peerIdentity(userId: userId), !remote.isEmpty,
              !localIdentity.isEmpty
        else {
            return nil
        }
        return E2eeSafety.number(localIdentityB64: localIdentity, remoteIdentityB64: remote)
    }

    public func rememberPeerIdentity(userId: String, identityKey: String) throws {
        guard !identityKey.isEmpty else { return }
        try pickleStore.savePeerIdentity(userId: userId, identityKey: identityKey)
    }

    public func exportPickle(passphrase: String) throws -> Data {
        guard let backend else { throw AppError.e2ee("E2EE not ready") }
        return try backend.exportPickle(passphrase: passphrase)
    }

    public func importPickle(data: Data, passphrase: String, deviceId: String) async throws {
        backend = try MockE2eeBackend.importPickle(data: data, passphrase: passphrase, session: session)
        localIdentity = try backend?.identityKey() ?? ""
        try pickleStore.save(data)
        try await backend?.publish(deviceId: deviceId, otkCount: 20)
        isReady = true
    }

    private func persistPickle() async throws {
        guard let backend else { return }
        let data = try backend.exportPickle(passphrase: autoPassphrase)
        try pickleStore.save(data)
    }

    private func isLikelyCiphertext(_ body: Data) -> Bool {
        body.starts(with: Data("OLM1".utf8))
            || body.starts(with: Data("MGL1".utf8))
            || body.starts(with: Data("GSK1".utf8))
            || body.count > 64
    }
}
