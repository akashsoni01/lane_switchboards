import Foundation

/// Pluggable Olm/Megolm backend. Mock for tests; native FFI when XCFramework is linked.
public protocol E2eeBackend: AnyObject, Sendable {
    func identityKey() throws -> String
    func publish(deviceId: String, otkCount: UInt32) async throws
    func sendEncryptedChat(to: String, messageId: String, plaintext: Data) async throws -> UInt64
    func decryptChat(from: String, body: Data) throws -> Data
    func createGroupSession(groupId: String) throws -> String
    func distributeGroupKey(groupId: String, members: [String]) async throws
    func sendEncryptedGroup(groupId: String, messageId: String, plaintext: Data) async throws
    func decryptGroup(groupId: String, body: Data) throws -> Data
    func tryImportGroupKey(from: String, body: Data) throws -> Bool
    func exportPickle(passphrase: String) throws -> Data
}

public enum E2eeSafety {
    public static func number(localIdentityB64: String, remoteIdentityB64: String) -> String {
        // Stable fingerprint without native lib: sorted SHA-256 of both keys.
        let a = localIdentityB64
        let b = remoteIdentityB64
        let (x, y) = a < b ? (a, b) : (b, a)
        let data = Data("\(x)|\(y)".utf8)
        return MediaHasher.sha256Hex(data)
    }
}

/// In-memory reversible “Olm” for smoke tests (not real crypto).
public final class MockE2eeBackend: E2eeBackend, @unchecked Sendable {
    public private(set) var identity: String
    public private(set) var published = false
    public private(set) var groupSessions: Set<String> = []
    public private(set) var importedGroupKeys: Set<String> = []
    private let session: SessionActor
    private static let chatMagic = Data("OLM1".utf8)
    private static let groupMagic = Data("MGL1".utf8)
    private static let shareMagic = Data("GSK1".utf8)

    public init(session: SessionActor, identity: String = "mock-id-\(UUID().uuidString.prefix(8))") {
        self.session = session
        self.identity = identity
    }

    public static func importPickle(data: Data, passphrase: String, session: SessionActor) throws -> MockE2eeBackend {
        guard let json = try? JSONSerialization.jsonObject(with: data) as? [String: String],
              let id = json["identity"],
              let pass = json["pass"],
              pass == passphrase
        else {
            throw AppError.e2ee("invalid pickle or passphrase")
        }
        return MockE2eeBackend(session: session, identity: id)
    }

    public func identityKey() throws -> String { identity }

    public func publish(deviceId: String, otkCount: UInt32) async throws {
        _ = deviceId
        _ = otkCount
        published = true
    }

    public func sendEncryptedChat(to: String, messageId: String, plaintext: Data) async throws -> UInt64 {
        var body = Self.chatMagic
        body.append(plaintext)
        return try await session.sendChat(to: to, messageId: messageId, body: body)
    }

    public func decryptChat(from: String, body: Data) throws -> Data {
        _ = from
        if body.starts(with: Self.shareMagic) {
            // Group key share — not a chat body.
            throw AppError.e2ee("not a chat payload")
        }
        guard body.starts(with: Self.chatMagic) else {
            throw AppError.e2ee("not encrypted")
        }
        return body.dropFirst(Self.chatMagic.count)
    }

    public func createGroupSession(groupId: String) throws -> String {
        groupSessions.insert(groupId)
        return "megolm-\(groupId)"
    }

    public func distributeGroupKey(groupId: String, members: [String]) async throws {
        guard groupSessions.contains(groupId) else { throw AppError.e2ee("no group session") }
        var share = Self.shareMagic
        share.append(Data(groupId.utf8))
        for member in members {
            let mid = "gsk-\(groupId)-\(member)"
            _ = try await session.sendChat(to: member, messageId: mid, body: share)
        }
    }

    public func sendEncryptedGroup(groupId: String, messageId: String, plaintext: Data) async throws {
        guard groupSessions.contains(groupId) || importedGroupKeys.contains(groupId) else {
            throw AppError.e2ee("no group session")
        }
        var body = Self.groupMagic
        body.append(Data(groupId.utf8))
        body.append(0)
        body.append(plaintext)
        try await session.sendGroup(groupId: groupId, messageId: messageId, body: body)
    }

    public func decryptGroup(groupId: String, body: Data) throws -> Data {
        guard body.starts(with: Self.groupMagic) else {
            throw AppError.e2ee("not encrypted group")
        }
        var rest = body.dropFirst(Self.groupMagic.count)
        guard let z = rest.firstIndex(of: 0) else { throw AppError.e2ee("bad group payload") }
        let gid = String(bytes: rest[..<z], encoding: .utf8) ?? ""
        guard gid == groupId else { throw AppError.e2ee("group mismatch") }
        rest = rest[rest.index(after: z)...]
        return Data(rest)
    }

    public func tryImportGroupKey(from: String, body: Data) throws -> Bool {
        _ = from
        guard body.starts(with: Self.shareMagic) else { return false }
        let gid = String(bytes: body.dropFirst(Self.shareMagic.count), encoding: .utf8) ?? ""
        guard !gid.isEmpty else { return false }
        importedGroupKeys.insert(gid)
        groupSessions.insert(gid)
        return true
    }

    public func exportPickle(passphrase: String) throws -> Data {
        let obj: [String: String] = ["identity": identity, "pass": passphrase]
        return try JSONSerialization.data(withJSONObject: obj)
    }
}

/// Keychain + Application Support pickle persistence.
public final class E2eePickleStore: @unchecked Sendable {
    private let keychain: KeychainStore
    private let fileURL: URL

    public init(keychain: KeychainStore = KeychainStore(), fileURL: URL? = nil) throws {
        self.keychain = keychain
        if let fileURL {
            self.fileURL = fileURL
        } else {
            let base = try FileManager.default.url(
                for: .applicationSupportDirectory,
                in: .userDomainMask,
                appropriateFor: nil,
                create: true
            )
            let dir = base.appendingPathComponent("LaneMessenger", isDirectory: true)
            try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
            self.fileURL = dir.appendingPathComponent("e2ee.pickle")
        }
    }

    public func load() throws -> Data? {
        if let data = try keychain.data(account: "e2ee_pickle"), !data.isEmpty {
            return data
        }
        guard FileManager.default.fileExists(atPath: fileURL.path) else { return nil }
        return try Data(contentsOf: fileURL)
    }

    public func save(_ pickle: Data) throws {
        try keychain.setData(pickle, account: "e2ee_pickle")
        try pickle.write(to: fileURL, options: .atomic)
    }

    public func clear() throws {
        try keychain.delete(account: "e2ee_pickle")
        if FileManager.default.fileExists(atPath: fileURL.path) {
            try FileManager.default.removeItem(at: fileURL)
        }
    }

    public func savePeerIdentity(userId: String, identityKey: String) throws {
        try keychain.set(identityKey, account: "e2ee_peer_\(userId)")
    }

    public func peerIdentity(userId: String) throws -> String? {
        try keychain.string(account: "e2ee_peer_\(userId)")
    }
}
