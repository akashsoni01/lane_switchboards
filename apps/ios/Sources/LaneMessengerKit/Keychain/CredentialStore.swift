import Foundation

public struct AuthCredentials: Sendable, Equatable {
    public var userId: String
    public var deviceId: String
    public var authToken: String

    public init(userId: String, deviceId: String, authToken: String) {
        self.userId = userId
        self.deviceId = deviceId
        self.authToken = authToken
    }

    public var isComplete: Bool {
        !userId.isEmpty && !deviceId.isEmpty && !authToken.isEmpty
    }
}

/// Persists identity in Keychain. `device_id` survives logout; token does not.
public final class CredentialStore: @unchecked Sendable {
    private enum Account {
        static let userId = "user_id"
        static let deviceId = "device_id"
        static let authToken = "auth_token"
    }

    private let keychain: KeychainStore

    public init(keychain: KeychainStore = KeychainStore()) {
        self.keychain = keychain
    }

    /// Stable device id: create once, keep across logout/reinstall-of-token.
    public func deviceId() throws -> String {
        if let existing = try keychain.string(account: Account.deviceId), !existing.isEmpty {
            return existing
        }
        let id = UUID().uuidString.lowercased()
        try keychain.set(id, account: Account.deviceId)
        LaneLog.auth.info("created new device_id")
        return id
    }

    public func load() throws -> AuthCredentials? {
        let deviceId = try deviceId()
        guard let userId = try keychain.string(account: Account.userId), !userId.isEmpty,
              let token = try keychain.string(account: Account.authToken), !token.isEmpty
        else {
            return nil
        }
        return AuthCredentials(userId: userId, deviceId: deviceId, authToken: token)
    }

    public func save(_ credentials: AuthCredentials) throws {
        try keychain.set(credentials.userId, account: Account.userId)
        try keychain.set(credentials.deviceId, account: Account.deviceId)
        try keychain.set(credentials.authToken, account: Account.authToken)
        LaneLog.auth.info("saved credentials for user_id length=\(credentials.userId.count, privacy: .public)")
    }

    /// Clears token + user id; keeps `device_id`.
    public func clearSession() throws {
        try keychain.delete(account: Account.userId)
        try keychain.delete(account: Account.authToken)
        LaneLog.auth.info("cleared session credentials (device_id retained)")
    }
}
