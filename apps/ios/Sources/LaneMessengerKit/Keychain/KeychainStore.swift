import Foundation
import Security

/// Thin Keychain wrapper. Secrets never go to UserDefaults or logs.
public struct KeychainStore: Sendable {
    public let service: String
    public let accessGroup: String?

    public init(service: String = "com.lane.messenger", accessGroup: String? = nil) {
        self.service = service
        self.accessGroup = accessGroup
    }

    public func set(_ value: String, account: String) throws {
        guard let data = value.data(using: .utf8) else {
            throw AppError.keychain("invalid utf-8")
        }
        try setData(data, account: account)
    }

    public func setData(_ data: Data, account: String) throws {
        try delete(account: account)
        var query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: account,
            kSecValueData as String: data,
            kSecAttrAccessible as String: kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly,
        ]
        if let accessGroup {
            query[kSecAttrAccessGroup as String] = accessGroup
        }
        let status = SecItemAdd(query as CFDictionary, nil)
        guard status == errSecSuccess else {
            throw AppError.keychain("SecItemAdd \(status)")
        }
    }

    public func string(account: String) throws -> String? {
        guard let data = try data(account: account) else { return nil }
        return String(data: data, encoding: .utf8)
    }

    public func data(account: String) throws -> Data? {
        var query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: account,
            kSecReturnData as String: true,
            kSecMatchLimit as String: kSecMatchLimitOne,
        ]
        if let accessGroup {
            query[kSecAttrAccessGroup as String] = accessGroup
        }
        var item: CFTypeRef?
        let status = SecItemCopyMatching(query as CFDictionary, &item)
        if status == errSecItemNotFound { return nil }
        guard status == errSecSuccess else {
            throw AppError.keychain("SecItemCopyMatching \(status)")
        }
        return item as? Data
    }

    public func delete(account: String) throws {
        var query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: account,
        ]
        if let accessGroup {
            query[kSecAttrAccessGroup as String] = accessGroup
        }
        let status = SecItemDelete(query as CFDictionary)
        guard status == errSecSuccess || status == errSecItemNotFound else {
            throw AppError.keychain("SecItemDelete \(status)")
        }
    }
}
