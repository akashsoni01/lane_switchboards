import Foundation

public protocol AuthService: Sendable {
    /// Resolve credentials for connect. Must not log the token.
    func login(userId: String, secret: String) async throws -> AuthCredentials
}

/// Production path: HTTPS identity service mints tokens (no HMAC secret in app).
public struct HttpAuthService: AuthService {
    public var endpoint: URL
    public var session: URLSession
    public var deviceIdProvider: @Sendable () throws -> String

    public init(
        endpoint: URL,
        session: URLSession = .shared,
        deviceIdProvider: @escaping @Sendable () throws -> String
    ) {
        self.endpoint = endpoint
        self.session = session
        self.deviceIdProvider = deviceIdProvider
    }

    private struct LoginRequest: Encodable {
        let user_id: String
        let password: String
        let device_id: String
    }

    private struct LoginResponse: Decodable {
        let user_id: String
        let device_id: String
        let auth_token: String
    }

    public func login(userId: String, secret: String) async throws -> AuthCredentials {
        let deviceId = try deviceIdProvider()
        var req = URLRequest(url: endpoint)
        req.httpMethod = "POST"
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")
        req.httpBody = try JSONEncoder().encode(
            LoginRequest(user_id: userId, password: secret, device_id: deviceId)
        )
        let (data, response) = try await session.data(for: req)
        guard let http = response as? HTTPURLResponse else {
            throw AppError.authFailed("invalid response")
        }
        guard (200..<300).contains(http.statusCode) else {
            throw AppError.authFailed("login HTTP \(http.statusCode)")
        }
        let body = try JSONDecoder().decode(LoginResponse.self, from: data)
        return AuthCredentials(
            userId: body.user_id,
            deviceId: body.device_id,
            authToken: body.auth_token
        )
    }
}

#if DEBUG
import CryptoKit

/// DEBUG-only: mint HMAC tokens matching `HmacAuthenticator` / messenger_demo.
/// Compile-stripped from Release via `#if DEBUG`.
public struct DebugAuthService: AuthService {
    public var sharedSecret: String
    public var deviceIdProvider: @Sendable () throws -> String

    public init(
        sharedSecret: String = "demo-secret",
        deviceIdProvider: @escaping @Sendable () throws -> String
    ) {
        self.sharedSecret = sharedSecret
        self.deviceIdProvider = deviceIdProvider
    }

    public func login(userId: String, secret: String) async throws -> AuthCredentials {
        let deviceId = try deviceIdProvider()
        // `secret` may be a pasted token OR empty to mint with sharedSecret.
        let token: String
        if secret.count >= 32, secret.allSatisfy(\.isHexDigit) {
            token = secret.lowercased()
        } else {
            token = Self.mintToken(userId: userId, deviceId: deviceId, sharedSecret: sharedSecret)
        }
        return AuthCredentials(userId: userId, deviceId: deviceId, authToken: token)
    }

    /// `hex(HMAC-SHA256(secret, "user_id:device_id"))` — parity with Rust.
    public static func mintToken(userId: String, deviceId: String, sharedSecret: String) -> String {
        let key = SymmetricKey(data: Data(sharedSecret.utf8))
        let mac = HMAC<SHA256>.authenticationCode(
            for: Data("\(userId):\(deviceId)".utf8),
            using: key
        )
        return mac.map { String(format: "%02x", $0) }.joined()
    }
}

private extension Character {
    var isHexDigit: Bool {
        ("0"..."9").contains(self) || ("a"..."f").contains(self) || ("A"..."F").contains(self)
    }
}
#endif
