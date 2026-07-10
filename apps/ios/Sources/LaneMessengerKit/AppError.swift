import Foundation

/// Typed errors for UI and session layers. Never embed secrets in `errorDescription`.
public enum AppError: Error, Equatable, Sendable {
    case notConfigured
    case notAuthenticated
    case authFailed(String)
    case replacedByNewSession
    case connection(String)
    case protocolError(code: String, message: String)
    case keychain(String)
    case cancelled
    case internalError(String)

    public var errorDescription: String {
        switch self {
        case .notConfigured:
            return "Messenger is not configured."
        case .notAuthenticated:
            return "Please sign in."
        case .authFailed(let detail):
            return detail.isEmpty ? "Authentication failed." : detail
        case .replacedByNewSession:
            return "This device was signed in elsewhere. Sign in again to continue."
        case .connection(let detail):
            return detail.isEmpty ? "Connection failed." : detail
        case .protocolError(_, let message):
            return message.isEmpty ? "Protocol error." : message
        case .keychain(let detail):
            return detail.isEmpty ? "Keychain error." : detail
        case .cancelled:
            return "Cancelled."
        case .internalError(let detail):
            return detail.isEmpty ? "Internal error." : detail
        }
    }
}
