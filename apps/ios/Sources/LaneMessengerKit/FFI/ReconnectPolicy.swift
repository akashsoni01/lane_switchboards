import Foundation

/// Exponential backoff with jitter for reconnect attempts.
public struct ReconnectPolicy: Sendable, Equatable {
    public var baseDelayMs: UInt64
    public var maxDelayMs: UInt64
    public var maxAttempts: UInt32

    public init(
        baseDelayMs: UInt64 = 250,
        maxDelayMs: UInt64 = 30_000,
        maxAttempts: UInt32 = 0 // 0 = unlimited
    ) {
        self.baseDelayMs = baseDelayMs
        self.maxDelayMs = maxDelayMs
        self.maxAttempts = maxAttempts
    }

    public static let `default` = ReconnectPolicy()

    /// Delay before attempt `n` (1-based). Caps at `maxDelayMs`, adds ±20% jitter.
    public func delayMs(forAttempt attempt: UInt32) -> UInt64 {
        let exp = min(attempt &- 1, 16)
        let raw = min(baseDelayMs &<< exp, maxDelayMs)
        let jitterRange = max(raw / 5, 1)
        let jitter = UInt64.random(in: 0...(jitterRange * 2))
        let withJitter = raw > jitterRange ? raw - jitterRange + jitter : raw
        return min(withJitter, maxDelayMs)
    }

    public func shouldRetry(attempt: UInt32) -> Bool {
        maxAttempts == 0 || attempt <= maxAttempts
    }
}

/// Maps wire / FFI protocol error codes to UX actions.
public enum ProtocolErrorCode: Int, Sendable {
    case unspecified = 0
    case unsupportedVersion = 1
    case notAuthenticated = 2
    case authFailed = 3
    case replacedByNewSession = 4
    case frameTooLarge = 5
    case malformedFrame = 6
    case rateLimited = 7
    case unknownRecipient = 8
    case mediaTransferFailed = 9

    public init(raw: Int) {
        self = ProtocolErrorCode(rawValue: raw) ?? .unspecified
    }

    public var pausesReconnect: Bool {
        switch self {
        case .unsupportedVersion, .replacedByNewSession, .authFailed, .notAuthenticated:
            return true
        default:
            return false
        }
    }

    public var userMessage: String {
        switch self {
        case .unsupportedVersion:
            return "Please update the app to continue."
        case .notAuthenticated, .authFailed:
            return "Authentication failed. Sign in again."
        case .replacedByNewSession:
            return AppError.replacedByNewSession.errorDescription
        case .rateLimited:
            return "Too many requests. Try again later."
        case .frameTooLarge, .malformedFrame:
            return "A message could not be sent. Please retry."
        case .unknownRecipient:
            return "Recipient not found."
        case .mediaTransferFailed:
            return "Media transfer failed."
        case .unspecified:
            return "Protocol error."
        }
    }
}
