import Foundation

public enum FeatureFlags: Sendable {
    /// E2EE on by default for staging/prod; DEBUG builds may override via UserDefaults.
    public static var e2eeEnabled: Bool {
        #if DEBUG
        if UserDefaults.standard.object(forKey: "E2EE_ENABLED") != nil {
            return UserDefaults.standard.bool(forKey: "E2EE_ENABLED")
        }
        #endif
        return true
    }

    /// DEBUG-only plaintext send. Always false in Release.
    public static var debugPlaintextFallback: Bool {
        #if DEBUG
        return UserDefaults.standard.bool(forKey: "DEBUG_PLAINTEXT_FALLBACK")
        #else
        return false
        #endif
    }
}
