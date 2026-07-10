import Foundation

/// Gateway + client identity settings (xcconfig / Info.plist / env).
public struct AppConfig: Sendable, Equatable {
    public var host: String
    public var port: UInt16
    public var useTls: Bool
    public var pingIntervalSecs: UInt64

    public init(
        host: String = AppConfig.defaultHost,
        port: UInt16 = AppConfig.defaultPort,
        useTls: Bool = AppConfig.defaultUseTls,
        pingIntervalSecs: UInt64 = 30
    ) {
        self.host = host
        self.port = port
        self.useTls = useTls
        self.pingIntervalSecs = pingIntervalSecs
    }

    public static var `default`: AppConfig { AppConfig() }

    public static var defaultHost: String {
        if let v = Bundle.main.object(forInfoDictionaryKey: "MESSENGER_HOST") as? String, !v.isEmpty {
            return v
        }
        return ProcessInfo.processInfo.environment["MESSENGER_HOST"] ?? "127.0.0.1"
    }

    public static var defaultPort: UInt16 {
        if let v = Bundle.main.object(forInfoDictionaryKey: "MESSENGER_PORT") as? String,
           let p = UInt16(v) {
            return p
        }
        if let v = ProcessInfo.processInfo.environment["MESSENGER_PORT"], let p = UInt16(v) {
            return p
        }
        return 9000
    }

    public static var defaultUseTls: Bool {
        #if DEBUG
        if let v = Bundle.main.object(forInfoDictionaryKey: "MESSENGER_USE_TLS") as? String {
            return v == "1" || v.lowercased() == "true"
        }
        return false
        #else
        return true
        #endif
    }

    public static var clientVersion: String {
        let marketing = Bundle.main.infoDictionary?["CFBundleShortVersionString"] as? String ?? "0.0"
        let build = Bundle.main.infoDictionary?["CFBundleVersion"] as? String ?? "0"
        return "ios-\(marketing)(\(build))"
    }
}
