import Foundation

/// Drop this file into an iOS/macOS app target that links `LaneMessengerFFI`
/// (C ABI wrapper) or the UniFFI-generated Swift sources.
///
/// Expects a local messenger gateway on 127.0.0.1:9000.
@main
struct DemoMain {
    static func main() throws {
        let token = ProcessInfo.processInfo.environment["LANE_AUTH_TOKEN"] ?? "demo-token"
        let session = try LaneSession(
            host: "127.0.0.1",
            port: 9000,
            useTls: false,
            userId: ProcessInfo.processInfo.environment["LANE_USER"] ?? "alice",
            deviceId: ProcessInfo.processInfo.environment["LANE_DEVICE"] ?? "ios-demo-1",
            authToken: token,
            clientVersion: "ios-ffi-demo"
        )
        defer { try? session.close() }

        try session.ping()
        print("ping ok; polling events…")

        let deadline = Date().addingTimeInterval(5)
        var sawLogin = false
        var sawSync = false
        while Date() < deadline {
            if let ev = session.pollEvent(timeoutMs: 200) {
                print(ev)
                if ev.contains("LoginAck") { sawLogin = true }
                if ev.contains("SyncComplete") { sawSync = true }
            }
            if sawLogin && sawSync { break }
        }

        let mid = "ios-demo-\(UUID().uuidString)"
        let seq = try session.sendChat(
            to: ProcessInfo.processInfo.environment["LANE_PEER"] ?? "bob",
            messageId: mid,
            body: Data("hello from ios".utf8)
        )
        print("sent chat seq=\(seq) id=\(mid)")

        for _ in 0..<20 {
            if let ev = session.pollEvent(timeoutMs: 200) {
                print(ev)
            }
        }
    }
}
