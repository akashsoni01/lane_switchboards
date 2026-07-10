import Foundation
import LaneMessengerKit

@main
struct Smoke {
    static func main() async {
        var failed = 0
        failed += check("parse login/sync", testParseEvents())
        failed += check("parse replaced", testParseReplaced())
        #if DEBUG
        failed += check("mint token shape", testMintShape())
        failed += await checkAsync("debug login mints", testDebugLogin)
        #endif
        failed += check("keychain device stable", testKeychain())
        failed += await checkAsync("session reaches ready", testSessionReady)
        failed += await checkAsync("replaced locks reconnect", testReplacedLock)
        failed += await checkAsync("auth failed maps", testAuthFailed)

        if failed > 0 {
            fputs("FAILED \(failed) check(s)\n", stderr)
            exit(1)
        }
        print("OK — all smoke checks passed")
    }
}

private func check(_ name: String, _ ok: Bool) -> Int {
    if ok {
        print("ok  \(name)")
        return 0
    }
    print("FAIL \(name)")
    return 1
}

private func checkAsync(_ name: String, _ body: () async -> Bool) async -> Int {
    check(name, await body())
}

private func testParseEvents() -> Bool {
    let login = LaneEvent.parse(json: #"{"type":"LoginAck","session_id":"s1","ok":true}"#)
    let sync = LaneEvent.parse(json: #"{"type":"SyncComplete","latest_seq":42}"#)
    return login == .loginAck(sessionId: "s1", ok: true)
        && sync == .syncComplete(latestSeq: 42)
}

private func testParseReplaced() -> Bool {
    LaneEvent.parse(json: #"{"type":"ReplacedByNewSession"}"#) == .replacedByNewSession
}

#if DEBUG
private func testMintShape() -> Bool {
    let token = DebugAuthService.mintToken(
        userId: "alice",
        deviceId: "phone-1",
        sharedSecret: "demo-secret"
    )
    return token.count == 64 && token.allSatisfy { $0.isHexDigit }
}

private func testDebugLogin() async -> Bool {
    let svc = DebugAuthService(sharedSecret: "demo-secret") { "device-fixed" }
    do {
        let creds = try await svc.login(userId: "alice", secret: "")
        return creds.userId == "alice"
            && creds.deviceId == "device-fixed"
            && creds.authToken.count == 64
    } catch {
        return false
    }
}
#endif

private func testKeychain() -> Bool {
    do {
        let service = "com.lane.messenger.smoke.\(UUID().uuidString)"
        let store = CredentialStore(keychain: KeychainStore(service: service))
        let d1 = try store.deviceId()
        let d2 = try store.deviceId()
        guard d1 == d2 else { return false }
        try store.save(AuthCredentials(userId: "alice", deviceId: d1, authToken: "abc123"))
        guard try store.load() != nil else { return false }
        try store.clearSession()
        guard try store.load() == nil else { return false }
        return try store.deviceId() == d1
    } catch {
        print("  keychain error: \(error)")
        return false
    }
}

private func testSessionReady() async -> Bool {
    let transport = MockMessengerTransport()
    let session = SessionActor(transport: transport)
    let creds = AuthCredentials(userId: "alice", deviceId: "d1", authToken: "tok")
    do {
        try await session.connect(ConnectRequest(config: .default, credentials: creds))
        for _ in 0..<40 {
            if await session.state == .ready {
                await session.close()
                return true
            }
            try await Task.sleep(nanoseconds: 25_000_000)
        }
        await session.close()
        return false
    } catch {
        return false
    }
}

private func testReplacedLock() async -> Bool {
    let transport = MockMessengerTransport()
    transport.events = [
        #"{"type":"LoginAck","session_id":"s","ok":true}"#,
        #"{"type":"SyncComplete","latest_seq":0}"#,
        #"{"type":"ReplacedByNewSession"}"#,
    ]
    let session = SessionActor(transport: transport)
    let creds = AuthCredentials(userId: "alice", deviceId: "d1", authToken: "tok")
    do {
        try await session.connect(ConnectRequest(config: .default, credentials: creds))
        var saw = false
        for _ in 0..<40 {
            if await session.state == .replaced {
                saw = true
                break
            }
            try await Task.sleep(nanoseconds: 25_000_000)
        }
        guard saw else { return false }
        do {
            try await session.connect(ConnectRequest(config: .default, credentials: creds))
            return false
        } catch let err as AppError {
            return err == .replacedByNewSession
        }
    } catch {
        return false
    }
}

private func testAuthFailed() async -> Bool {
    let transport = MockMessengerTransport()
    transport.connectError = .authFailed("bad token")
    let session = SessionActor(transport: transport)
    let creds = AuthCredentials(userId: "alice", deviceId: "d1", authToken: "bad")
    do {
        try await session.connect(ConnectRequest(config: .default, credentials: creds))
        return false
    } catch let err as AppError {
        return err == .authFailed("bad token")
    } catch {
        return false
    }
}

private extension Character {
    var isHexDigit: Bool {
        ("0"..."9").contains(self) || ("a"..."f").contains(self) || ("A"..."F").contains(self)
    }
}
