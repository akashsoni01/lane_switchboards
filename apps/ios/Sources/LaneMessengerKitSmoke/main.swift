import Foundation
import LaneMessengerKit

@main
struct Smoke {
    static func main() async {
        var failed = 0
        failed += check("parse login/sync", testParseEvents())
        failed += check("parse replaced + protocol", testParseProtocol())
        failed += check("reconnect delay monotonic cap", testBackoff())
        #if DEBUG
        failed += check("mint token shape", testMintShape())
        failed += await checkAsync("debug login mints", testDebugLogin)
        #endif
        failed += check("keychain device stable", testKeychain())
        failed += check("store idempotent + resume", testStore())
        failed += await checkAsync("session reaches ready", testSessionReady)
        failed += await checkAsync("replaced locks reconnect", testReplacedLock)
        failed += await checkAsync("auth failed maps", testAuthFailed)
        failed += await checkAsync("disconnect schedules reconnect", testReconnect)
        failed += await checkAsync("resume seq stamped on connect", testResumeStamp)

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
    let login = LaneEvent.parse(
        json: #"{"type":"LoginAck","session_id":"s1","pending_messages":3,"ok":true,"error":""}"#
    )
    let sync = LaneEvent.parse(json: #"{"type":"SyncComplete","delivered":2,"latest_seq":42}"#)
    return login == .loginAck(sessionId: "s1", ok: true, pendingMessages: 3, error: "")
        && sync == .syncComplete(latestSeq: 42, delivered: 2)
}

private func testParseProtocol() -> Bool {
    let replaced = LaneEvent.parse(json: #"{"type":"ReplacedByNewSession"}"#)
    let proto = LaneEvent.parse(json: #"{"type":"ProtocolError","code":1,"detail":"upgrade"}"#)
    return replaced == .replacedByNewSession
        && proto == .protocolError(code: 1, detail: "upgrade")
        && ProtocolErrorCode.unsupportedVersion.pausesReconnect
}

private func testBackoff() -> Bool {
    let p = ReconnectPolicy(baseDelayMs: 250, maxDelayMs: 30_000, maxAttempts: 0)
    let d1 = p.delayMs(forAttempt: 1)
    let d8 = p.delayMs(forAttempt: 8)
    return d1 <= 500 && d8 <= 30_000 && p.shouldRetry(attempt: 99)
}

#if DEBUG
private func testMintShape() -> Bool {
    let token = DebugAuthService.mintToken(
        userId: "alice",
        deviceId: "phone-1",
        sharedSecret: "demo-secret"
    )
    return token.count == 64 && token.allSatisfy(\.isHexDigit)
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

private func testStore() -> Bool {
    do {
        let store = InMemoryLocalStore()
        let m = StoredMessage(
            messageId: "m1",
            conversationId: "bob",
            direction: .inbound,
            body: "hi",
            seq: 7,
            status: .delivered
        )
        let first = try store.upsertMessage(m)
        let second = try store.upsertMessage(m)
        guard first, !second else { return false }
        guard try store.resumeAfterSeq() == 7 else { return false }
        try store.updateStatus(messageId: "m1", status: .read, seq: 9)
        guard try store.resumeAfterSeq() == 9 else { return false }
        let msgs = try store.messages(conversationId: "bob", limit: 10)
        return msgs.count == 1 && msgs[0].status == .read
    } catch {
        print("  store error: \(error)")
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
        #"{"type":"LoginAck","session_id":"s","pending_messages":0,"ok":true,"error":""}"#,
        #"{"type":"SyncComplete","delivered":0,"latest_seq":0}"#,
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
    let session = SessionActor(
        transport: transport,
        policy: ReconnectPolicy(baseDelayMs: 10, maxDelayMs: 20, maxAttempts: 1)
    )
    await session.setAutoReconnect(false)
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

private func testReconnect() async -> Bool {
    let transport = MockMessengerTransport()
    transport.disconnectAfterReady = true
    transport.events = [
        #"{"type":"LoginAck","session_id":"s","pending_messages":0,"ok":true,"error":""}"#,
        #"{"type":"SyncComplete","delivered":0,"latest_seq":0}"#,
    ]
    let session = SessionActor(
        transport: transport,
        policy: ReconnectPolicy(baseDelayMs: 20, maxDelayMs: 50, maxAttempts: 3)
    )
    let creds = AuthCredentials(userId: "alice", deviceId: "d1", authToken: "tok")
    do {
        try await session.connect(ConnectRequest(config: .default, credentials: creds))
        // Wait until offline then reconnect bumps connectCount.
        for _ in 0..<80 {
            if transport.connectCount >= 2 {
                await session.close()
                return true
            }
            try await Task.sleep(nanoseconds: 25_000_000)
        }
        await session.close()
        return transport.connectCount >= 2
    } catch {
        return false
    }
}

private func testResumeStamp() async -> Bool {
    let transport = MockMessengerTransport()
    let session = SessionActor(transport: transport)
    await session.setResumeSeqProvider { 99 }
    let creds = AuthCredentials(userId: "alice", deviceId: "d1", authToken: "tok")
    do {
        try await session.connect(
            ConnectRequest(config: .default, credentials: creds, resumeAfterSeq: 0)
        )
        let stamped = transport.lastRequest?.resumeAfterSeq == 99
        await session.close()
        return stamped
    } catch {
        return false
    }
}

private extension Character {
    var isHexDigit: Bool {
        ("0"..."9").contains(self) || ("a"..."f").contains(self) || ("A"..."F").contains(self)
    }
}
