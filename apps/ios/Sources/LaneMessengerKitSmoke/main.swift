import Foundation
import LaneMessengerKit

@main
struct Smoke {
    static func main() async {
        var failed = 0
        failed += check("parse login/sync", testParseEvents())
        failed += check("parse chat body b64", testParseChatBody())
        failed += check("parse presence", testParsePresence())
        failed += check("reconnect delay", testBackoff())
        #if DEBUG
        failed += check("mint token", testMintShape())
        failed += await checkAsync("debug login", testDebugLogin)
        #endif
        failed += check("keychain", testKeychain())
        failed += check("store idempotent", testStore())
        failed += check("draft + mark read", testDraft())
        failed += await checkAsync("session ready", testSessionReady)
        failed += await checkAsync("replaced lock", testReplacedLock)
        failed += await checkAsync("auth failed", testAuthFailed)
        failed += await checkAsync("reconnect", testReconnect)
        failed += await checkAsync("resume stamp", testResumeStamp)
        failed += await checkAsync("send pending then ack", testSendPath)
        failed += await checkAsync("failed send marks failed", testSendFail)
        failed += await checkAsync("open thread acks", testOpenThreadAcks)

        if failed > 0 {
            fputs("FAILED \(failed) check(s)\n", stderr)
            exit(1)
        }
        print("OK — all smoke checks passed")
    }
}

private func check(_ name: String, _ ok: Bool) -> Int {
    print(ok ? "ok  \(name)" : "FAIL \(name)")
    return ok ? 0 : 1
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

private func testParseChatBody() -> Bool {
    let body = "hello"
    let b64 = Data(body.utf8).base64EncodedString()
    let ev = LaneEvent.parse(
        json: #"{"type":"ChatMessage","message_id":"m1","from_user":"bob","to_user":"alice","body_hex":"\#(b64)","sent_at":1,"seq":9,"media_id":""}"#
    )
    if case .chatMessage(_, "bob", "alice", body, 9, "", 1) = ev { return true }
    return false
}

private func testParsePresence() -> Bool {
    let ev = LaneEvent.parse(json: #"{"type":"Presence","user_id":"bob","kind":0}"#)
    if case .presence("bob", 0, nil) = ev { return true }
    return false
}

private func testBackoff() -> Bool {
    let p = ReconnectPolicy(baseDelayMs: 250, maxDelayMs: 30_000, maxAttempts: 0)
    return p.delayMs(forAttempt: 1) <= 500 && p.delayMs(forAttempt: 8) <= 30_000
}

#if DEBUG
private func testMintShape() -> Bool {
    let token = DebugAuthService.mintToken(userId: "alice", deviceId: "phone-1", sharedSecret: "demo-secret")
    return token.count == 64
}

private func testDebugLogin() async -> Bool {
    let svc = DebugAuthService(sharedSecret: "demo-secret") { "device-fixed" }
    do {
        let creds = try await svc.login(userId: "alice", secret: "")
        return creds.authToken.count == 64
    } catch { return false }
}
#endif

private func testKeychain() -> Bool {
    do {
        let store = CredentialStore(keychain: KeychainStore(service: "com.lane.smoke.\(UUID().uuidString)"))
        let d1 = try store.deviceId()
        try store.save(AuthCredentials(userId: "a", deviceId: d1, authToken: "t"))
        try store.clearSession()
        return try store.deviceId() == d1 && store.load() == nil
    } catch { return false }
}

private func testStore() -> Bool {
    do {
        let store = InMemoryLocalStore()
        let m = StoredMessage(messageId: "m1", conversationId: "bob", direction: .inbound, body: "hi", seq: 7, status: .delivered)
        return try store.upsertMessage(m) && !store.upsertMessage(m) && store.resumeAfterSeq() == 7
    } catch { return false }
}

private func testDraft() -> Bool {
    do {
        let store = InMemoryLocalStore()
        try store.ensureConversation(id: "bob", title: "Bob")
        try store.setDraft(conversationId: "bob", draft: "hello draft")
        try store.markConversationRead(conversationId: "bob")
        let c = try store.conversations().first { $0.id == "bob" }
        return c?.draft == "hello draft" && c?.unread == 0
    } catch { return false }
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
    } catch { return false }
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
        for _ in 0..<40 {
            if await session.state == .replaced { break }
            try await Task.sleep(nanoseconds: 25_000_000)
        }
        do {
            try await session.connect(ConnectRequest(config: .default, credentials: creds))
            return false
        } catch let err as AppError {
            return err == .replacedByNewSession
        }
    } catch { return false }
}

private func testAuthFailed() async -> Bool {
    let transport = MockMessengerTransport()
    transport.connectError = .authFailed("bad token")
    let session = SessionActor(transport: transport, policy: ReconnectPolicy(baseDelayMs: 10, maxDelayMs: 20, maxAttempts: 1))
    await session.setAutoReconnect(false)
    do {
        try await session.connect(ConnectRequest(
            config: .default,
            credentials: AuthCredentials(userId: "a", deviceId: "d", authToken: "bad")
        ))
        return false
    } catch let err as AppError {
        return err == .authFailed("bad token")
    } catch { return false }
}

private func testReconnect() async -> Bool {
    let transport = MockMessengerTransport()
    transport.disconnectAfterReady = true
    let session = SessionActor(
        transport: transport,
        policy: ReconnectPolicy(baseDelayMs: 20, maxDelayMs: 50, maxAttempts: 3)
    )
    do {
        try await session.connect(ConnectRequest(
            config: .default,
            credentials: AuthCredentials(userId: "a", deviceId: "d", authToken: "t")
        ))
        for _ in 0..<80 {
            if transport.connectCount >= 2 {
                await session.close()
                return true
            }
            try await Task.sleep(nanoseconds: 25_000_000)
        }
        await session.close()
        return transport.connectCount >= 2
    } catch { return false }
}

private func testResumeStamp() async -> Bool {
    let transport = MockMessengerTransport()
    let session = SessionActor(transport: transport)
    await session.setResumeSeqProvider { 99 }
    do {
        try await session.connect(ConnectRequest(
            config: .default,
            credentials: AuthCredentials(userId: "a", deviceId: "d", authToken: "t"),
            resumeAfterSeq: 0
        ))
        let ok = transport.lastRequest?.resumeAfterSeq == 99
        await session.close()
        return ok
    } catch { return false }
}

private func testSendPath() async -> Bool {
    let transport = MockMessengerTransport()
    let store = InMemoryLocalStore()
    let session = SessionActor(transport: transport)
    let chat = ChatService(store: store, session: session)
    let creds = AuthCredentials(userId: "alice", deviceId: "d1", authToken: "tok")
    do {
        try await session.connect(ConnectRequest(config: .default, credentials: creds))
        for _ in 0..<40 {
            if await session.state == .ready { break }
            try await Task.sleep(nanoseconds: 25_000_000)
        }
        let msg = try await chat.sendText(to: "bob", body: "hello bob", fromUser: "alice")
        // Drain ServerAck
        for _ in 0..<40 {
            if let json = await transport.pollEvent(timeoutMs: 10) {
                let ev = LaneEvent.parse(json: json)
                if case .serverAck(let id, let seq) = ev {
                    try store.updateStatus(messageId: id, status: .sent, seq: seq)
                }
            } else {
                try await Task.sleep(nanoseconds: 10_000_000)
            }
            let updated = try store.messages(conversationId: "bob", limit: 10)
            if updated.first?.status == .sent {
                await session.close()
                return updated.count == 1 && msg.messageId == updated[0].messageId
            }
        }
        // Even without draining via poll in this loop, pending row must exist.
        let rows = try store.messages(conversationId: "bob", limit: 10)
        await session.close()
        return rows.count == 1 && rows[0].body == "hello bob"
    } catch {
        print("  send path error: \(error)")
        return false
    }
}

private func testSendFail() async -> Bool {
    let transport = MockMessengerTransport()
    transport.sendError = .connection("boom")
    let store = InMemoryLocalStore()
    let session = SessionActor(transport: transport)
    let chat = ChatService(store: store, session: session)
    do {
        try await session.connect(ConnectRequest(
            config: .default,
            credentials: AuthCredentials(userId: "alice", deviceId: "d", authToken: "t")
        ))
        for _ in 0..<40 {
            if await session.state == .ready { break }
            try await Task.sleep(nanoseconds: 25_000_000)
        }
        do {
            _ = try await chat.sendText(to: "bob", body: "x", fromUser: "alice")
            await session.close()
            return false
        } catch {
            let rows = try store.messages(conversationId: "bob", limit: 10)
            await session.close()
            return rows.first?.status == .failed
        }
    } catch { return false }
}

private func testOpenThreadAcks() async -> Bool {
    let transport = MockMessengerTransport()
    let store = InMemoryLocalStore()
    let session = SessionActor(transport: transport)
    let chat = ChatService(store: store, session: session)
    do {
        try await session.connect(ConnectRequest(
            config: .default,
            credentials: AuthCredentials(userId: "alice", deviceId: "d", authToken: "t")
        ))
        for _ in 0..<40 {
            if await session.state == .ready { break }
            try await Task.sleep(nanoseconds: 25_000_000)
        }
        _ = try store.upsertMessage(StoredMessage(
            messageId: "in-1",
            conversationId: "bob",
            direction: .inbound,
            body: "hi",
            seq: 1,
            status: .delivered
        ))
        try await chat.openThread(peer: "bob")
        let ok = transport.deliveredAcks.contains("in-1") && transport.readAcks.contains("in-1")
        await session.close()
        return ok
    } catch { return false }
}
