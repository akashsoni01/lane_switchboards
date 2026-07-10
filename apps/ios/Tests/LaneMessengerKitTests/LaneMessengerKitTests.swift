import XCTest
@testable import LaneMessengerKit

final class LaneEventTests: XCTestCase {
    func testParseLoginAndSync() {
        let login = LaneEvent.parse(json: #"{"type":"LoginAck","session_id":"s1","ok":true}"#)
        XCTAssertEqual(login, .loginAck(sessionId: "s1", ok: true))
        let sync = LaneEvent.parse(json: #"{"type":"SyncComplete","latest_seq":42}"#)
        XCTAssertEqual(sync, .syncComplete(latestSeq: 42))
    }

    func testParseReplaced() {
        let ev = LaneEvent.parse(json: #"{"type":"ReplacedByNewSession"}"#)
        XCTAssertEqual(ev, .replacedByNewSession)
    }
}

final class DebugAuthTests: XCTestCase {
    #if DEBUG
    func testMintMatchesKnownShape() {
        let token = DebugAuthService.mintToken(
            userId: "alice",
            deviceId: "phone-1",
            sharedSecret: "demo-secret"
        )
        XCTAssertEqual(token.count, 64)
        XCTAssertTrue(token.allSatisfy { $0.isHexDigit })
    }

    func testLoginMintsWhenSecretEmpty() async throws {
        let svc = DebugAuthService(sharedSecret: "demo-secret") { "device-fixed" }
        let creds = try await svc.login(userId: "alice", secret: "")
        XCTAssertEqual(creds.userId, "alice")
        XCTAssertEqual(creds.deviceId, "device-fixed")
        XCTAssertEqual(creds.authToken.count, 64)
    }
    #endif
}

final class CredentialStoreTests: XCTestCase {
    func testDeviceIdStableAndLogoutKeepsDevice() throws {
        let service = "com.lane.messenger.tests.\(UUID().uuidString)"
        let store = CredentialStore(keychain: KeychainStore(service: service))
        let d1 = try store.deviceId()
        let d2 = try store.deviceId()
        XCTAssertEqual(d1, d2)

        try store.save(AuthCredentials(userId: "alice", deviceId: d1, authToken: "abc123"))
        XCTAssertNotNil(try store.load())
        try store.clearSession()
        XCTAssertNil(try store.load())
        XCTAssertEqual(try store.deviceId(), d1)
    }
}

final class SessionActorTests: XCTestCase {
    func testConnectReachesReady() async throws {
        let transport = MockMessengerTransport()
        let session = SessionActor(transport: transport)
        let creds = AuthCredentials(userId: "alice", deviceId: "d1", authToken: "tok")
        try await session.connect(ConnectRequest(config: .default, credentials: creds))

        var sawReady = false
        for _ in 0..<40 {
            if await session.state == .ready {
                sawReady = true
                break
            }
            try await Task.sleep(nanoseconds: 25_000_000)
        }
        XCTAssertTrue(sawReady)
        await session.close()
    }

    func testReplacedLocksReconnect() async throws {
        let transport = MockMessengerTransport()
        transport.events = [
            #"{"type":"LoginAck","session_id":"s","ok":true}"#,
            #"{"type":"SyncComplete","latest_seq":0}"#,
            #"{"type":"ReplacedByNewSession"}"#,
        ]
        let session = SessionActor(transport: transport)
        let creds = AuthCredentials(userId: "alice", deviceId: "d1", authToken: "tok")
        try await session.connect(ConnectRequest(config: .default, credentials: creds))

        var sawReplaced = false
        for _ in 0..<40 {
            if await session.state == .replaced {
                sawReplaced = true
                break
            }
            try await Task.sleep(nanoseconds: 25_000_000)
        }
        XCTAssertTrue(sawReplaced)

        do {
            try await session.connect(ConnectRequest(config: .default, credentials: creds))
            XCTFail("expected replaced lock")
        } catch let err as AppError {
            XCTAssertEqual(err, .replacedByNewSession)
        }

        await session.acknowledgeReplacement()
        transport.events = [
            #"{"type":"LoginAck","session_id":"s2","ok":true}"#,
            #"{"type":"SyncComplete","latest_seq":0}"#,
        ]
        // Reset mock connect flags for second connect
        let transport2 = MockMessengerTransport()
        let session2 = SessionActor(transport: transport2)
        try await session2.connect(ConnectRequest(config: .default, credentials: creds))
        await session2.close()
    }

    func testAuthFailedMapsFromTransport() async {
        let transport = MockMessengerTransport()
        transport.connectError = .authFailed("bad token")
        let session = SessionActor(transport: transport)
        let creds = AuthCredentials(userId: "alice", deviceId: "d1", authToken: "bad")
        do {
            try await session.connect(ConnectRequest(config: .default, credentials: creds))
            XCTFail("expected auth failure")
        } catch let err as AppError {
            XCTAssertEqual(err, .authFailed("bad token"))
        }
    }
}

private extension Character {
    var isHexDigit: Bool {
        ("0"..."9").contains(self) || ("a"..."f").contains(self) || ("A"..."F").contains(self)
    }
}
