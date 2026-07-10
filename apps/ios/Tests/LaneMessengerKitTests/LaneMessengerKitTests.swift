import XCTest
@testable import LaneMessengerKit

/// XCTest suite for full Xcode. Prefer `swift run lane-messenger-kit-smoke` on CLT-only hosts.
final class LaneMessengerKitTests: XCTestCase {
    func testParseLogin() {
        let login = LaneEvent.parse(
            json: #"{"type":"LoginAck","session_id":"s1","pending_messages":1,"ok":true,"error":""}"#
        )
        XCTAssertEqual(login, .loginAck(sessionId: "s1", ok: true, pendingMessages: 1, error: ""))
    }

    func testStoreIdempotent() throws {
        let store = InMemoryLocalStore()
        let m = StoredMessage(
            messageId: "m1",
            conversationId: "bob",
            direction: .inbound,
            body: "hi",
            seq: 3,
            status: .delivered
        )
        XCTAssertTrue(try store.upsertMessage(m))
        XCTAssertFalse(try store.upsertMessage(m))
        XCTAssertEqual(try store.resumeAfterSeq(), 3)
    }
}
