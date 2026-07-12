import Foundation
import LaneMessengerC

/// Thin Swift wrapper over the C ABI (`lane_messenger_ffi.h` via `LaneMessengerC`).
///
/// **Do not** compile UniFFI `generated/lane_messenger.swift` into this target —
/// it also defines `LaneSession` and would conflict. Use either:
/// - this hand-written C ABI module (default SPM product), or
/// - UniFFI-generated Swift + XCFramework (separate integration path).
///
/// Link `liblane_messenger_ffi` / `LaneMessengerFFI.xcframework` at the app level.
public final class LaneSession: @unchecked Sendable {
    private var ptr: OpaquePointer?

    /// Opaque session pointer for companion C APIs (E2EE).
    public var opaquePointer: OpaquePointer? { ptr }

    /// Raw mutable pointer when needed by hosts.
    public var rawPointer: UnsafeMutableRawPointer? {
        ptr.map { UnsafeMutableRawPointer($0) }
    }

    public init(
        host: String,
        port: UInt16,
        useTls: Bool,
        userId: String,
        deviceId: String,
        authToken: String,
        clientVersion: String = "ios-ffi",
        resumeAfterSeq: UInt64 = 0,
        pingIntervalSecs: UInt64 = 30
    ) throws {
        var err: UnsafeMutablePointer<CChar>?
        let session = host.withCString { h in
            userId.withCString { u in
                deviceId.withCString { d in
                    authToken.withCString { t in
                        clientVersion.withCString { v in
                            lane_session_connect(
                                h, port, useTls ? 1 : 0, u, d, t, v,
                                resumeAfterSeq, pingIntervalSecs, &err
                            )
                        }
                    }
                }
            }
        }
        if let session {
            self.ptr = session
        } else {
            let msg = err.map { String(cString: $0) } ?? "connect failed"
            if let err { lane_string_free(err) }
            throw LaneFFIError.connect(msg)
        }
    }

    deinit {
        if let ptr {
            lane_session_free(ptr)
        }
    }

    public func ping() throws {
        try check(lane_session_ping(ptr))
    }

    public func setResumeSeq(_ seq: UInt64) throws {
        try check(lane_session_set_resume_seq(ptr, seq))
    }

    public func sendChat(to: String, messageId: String, body: Data) throws -> UInt64 {
        try sendChat(to: to, messageId: messageId, body: body, mediaId: "")
    }

    public func sendChat(to: String, messageId: String, body: Data, mediaId: String) throws -> UInt64 {
        var seq: UInt64 = 0
        let code = body.withUnsafeBytes { raw in
            to.withCString { t in
                messageId.withCString { m in
                    mediaId.withCString { mid in
                        if mediaId.isEmpty {
                            return lane_send_chat(
                                ptr, t, m,
                                raw.bindMemory(to: UInt8.self).baseAddress,
                                body.count, &seq
                            )
                        }
                        return lane_send_chat_with_media(
                            ptr, t, m,
                            raw.bindMemory(to: UInt8.self).baseAddress,
                            body.count, mid, &seq
                        )
                    }
                }
            }
        }
        try check(code)
        return seq
    }

    public func sendChatRetry(
        to: String,
        messageId: String,
        body: Data,
        maxAttempts: UInt32 = 3
    ) throws -> UInt64 {
        var seq: UInt64 = 0
        let code = body.withUnsafeBytes { raw in
            to.withCString { t in
                messageId.withCString { m in
                    lane_send_chat_retry(
                        ptr, t, m,
                        raw.bindMemory(to: UInt8.self).baseAddress,
                        body.count, maxAttempts, &seq
                    )
                }
            }
        }
        try check(code)
        return seq
    }

    public func ackDelivered(messageId: String) throws {
        let code = messageId.withCString { m in
            lane_ack_delivered(ptr, m)
        }
        try check(code)
    }

    public func ackRead(messageId: String) throws {
        let code = messageId.withCString { m in
            lane_ack_read(ptr, m)
        }
        try check(code)
    }

    public func subscribePresence(contactIds: [String]) throws {
        let csv = contactIds.joined(separator: ",")
        let code = csv.withCString { c in
            lane_subscribe_presence(ptr, c)
        }
        try check(code)
    }

    /// Presence kind: `0` available, `1` unavailable (see wire Presence).
    public func sendPresence(kind: Int32) throws {
        try check(lane_send_presence(ptr, kind))
    }

    public func createGroup(groupId: String) throws -> UInt64 {
        var version: UInt64 = 0
        let code = groupId.withCString { g in
            lane_create_group(ptr, g, &version)
        }
        try check(code)
        return version
    }

    public func addMember(groupId: String, user: String) throws -> UInt64 {
        var version: UInt64 = 0
        let code = groupId.withCString { g in
            user.withCString { u in
                lane_add_member(ptr, g, u, &version)
            }
        }
        try check(code)
        return version
    }

    public func removeMember(groupId: String, user: String) throws -> UInt64 {
        var version: UInt64 = 0
        let code = groupId.withCString { g in
            user.withCString { u in
                lane_remove_member(ptr, g, u, &version)
            }
        }
        try check(code)
        return version
    }

    public func leaveGroup(groupId: String) throws -> UInt64 {
        var version: UInt64 = 0
        let code = groupId.withCString { g in
            lane_leave_group(ptr, g, &version)
        }
        try check(code)
        return version
    }

    public func sendGroup(groupId: String, messageId: String, body: Data) throws {
        try sendGroup(groupId: groupId, messageId: messageId, body: body, mediaId: "")
    }

    public func sendGroup(groupId: String, messageId: String, body: Data, mediaId: String) throws {
        let code = body.withUnsafeBytes { raw in
            groupId.withCString { g in
                messageId.withCString { m in
                    mediaId.withCString { mid in
                        if mediaId.isEmpty {
                            return lane_send_group(
                                ptr, g, m,
                                raw.bindMemory(to: UInt8.self).baseAddress,
                                body.count
                            )
                        }
                        return lane_send_group_with_media(
                            ptr, g, m,
                            raw.bindMemory(to: UInt8.self).baseAddress,
                            body.count, mid
                        )
                    }
                }
            }
        }
        try check(code)
    }

    public func uploadMedia(
        mediaId: String,
        fileName: String,
        mimeType: String,
        data: Data
    ) throws -> UInt64 {
        var out: UInt64 = 0
        let code = data.withUnsafeBytes { raw in
            mediaId.withCString { mid in
                fileName.withCString { name in
                    mimeType.withCString { mime in
                        lane_upload_media(
                            ptr, mid, name, mime,
                            raw.bindMemory(to: UInt8.self).baseAddress,
                            data.count, &out
                        )
                    }
                }
            }
        }
        try check(code)
        return out
    }

    public func fetchMedia(mediaId: String) throws -> FetchedMedia {
        var dataPtr: UnsafeMutablePointer<UInt8>?
        var len: Int = 0
        var fileNamePtr: UnsafeMutablePointer<CChar>?
        var mimePtr: UnsafeMutablePointer<CChar>?
        var shaPtr: UnsafeMutablePointer<CChar>?
        let code = mediaId.withCString { mid in
            lane_fetch_media(
                ptr, mid,
                &dataPtr, &len, &fileNamePtr, &mimePtr, &shaPtr
            )
        }
        defer {
            if let fileNamePtr { lane_string_free(fileNamePtr) }
            if let mimePtr { lane_string_free(mimePtr) }
            if let shaPtr { lane_string_free(shaPtr) }
            if let dataPtr { lane_bytes_free(dataPtr, len) }
        }
        try check(code)
        let bytes: Data
        if let dataPtr, len > 0 {
            bytes = Data(bytes: dataPtr, count: len)
        } else {
            bytes = Data()
        }
        return FetchedMedia(
            data: bytes,
            fileName: fileNamePtr.map { String(cString: $0) } ?? "",
            mimeType: mimePtr.map { String(cString: $0) } ?? "",
            sha256: shaPtr.map { String(cString: $0) } ?? ""
        )
    }

    public func pollEvent(timeoutMs: UInt64) -> String? {
        var json: UnsafeMutablePointer<CChar>?
        let n = lane_session_poll_event(ptr, timeoutMs, &json)
        guard n == 1, let json else { return nil }
        defer { lane_string_free(json) }
        return String(cString: json)
    }

    public func close() throws {
        try check(lane_session_close(ptr))
    }

    private func check(_ code: Int32) throws {
        if code != 0 { throw LaneFFIError.code(code) }
    }
}

public enum LaneFFIError: Error, Sendable {
    case connect(String)
    case code(Int32)
}

public struct FetchedMedia: Sendable {
    public var data: Data
    public var fileName: String
    public var mimeType: String
    public var sha256: String

    public init(data: Data, fileName: String, mimeType: String, sha256: String) {
        self.data = data
        self.fileName = fileName
        self.mimeType = mimeType
        self.sha256 = sha256
    }
}

/// Crate / wire version helpers (same Rust binary).
public enum LaneFFI {
    public static var version: String {
        String(cString: lane_version())
    }

    public static var protocolVersion: UInt8 {
        lane_protocol_version()
    }
}
