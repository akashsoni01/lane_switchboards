import Foundation

/// Thin Swift wrapper over the C ABI (`lane_messenger_ffi.h`).
/// Link `liblane_messenger_ffi.a` / XCFramework produced by:
/// `cargo build -p lane_messenger_ffi --release --target aarch64-apple-ios`
public final class LaneSession {
    private var ptr: OpaquePointer?

    /// Raw session pointer for companion C APIs (E2EE).
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
            self.ptr = OpaquePointer(session)
        } else {
            let msg = err.map { String(cString: $0) } ?? "connect failed"
            if let err { lane_string_free(err) }
            throw LaneFFIError.connect(msg)
        }
    }

    deinit {
        if let ptr {
            lane_session_free(UnsafeMutablePointer(ptr))
        }
    }

    public func ping() throws {
        try check(lane_session_ping(UnsafeMutablePointer(ptr)))
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
                                UnsafeMutablePointer(ptr), t, m,
                                raw.bindMemory(to: UInt8.self).baseAddress,
                                body.count, &seq
                            )
                        }
                        return lane_send_chat_with_media(
                            UnsafeMutablePointer(ptr), t, m,
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

    public func ackDelivered(messageId: String) throws {
        let code = messageId.withCString { m in
            lane_ack_delivered(UnsafeMutablePointer(ptr), m)
        }
        try check(code)
    }

    public func ackRead(messageId: String) throws {
        let code = messageId.withCString { m in
            lane_ack_read(UnsafeMutablePointer(ptr), m)
        }
        try check(code)
    }

    public func subscribePresence(contactIds: [String]) throws {
        let csv = contactIds.joined(separator: ",")
        let code = csv.withCString { c in
            lane_subscribe_presence(UnsafeMutablePointer(ptr), c)
        }
        try check(code)
    }

    public func createGroup(groupId: String) throws -> UInt64 {
        var version: UInt64 = 0
        let code = groupId.withCString { g in
            lane_create_group(UnsafeMutablePointer(ptr), g, &version)
        }
        try check(code)
        return version
    }

    public func addMember(groupId: String, user: String) throws -> UInt64 {
        var version: UInt64 = 0
        let code = groupId.withCString { g in
            user.withCString { u in
                lane_add_member(UnsafeMutablePointer(ptr), g, u, &version)
            }
        }
        try check(code)
        return version
    }

    public func removeMember(groupId: String, user: String) throws -> UInt64 {
        var version: UInt64 = 0
        let code = groupId.withCString { g in
            user.withCString { u in
                lane_remove_member(UnsafeMutablePointer(ptr), g, u, &version)
            }
        }
        try check(code)
        return version
    }

    public func leaveGroup(groupId: String) throws -> UInt64 {
        var version: UInt64 = 0
        let code = groupId.withCString { g in
            lane_leave_group(UnsafeMutablePointer(ptr), g, &version)
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
                                UnsafeMutablePointer(ptr), g, m,
                                raw.bindMemory(to: UInt8.self).baseAddress,
                                body.count
                            )
                        }
                        return lane_send_group_with_media(
                            UnsafeMutablePointer(ptr), g, m,
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
                            UnsafeMutablePointer(ptr), mid, name, mime,
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
                UnsafeMutablePointer(ptr), mid,
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
        let n = lane_session_poll_event(UnsafeMutablePointer(ptr), timeoutMs, &json)
        guard n == 1, let json else { return nil }
        defer { lane_string_free(json) }
        return String(cString: json)
    }

    public func close() throws {
        try check(lane_session_close(UnsafeMutablePointer(ptr)))
    }

    private func check(_ code: Int32) throws {
        if code != 0 { throw LaneFFIError.code(code) }
    }
}

public enum LaneFFIError: Error {
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

// Declarations mirror `lane_messenger_ffi/include/lane_messenger_ffi.h`.
// Prefer UniFFI-generated Swift under `generated/` (see scripts/generate_uniffi_bindings.sh).
@_silgen_name("lane_session_connect")
func lane_session_connect(
    _ host: UnsafePointer<CChar>, _ port: UInt16, _ useTls: Int32,
    _ userId: UnsafePointer<CChar>, _ deviceId: UnsafePointer<CChar>,
    _ authToken: UnsafePointer<CChar>, _ clientVersion: UnsafePointer<CChar>,
    _ resume: UInt64, _ ping: UInt64, _ err: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>
) -> OpaquePointer?

@_silgen_name("lane_session_free")
func lane_session_free(_ s: UnsafeMutablePointer<CChar>?)

@_silgen_name("lane_session_close")
func lane_session_close(_ s: UnsafeMutableRawPointer?) -> Int32

@_silgen_name("lane_session_ping")
func lane_session_ping(_ s: UnsafeMutableRawPointer?) -> Int32

@_silgen_name("lane_session_poll_event")
func lane_session_poll_event(
    _ s: UnsafeMutableRawPointer?, _ timeout: UInt64,
    _ out: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>
) -> Int32

@_silgen_name("lane_send_chat")
func lane_send_chat(
    _ s: UnsafeMutableRawPointer?, _ to: UnsafePointer<CChar>,
    _ mid: UnsafePointer<CChar>, _ body: UnsafePointer<UInt8>?,
    _ len: Int, _ seq: UnsafeMutablePointer<UInt64>
) -> Int32

@_silgen_name("lane_send_chat_with_media")
func lane_send_chat_with_media(
    _ s: UnsafeMutableRawPointer?, _ to: UnsafePointer<CChar>,
    _ mid: UnsafePointer<CChar>, _ body: UnsafePointer<UInt8>?,
    _ len: Int, _ mediaId: UnsafePointer<CChar>,
    _ seq: UnsafeMutablePointer<UInt64>
) -> Int32

@_silgen_name("lane_ack_delivered")
func lane_ack_delivered(_ s: UnsafeMutableRawPointer?, _ mid: UnsafePointer<CChar>) -> Int32

@_silgen_name("lane_ack_read")
func lane_ack_read(_ s: UnsafeMutableRawPointer?, _ mid: UnsafePointer<CChar>) -> Int32

@_silgen_name("lane_subscribe_presence")
func lane_subscribe_presence(_ s: UnsafeMutableRawPointer?, _ csv: UnsafePointer<CChar>) -> Int32

@_silgen_name("lane_create_group")
func lane_create_group(
    _ s: UnsafeMutableRawPointer?, _ groupId: UnsafePointer<CChar>,
    _ outVersion: UnsafeMutablePointer<UInt64>
) -> Int32

@_silgen_name("lane_add_member")
func lane_add_member(
    _ s: UnsafeMutableRawPointer?, _ groupId: UnsafePointer<CChar>,
    _ user: UnsafePointer<CChar>, _ outVersion: UnsafeMutablePointer<UInt64>
) -> Int32

@_silgen_name("lane_remove_member")
func lane_remove_member(
    _ s: UnsafeMutableRawPointer?, _ groupId: UnsafePointer<CChar>,
    _ user: UnsafePointer<CChar>, _ outVersion: UnsafeMutablePointer<UInt64>
) -> Int32

@_silgen_name("lane_leave_group")
func lane_leave_group(
    _ s: UnsafeMutableRawPointer?, _ groupId: UnsafePointer<CChar>,
    _ outVersion: UnsafeMutablePointer<UInt64>
) -> Int32

@_silgen_name("lane_send_group")
func lane_send_group(
    _ s: UnsafeMutableRawPointer?, _ groupId: UnsafePointer<CChar>,
    _ mid: UnsafePointer<CChar>, _ body: UnsafePointer<UInt8>?,
    _ len: Int
) -> Int32

@_silgen_name("lane_send_group_with_media")
func lane_send_group_with_media(
    _ s: UnsafeMutableRawPointer?, _ groupId: UnsafePointer<CChar>,
    _ mid: UnsafePointer<CChar>, _ body: UnsafePointer<UInt8>?,
    _ len: Int, _ mediaId: UnsafePointer<CChar>
) -> Int32

@_silgen_name("lane_upload_media")
func lane_upload_media(
    _ s: UnsafeMutableRawPointer?, _ mediaId: UnsafePointer<CChar>,
    _ fileName: UnsafePointer<CChar>, _ mimeType: UnsafePointer<CChar>,
    _ data: UnsafePointer<UInt8>?, _ len: Int,
    _ outBytes: UnsafeMutablePointer<UInt64>
) -> Int32

@_silgen_name("lane_fetch_media")
func lane_fetch_media(
    _ s: UnsafeMutableRawPointer?, _ mediaId: UnsafePointer<CChar>,
    _ outData: UnsafeMutablePointer<UnsafeMutablePointer<UInt8>?>,
    _ outLen: UnsafeMutablePointer<Int>,
    _ outFileName: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>,
    _ outMime: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>,
    _ outSha: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>
) -> Int32

@_silgen_name("lane_bytes_free")
func lane_bytes_free(_ p: UnsafeMutablePointer<UInt8>?, _ len: Int)

@_silgen_name("lane_string_free")
func lane_string_free(_ s: UnsafeMutablePointer<CChar>?)
