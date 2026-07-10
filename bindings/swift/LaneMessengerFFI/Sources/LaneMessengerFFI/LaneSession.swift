import Foundation

/// Thin Swift wrapper over the C ABI (`lane_messenger_ffi.h`).
/// Link `liblane_messenger_ffi.a` / XCFramework produced by:
/// `cargo build -p lane_messenger_ffi --release --target aarch64-apple-ios`
public final class LaneSession {
    private var ptr: OpaquePointer?

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
        var seq: UInt64 = 0
        let code = body.withUnsafeBytes { raw in
            to.withCString { t in
                messageId.withCString { m in
                    lane_send_chat(
                        UnsafeMutablePointer(ptr), t, m,
                        raw.bindMemory(to: UInt8.self).baseAddress,
                        body.count, &seq
                    )
                }
            }
        }
        try check(code)
        return seq
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

// Declarations mirror `lane_messenger_ffi/include/lane_messenger_ffi.h`.
// Prefer generating these via UniFFI (`uniffi/lane_messenger.udl`) in CI.
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

@_silgen_name("lane_string_free")
func lane_string_free(_ s: UnsafeMutablePointer<CChar>?)
