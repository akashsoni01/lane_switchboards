import Foundation

/// Host-owned Olm/Megolm device. Private keys never leave Rust.
public final class LaneE2eeDevice {
    private var ptr: OpaquePointer?

    public init() {
        ptr = OpaquePointer(lane_e2ee_generate())
    }

    public init(pickle: Data, passphrase: String) throws {
        let device = pickle.withUnsafeBytes { raw in
            passphrase.withCString { pass in
                lane_e2ee_import_pickle(
                    raw.bindMemory(to: UInt8.self).baseAddress,
                    pickle.count,
                    pass
                )
            }
        }
        guard let device else { throw LaneFFIError.code(-1) }
        ptr = OpaquePointer(device)
    }

    deinit {
        if let ptr {
            lane_e2ee_free(UnsafeMutablePointer(ptr))
        }
    }

    public func identityKey() throws -> String {
        var out: UnsafeMutablePointer<CChar>?
        try check(lane_e2ee_identity_key(UnsafeMutablePointer(ptr), &out))
        defer { if let out { lane_string_free(out) } }
        return out.map { String(cString: $0) } ?? ""
    }

    public static func safetyNumber(localIdentityB64: String, remoteIdentityB64: String) throws -> String {
        var out: UnsafeMutablePointer<CChar>?
        let code = localIdentityB64.withCString { local in
            remoteIdentityB64.withCString { remote in
                lane_e2ee_safety_number(local, remote, &out)
            }
        }
        defer { if let out { lane_string_free(out) } }
        if code != 0 { throw LaneFFIError.code(code) }
        return out.map { String(cString: $0) } ?? ""
    }

    public func publish(session: LaneSession, deviceId: String, otkCount: UInt32 = 20) throws {
        let code = deviceId.withCString { d in
            lane_e2ee_publish(session.rawPointer, UnsafeMutablePointer(ptr), d, otkCount)
        }
        try check(code)
    }

    public func sendEncryptedChat(
        session: LaneSession,
        to: String,
        messageId: String,
        plaintext: Data
    ) throws -> UInt64 {
        var seq: UInt64 = 0
        let code = plaintext.withUnsafeBytes { raw in
            to.withCString { t in
                messageId.withCString { m in
                    lane_send_encrypted_chat(
                        session.rawPointer,
                        UnsafeMutablePointer(ptr),
                        t, m,
                        raw.bindMemory(to: UInt8.self).baseAddress,
                        plaintext.count,
                        &seq
                    )
                }
            }
        }
        try check(code)
        return seq
    }

    public func decryptChat(from: String, body: Data) throws -> Data {
        var outPtr: UnsafeMutablePointer<UInt8>?
        var outLen: Int = 0
        let code = body.withUnsafeBytes { raw in
            from.withCString { f in
                lane_decrypt_chat(
                    UnsafeMutablePointer(ptr), f,
                    raw.bindMemory(to: UInt8.self).baseAddress,
                    body.count,
                    &outPtr, &outLen
                )
            }
        }
        defer {
            if let outPtr { lane_bytes_free(outPtr, outLen) }
        }
        try check(code)
        guard let outPtr, outLen > 0 else { return Data() }
        return Data(bytes: outPtr, count: outLen)
    }

    public func exportPickle(passphrase: String) throws -> Data {
        var outPtr: UnsafeMutablePointer<UInt8>?
        var outLen: Int = 0
        let code = passphrase.withCString { p in
            lane_e2ee_export_pickle(UnsafeMutablePointer(ptr), p, &outPtr, &outLen)
        }
        defer {
            if let outPtr { lane_bytes_free(outPtr, outLen) }
        }
        try check(code)
        guard let outPtr, outLen > 0 else { return Data() }
        return Data(bytes: outPtr, count: outLen)
    }

    public func createGroupSession(groupId: String) throws -> String {
        var out: UnsafeMutablePointer<CChar>?
        let code = groupId.withCString { g in
            lane_e2ee_create_group_session(UnsafeMutablePointer(ptr), g, &out)
        }
        defer { if let out { lane_string_free(out) } }
        try check(code)
        return out.map { String(cString: $0) } ?? ""
    }

    public func distributeGroupKey(session: LaneSession, groupId: String, members: [String]) throws {
        let csv = members.joined(separator: ",")
        let code = groupId.withCString { g in
            csv.withCString { m in
                lane_e2ee_distribute_group_key(
                    session.rawPointer, UnsafeMutablePointer(ptr), g, m
                )
            }
        }
        try check(code)
    }

    public func sendEncryptedGroup(
        session: LaneSession,
        groupId: String,
        messageId: String,
        plaintext: Data
    ) throws {
        let code = plaintext.withUnsafeBytes { raw in
            groupId.withCString { g in
                messageId.withCString { m in
                    lane_send_encrypted_group(
                        session.rawPointer,
                        UnsafeMutablePointer(ptr),
                        g, m,
                        raw.bindMemory(to: UInt8.self).baseAddress,
                        plaintext.count
                    )
                }
            }
        }
        try check(code)
    }

    public func decryptGroup(groupId: String, body: Data) throws -> Data {
        var outPtr: UnsafeMutablePointer<UInt8>?
        var outLen: Int = 0
        let code = body.withUnsafeBytes { raw in
            groupId.withCString { g in
                lane_decrypt_group(
                    UnsafeMutablePointer(ptr), g,
                    raw.bindMemory(to: UInt8.self).baseAddress,
                    body.count,
                    &outPtr, &outLen
                )
            }
        }
        defer {
            if let outPtr { lane_bytes_free(outPtr, outLen) }
        }
        try check(code)
        guard let outPtr, outLen > 0 else { return Data() }
        return Data(bytes: outPtr, count: outLen)
    }

    public func tryImportGroupKey(from: String, body: Data) throws -> Bool {
        var imported: Int32 = 0
        let code = body.withUnsafeBytes { raw in
            from.withCString { f in
                lane_e2ee_try_import_group_key(
                    UnsafeMutablePointer(ptr), f,
                    raw.bindMemory(to: UInt8.self).baseAddress,
                    body.count,
                    &imported
                )
            }
        }
        try check(code)
        return imported != 0
    }

    private func check(_ code: Int32) throws {
        if code != 0 { throw LaneFFIError.code(code) }
    }
}

@_silgen_name("lane_e2ee_generate")
func lane_e2ee_generate() -> OpaquePointer?

@_silgen_name("lane_e2ee_free")
func lane_e2ee_free(_ d: UnsafeMutableRawPointer?)

@_silgen_name("lane_e2ee_identity_key")
func lane_e2ee_identity_key(
    _ d: UnsafeMutableRawPointer?,
    _ out: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>
) -> Int32

@_silgen_name("lane_e2ee_safety_number")
func lane_e2ee_safety_number(
    _ local: UnsafePointer<CChar>,
    _ remote: UnsafePointer<CChar>,
    _ out: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>
) -> Int32

@_silgen_name("lane_e2ee_publish")
func lane_e2ee_publish(
    _ s: UnsafeMutableRawPointer?,
    _ d: UnsafeMutableRawPointer?,
    _ deviceId: UnsafePointer<CChar>,
    _ otk: UInt32
) -> Int32

@_silgen_name("lane_send_encrypted_chat")
func lane_send_encrypted_chat(
    _ s: UnsafeMutableRawPointer?,
    _ d: UnsafeMutableRawPointer?,
    _ to: UnsafePointer<CChar>,
    _ mid: UnsafePointer<CChar>,
    _ plain: UnsafePointer<UInt8>?,
    _ len: Int,
    _ seq: UnsafeMutablePointer<UInt64>
) -> Int32

@_silgen_name("lane_decrypt_chat")
func lane_decrypt_chat(
    _ d: UnsafeMutableRawPointer?,
    _ from: UnsafePointer<CChar>,
    _ body: UnsafePointer<UInt8>?,
    _ len: Int,
    _ out: UnsafeMutablePointer<UnsafeMutablePointer<UInt8>?>,
    _ outLen: UnsafeMutablePointer<Int>
) -> Int32

@_silgen_name("lane_e2ee_export_pickle")
func lane_e2ee_export_pickle(
    _ d: UnsafeMutableRawPointer?,
    _ pass: UnsafePointer<CChar>,
    _ out: UnsafeMutablePointer<UnsafeMutablePointer<UInt8>?>,
    _ outLen: UnsafeMutablePointer<Int>
) -> Int32

@_silgen_name("lane_e2ee_import_pickle")
func lane_e2ee_import_pickle(
    _ bytes: UnsafePointer<UInt8>?,
    _ len: Int,
    _ pass: UnsafePointer<CChar>
) -> OpaquePointer?

@_silgen_name("lane_e2ee_create_group_session")
func lane_e2ee_create_group_session(
    _ d: UnsafeMutableRawPointer?,
    _ groupId: UnsafePointer<CChar>,
    _ out: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>
) -> Int32

@_silgen_name("lane_e2ee_distribute_group_key")
func lane_e2ee_distribute_group_key(
    _ s: UnsafeMutableRawPointer?,
    _ d: UnsafeMutableRawPointer?,
    _ groupId: UnsafePointer<CChar>,
    _ members: UnsafePointer<CChar>
) -> Int32

@_silgen_name("lane_send_encrypted_group")
func lane_send_encrypted_group(
    _ s: UnsafeMutableRawPointer?,
    _ d: UnsafeMutableRawPointer?,
    _ groupId: UnsafePointer<CChar>,
    _ mid: UnsafePointer<CChar>,
    _ plain: UnsafePointer<UInt8>?,
    _ len: Int
) -> Int32

@_silgen_name("lane_decrypt_group")
func lane_decrypt_group(
    _ d: UnsafeMutableRawPointer?,
    _ groupId: UnsafePointer<CChar>,
    _ body: UnsafePointer<UInt8>?,
    _ len: Int,
    _ out: UnsafeMutablePointer<UnsafeMutablePointer<UInt8>?>,
    _ outLen: UnsafeMutablePointer<Int>
) -> Int32

@_silgen_name("lane_e2ee_try_import_group_key")
func lane_e2ee_try_import_group_key(
    _ d: UnsafeMutableRawPointer?,
    _ from: UnsafePointer<CChar>,
    _ body: UnsafePointer<UInt8>?,
    _ len: Int,
    _ outImported: UnsafeMutablePointer<Int32>
) -> Int32
