import Foundation
import LaneMessengerC

/// Host-owned Olm/Megolm device. Private keys never leave Rust.
///
/// Uses the same C ABI as Android/Java (`lane_e2ee_*`). Do not mix with
/// UniFFI-generated `LaneE2eeDevice` in the same target.
public final class LaneE2eeDevice: @unchecked Sendable {
    private var ptr: OpaquePointer?

    public init() {
        ptr = lane_e2ee_generate()
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
        ptr = device
    }

    deinit {
        if let ptr {
            lane_e2ee_free(ptr)
        }
    }

    public func identityKey() throws -> String {
        var out: UnsafeMutablePointer<CChar>?
        try check(lane_e2ee_identity_key(ptr, &out))
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
            lane_e2ee_publish(session.opaquePointer, ptr, d, otkCount)
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
                        session.opaquePointer,
                        ptr,
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
                    ptr, f,
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
            lane_e2ee_export_pickle(ptr, p, &outPtr, &outLen)
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
            lane_e2ee_create_group_session(ptr, g, &out)
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
                    session.opaquePointer, ptr, g, m
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
                        session.opaquePointer,
                        ptr,
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
                    ptr, g,
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
                    ptr, f,
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
