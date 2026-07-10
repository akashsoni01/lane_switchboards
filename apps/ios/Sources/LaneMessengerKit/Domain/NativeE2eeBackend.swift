import Foundation

#if canImport(LaneMessengerFFI)
import LaneMessengerFFI

/// Production E2EE backend wrapping `LaneE2eeDevice` (vodozemac via C ABI).
public final class NativeE2eeBackend: E2eeBackend, @unchecked Sendable {
    private let device: LaneE2eeDevice
    private let sessionProvider: @Sendable () -> LaneSession?

    public init(device: LaneE2eeDevice, sessionProvider: @escaping @Sendable () -> LaneSession?) {
        self.device = device
        self.sessionProvider = sessionProvider
    }

    public static func generate(sessionProvider: @escaping @Sendable () -> LaneSession?) -> NativeE2eeBackend {
        NativeE2eeBackend(device: LaneE2eeDevice(), sessionProvider: sessionProvider)
    }

    public static func importPickle(
        data: Data,
        passphrase: String,
        sessionProvider: @escaping @Sendable () -> LaneSession?
    ) throws -> NativeE2eeBackend {
        let device = try LaneE2eeDevice(pickle: data, passphrase: passphrase)
        return NativeE2eeBackend(device: device, sessionProvider: sessionProvider)
    }

    private func session() throws -> LaneSession {
        guard let s = sessionProvider() else { throw AppError.connection("not connected") }
        return s
    }

    public func identityKey() throws -> String {
        try device.identityKey()
    }

    public func publish(deviceId: String, otkCount: UInt32) async throws {
        try device.publish(session: try session(), deviceId: deviceId, otkCount: otkCount)
    }

    public func sendEncryptedChat(to: String, messageId: String, plaintext: Data) async throws -> UInt64 {
        try device.sendEncryptedChat(
            session: try session(),
            to: to,
            messageId: messageId,
            plaintext: plaintext
        )
    }

    public func decryptChat(from: String, body: Data) throws -> Data {
        try device.decryptChat(from: from, body: body)
    }

    public func createGroupSession(groupId: String) throws -> String {
        try device.createGroupSession(groupId: groupId)
    }

    public func distributeGroupKey(groupId: String, members: [String]) async throws {
        try device.distributeGroupKey(session: try session(), groupId: groupId, members: members)
    }

    public func sendEncryptedGroup(groupId: String, messageId: String, plaintext: Data) async throws {
        try device.sendEncryptedGroup(
            session: try session(),
            groupId: groupId,
            messageId: messageId,
            plaintext: plaintext
        )
    }

    public func decryptGroup(groupId: String, body: Data) throws -> Data {
        try device.decryptGroup(groupId: groupId, body: body)
    }

    public func tryImportGroupKey(from: String, body: Data) throws -> Bool {
        try device.tryImportGroupKey(from: from, body: body)
    }

    public func exportPickle(passphrase: String) throws -> Data {
        try device.exportPickle(passphrase: passphrase)
    }
}
#endif
