import CryptoKit
import Foundation

public enum MediaHasher {
    public static func sha256Hex(_ data: Data) -> String {
        SHA256.hash(data: data).map { String(format: "%02x", $0) }.joined()
    }
}

public struct MediaBlobMeta: Codable, Sendable, Equatable {
    public var mediaId: String
    public var fileName: String
    public var mimeType: String
    public var sha256: String
    public var size: UInt64
    public var complete: Bool

    public init(
        mediaId: String,
        fileName: String,
        mimeType: String,
        sha256: String,
        size: UInt64,
        complete: Bool
    ) {
        self.mediaId = mediaId
        self.fileName = fileName
        self.mimeType = mimeType
        self.sha256 = sha256
        self.size = size
        self.complete = complete
    }

    public var isImage: Bool {
        mimeType.hasPrefix("image/")
    }

    public var isPDF: Bool {
        mimeType == "application/pdf" || fileName.lowercased().hasSuffix(".pdf")
    }
}

public enum MediaTransferKind: String, Sendable, Equatable {
    case upload
    case download
}

public enum MediaTransferStatus: String, Sendable, Equatable {
    case queued
    case transferring
    case complete
    case failed
}

public struct MediaTransferProgress: Sendable, Equatable, Identifiable {
    public var id: String { mediaId }
    public var mediaId: String
    public var kind: MediaTransferKind
    public var fraction: Double
    public var status: MediaTransferStatus
    public var error: String?

    public init(
        mediaId: String,
        kind: MediaTransferKind,
        fraction: Double = 0,
        status: MediaTransferStatus = .queued,
        error: String? = nil
    ) {
        self.mediaId = mediaId
        self.kind = kind
        self.fraction = fraction
        self.status = status
        self.error = error
    }
}

/// Persists blobs under Application Support / LaneMessenger / media /.
public final class MediaBlobStore: @unchecked Sendable {
    private let root: URL
    private let lock = NSLock()

    public init(root: URL? = nil) throws {
        if let root {
            self.root = root
        } else {
            let base = try FileManager.default.url(
                for: .applicationSupportDirectory,
                in: .userDomainMask,
                appropriateFor: nil,
                create: true
            )
            self.root = base
                .appendingPathComponent("LaneMessenger", isDirectory: true)
                .appendingPathComponent("media", isDirectory: true)
        }
        try FileManager.default.createDirectory(at: self.root, withIntermediateDirectories: true)
    }

    public static func inMemoryTemp() throws -> MediaBlobStore {
        let dir = FileManager.default.temporaryDirectory
            .appendingPathComponent("lane-media-\(UUID().uuidString)", isDirectory: true)
        return try MediaBlobStore(root: dir)
    }

    public func dir(for mediaId: String) -> URL {
        root.appendingPathComponent(sanitize(mediaId), isDirectory: true)
    }

    public func blobURL(for mediaId: String) -> URL {
        dir(for: mediaId).appendingPathComponent("blob")
    }

    public func meta(for mediaId: String) throws -> MediaBlobMeta? {
        lock.lock(); defer { lock.unlock() }
        let url = dir(for: mediaId).appendingPathComponent("meta.json")
        guard FileManager.default.fileExists(atPath: url.path) else { return nil }
        let data = try Data(contentsOf: url)
        return try JSONDecoder().decode(MediaBlobMeta.self, from: data)
    }

    public func hasComplete(_ mediaId: String) -> Bool {
        (try? meta(for: mediaId))?.complete == true
            && FileManager.default.fileExists(atPath: blobURL(for: mediaId).path)
    }

    public func readData(_ mediaId: String) throws -> Data? {
        lock.lock(); defer { lock.unlock() }
        let url = blobURL(for: mediaId)
        guard FileManager.default.fileExists(atPath: url.path) else { return nil }
        return try Data(contentsOf: url)
    }

    /// Write local copy. Marks `complete` only when integrity matches declared sha (if non-empty).
    @discardableResult
    public func write(
        mediaId: String,
        fileName: String,
        mimeType: String,
        data: Data,
        expectedSha256: String? = nil,
        complete: Bool = true
    ) throws -> MediaBlobMeta {
        lock.lock(); defer { lock.unlock() }
        let sha = MediaHasher.sha256Hex(data)
        if let expected = expectedSha256, !expected.isEmpty, expected.lowercased() != sha {
            throw AppError.mediaCorrupt("sha256 mismatch")
        }
        let dir = dir(for: mediaId)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        try data.write(to: blobURL(for: mediaId), options: .atomic)
        let meta = MediaBlobMeta(
            mediaId: mediaId,
            fileName: fileName,
            mimeType: mimeType,
            sha256: sha,
            size: UInt64(data.count),
            complete: complete
        )
        let metaData = try JSONEncoder().encode(meta)
        try metaData.write(to: dir.appendingPathComponent("meta.json"), options: .atomic)
        return meta
    }

    public func markIncomplete(mediaId: String) throws {
        guard var m = try meta(for: mediaId) else { return }
        m.complete = false
        let data = try JSONEncoder().encode(m)
        try data.write(to: dir(for: mediaId).appendingPathComponent("meta.json"), options: .atomic)
    }

    public func wipeAll() throws {
        lock.lock(); defer { lock.unlock() }
        if FileManager.default.fileExists(atPath: root.path) {
            try FileManager.default.removeItem(at: root)
            try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        }
    }

    private func sanitize(_ id: String) -> String {
        id.replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: ":", with: "_")
    }
}
