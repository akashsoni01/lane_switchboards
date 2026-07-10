import Foundation
import SQLite3

/// SQLite-backed `LocalStore` using the system SQLite3 library (no GRDB dep).
public final class SQLiteLocalStore: LocalStore, @unchecked Sendable {
    private var db: OpaquePointer?
    private let path: String
    private let lock = NSLock()

    public init(path: String) throws {
        self.path = path
        try open()
        try migrate()
    }

    /// App Support / LaneMessenger / messenger.sqlite
    public static func defaultPath() throws -> String {
        let base = try FileManager.default.url(
            for: .applicationSupportDirectory,
            in: .userDomainMask,
            appropriateFor: nil,
            create: true
        )
        let dir = base.appendingPathComponent("LaneMessenger", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir.appendingPathComponent("messenger.sqlite").path
    }

    deinit {
        if let db { sqlite3_close(db) }
    }

    public func resumeAfterSeq() throws -> UInt64 {
        try withDB { db in
            var stmt: OpaquePointer?
            defer { sqlite3_finalize(stmt) }
            try check(sqlite3_prepare_v2(db, "SELECT value FROM meta WHERE key='resume_after_seq'", -1, &stmt, nil), db)
            if sqlite3_step(stmt) == SQLITE_ROW {
                if let cstr = sqlite3_column_text(stmt, 0) {
                    return UInt64(String(cString: cstr)) ?? 0
                }
            }
            return 0
        }
    }

    public func setResumeAfterSeq(_ seq: UInt64) throws {
        try withDB { db in
            try exec(
                db,
                """
                INSERT INTO meta(key, value) VALUES('resume_after_seq', '\(seq)')
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
                """
            )
        }
    }

    @discardableResult
    public func upsertMessage(_ message: StoredMessage) throws -> Bool {
        try withDB { db in
            var existed = false
            var checkStmt: OpaquePointer?
            defer { sqlite3_finalize(checkStmt) }
            try check(
                sqlite3_prepare_v2(db, "SELECT 1 FROM messages WHERE message_id=? LIMIT 1", -1, &checkStmt, nil),
                db
            )
            bindText(checkStmt, 1, message.messageId)
            if sqlite3_step(checkStmt) == SQLITE_ROW { existed = true }

            var stmt: OpaquePointer?
            defer { sqlite3_finalize(stmt) }
            let sql = """
            INSERT INTO messages(message_id, conversation_id, direction, body, media_id, seq, status, created_at)
            VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(message_id) DO UPDATE SET
              seq=MAX(messages.seq, excluded.seq),
              status=CASE
                WHEN excluded.status='read' THEN 'read'
                WHEN excluded.status='delivered' AND messages.status!='read' THEN 'delivered'
                WHEN excluded.status='sent' AND messages.status IN ('pending','failed') THEN 'sent'
                WHEN excluded.status='failed' AND messages.status='pending' THEN 'failed'
                ELSE messages.status END,
              body=CASE WHEN length(excluded.body)>0 THEN excluded.body ELSE messages.body END
            """
            try check(sqlite3_prepare_v2(db, sql, -1, &stmt, nil), db)
            bindText(stmt, 1, message.messageId)
            bindText(stmt, 2, message.conversationId)
            bindText(stmt, 3, message.direction.rawValue)
            bindText(stmt, 4, message.body)
            bindText(stmt, 5, message.mediaId)
            sqlite3_bind_int64(stmt, 6, Int64(message.seq))
            bindText(stmt, 7, message.status.rawValue)
            sqlite3_bind_double(stmt, 8, message.createdAt.timeIntervalSince1970)
            try check(sqlite3_step(stmt), db, ok: SQLITE_DONE)

            // Ensure conversation row + preview.
            let title = message.conversationId
            let preview = message.body.isEmpty ? "(media)" : String(message.body.prefix(120))
            let unreadInc = (!existed && message.direction == .inbound) ? 1 : 0
            try exec(
                db,
                """
                INSERT INTO conversations(id, title, sort_ts, unread, draft, last_preview)
                VALUES('\(escape(title))', '\(escape(title))', \(message.createdAt.timeIntervalSince1970), \(unreadInc), '', '\(escape(preview))')
                ON CONFLICT(id) DO UPDATE SET
                  sort_ts=MAX(conversations.sort_ts, excluded.sort_ts),
                  unread=conversations.unread + \(unreadInc),
                  last_preview=excluded.last_preview
                """
            )

            if message.seq > 0 {
                let current = try resumeAfterSeqUnlocked(db)
                if message.seq > current {
                    try exec(
                        db,
                        """
                        INSERT INTO meta(key, value) VALUES('resume_after_seq', '\(message.seq)')
                        ON CONFLICT(key) DO UPDATE SET value=excluded.value
                        """
                    )
                }
            }
            return !existed
        }
    }

    public func updateStatus(messageId: String, status: MessageStatus, seq: UInt64?) throws {
        try withDB { db in
            if let seq {
                try exec(
                    db,
                    """
                    UPDATE messages SET status='\(status.rawValue)', seq=MAX(seq, \(seq))
                    WHERE message_id='\(escape(messageId))'
                    """
                )
                let current = try resumeAfterSeqUnlocked(db)
                if seq > current {
                    try exec(
                        db,
                        """
                        INSERT INTO meta(key, value) VALUES('resume_after_seq', '\(seq)')
                        ON CONFLICT(key) DO UPDATE SET value=excluded.value
                        """
                    )
                }
            } else {
                try exec(
                    db,
                    "UPDATE messages SET status='\(status.rawValue)' WHERE message_id='\(escape(messageId))'"
                )
            }
        }
    }

    public func conversations() throws -> [Conversation] {
        try withDB { db in
            var stmt: OpaquePointer?
            defer { sqlite3_finalize(stmt) }
            try check(
                sqlite3_prepare_v2(
                    db,
                    """
                    SELECT c.id, c.title, c.sort_ts, c.unread, c.draft, c.last_preview,
                           k.presence, k.last_seen
                    FROM conversations c
                    LEFT JOIN contacts k ON k.user_id = c.id
                    ORDER BY c.sort_ts DESC
                    """,
                    -1,
                    &stmt,
                    nil
                ),
                db
            )
            var rows: [Conversation] = []
            while sqlite3_step(stmt) == SQLITE_ROW {
                let presenceRaw = sqlite3_column_type(stmt, 6) == SQLITE_NULL
                    ? nil
                    : PresenceKind(rawValue: Int(sqlite3_column_int(stmt, 6)))
                let lastSeen: Date? = sqlite3_column_type(stmt, 7) == SQLITE_NULL
                    ? nil
                    : Date(timeIntervalSince1970: TimeInterval(sqlite3_column_int64(stmt, 7)))
                rows.append(
                    Conversation(
                        id: string(stmt, 0),
                        title: string(stmt, 1),
                        sortTs: Date(timeIntervalSince1970: sqlite3_column_double(stmt, 2)),
                        unread: Int(sqlite3_column_int(stmt, 3)),
                        draft: string(stmt, 4),
                        lastPreview: string(stmt, 5),
                        presence: presenceRaw,
                        lastSeen: lastSeen
                    )
                )
            }
            return rows
        }
    }

    public func messages(conversationId: String, limit: Int) throws -> [StoredMessage] {
        try withDB { db in
            var stmt: OpaquePointer?
            defer { sqlite3_finalize(stmt) }
            try check(
                sqlite3_prepare_v2(
                    db,
                    """
                    SELECT message_id, conversation_id, direction, body, media_id, seq, status, created_at
                    FROM messages WHERE conversation_id=? ORDER BY created_at ASC LIMIT ?
                    """,
                    -1,
                    &stmt,
                    nil
                ),
                db
            )
            bindText(stmt, 1, conversationId)
            sqlite3_bind_int(stmt, 2, Int32(limit))
            var rows: [StoredMessage] = []
            while sqlite3_step(stmt) == SQLITE_ROW {
                rows.append(
                    StoredMessage(
                        messageId: string(stmt, 0),
                        conversationId: string(stmt, 1),
                        direction: MessageDirection(rawValue: string(stmt, 2)) ?? .inbound,
                        body: string(stmt, 3),
                        mediaId: string(stmt, 4),
                        seq: UInt64(sqlite3_column_int64(stmt, 5)),
                        status: MessageStatus(rawValue: string(stmt, 6)) ?? .pending,
                        createdAt: Date(timeIntervalSince1970: sqlite3_column_double(stmt, 7))
                    )
                )
            }
            return rows
        }
    }

    public func setDraft(conversationId: String, draft: String) throws {
        try withDB { db in
            try exec(
                db,
                """
                INSERT INTO conversations(id, title, sort_ts, unread, draft, last_preview)
                VALUES('\(escape(conversationId))', '\(escape(conversationId))', \(Date().timeIntervalSince1970), 0, '\(escape(draft))', '')
                ON CONFLICT(id) DO UPDATE SET draft=excluded.draft
                """
            )
        }
    }

    public func markConversationRead(conversationId: String) throws {
        try withDB { db in
            try exec(db, "UPDATE conversations SET unread=0 WHERE id='\(escape(conversationId))'")
        }
    }

    public func ensureConversation(id: String, title: String?) throws {
        try withDB { db in
            let t = title ?? id
            try exec(
                db,
                """
                INSERT INTO conversations(id, title, sort_ts, unread, draft, last_preview)
                VALUES('\(escape(id))', '\(escape(t))', \(Date().timeIntervalSince1970), 0, '', '')
                ON CONFLICT(id) DO UPDATE SET title=CASE WHEN length(excluded.title)>0 THEN excluded.title ELSE conversations.title END
                """
            )
        }
    }

    public func upsertContact(_ contact: Contact) throws {
        try withDB { db in
            let last = contact.lastSeen.map { String(Int64($0.timeIntervalSince1970)) } ?? "NULL"
            try exec(
                db,
                """
                INSERT INTO contacts(user_id, display_name, presence, last_seen)
                VALUES('\(escape(contact.userId))', '\(escape(contact.displayName))', \(contact.presence.rawValue), \(last))
                ON CONFLICT(user_id) DO UPDATE SET
                  display_name=excluded.display_name,
                  presence=excluded.presence,
                  last_seen=excluded.last_seen
                """
            )
        }
    }

    public func contacts() throws -> [Contact] {
        try withDB { db in
            var stmt: OpaquePointer?
            defer { sqlite3_finalize(stmt) }
            try check(
                sqlite3_prepare_v2(
                    db,
                    "SELECT user_id, display_name, presence, last_seen FROM contacts ORDER BY display_name ASC",
                    -1,
                    &stmt,
                    nil
                ),
                db
            )
            var rows: [Contact] = []
            while sqlite3_step(stmt) == SQLITE_ROW {
                let lastSeen: Date? = sqlite3_column_type(stmt, 3) == SQLITE_NULL
                    ? nil
                    : Date(timeIntervalSince1970: TimeInterval(sqlite3_column_int64(stmt, 3)))
                rows.append(
                    Contact(
                        userId: string(stmt, 0),
                        displayName: string(stmt, 1),
                        presence: PresenceKind(rawValue: Int(sqlite3_column_int(stmt, 2))) ?? .unavailable,
                        lastSeen: lastSeen
                    )
                )
            }
            return rows
        }
    }

    public func contact(userId: String) throws -> Contact? {
        try contacts().first { $0.userId == userId }
    }

    public func wipeUserData() throws {
        try withDB { db in
            try exec(db, "DELETE FROM messages")
            try exec(db, "DELETE FROM conversations")
            try exec(db, "DELETE FROM contacts")
            try exec(db, "DELETE FROM groups")
            try exec(db, "DELETE FROM group_members")
            try exec(db, "DELETE FROM meta WHERE key='resume_after_seq'")
        }
    }

    // MARK: - internals

    private func open() throws {
        if sqlite3_open(path, &db) != SQLITE_OK {
            throw AppError.internalError("sqlite open failed: \(path)")
        }
        try exec(db!, "PRAGMA foreign_keys=ON")
        try exec(db!, "PRAGMA journal_mode=WAL")
    }

    private func migrate() throws {
        try withDB { db in
            try exec(
                db,
                """
                CREATE TABLE IF NOT EXISTS meta(
                  key TEXT PRIMARY KEY NOT NULL,
                  value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS conversations(
                  id TEXT PRIMARY KEY NOT NULL,
                  title TEXT NOT NULL,
                  sort_ts REAL NOT NULL,
                  unread INTEGER NOT NULL DEFAULT 0,
                  draft TEXT NOT NULL DEFAULT '',
                  last_preview TEXT NOT NULL DEFAULT ''
                );
                CREATE TABLE IF NOT EXISTS messages(
                  message_id TEXT PRIMARY KEY NOT NULL,
                  conversation_id TEXT NOT NULL,
                  direction TEXT NOT NULL,
                  body TEXT NOT NULL,
                  media_id TEXT NOT NULL DEFAULT '',
                  seq INTEGER NOT NULL DEFAULT 0,
                  status TEXT NOT NULL,
                  created_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_messages_conv ON messages(conversation_id, created_at);
                CREATE TABLE IF NOT EXISTS contacts(
                  user_id TEXT PRIMARY KEY NOT NULL,
                  display_name TEXT NOT NULL DEFAULT '',
                  presence INTEGER NOT NULL DEFAULT 0,
                  last_seen INTEGER
                );
                CREATE TABLE IF NOT EXISTS groups(
                  id TEXT PRIMARY KEY NOT NULL,
                  title TEXT NOT NULL DEFAULT '',
                  version INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS group_members(
                  group_id TEXT NOT NULL,
                  user_id TEXT NOT NULL,
                  is_admin INTEGER NOT NULL DEFAULT 0,
                  PRIMARY KEY(group_id, user_id)
                );
                """
            )
        }
    }

    private func withDB<T>(_ body: (OpaquePointer) throws -> T) throws -> T {
        lock.lock()
        defer { lock.unlock() }
        guard let db else { throw AppError.internalError("db closed") }
        return try body(db)
    }

    private func resumeAfterSeqUnlocked(_ db: OpaquePointer) throws -> UInt64 {
        var stmt: OpaquePointer?
        defer { sqlite3_finalize(stmt) }
        try check(sqlite3_prepare_v2(db, "SELECT value FROM meta WHERE key='resume_after_seq'", -1, &stmt, nil), db)
        if sqlite3_step(stmt) == SQLITE_ROW, let cstr = sqlite3_column_text(stmt, 0) {
            return UInt64(String(cString: cstr)) ?? 0
        }
        return 0
    }

    private func exec(_ db: OpaquePointer, _ sql: String) throws {
        var err: UnsafeMutablePointer<CChar>?
        let rc = sqlite3_exec(db, sql, nil, nil, &err)
        if rc != SQLITE_OK {
            let msg = err.map { String(cString: $0) } ?? "sqlite error \(rc)"
            sqlite3_free(err)
            throw AppError.internalError(msg)
        }
    }

    private func check(_ rc: Int32, _ db: OpaquePointer, ok: Int32 = SQLITE_OK) throws {
        if rc != ok && rc != SQLITE_DONE && rc != SQLITE_ROW {
            let msg = String(cString: sqlite3_errmsg(db))
            throw AppError.internalError(msg)
        }
    }

    private func bindText(_ stmt: OpaquePointer?, _ idx: Int32, _ value: String) {
        _ = value.withCString { sqlite3_bind_text(stmt, idx, $0, -1, unsafeBitCast(-1, to: sqlite3_destructor_type.self)) }
    }

    private func string(_ stmt: OpaquePointer?, _ idx: Int32) -> String {
        guard let c = sqlite3_column_text(stmt, idx) else { return "" }
        return String(cString: c)
    }

    private func escape(_ s: String) -> String {
        s.replacingOccurrences(of: "'", with: "''")
    }
}

/// In-memory store for unit tests.
public final class InMemoryLocalStore: LocalStore, @unchecked Sendable {
    private var resume: UInt64 = 0
    private var messages: [String: StoredMessage] = [:]
    private var convos: [String: Conversation] = [:]
    private var people: [String: Contact] = [:]
    private let lock = NSLock()

    public init() {}

    public func resumeAfterSeq() throws -> UInt64 {
        lock.lock(); defer { lock.unlock() }
        return resume
    }

    public func setResumeAfterSeq(_ seq: UInt64) throws {
        lock.lock(); defer { lock.unlock() }
        resume = seq
    }

    public func upsertMessage(_ message: StoredMessage) throws -> Bool {
        lock.lock(); defer { lock.unlock() }
        let existed = messages[message.messageId] != nil
        if var existing = messages[message.messageId] {
            existing.seq = max(existing.seq, message.seq)
            if !message.body.isEmpty { existing.body = message.body }
            if message.status.rank >= existing.status.rank {
                existing.status = message.status
            }
            messages[message.messageId] = existing
        } else {
            messages[message.messageId] = message
        }
        if message.seq > resume { resume = message.seq }
        var c = convos[message.conversationId] ?? Conversation(id: message.conversationId)
        c.sortTs = max(c.sortTs, message.createdAt)
        c.lastPreview = message.body.isEmpty ? c.lastPreview : String(message.body.prefix(120))
        if !existed, message.direction == .inbound { c.unread += 1 }
        if let contact = people[message.conversationId] {
            c.presence = contact.presence
            c.lastSeen = contact.lastSeen
            c.title = contact.displayName
        }
        convos[message.conversationId] = c
        return !existed
    }

    public func updateStatus(messageId: String, status: MessageStatus, seq: UInt64?) throws {
        lock.lock(); defer { lock.unlock() }
        guard var m = messages[messageId] else { return }
        if status.rank >= m.status.rank { m.status = status }
        if let seq {
            m.seq = max(m.seq, seq)
            if seq > resume { resume = seq }
        }
        messages[messageId] = m
    }

    public func conversations() throws -> [Conversation] {
        lock.lock(); defer { lock.unlock() }
        return convos.values.map { c in
            var out = c
            if let contact = people[c.id] {
                out.presence = contact.presence
                out.lastSeen = contact.lastSeen
                out.title = contact.displayName
            }
            return out
        }
        .sorted { $0.sortTs > $1.sortTs }
    }

    public func messages(conversationId: String, limit: Int) throws -> [StoredMessage] {
        lock.lock(); defer { lock.unlock() }
        return messages.values
            .filter { $0.conversationId == conversationId }
            .sorted { $0.createdAt < $1.createdAt }
            .prefix(limit)
            .map { $0 }
    }

    public func setDraft(conversationId: String, draft: String) throws {
        lock.lock(); defer { lock.unlock() }
        var c = convos[conversationId] ?? Conversation(id: conversationId)
        c.draft = draft
        convos[conversationId] = c
    }

    public func markConversationRead(conversationId: String) throws {
        lock.lock(); defer { lock.unlock() }
        guard var c = convos[conversationId] else { return }
        c.unread = 0
        convos[conversationId] = c
    }

    public func ensureConversation(id: String, title: String?) throws {
        lock.lock(); defer { lock.unlock() }
        if convos[id] == nil {
            convos[id] = Conversation(id: id, title: title ?? id)
        } else if let title, !title.isEmpty {
            convos[id]?.title = title
        }
    }

    public func upsertContact(_ contact: Contact) throws {
        lock.lock(); defer { lock.unlock() }
        people[contact.userId] = contact
        if var c = convos[contact.userId] {
            c.presence = contact.presence
            c.lastSeen = contact.lastSeen
            c.title = contact.displayName
            convos[contact.userId] = c
        }
    }

    public func contacts() throws -> [Contact] {
        lock.lock(); defer { lock.unlock() }
        return people.values.sorted { $0.displayName < $1.displayName }
    }

    public func contact(userId: String) throws -> Contact? {
        lock.lock(); defer { lock.unlock() }
        return people[userId]
    }

    public func wipeUserData() throws {
        lock.lock(); defer { lock.unlock() }
        messages.removeAll()
        convos.removeAll()
        people.removeAll()
        resume = 0
    }
}

private extension MessageStatus {
    var rank: Int {
        switch self {
        case .pending: return 0
        case .failed: return 0
        case .sent: return 1
        case .delivered: return 2
        case .read: return 3
        }
    }
}
