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
            INSERT INTO messages(
              message_id, conversation_id, direction, body, media_id, seq, status, created_at,
              from_user, delivered_count, read_count, member_count
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(message_id) DO UPDATE SET
              seq=MAX(messages.seq, excluded.seq),
              status=CASE
                WHEN excluded.status='read' THEN 'read'
                WHEN excluded.status='delivered' AND messages.status!='read' THEN 'delivered'
                WHEN excluded.status='sent' AND messages.status IN ('pending','failed') THEN 'sent'
                WHEN excluded.status='failed' AND messages.status='pending' THEN 'failed'
                ELSE messages.status END,
              body=CASE WHEN length(excluded.body)>0 THEN excluded.body ELSE messages.body END,
              from_user=CASE WHEN length(excluded.from_user)>0 THEN excluded.from_user ELSE messages.from_user END,
              delivered_count=MAX(messages.delivered_count, excluded.delivered_count),
              read_count=MAX(messages.read_count, excluded.read_count),
              member_count=CASE WHEN excluded.member_count>0 THEN excluded.member_count ELSE messages.member_count END
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
            bindText(stmt, 9, message.fromUser)
            sqlite3_bind_int(stmt, 10, Int32(message.deliveredCount))
            sqlite3_bind_int(stmt, 11, Int32(message.readCount))
            sqlite3_bind_int(stmt, 12, Int32(message.memberCount))
            try check(sqlite3_step(stmt), db, ok: SQLITE_DONE)

            let isGroup = Conversation.groupId(fromConversationId: message.conversationId) != nil
            let preview = message.body.isEmpty ? "(media)" : String(message.body.prefix(120))
            let unreadInc = (!existed && message.direction == .inbound) ? 1 : 0
            try exec(
                db,
                """
                INSERT INTO conversations(id, title, sort_ts, unread, draft, last_preview, is_group)
                VALUES('\(escape(message.conversationId))', '\(escape(message.conversationId))', \(message.createdAt.timeIntervalSince1970), \(unreadInc), '', '\(escape(preview))', \(isGroup ? 1 : 0))
                ON CONFLICT(id) DO UPDATE SET
                  sort_ts=MAX(conversations.sort_ts, excluded.sort_ts),
                  unread=conversations.unread + \(unreadInc),
                  last_preview=excluded.last_preview,
                  is_group=MAX(conversations.is_group, excluded.is_group)
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

    public func updateGroupAckSummary(
        messageId: String,
        deliveredCount: UInt32,
        readCount: UInt32,
        memberCount: UInt32
    ) throws {
        try withDB { db in
            try exec(
                db,
                """
                UPDATE messages SET
                  delivered_count=MAX(delivered_count, \(deliveredCount)),
                  read_count=MAX(read_count, \(readCount)),
                  member_count=CASE WHEN \(memberCount)>0 THEN \(memberCount) ELSE member_count END,
                  status=CASE
                    WHEN \(readCount)>0 AND status!='read' THEN 'delivered'
                    WHEN \(deliveredCount)>0 AND status IN ('pending','sent') THEN 'delivered'
                    ELSE status END
                WHERE message_id='\(escape(messageId))'
                """
            )
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
                           k.presence, k.last_seen, c.is_group
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
                let isGroup = sqlite3_column_int(stmt, 8) != 0
                    || Conversation.groupId(fromConversationId: string(stmt, 0)) != nil
                rows.append(
                    Conversation(
                        id: string(stmt, 0),
                        title: string(stmt, 1),
                        sortTs: Date(timeIntervalSince1970: sqlite3_column_double(stmt, 2)),
                        unread: Int(sqlite3_column_int(stmt, 3)),
                        draft: string(stmt, 4),
                        lastPreview: string(stmt, 5),
                        presence: isGroup ? nil : presenceRaw,
                        lastSeen: isGroup ? nil : lastSeen,
                        isGroup: isGroup
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
                    SELECT message_id, conversation_id, direction, body, media_id, seq, status, created_at,
                           from_user, delivered_count, read_count, member_count
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
                        createdAt: Date(timeIntervalSince1970: sqlite3_column_double(stmt, 7)),
                        fromUser: string(stmt, 8),
                        deliveredCount: UInt32(sqlite3_column_int(stmt, 9)),
                        readCount: UInt32(sqlite3_column_int(stmt, 10)),
                        memberCount: UInt32(sqlite3_column_int(stmt, 11))
                    )
                )
            }
            return rows
        }
    }

    public func setDraft(conversationId: String, draft: String) throws {
        try withDB { db in
            let isGroup = Conversation.groupId(fromConversationId: conversationId) != nil ? 1 : 0
            try exec(
                db,
                """
                INSERT INTO conversations(id, title, sort_ts, unread, draft, last_preview, is_group)
                VALUES('\(escape(conversationId))', '\(escape(conversationId))', \(Date().timeIntervalSince1970), 0, '\(escape(draft))', '', \(isGroup))
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

    public func ensureConversation(id: String, title: String?, isGroup: Bool) throws {
        try withDB { db in
            let t = title ?? id
            let g = isGroup || Conversation.groupId(fromConversationId: id) != nil
            try exec(
                db,
                """
                INSERT INTO conversations(id, title, sort_ts, unread, draft, last_preview, is_group)
                VALUES('\(escape(id))', '\(escape(t))', \(Date().timeIntervalSince1970), 0, '', '', \(g ? 1 : 0))
                ON CONFLICT(id) DO UPDATE SET
                  title=CASE WHEN length(excluded.title)>0 THEN excluded.title ELSE conversations.title END,
                  is_group=MAX(conversations.is_group, excluded.is_group)
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

    public func upsertGroup(_ group: GroupInfo) throws {
        try withDB { db in
            try exec(
                db,
                """
                INSERT INTO groups(id, title, version)
                VALUES('\(escape(group.id))', '\(escape(group.title))', \(group.version))
                ON CONFLICT(id) DO UPDATE SET
                  title=CASE WHEN length(excluded.title)>0 THEN excluded.title ELSE groups.title END,
                  version=MAX(groups.version, excluded.version)
                """
            )
            try exec(db, "DELETE FROM group_members WHERE group_id='\(escape(group.id))'")
            for member in group.members {
                try exec(
                    db,
                    """
                    INSERT INTO group_members(group_id, user_id, is_admin)
                    VALUES('\(escape(group.id))', '\(escape(member.userId))', \(member.isAdmin ? 1 : 0))
                    """
                )
            }
            let convoId = Conversation.groupConversationId(group.id)
            try exec(
                db,
                """
                INSERT INTO conversations(id, title, sort_ts, unread, draft, last_preview, is_group)
                VALUES('\(escape(convoId))', '\(escape(group.title))', \(Date().timeIntervalSince1970), 0, '', '', 1)
                ON CONFLICT(id) DO UPDATE SET title=excluded.title, is_group=1
                """
            )
        }
    }

    public func group(id: String) throws -> GroupInfo? {
        try withDB { db in
            var stmt: OpaquePointer?
            defer { sqlite3_finalize(stmt) }
            try check(
                sqlite3_prepare_v2(db, "SELECT id, title, version FROM groups WHERE id=?", -1, &stmt, nil),
                db
            )
            bindText(stmt, 1, id)
            guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
            let title = string(stmt, 1)
            let version = UInt64(sqlite3_column_int64(stmt, 2))
            var membersStmt: OpaquePointer?
            defer { sqlite3_finalize(membersStmt) }
            try check(
                sqlite3_prepare_v2(
                    db,
                    "SELECT user_id, is_admin FROM group_members WHERE group_id=? ORDER BY user_id",
                    -1,
                    &membersStmt,
                    nil
                ),
                db
            )
            bindText(membersStmt, 1, id)
            var members: [GroupMember] = []
            while sqlite3_step(membersStmt) == SQLITE_ROW {
                members.append(
                    GroupMember(userId: string(membersStmt, 0), isAdmin: sqlite3_column_int(membersStmt, 1) != 0)
                )
            }
            return GroupInfo(id: id, title: title, version: version, members: members)
        }
    }

    public func groups() throws -> [GroupInfo] {
        try withDB { db in
            var stmt: OpaquePointer?
            defer { sqlite3_finalize(stmt) }
            try check(sqlite3_prepare_v2(db, "SELECT id FROM groups ORDER BY title ASC", -1, &stmt, nil), db)
            var ids: [String] = []
            while sqlite3_step(stmt) == SQLITE_ROW {
                ids.append(string(stmt, 0))
            }
            return try ids.compactMap { try groupUnlocked(db, id: $0) }
        }
    }

    @discardableResult
    public func applyGroupEvent(
        groupId: String,
        op: GroupOp,
        actor: String,
        subject: String,
        version: UInt64
    ) throws -> Bool {
        try withDB { db in
            let current = try groupUnlocked(db, id: groupId)
            if let current, version <= current.version {
                return false
            }
            var members = current?.members ?? []
            let title = current?.title ?? groupId
            switch op {
            case .create:
                if !members.contains(where: { $0.userId == actor }) {
                    members.append(GroupMember(userId: actor, isAdmin: true))
                } else {
                    members = members.map {
                        $0.userId == actor ? GroupMember(userId: actor, isAdmin: true) : $0
                    }
                }
            case .addMember:
                if !members.contains(where: { $0.userId == subject }) {
                    members.append(GroupMember(userId: subject, isAdmin: false))
                }
            case .removeMember:
                members.removeAll { $0.userId == subject }
            case .leave:
                members.removeAll { $0.userId == actor }
            case .unspecified:
                break
            }
            try exec(
                db,
                """
                INSERT INTO groups(id, title, version)
                VALUES('\(escape(groupId))', '\(escape(title))', \(version))
                ON CONFLICT(id) DO UPDATE SET version=excluded.version
                """
            )
            try exec(db, "DELETE FROM group_members WHERE group_id='\(escape(groupId))'")
            for member in members {
                try exec(
                    db,
                    """
                    INSERT INTO group_members(group_id, user_id, is_admin)
                    VALUES('\(escape(groupId))', '\(escape(member.userId))', \(member.isAdmin ? 1 : 0))
                    """
                )
            }
            let convoId = Conversation.groupConversationId(groupId)
            try exec(
                db,
                """
                INSERT INTO conversations(id, title, sort_ts, unread, draft, last_preview, is_group)
                VALUES('\(escape(convoId))', '\(escape(title))', \(Date().timeIntervalSince1970), 0, '', '', 1)
                ON CONFLICT(id) DO UPDATE SET is_group=1
                """
            )
            return true
        }
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
                  last_preview TEXT NOT NULL DEFAULT '',
                  is_group INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS messages(
                  message_id TEXT PRIMARY KEY NOT NULL,
                  conversation_id TEXT NOT NULL,
                  direction TEXT NOT NULL,
                  body TEXT NOT NULL,
                  media_id TEXT NOT NULL DEFAULT '',
                  seq INTEGER NOT NULL DEFAULT 0,
                  status TEXT NOT NULL,
                  created_at REAL NOT NULL,
                  from_user TEXT NOT NULL DEFAULT '',
                  delivered_count INTEGER NOT NULL DEFAULT 0,
                  read_count INTEGER NOT NULL DEFAULT 0,
                  member_count INTEGER NOT NULL DEFAULT 0
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
            try? exec(db, "ALTER TABLE conversations ADD COLUMN is_group INTEGER NOT NULL DEFAULT 0")
            try? exec(db, "ALTER TABLE messages ADD COLUMN from_user TEXT NOT NULL DEFAULT ''")
            try? exec(db, "ALTER TABLE messages ADD COLUMN delivered_count INTEGER NOT NULL DEFAULT 0")
            try? exec(db, "ALTER TABLE messages ADD COLUMN read_count INTEGER NOT NULL DEFAULT 0")
            try? exec(db, "ALTER TABLE messages ADD COLUMN member_count INTEGER NOT NULL DEFAULT 0")
        }
    }

    private func groupUnlocked(_ db: OpaquePointer, id: String) throws -> GroupInfo? {
        var stmt: OpaquePointer?
        defer { sqlite3_finalize(stmt) }
        try check(sqlite3_prepare_v2(db, "SELECT id, title, version FROM groups WHERE id=?", -1, &stmt, nil), db)
        bindText(stmt, 1, id)
        guard sqlite3_step(stmt) == SQLITE_ROW else { return nil }
        let title = string(stmt, 1)
        let version = UInt64(sqlite3_column_int64(stmt, 2))
        var membersStmt: OpaquePointer?
        defer { sqlite3_finalize(membersStmt) }
        try check(
            sqlite3_prepare_v2(
                db,
                "SELECT user_id, is_admin FROM group_members WHERE group_id=? ORDER BY user_id",
                -1,
                &membersStmt,
                nil
            ),
            db
        )
        bindText(membersStmt, 1, id)
        var members: [GroupMember] = []
        while sqlite3_step(membersStmt) == SQLITE_ROW {
            members.append(
                GroupMember(userId: string(membersStmt, 0), isAdmin: sqlite3_column_int(membersStmt, 1) != 0)
            )
        }
        return GroupInfo(id: id, title: title, version: version, members: members)
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
    private var groupInfos: [String: GroupInfo] = [:]
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
            if !message.fromUser.isEmpty { existing.fromUser = message.fromUser }
            existing.deliveredCount = max(existing.deliveredCount, message.deliveredCount)
            existing.readCount = max(existing.readCount, message.readCount)
            if message.memberCount > 0 { existing.memberCount = message.memberCount }
            if message.status.rank >= existing.status.rank {
                existing.status = message.status
            }
            messages[message.messageId] = existing
        } else {
            messages[message.messageId] = message
        }
        if message.seq > resume { resume = message.seq }
        let isGroup = Conversation.groupId(fromConversationId: message.conversationId) != nil
        var c = convos[message.conversationId] ?? Conversation(id: message.conversationId, isGroup: isGroup)
        c.isGroup = c.isGroup || isGroup
        c.sortTs = max(c.sortTs, message.createdAt)
        c.lastPreview = message.body.isEmpty ? c.lastPreview : String(message.body.prefix(120))
        if !existed, message.direction == .inbound { c.unread += 1 }
        if !c.isGroup, let contact = people[message.conversationId] {
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

    public func updateGroupAckSummary(
        messageId: String,
        deliveredCount: UInt32,
        readCount: UInt32,
        memberCount: UInt32
    ) throws {
        lock.lock(); defer { lock.unlock() }
        guard var m = messages[messageId] else { return }
        m.deliveredCount = max(m.deliveredCount, deliveredCount)
        m.readCount = max(m.readCount, readCount)
        if memberCount > 0 { m.memberCount = memberCount }
        if deliveredCount > 0, m.status == .pending || m.status == .sent {
            m.status = .delivered
        }
        messages[messageId] = m
    }

    public func conversations() throws -> [Conversation] {
        lock.lock(); defer { lock.unlock() }
        return convos.values.map { c in
            var out = c
            if !out.isGroup, let contact = people[c.id] {
                out.presence = contact.presence
                out.lastSeen = contact.lastSeen
                out.title = contact.displayName
            }
            if out.isGroup, let gid = Conversation.groupId(fromConversationId: c.id),
               let g = groupInfos[gid] {
                out.title = g.title
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
        var c = convos[conversationId] ?? Conversation(
            id: conversationId,
            isGroup: Conversation.groupId(fromConversationId: conversationId) != nil
        )
        c.draft = draft
        convos[conversationId] = c
    }

    public func markConversationRead(conversationId: String) throws {
        lock.lock(); defer { lock.unlock() }
        guard var c = convos[conversationId] else { return }
        c.unread = 0
        convos[conversationId] = c
    }

    public func ensureConversation(id: String, title: String?, isGroup: Bool) throws {
        lock.lock(); defer { lock.unlock() }
        let group = isGroup || Conversation.groupId(fromConversationId: id) != nil
        if convos[id] == nil {
            convos[id] = Conversation(id: id, title: title ?? id, isGroup: group)
        } else {
            if let title, !title.isEmpty { convos[id]?.title = title }
            if group { convos[id]?.isGroup = true }
        }
    }

    public func upsertContact(_ contact: Contact) throws {
        lock.lock(); defer { lock.unlock() }
        people[contact.userId] = contact
        if var c = convos[contact.userId], !c.isGroup {
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

    public func upsertGroup(_ group: GroupInfo) throws {
        lock.lock(); defer { lock.unlock() }
        groupInfos[group.id] = group
        let convoId = Conversation.groupConversationId(group.id)
        var c = convos[convoId] ?? Conversation(id: convoId, title: group.title, isGroup: true)
        c.title = group.title
        c.isGroup = true
        convos[convoId] = c
    }

    public func group(id: String) throws -> GroupInfo? {
        lock.lock(); defer { lock.unlock() }
        return groupInfos[id]
    }

    public func groups() throws -> [GroupInfo] {
        lock.lock(); defer { lock.unlock() }
        return groupInfos.values.sorted { $0.title < $1.title }
    }

    @discardableResult
    public func applyGroupEvent(
        groupId: String,
        op: GroupOp,
        actor: String,
        subject: String,
        version: UInt64
    ) throws -> Bool {
        lock.lock(); defer { lock.unlock() }
        if let existing = groupInfos[groupId], version <= existing.version {
            return false
        }
        var info = groupInfos[groupId] ?? GroupInfo(id: groupId, title: groupId, version: 0)
        if version <= info.version { return false }
        switch op {
        case .create:
            if !info.members.contains(where: { $0.userId == actor }) {
                info.members.append(GroupMember(userId: actor, isAdmin: true))
            } else {
                info.members = info.members.map {
                    $0.userId == actor ? GroupMember(userId: actor, isAdmin: true) : $0
                }
            }
        case .addMember:
            if !info.members.contains(where: { $0.userId == subject }) {
                info.members.append(GroupMember(userId: subject, isAdmin: false))
            }
        case .removeMember:
            info.members.removeAll { $0.userId == subject }
        case .leave:
            info.members.removeAll { $0.userId == actor }
        case .unspecified:
            break
        }
        info.version = version
        groupInfos[groupId] = info
        let convoId = Conversation.groupConversationId(groupId)
        var c = convos[convoId] ?? Conversation(id: convoId, title: info.title, isGroup: true)
        c.isGroup = true
        c.title = info.title
        convos[convoId] = c
        return true
    }

    public func wipeUserData() throws {
        lock.lock(); defer { lock.unlock() }
        messages.removeAll()
        convos.removeAll()
        people.removeAll()
        groupInfos.removeAll()
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
