# iOS Swift Client UI — FunXMPP Messenger Plan

Goal: ship a production-quality **iOS (Swift / SwiftUI)** client for the
WhatsApp-like compact binary messenger defined in [`todo.md`](todo.md) and
implemented in `src/messenger/` + `proto/messenger.proto`.

This plan is the **client-side counterpart** of the server roadmap. Every
server capability that a mobile client must speak or display is listed here.
Do **not** skip a phase’s Tests / Docs / Exit criteria.

Legend: `[ ]` pending · `[~]` in progress / partial · `[x]` done

**Server dependency (must stay green):** Phase 0–10 of `todo.md` (wire,
sessions, auth, presence, routing/acks, offline sync, heartbeats, groups,
media, E2EE). Client may target a single gateway first; cluster is
transparent on the wire.

**Canonical references (read before coding each phase):**

| Doc | Why the iOS client needs it |
|-----|-----------------------------|
| [`docs/messenger/00_overview.md`](docs/messenger/00_overview.md) | Architecture, delivery guarantees |
| [`docs/messenger/01_wire_protocol.md`](docs/messenger/01_wire_protocol.md) | Frame layout, packet table, lifecycle, errors |
| [`docs/messenger/02_bulk_data.md`](docs/messenger/02_bulk_data.md) | Media upload/download contract |
| [`docs/messenger/03_sessions.md`](docs/messenger/03_sessions.md) | Login → sync → idle / kick semantics |
| [`docs/messenger/04_auth.md`](docs/messenger/04_auth.md) | Token format, multi-device, backoff |
| [`docs/messenger/06_offline_store.md`](docs/messenger/06_offline_store.md) | `resume_after_seq`, SyncComplete |
| [`docs/messenger/10_e2ee.md`](docs/messenger/10_e2ee.md) | Olm / Megolm client crypto |
| [`docs/messenger/11_security_review.md`](docs/messenger/11_security_review.md) | Client threat-model checklist |
| [`docs/messenger/limits.md`](docs/messenger/limits.md) | Frame/media/idle limits the client must respect |
| [`proto/messenger.proto`](proto/messenger.proto) | Exact field numbers (append-only) |
| Rust reference client | `src/messenger/client.rs`, `src/messenger/codec.rs`, `src/messenger/e2ee.rs` |

---

## Product scope (UI + protocol)

### In scope (WhatsApp-like MVP → production)

- Login / logout / multi-device session awareness
- Contact list with presence (Available / Unavailable / LastSeen)
- 1:1 chat with WhatsApp-style ticks (sent → delivered → read)
- Offline catch-up on reconnect (`resume_after_seq` → `SyncComplete`)
- Group create / add / remove / leave + group chat
- Media: images, PDFs, generic files (chunked upload + fetch)
- E2EE: Olm 1:1 + Megolm groups + safety numbers + multi-device keys
- Push-ready architecture (APNs hook; server push gateway may land later)
- Local message store, draft, search, unread badges
- TLS to gateway; Keychain for tokens / identity keys

### Explicitly out of scope (server / peer only — do not implement on iOS)

- Peer packets `0x50`–`0x57` (`PeerHello`, `PeerPresence`, `PeerSync`,
  `PeerJoin`/`Leave`, handoffs, `PeerMediaReady`) — gateway-to-gateway only
- Hash-ring / home-shard logic — transparent to clients
- Server WAL / StorageNode / Prometheus — ops only

### Platform targets

| Item | Requirement |
|------|-------------|
| Language | Swift 5.9+ |
| UI | SwiftUI (primary); UIKit bridges only where needed (e.g. document picker) |
| Min OS | iOS 17+ (Network.framework, Observation, SwiftData optional) |
| Architecture | MVVM + actor-isolated networking (`MessengerConnection` actor) |
| Concurrency | Swift Concurrency (`async`/`await`, `AsyncStream` for inbound packets) |
| Protobuf | `swift-protobuf` generated from `proto/messenger.proto` |
| Crypto (E2EE) | Prefer **Olm** via `libolm` / Matrix crypto bindings; do not hand-roll |
| Persistence | SwiftData or GRDB; Keychain for secrets |
| Package | Xcode project + optional SPM modules (`LaneMessengerCore`, `LaneMessengerUI`) |

---

## Phase C0 — Project foundation & hygiene

Mirror server Phase 0: isolate protocol from UI.

- [ ] Create Xcode workspace / app target: `LaneMessenger` (iOS app).
- [ ] Create SPM (or Xcode) library targets:
      - [ ] `LaneMessengerWire` — frame codec + protobuf types only
      - [ ] `LaneMessengerClient` — TCP/TLS session, APIs mirroring
            `MessengerClient` in Rust
      - [ ] `LaneMessengerE2EE` — Olm/Megolm wrapper
      - [ ] `LaneMessengerUI` — SwiftUI views (optional separate module)
- [ ] Vendor / generate Swift types from `proto/messenger.proto`:
      - [ ] Add `swift-protobuf` plugin to CI / `Scripts/generate_proto.sh`
      - [ ] Package name mapping: `lane_switchboard.messenger` →
            `LaneMessengerWire` Swift module
      - [ ] **Never** hand-edit generated `.pb.swift`; regenerate on proto change
- [ ] App configuration:
      - [ ] `Info.plist` ATS exceptions only for local debug hosts (document)
      - [ ] Entitlements: Keychain sharing (if multi-app), Push (later),
            Background Modes: `voip` **or** `fetch` only if justified
      - [ ] Feature flags: `USE_TLS`, `E2EE_ENABLED`, `DEBUG_PLAINTEXT_FALLBACK`
- [ ] Logging: `os.Logger` categories (`wire`, `session`, `chat`, `media`,
      `e2ee`); **never** log `auth_token`, private keys, or plaintext when
      E2EE is on (align with `04_auth.md` / `11_security_review.md`).
- [ ] Dependency policy: pin versions; document licenses (esp. libolm).

**Tests**
- [ ] Empty app launches on simulator + device.
- [ ] Proto generation script is idempotent in CI.
- [ ] Module graph compiles with `E2EE_ENABLED=0` and `=1`.

**Docs**
- [ ] `docs/client-ios/00_overview.md` — module map, how it maps to
      `docs/messenger/00_overview.md`.
- [ ] `docs/client-ios/README.md` — build, run against local gateway,
      env vars (`MESSENGER_HOST`, `MESSENGER_PORT`, token minting for debug).

**Exit criteria**: clean build; generated protobuf types match every
client-facing message in `messenger.proto` (exclude peer-only messages from
public client API surface, but keep types if shared package).

---

## Phase C1 — Compact binary wire codec (Swift)

Mirror server Phase 1 / `docs/messenger/01_wire_protocol.md`.

### Frame layout (must match Rust `FrameCodec` exactly)

```text
+---------+--------------+-------------+------------------+
| u8 ver  | u8 pkt_type  | u32 length  | protobuf payload |
+---------+--------------+-------------+------------------+
   1 byte     1 byte        4 bytes        `length` bytes
```

- [ ] Implement `FrameEncoder` / `FrameDecoder`:
      - [ ] Big-endian `UInt32` length
      - [ ] `PROTOCOL_VERSION = 1`
      - [ ] Reject unknown `pkt_type` **before** allocating payload buffer
      - [ ] Reject `length > maxFrame` (default **256 KiB**, same as
            `ServerConfig.max_frame` / `limits.md`) before allocation
      - [ ] Partial-frame buffering (TCP stream may split mid-frame)
      - [ ] Pipelined frames: decode multiple frames from one read buffer
- [ ] Packet type enum (client-relevant subset; keep numeric IDs identical):

| ID | Packet | Direction | Client must |
|----|--------|-----------|-------------|
| 0x01 | `Login` | C→S | send first |
| 0x02 | `LoginAck` | S→C | handle |
| 0x03 | `Ping` | C→S | send on timer |
| 0x04 | `Pong` | S→C | match seq |
| 0x0F | `ProtocolError` | S→C | typed fatal (except media) |
| 0x10 | `Presence` | both | send/receive |
| 0x20 | `ChatMessage` | both | send/receive |
| 0x21 | `ServerAck` | S→C | ticks |
| 0x22 | `DeliveredAck` | both | send/receive |
| 0x23 | `ReadAck` | both | send/receive |
| 0x24 | `SyncComplete` | S→C | end offline replay |
| 0x30 | `MediaStart` | both | upload/download |
| 0x31 | `MediaChunk` | both | ≤64 KiB chunks |
| 0x32 | `MediaAck` | S→C | flow control |
| 0x33 | `MediaFetch` | C→S | download |
| 0x40 | `GroupMessage` | both | groups |
| 0x41 | `GroupEvent` | both | membership |
| 0x60 | `PublishKeys` | C→S | E2EE |
| 0x61 | `FetchKeys` | C→S | E2EE |
| 0x62 | `KeyBundle` | S→C | E2EE |
| 0x63 | `RemoveDeviceKeys` | C→S | E2EE revoke |

- [ ] Map every protobuf message field used by the Rust client (including
      `EncryptedPayload`, `EncryptedGroupPayload`, `GroupSessionKeyShare` —
      **not** frame types; embedded in `body` bytes).
- [ ] Typed Swift errors mirroring `MessengerError`:
      `unsupportedVersion`, `unknownPacketType`, `frameTooLarge`,
      `decode`, `closed`, `timeout`, `protocol`, `authFailed`, `media`, `e2ee`.

**Tests**
- [ ] Unit: encode/decode round-trip for **every** client packet type above
      (golden vectors exported from Rust `codec.rs` tests preferred).
- [ ] Unit: malformed / truncated / oversized / garbage payload / wrong
      version / pipelined frames (parity with Rust’s 7 codec tests).
- [ ] Cross-language: Rust encodes → Swift decodes; Swift encodes → Rust
      decodes (fixture files under `testdata/frames/`).

**Docs**
- [ ] `docs/client-ios/01_wire_codec.md` — Swift API, endianness, max frame,
      link to server `01_wire_protocol.md`.

**Exit criteria**: Swift codec interoperates with Rust fixtures; no silent
truncation; unknown types fail closed.

---

## Phase C2 — Transport, session state machine, TLS

Mirror server Phases 2 + 7 / `03_sessions.md`.

### Transport

- [ ] `NWConnection` (Network.framework) TCP client to `host:port`.
- [ ] TLS path (production default):
      - [ ] `NWProtocolTLS` with certificate validation against host
      - [ ] Debug: optional pinned self-signed CA for local `bind_tls` demos
      - [ ] Plaintext only behind explicit debug flag (never App Store builds)
- [ ] Socket options: TCP_NODELAY if available; rely on OS keepalive + app
      Ping/Pong (server `tcp_keepalive` default 60 s; client idle must stay
      under server `idle_timeout` **90 s**).
- [ ] Single connection actor owns read loop + write queue (ordered writes).

### Session state machine (client)

```text
Disconnected
    │ connect
    ▼
Connecting (TCP/TLS handshake)
    │
    ▼
AwaitingLogin ──(must send Login within < server login_deadline 10s)──► fail
    │ LoginAck.ok
    ▼
Syncing (receive Chat/Group/… until SyncComplete)
    │
    ▼
Ready (steady state: chat, presence, media, ping)
    │ ProtocolError / EOF / idle / kick
    ▼
Disconnected → auto-reconnect policy
```

- [ ] Implement states above; UI binds to `ConnectionState`.
- [ ] First frame **must** be `Login`; reject local attempts to send chat
      pre-auth (server will close with `NOT_AUTHENTICATED`).
- [ ] Handle `ProtocolError` codes (from proto `ErrorCode`):

| Code | Client behaviour |
|------|------------------|
| `UNSUPPORTED_VERSION` | Force upgrade UI; do not reconnect loop |
| `NOT_AUTHENTICATED` | Re-login / clear bad session |
| `AUTH_FAILED` | Show auth error; apply client-side backoff hint |
| `REPLACED_BY_NEW_SESSION` | “Logged in elsewhere on this device”; stop reconnect with same device until user confirms |
| `FRAME_TOO_LARGE` | Bug report; shrink payload |
| `MALFORMED_FRAME` | Disconnect; reconnect with backoff |
| `RATE_LIMITED` | Backoff; surface “try again later” |
| `UNKNOWN_RECIPIENT` | Non-fatal for that send (if server ever returns non-fatal); else show error |
| `MEDIA_TRANSFER_FAILED` | Fail transfer only; keep session |

- [ ] Backpressure: do not unbounded-buffer outbound frames; if write queue
      grows past N, fail sends with retry (server drops slow consumers;
      inbox recovers on sync).
- [ ] Graceful close: cancel ping timer, finish in-flight writes, cancel
      `NWConnection`.

### Heartbeats (server Phase 7)

- [ ] Client→server `Ping { seq }` every **~30 s** while `Ready`.
- [ ] Expect `Pong` with same `seq`; track RTT for diagnostics UI (optional).
- [ ] If no inbound traffic for ~60–75 s, reconnect **before** server’s 90 s
      idle sweep closes the socket.
- [ ] On foreground/background:
      - [ ] Foreground: ensure connection + ping resume
      - [ ] Background: expect disconnect; on foreground use
            `resume_after_seq` (Phase C5)

**Tests**
- [ ] Integration against local `MessengerServer::bind` / `bind_tls`:
      connect → Login → LoginAck → SyncComplete → Ping/Pong.
- [ ] Login after deadline simulation (server closes).
- [ ] TLS handshake failure surfaces typed error.
- [ ] Same-device second login receives `REPLACED_BY_NEW_SESSION` on first
      (parity with `same_device_reconnect_replaces_old_session`).

**Docs**
- [ ] `docs/client-ios/02_session.md` — state machine diagram, TLS setup,
      ping intervals, mapping to `03_sessions.md`.

**Exit criteria**: stable Ready state against local gateway; idle reconnect
works; kick handled without silent dual sessions on one device id.

---

## Phase C3 — Authentication & identity UX

Mirror server Phase 3 / `04_auth.md`.

### Token model (current server)

- Token: `hex(HMAC-SHA256(secret, "user_id:device_id"))` via
  `HmacAuthenticator`.
- Production path: tokens minted by a **separate identity service** (JWT
  planned server-side). iOS must not embed the HMAC secret in App Store
  builds.

- [ ] Debug/dev: optional “mint with shared secret” **DEBUG-only** helper
      (compile-time stripped).
- [ ] Production: `AuthService` protocol:
      - [ ] `login(username, password) → (userId, deviceId, authToken, expiresAt?)`
      - [ ] Refresh / logout hooks (even if server tokens don’t expire yet)
- [ ] Persist in **Keychain**:
      - [ ] `user_id`, `device_id`, `auth_token` (and refresh token if any)
      - [ ] Accessibility: `kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly`
- [ ] Stable `device_id`:
      - [ ] Generate UUID once; store in Keychain (not UserDefaults alone)
      - [ ] Session key is `(user_id, device_id)` — changing device_id creates
            a new logical device (multi-device E2EE implications)
- [ ] `Login` fields (proto):
      - [ ] `user_id`, `device_id`, `auth_token`
      - [ ] `client_version` — e.g. `"ios-1.0.0 (build N)"`
      - [ ] `resume_after_seq` — from local DB high-water mark (Phase C5)
- [ ] Handle `LoginAck`:
      - [ ] `ok == false` → show `error`, stay logged out
      - [ ] `session_id` → diagnostics
      - [ ] `pending_messages` → show sync progress UI (“Downloading N messages…”)
- [ ] Client-side backoff on repeated `AUTH_FAILED` (mirror server exponential
      backoff UX: 250 ms → 30 s cap awareness — don’t hammer).
- [ ] UI screens:
      - [ ] Splash / session restore
      - [ ] Login form (user id + password or token paste for staging)
      - [ ] “This device was replaced” alert on `REPLACED_BY_NEW_SESSION`
      - [ ] Settings → Devices / Sign out

**Tests**
- [ ] Unit: Keychain round-trip; device_id stability across relaunch.
- [ ] Integration: valid token → LoginAck.ok; bad token → AUTH_FAILED /
      LoginAck failure path.
- [ ] Unauthenticated chat attempt never leaves the client (guard) and
      matches server `unauthenticated_chat_rejected` behaviour if forced.

**Docs**
- [ ] `docs/client-ios/03_auth.md` — Keychain layout, debug minting rules,
      production identity service contract.

**Exit criteria**: cold start restores session; no secrets in logs or
UserDefaults; multi-device kick UX is clear.

---

## Phase C4 — Presence registry (client model + UI)

Mirror server Phase 4 (single-node broadcast today; contact filtering may
still be pending server-side).

- [ ] Inbound `Presence { user_id, kind, last_seen }`:
      - [ ] `AVAILABLE` → green / online
      - [ ] `UNAVAILABLE` → offline; store `last_seen` if provided
      - [ ] `LAST_SEEN` → update last-seen timestamp (unix seconds)
- [ ] Local `ContactStore`:
      - [ ] Contacts list (bootstrap: manual add / QR / debug seed list until
            server contact API exists)
      - [ ] Cache last presence per `user_id`
      - [ ] Privacy: do not invent last-seen if server didn’t send it
- [ ] Optional outbound presence (if product wants “typing” later — **not**
      in proto today; do not invent packet types)
- [ ] UI:
      - [ ] Chats list row: avatar placeholder, name, presence dot, last message preview
      - [ ] Chat header: “online” / “last seen …” formatting (local timezone)
      - [ ] Note server limitation: presence may broadcast to all online users
            until contact-list subscription filtering ships (`todo.md` Phase 4)

**Tests**
- [ ] Integration: second user login → first receives `AVAILABLE`; disconnect
      → `UNAVAILABLE` (parity with `presence_broadcast_on_login_and_disconnect`).
- [ ] UI snapshot / preview tests for presence states.

**Docs**
- [ ] `docs/client-ios/04_presence.md` — mapping to server presence kinds;
      known server broadcast limitation.

**Exit criteria**: presence updates appear live without polling; last-seen
renders correctly from unix seconds.

---

## Phase C5 — 1:1 chat, ack ladder, local store, offline sync

Mirror server Phases 5 + 6 / wire delivery semantics.

### Send path

- [ ] Generate `message_id` = UUID string (dedup key; retries safe).
- [ ] `ChatMessage` fields:
      - [ ] `from_user` set to authenticated user (server overwrites anyway)
      - [ ] `to_user`, `body` (UTF-8 plaintext **or** E2EE payload bytes),
            `sent_at` = unix millis, `media_id` optional, `seq=0` on send
- [ ] Wait for `ServerAck { message_id, seq }` before marking send “complete”
      (parity with Rust `send_chat`).
- [ ] Retry with backoff until `ServerAck` (server dedups; duplicate may
      return `seq = 0` sentinel — treat as success, do not create a second
      local row).
- [ ] Optimistic UI: show message as “clock” / pending until ServerAck →
      single tick.

### Ack ladder (WhatsApp ticks)

| Event | Packet | UI |
|-------|--------|-----|
| Persisted on server | `ServerAck` | single grey tick |
| Recipient device got it | inbound `DeliveredAck` for our `message_id` | double grey tick |
| Recipient read it | inbound `ReadAck` | double blue tick |

- [ ] On receiving `ChatMessage`:
      - [ ] Persist locally with server `seq`
      - [ ] Send `DeliveredAck { message_id, from_user=me }` promptly
      - [ ] When chat is visible / marked read → send `ReadAck`
- [ ] Dedup inbound by `message_id` (and/or `seq`) so sync + live push don’t
      double-render.

### Offline sync

- [ ] Persist `latest_seq` (per account) after processing inbox messages.
- [ ] On next `Login`, set `resume_after_seq` to that value (parity with
      `resume_after_seq_skips_already_seen_messages`).
- [ ] During Syncing:
      - [ ] Accept streamed `ChatMessage` / `GroupMessage` / acks / presence
      - [ ] End on `SyncComplete { delivered, latest_seq }`
      - [ ] Update UI progress from `LoginAck.pending_messages` + counts
- [ ] Ordering: display by `seq` within a conversation where available;
      fall back to `sent_at` for local-only pending.

### Local persistence

- [ ] Tables / models (minimum):
      - [ ] `Conversation` (1:1 peer id or group id, unread count, sort key)
      - [ ] `Message` (id, conversation, direction, body/ciphertext, media_id,
            sent_at, seq, status: pending/sent/delivered/read/failed)
      - [ ] `SyncState` (latest_seq, last_sync_at)
- [ ] Drafts per conversation
- [ ] Search: local FTS on plaintext only when decrypted / non-E2EE

### UI

- [ ] Chats tab (conversation list)
- [ ] 1:1 thread: bubbles, ticks, day separators, send box, attach button
- [ ] Failed send → tap to retry
- [ ] Unread badge on tab + conversation row
- [ ] Empty states

**Tests**
- [ ] Integration: online delivery + full tick ladder (parity
      `online_delivery_with_full_tick_ladder`).
- [ ] Integration: offline peer → message stored → peer login → ordered
      replay (parity `offline_messages_replayed_in_order_on_login`).
- [ ] Integration: duplicate `message_id` does not duplicate UI (parity
      `duplicate_send_is_deduplicated`).
- [ ] Integration: kill app mid-flight → relaunch with `resume_after_seq`
      fills gaps only.
- [ ] Unit: tick state machine transitions.

**Docs**
- [ ] `docs/client-ios/05_chat_and_sync.md` — ack ladder, seq cursor, retry
      rules, local schema.

**Exit criteria**: ticks match server semantics; no duplicate bubbles after
sync; seq high-water mark never regresses on device.

---

## Phase C6 — Group chat UI + protocol

Mirror server Phase 8.

### Protocol

- [ ] `GroupEvent` ops: `CREATE`, `ADD_MEMBER`, `REMOVE_MEMBER`, `LEAVE`
      with monotonic `version` (display membership changes in-thread).
- [ ] `GroupMessage` send/receive; wait `ServerAck` like 1:1.
- [ ] Authz errors: non-member send / non-admin add → surface server
      `ProtocolError` / error detail (parity
      `non_member_cannot_send_and_non_admin_cannot_add`).
- [ ] Respect `max_group_members` (default **1024**) in UI (disable add when
      full if known).
- [ ] Offline member: group messages arrive on sync (parity
      `offline_group_member_gets_message_on_login`).

### Local model

- [ ] `Group` (id, title/subject local cache, members, admins, version)
- [ ] Apply `GroupEvent` in version order; ignore stale versions
- [ ] Per-member delivery/read aggregation: **not** on server yet
      (`todo.md` Phase 8 open) — UI may show only sender’s ServerAck for
      group sends until server adds aggregation; document this limitation

### UI

- [ ] Create group sheet (name + member picker)
- [ ] Group info: members, admins, leave, add/remove (admin-only controls)
- [ ] Group thread (reuse bubble UI; show sender name on each bubble)
- [ ] System messages for membership events (“Alice added Bob”)

**Tests**
- [ ] Integration: create/add/fan-out (parity `group_create_add_and_fanout`).
- [ ] Integration: authz failures show errors, no crash.
- [ ] UI: membership version updates list without flicker.

**Docs**
- [ ] `docs/client-ios/06_groups.md` — ops, versioning, known ack-aggregation
      gap vs WhatsApp.

**Exit criteria**: group golden path works against single-node gateway;
membership UI stays consistent with `version`.

---

## Phase C7 — Media / bulk data (PDFs, images, files)

Mirror server Bonus + `02_bulk_data.md` / limits.

### Upload (strict contract)

```text
MediaStart → MediaAck(ok, complete=false)
MediaChunk(offset contiguous, ≤64 KiB) → MediaAck(received_bytes) …
MediaChunk(last=true) → MediaAck(ok, complete=true)  // sha256 verified
```

- [ ] Client-generated `media_id` (UUID).
- [ ] Compute SHA-256 hex of full blob **before** `MediaStart`.
- [ ] Enforce client-side cap aligned with server default **64 MiB**
      (`max_media_bytes`); reject earlier with friendly UI.
- [ ] Chunk size ≤ **64 KiB**; offsets strictly sequential.
- [ ] One upload at a time per connection (server rule) — queue UI uploads.
- [ ] On `complete=true`, send `ChatMessage` / `GroupMessage` with
      `media_id` + caption in `body`.
- [ ] Progress UI from `received_bytes / total_size`.

### Download

- [ ] `MediaFetch { media_id, from_offset }` for resume.
- [ ] Expect `MediaStart` metadata then `MediaChunk` stream until `last`.
- [ ] Re-verify SHA-256 after reassembly (parity Rust `fetch_media`).
- [ ] Persist blob in app container / FileProvider-friendly cache; reference
      from message row.
- [ ] Cross-node fetch is transparent (server `PeerMediaReady`); client
      unchanged.

### UI

- [ ] Attachment picker: PhotoLibrary, Files (PDF), Camera
- [ ] Image bubble + full-screen viewer
- [ ] PDF quick-look / document icon + open in place
- [ ] Upload/download progress + failure retry
- [ ] MIME + filename from `MediaStart`

**Tests**
- [ ] Integration: upload + fetch round-trip (parity
      `upload_and_fetch_pdf_blob_round_trip`).
- [ ] Oversized rejected (parity `oversized_media_rejected`).
- [ ] Corrupted bytes fail sha check (parity `corrupted_upload_fails_sha_check`).
- [ ] Resume download from `from_offset` after simulated kill.

**Docs**
- [ ] `docs/client-ios/07_media.md` — chunking, sha256, resume, storage paths.

**Exit criteria**: PDF and image send/receive work end-to-end; integrity
failures never present partial files as complete.

---

## Phase C8 — End-to-end encryption (Olm + Megolm)

Mirror server Phase 10 / `10_e2ee.md` / `11_security_review.md`.

### Crypto library choice

- [ ] Prefer **libolm** (C) via Swift package / XCFramework, or Matrix
      Rust crypto via FFI — must interoperate with **vodozemac** on server
      tests / other clients.
- [ ] Run / port vectors from `tests/libolm_compat.rs` conceptually
      (Megolm encrypt/decrypt cross-impl).
- [ ] **Forbidden:** custom Double Ratchet / AES homebrew.

### Device identity & key directory

- [ ] On first launch (or E2EE enable): generate Olm account / identity keys.
- [ ] Store private keys in Keychain (or Secure Enclave-wrapped where
      practical); never backup plaintext keys to iCloud unless explicitly
      designed.
- [ ] `PublishKeys { user_id, device_id, identity_key, one_time_keys }`:
      - [ ] Publish batch of one-time prekeys (e.g. 50–100); refill when low
      - [ ] Mark published locally after successful send
- [ ] `FetchKeys { user_id, for_user, device_id }`:
      - [ ] Empty `device_id` → primary (most recently published) device
      - [ ] Specific device for multi-device targeting
- [ ] Handle `KeyBundle { …, found, device_ids }`:
      - [ ] Incomplete / `found=false` → UI “recipient has no keys”
- [ ] `RemoveDeviceKeys` on sign-out / device revoke UI

### 1:1 Olm

- [ ] Establish outbound session from bundle (X3DH-style).
- [ ] Encode `ChatMessage.body` as protobuf `EncryptedPayload`:
      - [ ] `sender_key`, `message_type` (0 pre-key / 1 normal), `ciphertext`
- [ ] Decrypt inbound; create inbound session on first pre-key message.
- [ ] Out-of-order decrypt support (ratchet).
- [ ] Safety number UI: fingerprint from both identity keys (match server
      `E2eeDevice::safety_number` algorithm: sorted keys, SHA-256, hex groups)
      for out-of-band verify screen.

### Group Megolm (sender-keys)

- [ ] `create_group_sender_session(group_id)`
- [ ] Distribute `GroupSessionKeyShare` inside Olm-encrypted 1:1 chats to
      each member (`distribute_group_session_key` parity)
- [ ] On inbound 1:1: `try_import_group_key_share` before treating as normal
      chat
- [ ] `GroupMessage.body` = `EncryptedGroupPayload { session_id, ciphertext }`
- [ ] Decrypt with inbound Megolm; handle missing session key (request
      re-share UX)
- [ ] Document limitation: periodic Megolm rotation not automated server-side
      yet — client should offer “reset group encryption” later

### UI / product rules

- [ ] Lock icon on E2EE chats; safety number screen
- [ ] Feature flag: refuse to send plaintext when `E2EE_ENABLED` (server still
      allows plaintext — client policy)
- [ ] Multi-device: show peer `device_ids` from `KeyBundle`; warn on new
      device identity change (safety number change banner)
- [ ] Opacity: debug builds may assert ciphertext does not contain plaintext
      UTF-8 (parity e2e opacity tests)

**Tests**
- [ ] Unit: Olm round-trip; out-of-order; safety number stability.
- [ ] Unit: Megolm round-trip; key-share via Olm.
- [ ] Integration against gateway: 1:1 E2EE opacity (parity
      `e2ee_chat_single_node_server_never_sees_plaintext`).
- [ ] Integration: group Megolm (parity `e2ee_group_megolm_single_node`).
- [ ] Integration: multi-device fetch (parity `multi_device_key_directory`).
- [ ] Optional: cross-node E2EE (parity `e2ee_chat_cross_node_cluster`).

**Docs**
- [ ] `docs/client-ios/08_e2ee.md` — key lifecycle, Keychain, UI flows,
      interop notes with vodozemac.
- [ ] Complete client rows of `11_security_review.md` checklist.

**Exit criteria**: E2EE on by default in Release; plaintext send blocked;
safety numbers verifiable; group sender-keys work for ≥3 members.

---

## Phase C9 — SwiftUI information architecture & polish

Build the WhatsApp-like shell on top of C3–C8 APIs.

### Navigation

- [ ] Tab bar: **Chats** | **Settings** (Calls optional / out of scope)
- [ ] Chats → Conversation → Detail / Media / Safety number
- [ ] Deep link placeholder: `lanemessenger://chat/{userOrGroup}`

### Chats list

- [ ] Sorted by last activity
- [ ] Swipe actions: pin, mute, delete (local)
- [ ] Mute suppresses banners only (no server mute API yet — document)

### Composer

- [ ] Text, emoji, attach, send
- [ ] Disabled while `Syncing` / `Disconnected` with clear banner
      (“Connecting…”, “Catching up…”)
- [ ] Character / size limits derived from frame max (body + overhead ≪ 256 KiB)

### Settings

- [ ] Profile (user id, device id)
- [ ] Network: host, port, TLS toggle (debug)
- [ ] E2EE: publish keys, verify safety numbers, revoke device
- [ ] Storage: clear media cache
- [ ] Sign out (RemoveDeviceKeys + Keychain wipe + DB wipe options)

### Accessibility & i18n

- [ ] Dynamic Type, VoiceOver labels on ticks / presence
- [ ] Localizable strings (EN first)
- [ ] Dark mode (system)

### Performance

- [ ] Lazy message windowing (don’t load entire history into memory)
- [ ] Image thumbnails; decode off main actor
- [ ] Avoid main-thread protobuf / crypto

**Tests**
- [ ] UI tests: login → send → tick appears; kill/relaunch → history remains.
- [ ] Snapshot tests for chat bubbles / ticks (optional).

**Docs**
- [ ] `docs/client-ios/09_ui.md` — screens, navigation map, empty/error states.

**Exit criteria**: demoable WhatsApp-like UX against local gateway covering
login, presence, 1:1, groups, media, E2EE.

---

## Phase C10 — Reliability, reconnect, push-ready, App Store hardening

Mirror server Phase 11 quality bar on the client.

### Reconnect policy

- [ ] Exponential backoff with jitter on disconnect (respect
      `RATE_LIMITED` / auth backoff).
- [ ] Network path monitor (`NWPathMonitor`): reconnect on wifi/cell restore.
- [ ] Always resume with `resume_after_seq`; never full-reset seq unless
      user clears data.
- [ ] Simulate packet loss / airplane mode → gap-only sync (client analogue
      of server’s pending partition test).

### Notifications (push-ready)

- [ ] Abstract `NotificationService` — local notifications when app
      backgrounded and message arrives **while socket still up** (limited).
- [ ] Design for future APNs: server push gateway not in `todo.md` yet —
      document payload shape wish (`message_id`, `conversation_id`,
      encrypted content **not** in push if E2EE).
- [ ] Notification Service Extension placeholder for decrypt-at-display
      (future).

### Security hardening

- [ ] SSL pinning option for production gateway CA
- [ ] Screen privacy: hide preview in app switcher for E2EE chats (optional)
- [ ] Jailbreak detection **not** required; focus on Keychain + ATS
- [ ] No secrets in crash logs (Crashlytics scrubbers)

### CI / quality

- [ ] GitHub Actions / Xcode Cloud: build, unit tests, swiftlint
- [ ] Integration tests job with Docker/local messenger binary if feasible
- [ ] Versioning: `CFBundleShortVersionString` + changelog
- [ ] Privacy nutrition labels: plaintext vs E2EE data collection claims

**Tests**
- [ ] Chaos: flaky network toggle script; assert no dupes / no seq regression.
- [ ] Memory leak checks on connect/disconnect cycles.
- [ ] 100-conversation scroll performance smoke.

**Docs**
- [ ] `docs/client-ios/10_operations.md` — reconnect, push future, release
      checklist.
- [ ] Update root pointer from this file’s status section when shipping.

**Exit criteria**: TestFlight build against staging gateway; crash-free
golden path; security checklist signed off for client rows.

---

## Cross-cutting client standards (every phase)

- [ ] No force-unwrap on network / proto paths; typed `MessengerClientError`.
- [ ] All limits respected (`limits.md`): max frame 256 KiB, media 64 MiB,
      chunk 64 KiB, ping ~30 s, login first frame &lt; 10 s.
- [ ] Proto evolution: client must ignore unknown protobuf fields; never
      invent new packet IDs.
- [ ] Feature parity matrix maintained (below) — update when server gains
      contact-list filtering, group ack aggregation, JWT auth, WebSocket, etc.
- [ ] Reference parity: every public method on Rust `MessengerClient` has a
      Swift equivalent or an explicit “won’t port” note.

### Rust `MessengerClient` → Swift API checklist

| Rust API (approx.) | Swift target | Phase |
|--------------------|--------------|-------|
| `connect` / `connect_tls` | `MessengerSession.connect` | C2 |
| `login` (via connect) | `login(resumeAfterSeq:)` | C3 |
| `ping` | `ping()` | C2 |
| `send_chat` / `send_chat_with_media` | `sendChat` | C5 / C7 |
| `send_delivered` / `send_read` | `ackDelivered` / `ackRead` | C5 |
| `create_group` / `add_member` / … | `groupEvent` | C6 |
| `send_group` | `sendGroup` | C6 |
| `upload_media` / `fetch_media` | `uploadMedia` / `fetchMedia` | C7 |
| `publish_keys` / `fetch_key_bundle` | `publishKeys` / `fetchKeyBundle` | C8 |
| `fetch_key_bundle_for_device` | `fetchKeyBundle(deviceId:)` | C8 |
| `remove_device_keys` | `removeDeviceKeys` | C8 |
| `publish_e2ee_device` | `publishE2EEDevice` | C8 |
| `establish_e2ee_session` | `establishE2EESession` | C8 |
| `send_encrypted_chat` / `decrypt_chat` | `sendEncryptedChat` / `decryptChat` | C8 |
| `create_group_e2ee_session` | `createGroupE2EESession` | C8 |
| `distribute_group_session_key` | `distributeGroupSessionKey` | C8 |
| `send_encrypted_group` / `decrypt_group` | `sendEncryptedGroup` / `decryptGroup` | C8 |
| `try_import_group_key_from_chat` | `tryImportGroupKey` | C8 |
| inbound `AsyncStream`/callback | `events: AsyncStream<MessengerEvent>` | C2+ |

---

## Feature parity matrix (server `todo.md` ↔ iOS)

| Server capability | Server status | iOS phase | Notes |
|-------------------|---------------|-----------|-------|
| Binary framing v1 | done | C1 | Must match exactly |
| TLS client sockets | done | C2 | Production default |
| HMAC login | done | C3 | No secret in Release app |
| Multi-device kick | done | C2/C3 | UX for REPLACED_BY_NEW_SESSION |
| Presence broadcast | partial | C4 | Contact filter may still be open server-side |
| 1:1 + ack ladder | done | C5 | Full ticks |
| Offline inbox + resume | done | C5 | resume_after_seq |
| Ping/Pong + idle | done | C2 | 30 s / &lt;90 s |
| Groups | done (single-node+) | C6 | Ack aggregation open server-side |
| Media chunked | done | C7 | sha256 + 64 MiB |
| Cluster routing | done | — | Transparent |
| E2EE Olm/Megolm | done | C8 | libolm / interop |
| WebSocket transport | open server | — | Skip until server `ws` feature |
| JWT auth | open server | C3 adapter | Pluggable AuthService |
| APNs push gateway | not in todo | C10 design only | Document only |
| Contact subscription filter | open server | C4 limited | Manual contacts until then |

---

## Suggested implementation order (do not reorder lightly)

1. **C0 → C1 → C2 → C3** — must speak Login/Ping before any UI value  
2. **C5** (plaintext 1:1 + sync) — core product loop  
3. **C4** presence polish (can overlap late C5)  
4. **C6** groups  
5. **C7** media  
6. **C8** E2EE (hardest; keep plaintext path behind flag until green)  
7. **C9** UI polish throughout, finalize shell here  
8. **C10** TestFlight / hardening  

---

## Directory layout (proposed)

```text
ios/
  LaneMessenger.xcworkspace
  Packages/
    LaneMessengerWire/          # frames + protobuf
    LaneMessengerClient/        # session, APIs
    LaneMessengerE2EE/          # olm/megolm
  App/
    LaneMessengerApp/           # SwiftUI app
  docs/client-ios/              # client docs (linked from this file)
  Scripts/generate_proto.sh
  TestData/frames/              # cross-lang fixtures from Rust
```

Alternatively keep docs under repo `docs/client-ios/` next to
`docs/messenger/`.

---

## Definition of done (client v1.0)

- [ ] All phases C0–C10 exit criteria checked.
- [ ] Golden path demo: login → presence → 1:1 ticks → offline resume →
      group → PDF → E2EE 1:1 → E2EE group → safety number.
- [ ] Interops with Rust `examples/messenger_demo.rs` gateway and
      `tests/messenger.rs` scenarios (manual or automated).
- [ ] No peer-packet usage; no forged packet IDs.
- [ ] Security review client section signed (`11_security_review.md`).
- [ ] This file’s legend items for shipped work flipped to `[x]`.

---

## Status

**Created:** 2026-07-10  
**Server baseline:** `todo.md` Phase 10 E2EE complete; Phase 11 release
hardening still open on server.  
**Client baseline:** plan only — no iOS code yet.
