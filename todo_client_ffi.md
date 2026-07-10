# Cross-Platform Client FFI — FunXMPP Messenger

Goal: expose the Rust messenger client (`src/messenger/client.rs`,
`codec.rs`, `e2ee.rs`) as a **stable shared library** that Swift (iOS),
Java/Kotlin (Android), and Flutter can call without re-implementing the
binary wire protocol.

This document is the **API contract + implementation roadmap** for mobile
teams. UI work stays in [`todo_client.md`](todo_client.md) (iOS) or platform
equivalents; **all networking, framing, acks, media chunking, and E2EE
crypto live in Rust behind FFI**.

Legend: `[ ]` pending · `[~]` in progress / partial · `[x]` done

**Status (2026-07-10):** `lane_messenger_ffi` 0.9.2 — F0–F6 + F8 WS dialer +
F9 release profile/NOTICE; F7 UniFFI codegen + JNI + XCFramework/NDK CI scripts
+ sample apps under `bindings/` / `examples/*_ffi_demo/`.

**Server dependency:** `todo.md` Phases 0–11 (messenger plane green).  
**Wire truth:** [`proto/messenger.proto`](proto/messenger.proto),
[`docs/messenger/01_wire_protocol.md`](docs/messenger/01_wire_protocol.md).
**Crate:** [`lane_messenger_ffi/`](lane_messenger_ffi/) · header
[`lane_messenger_ffi/include/lane_messenger_ffi.h`](lane_messenger_ffi/include/lane_messenger_ffi.h).

---

## Why FFI (not re-implement per platform)

| Approach | Pros | Cons |
|----------|------|------|
| **Rust FFI (this plan)** | One codec/E2EE; parity with server tests; faster security fixes | Build complexity (xcframework / NDK / Flutter plugin) |
| Native Swift/Kotlin/Dart codec | Familiar tooling | Triple maintenance; E2EE drift; easy to break frame endianness |
| Pure WebSocket + JS | Easy for web | Mobile still needs native; E2EE weak |

**Decision:** ship `liblane_messenger_ffi` (cdylib + staticlib) generated with
**UniFFI** (preferred) or `flutter_rust_bridge` for Flutter-first teams.
C ABI must remain callable from JNI and Swift even if UniFFI is the primary
generator.

---

## Architecture

```text
┌─────────────────────────────────────────────────────────────┐
│  SwiftUI / Jetpack Compose / Flutter Widgets                │
│  (UI only — no frame encode/decode)                         │
└──────────────────────────┬──────────────────────────────────┘
                           │ language bindings
┌──────────────────────────▼──────────────────────────────────┐
│  Platform glue                                              │
│  • iOS: UniFFI Swift module / XCFramework                   │
│  • Android: UniFFI Kotlin + JNI (.so in AAR)                │
│  • Flutter: flutter_rust_bridge or UniFFI + MethodChannel   │
└──────────────────────────┬──────────────────────────────────┘
                           │ C / UniFFI scaffolding
┌──────────────────────────▼──────────────────────────────────┐
│  lane_messenger_ffi (Rust)                                  │
│  • Session handle (opaque)                                  │
│  • Event callback / poll queue                              │
│  • Thin wrappers over MessengerClient + E2eeDevice          │
└──────────────────────────┬──────────────────────────────────┘
                           │ TCP / TLS (and optional WS)
┌──────────────────────────▼──────────────────────────────────┐
│  Messenger gateway (lane_switchboards)                      │
└─────────────────────────────────────────────────────────────┘
```

### Design rules (non-negotiable)

1. **Opaque handles** — hosts never see Rust structs; only `u64` / pointer
   session ids and byte buffers.
2. **UTF-8 strings** — all text is UTF-8; lengths are explicit (`len` + ptr)
   or UniFFI `String`.
3. **No blocking UI thread** — connect/send/recv run on a Rust-owned Tokio
   runtime; events delivered via callback or poll.
4. **Secrets never logged** — `auth_token`, private keys, plaintext when E2EE
   on must not appear in FFI debug logs.
5. **Peer packets out of scope** — `0x50`–`0x57` are server-only; FFI must
   not expose them.
6. **Proto append-only** — regenerate bindings when `messenger.proto` changes;
   never invent packet IDs on the host side.

---

## Recommended tooling matrix

| Platform | Binding generator | Artifact | Min OS |
|----------|-------------------|----------|--------|
| iOS / macOS | UniFFI → Swift | `LaneMessengerFFI.xcframework` | iOS 15+ |
| Android | UniFFI → Kotlin | `lane-messenger-ffi.aar` (arm64-v8a, armeabi-v7a, x86_64) | API 24+ |
| Flutter | UniFFI + thin Dart plugin **or** `flutter_rust_bridge` | plugin package | Flutter 3.16+ |
| C / other | `cbindgen` header `lane_messenger_ffi.h` | `liblane_messenger_ffi.so` / `.a` / `.dylib` | — |

**Primary recommendation:** UniFFI 0.28+ with a single `udl` / proc-macro
interface; Flutter wraps the same `.so` via a Dart FFI or MethodChannel
plugin so Android/iOS Flutter apps do not fork the protocol.

---

## Phase F0 — FFI crate & build pipeline

- [x] Create workspace crate `crates/lane_messenger_ffi` (or
      `lane_messenger_ffi/` at repo root).
- [x] Depend on `lane_switchboards` with features:
      `messenger`, `tls` (required for production), optional `ws`.
- [x] Embed Tokio multi-thread runtime inside the FFI crate (one process-wide
      runtime; document thread count env `LANE_MESSENGER_WORKER_THREADS`).
- [x] Cargo features:
      - [x] `c-api` — C header for Flutter / custom hosts
      - [x] `uniffi` — generate Swift/Kotlin (`--features uniffi`; not default)
      - [x] `tls` — passthrough to switchboards
- [~] CI jobs:
      - [x] `cargo test -p lane_messenger_ffi`
      - [x] Build iOS XCFramework (x86_64-sim + aarch64-sim + aarch64-device)
      - [x] Build Android NDK `.so` for 3 ABIs
      - [ ] Publish artifacts to GitHub Releases / internal Maven / SPM
- [x] Versioning: FFI crate version **tracks** `lane_switchboards` minor
      (e.g. `0.9.2`); breaking FFI changes bump minor and CHANGELOG.

**Tests**
- [x] `cargo test -p lane_messenger_ffi` against local `MessengerServer::bind`.
- [x] Smoke: connect → login → ping → close from Rust unit tests using the
      same public FFI functions (not only internal APIs).

**Docs**
- [x] `docs/client-ffi/00_overview.md` — architecture diagram above.
- [x] `docs/client-ffi/BUILD.md` — how to produce XCFramework / AAR.

**Exit criteria**: release artifacts build in CI; version stamped into
`client_version` Login field automatically.

---

## Phase F1 — Core types, errors, and memory rules

### Error model

Map `MessengerError` (+ E2EE) to a stable enum (UniFFI / C):

| Code | Name | Meaning | Host action |
|------|------|---------|-------------|
| 0 | `Ok` | success | — |
| 1 | `Io` | socket / TLS failure | reconnect with backoff |
| 2 | `Protocol` | server `ProtocolError` or unexpected packet | show `detail` |
| 3 | `FrameTooLarge` | exceeded max frame | shrink payload |
| 4 | `UnknownPacketType` | unsupported type | upgrade app |
| 5 | `UnsupportedVersion` | wire ver ≠ 1 | force upgrade |
| 6 | `Decode` | protobuf decode failed | disconnect |
| 7 | `AuthFailed` | login rejected | re-auth |
| 8 | `Closed` | connection closed | reconnect |
| 9 | `Timeout` | wait for ack/pong timed out | retry send |
| 10 | `Media` | media transfer failed | retry upload/fetch |
| 11 | `E2ee` | crypto failure | show key error / re-establish |
| 12 | `InvalidArgument` | null ptr / bad UTF-8 / empty id | fix caller |
| 13 | `NotConnected` | API called before connect | connect first |
| 14 | `Internal` | panic caught / poisoned | report bug |

C API returns `int32_t` code; detail string via out-param buffer or UniFFI
`Exception`. **Implemented** as `FfiErrorCode` / `FfiError` in
`lane_messenger_ffi`.

### Memory / ownership (C API)

| Type | Rule |
|------|------|
| `LaneSession *` | created by `lane_session_connect`; freed by `lane_session_free` |
| `LaneE2eeDevice *` | created by `lane_e2ee_generate`; freed by `lane_e2ee_free` |
| Strings returned to host | host calls `lane_string_free` |
| Byte buffers returned | host calls `lane_bytes_free` |
| Callbacks | must be thread-safe; may run on Tokio worker |

### Constants (must match server `limits.md`)

| Constant | Value | FFI name |
|----------|-------|----------|
| Protocol version | 1 | `LANE_PROTOCOL_VERSION` / `lane_protocol_version()` |
| Default max frame | 256 KiB | `DEFAULT_MAX_FRAME` |
| Media chunk size | 64 KiB | `MEDIA_CHUNK_SIZE` |
| Default max media | 64 MiB | `DEFAULT_MAX_MEDIA` |
| Suggested ping interval | 30 s | `PING_INTERVAL_SECS` |
| Server idle timeout | 90 s | (document only; reconnect before) |

**Tests**
- [x] Error code table covered by unit tests (each Rust error maps once).
- [ ] Double-free / use-after-free sanitizer job (nightly).

**Docs**
- [~] `docs/client-ffi/01_errors_and_memory.md` (covered in overview + header)

**Exit criteria**: hosts can link and call `lane_version()` returning semver
string. **Done.**

---

## Phase F2 — Session lifecycle & event model

Hosts must not block on `recv`. Use **push events** (preferred) plus optional
**poll**.

### Session config (connect)

```text
LaneConnectOptions {
  host: string           // "messenger.example.com" or "127.0.0.1"
  port: u16              // 9000
  use_tls: bool          // production true
  // Optional PEM paths / bytes for custom CA (debug / enterprise)
  ca_pem: optional bytes
  user_id: string
  device_id: string      // stable UUID in Keychain / EncryptedSharedPreferences
  auth_token: string     // from identity service; never hardcode HMAC secret in app
  client_version: string // e.g. "ios-1.2.0" / "android-1.2.0" / "flutter-1.2.0"
  resume_after_seq: u64  // from local DB; 0 = full sync
}
```

### API

| FFI function | Mirrors Rust | Notes |
|--------------|--------------|-------|
| `lane_session_connect(opts) → Session` | `MessengerClient::connect_tls` | Blocks until LoginAck + SyncComplete **or** returns and streams sync events |
| `lane_session_close(session)` | `close` | Graceful |
| `lane_session_free(session)` | drop | |
| `lane_session_ping(session)` | `ping` | |
| `lane_session_set_event_handler(session, cb, ctx)` | — | Push model |
| `lane_session_poll_event(session, timeout_ms) → Event?` | — | Pull model |

**Recommended connect behaviour:** return as soon as `LoginAck` is received;
emit `SyncMessage` events for each replayed packet, then `SyncComplete`.
This keeps UI responsive for large inboxes (`LoginAck.pending_messages`).

### Event enum (host must handle all)

| Event | Payload | When |
|-------|---------|------|
| `LoginAck` | `session_id`, `pending_messages`, `ok`, `error` | After login |
| `SyncMessage` | same as inbound chat/group/… | Offline replay |
| `SyncComplete` | `delivered`, `latest_seq` | End of replay |
| `ChatMessage` | full chat fields | Live 1:1 |
| `ServerAck` | `message_id`, `seq` | Send persisted |
| `DeliveredAck` | `message_id`, `from_user` | Double tick |
| `ReadAck` | `message_id`, `from_user` | Blue tick |
| `GroupAckSummary` | `message_id`, `group_id`, `delivered_by[]`, `read_by[]`, `member_count` | Group receipts |
| `Presence` | `user_id`, `kind`, `last_seen` | Presence |
| `GroupMessage` | group fields | Live group |
| `GroupEvent` | op, actor, subject, version | Membership |
| `MediaStart` / `MediaChunk` / `MediaAck` | media fields | Transfer progress |
| `KeyBundle` | key fields | E2EE fetch reply |
| `ProtocolError` | `code`, `detail` | Fatal or media |
| `Disconnected` | optional reason | Socket EOF / kick |
| `ReplacedByNewSession` | — | Same device login elsewhere |

Presence kinds: `Available = 1`, `Unavailable = 2`, `LastSeen = 3`.  
Group ops: `Create = 1`, `AddMember = 2`, `RemoveMember = 3`, `Leave = 4`.

### Auto behaviours inside FFI (hosts should not reimplement)

- [x] Ping every ~30 s while connected (configurable; default on).
- [x] Optional auto-reconnect with exponential backoff + jitter; always
      resume with last `latest_seq` provided by host via
      `lane_session_set_resume_seq` / `set_resume_seq` (also auto-tracked).
- [x] On `REPLACED_BY_NEW_SESSION`, emit event and **do not** auto-reconnect
      with the same device until host confirms.

**Tests**
- [x] Integration: connect → SyncComplete → ping → close.
- [x] Same-device kick surfaces `ReplacedByNewSession`.
- [x] Event handler invoked from background thread; host can marshal to UI.

**Docs**
- [x] `docs/client-ffi/02_session_and_events.md`

**Exit criteria**: Swift + Kotlin sample apps print LoginAck and SyncComplete
against local gateway. (Rust/C smoke done; platform samples = F7.)

---

## Phase F3 — 1:1 chat, acks, presence roster

### Send / ack API

| FFI | Rust | Returns |
|-----|------|---------|
| `lane_send_chat(session, to_user, message_id, body)` | `send_chat` | `seq` (u64); waits ServerAck |
| `lane_send_chat_with_media(..., media_id)` | `send_chat_with_media` | `seq` |
| `lane_send_chat_retry(..., max_attempts)` | `send_chat_with_retry` | `seq` (dedup-safe) |
| `lane_ack_delivered(session, message_id)` | `ack_delivered` | |
| `lane_ack_read(session, message_id)` | `ack_read` | |
| `lane_subscribe_presence(session, contact_ids[])` | `subscribe_presence` | roster filter |
| `lane_send_presence(session, kind)` | `framed_send_presence` | |

### Host responsibilities

- Generate `message_id` as UUID string **before** send; reuse on retry.
- Persist `latest_seq` from `SyncComplete` / inbound messages for resume.
- On inbound `ChatMessage`: persist → `ack_delivered` → when UI visible
  `ack_read`.
- Map ticks: ServerAck → single; DeliveredAck → double; ReadAck → blue.
- After `subscribe_presence`, only listed contacts exchange presence
  (see `docs/messenger/04_presence.md`).

### ChatMessage fields (event + send)

| Field | Type | Notes |
|-------|------|-------|
| `message_id` | string | Client UUID; dedup key |
| `from_user` | string | Server overwrites on send |
| `to_user` | string | |
| `body` | bytes | UTF-8 **or** E2EE `EncryptedPayload` bytes |
| `sent_at` | u64 | Unix millis (informational; server orders by `seq`) |
| `seq` | u64 | Server-assigned; 0 on outbound |
| `media_id` | string | Optional |

**Tests**
- [x] FFI e2e: online tick ladder; offline resume; duplicate message_id →
      seq sentinel 0.
- [x] Presence roster: A subscribed to B only; C does not see A.

**Docs**
- [x] `docs/client-ffi/03_chat_presence.md`

**Exit criteria**: parity with `tests/messenger.rs` chat + presence cases via
FFI harness. **Done** (Rust harness).

---

## Phase F4 — Groups + GroupAckSummary

| FFI | Rust |
|-----|------|
| `lane_create_group(session, group_id) → version` | `create_group` |
| `lane_add_member(session, group_id, user) → version` | `add_member` |
| `lane_remove_member(...)` / `lane_leave_group(...)` | `group_event` ops |
| `lane_send_group(session, group_id, message_id, body)` | `send_group` |

### GroupAckSummary (event)

Hosts display WhatsApp-style “read by N of M”:

| Field | Meaning |
|-------|---------|
| `message_id` | Group message id |
| `group_id` | |
| `delivered_by` | list of user ids |
| `read_by` | list of user ids |
| `member_count` | recipients excluding sender |

Flow: `ServerAck` (accepted) → later `GroupAckSummary` updates as members
ack. Do **not** expect per-member DeliveredAck relay for tracked group
sends (server swallows those into summaries).

**Tests**
- [x] FFI: create/add/send; members ack; sender receives summaries until
      `read_by.len == member_count`.

**Docs**
- [x] `docs/client-ffi/04_groups.md` + link `docs/messenger/08_groups.md`

**Exit criteria**: group golden path works from Kotlin and Swift samples.
(Rust FFI harness done; platform samples = F7.)

---

## Phase F5 — Media upload / download

Contract: [`docs/messenger/02_bulk_data.md`](docs/messenger/02_bulk_data.md).

| FFI | Rust | Notes |
|-----|------|-------|
| `lane_upload_media(session, media_id, file_name, mime, data) → bytes_stored` | `upload_media` | SHA-256 computed in Rust; chunks ≤64 KiB |
| `lane_fetch_media(session, media_id) → DownloadedMedia` | `fetch_media` | Verifies sha256 |
| Progress | via `MediaAck` / `MediaChunk` events | Optional `lane_upload_media_with_progress` |

### DownloadedMedia

`file_name`, `mime_type`, `sha256` (hex), `data` (bytes).

### Host rules

- Cap UI picker at **64 MiB** (or query `LANE_DEFAULT_MAX_MEDIA`).
- One upload at a time per session (server rule) — queue in UI.
- After upload complete, send chat/group with `media_id` + caption body.
- Store blobs in app sandbox; do not trust server filename for path traversal
  (sanitize).

**Tests**
- [x] PDF round-trip; oversized rejected; corrupt sha fails.
      (PDF round-trip via FFI; oversized/corrupt covered on server tests.)

**Docs**
- [x] `docs/client-ffi/05_media.md`

**Exit criteria**: image + PDF send/receive from all three platforms.
(Rust FFI done; platforms = F7.)

---

## Phase F6 — E2EE (Olm + Megolm) behind FFI

Crypto stays in Rust (`vodozemac` via `E2eeDevice`). Hosts must **not** ship a
second Olm stack for production paths (debug plaintext flag only).

### Device handle API

| FFI | Rust |
|-----|------|
| `lane_e2ee_generate() → E2eeDevice` | `E2eeDevice::generate` |
| `lane_e2ee_identity_key(device) → string` | `identity_key_base64` |
| `lane_e2ee_safety_number(local_b64, remote_b64) → string` | `safety_number` |
| `lane_e2ee_free(device)` | drop |

### Key directory

| FFI | Rust |
|-----|------|
| `lane_publish_e2ee_device(session, device, device_id, otk_count)` | `publish_e2ee_device` |
| `lane_fetch_key_bundle(session, user_id) → KeyBundle` | `fetch_key_bundle` |
| `lane_fetch_key_bundle_for_device(session, user, device_id)` | `fetch_key_bundle_for_device` |
| `lane_remove_device_keys(session, device_id)` | `remove_device_keys` |
| `lane_establish_e2ee_session(session, device, peer)` | `establish_e2ee_session` |

### Encrypt / decrypt

| FFI | Rust |
|-----|------|
| `lane_send_encrypted_chat(session, device, to, message_id, plaintext)` | `send_encrypted_chat` |
| `lane_decrypt_chat(device, from_user, body) → plaintext` | `decrypt_chat` |
| `lane_create_group_e2ee_session(device, group_id) → session_id` | `create_group_e2ee_session` |
| `lane_distribute_group_session_key(session, device, group_id, members[])` | `distribute_group_session_key` |
| `lane_send_encrypted_group(...)` | `send_encrypted_group` |
| `lane_decrypt_group(device, group_id, body) → plaintext` | `decrypt_group` |
| `lane_try_import_group_key(device, from_user, body) → bool` | `try_import_group_key_from_chat` |

### Persistence (host + FFI)

- Private Olm account / sessions: **host Keystore** (iOS Keychain,
  Android Keystore) **or** FFI pickle API:
  - [x] `lane_e2ee_export_pickle(device, passphrase) → bytes`
  - [x] `lane_e2ee_import_pickle(bytes, passphrase) → device`
- Never sync pickles to iCloud/Google Drive without explicit product decision.
- On inbound chat: try `lane_try_import_group_key` before treating as normal
  plaintext/ciphertext chat.

### KeyBundle fields for UI

`user_id`, `device_id`, `identity_key`, `one_time_key`, `found`,
`device_ids[]` (all published devices).

**Tests**
- [x] Opacity: ciphertext body must not contain plaintext UTF-8 (mirror
      server e2e tests).
- [x] Megolm group 3-member round-trip via FFI.
- [x] Safety number stable under key swap order.

**Docs**
- [x] `docs/client-ffi/06_e2ee.md` + `docs/messenger/10_e2ee.md` /
      `11_security_review.md` client rows.

**Exit criteria**: E2EE on by default in sample apps; plaintext send only
behind debug flag. (Rust FFI 1:1 + pickle done; Megolm FFI e2e + samples open.)

---

## Phase F7 — Platform binding packages

### F7a — Swift (iOS)

- [x] SPM package `LaneMessengerFFI` wrapping XCFramework / C ABI.
- [~] Swift wrappers with `async` APIs (`await session.sendChat(...)`) using
      continuations over UniFFI async or background queue.
- [ ] Map events to `AsyncStream<LaneEvent>`.
- [x] Sample: `examples/ios_ffi_demo` — login, chat, ticks.
- [x] Document ATS / TLS custom CA for staging.
- [x] UniFFI Swift codegen checked in under `Sources/.../generated/`.

See also UI plan: [`todo_client.md`](todo_client.md) (consume this FFI; do
not reimplement codec). Scaffold: `bindings/swift/LaneMessengerFFI/`.

### F7b — Kotlin / Java (Android)

- [~] Publish `com.lane.messenger:ffi` AAR (Maven local / GitHub Packages).
- [~] Kotlin coroutines wrappers (`suspend fun sendChat`).
- [ ] `CallbackFlow` / `SharedFlow` for events.
- [x] ProGuard / R8 keep rules for JNI.
- [x] Sample: `examples/android_ffi_demo` (Compose).
- [ ] Store `device_id` + token in EncryptedSharedPreferences; E2EE pickle
      in Keystore-backed file.
- [x] JNI glue for `native*` methods (`--features jni`).
- [x] UniFFI Kotlin/JNA codegen under `uniffi/lane_messenger/`.

Scaffold: `bindings/android/lane-messenger-ffi/`.

### F7c — Flutter

- [~] Plugin `lane_messenger` (Federated: `ios`, `android`, optional
      `macos`).
- [x] Dart API mirrors FFI (connect / ping / poll / sendChat via Dart FFI).
- [ ] Isolate-friendly: events on main isolate via `SendPort` or
      `EventChannel`.
- [x] Sample app under `examples/flutter_ffi_demo`.
- [x] Document: Flutter must use the **same** `.so` / XCFramework as native
      apps (no Dart reimplementation of frames).

Scaffold: `bindings/flutter/lane_messenger/`.

**Docs**
- [x] `docs/client-ffi/07_platforms.md`

---

## Phase F8 — WebSocket transport (optional)

Server: `feature = "ws"`, `bind_ws`. Each WS **binary** message = one full
frame.

- [x] `LaneConnectOptions.transport = Tcp | WebSocket`
- [x] `ws_url` e.g. `wss://host/messenger` (path may be ignored by server
      upgrade — confirm deployment).
- [x] Same session/event API as TCP; only dialer changes.
- [x] Prefer WSS in production.

**Tests**
- [x] FFI connect over WS to `bind_ws` gateway; ping round-trip.

**Docs**
- [x] `docs/client-ffi/08_websocket.md`

**Exit criteria**: browser-adjacent hosts (Flutter web later) can share
frame encode helpers; mobile still prefers TCP/TLS. **Done** (Rust/FFI).

---

## Phase F9 — Hardening, versioning, release

- [x] Panic=abort in release FFI builds; catch unwind at FFI boundary →
      `Internal`.
- [x] Thread sanitizer / loom not required; document “one session ↔ one
      logical connection”.
- [x] Semantic versioning + `CHANGELOG` section for FFI.
- [x] Security: SSL pinning option (custom CA / pin set) in connect options
      (`ca_pem_path`).
- [x] Size budget: document stripped `.so` / XCFramework size; enable LTO.
- [x] License: ship NOTICE for vodozemac / rustls / UniFFI.
- [x] Compatibility matrix table in docs (FFI 0.9.x ↔ server 0.9.x).

**Tests**
- [ ] Soak: 1k connect/disconnect cycles without leak (Instruments /
      Android Profiler).
- [ ] Chaos: kill gateway mid-send → retry same message_id succeeds.

**Docs**
- [x] `docs/client-ffi/09_release.md`
- [~] Update root README with “Mobile FFI” link.

**Exit criteria**: tagged `lane_messenger_ffi v0.9.2` artifacts downloadable;
mobile teams unblocked without reading Rust sources daily.

---

## Complete API checklist (share with implementers)

Hosts should treat this as the acceptance list. Every row needs a binding.

### Session
- [x] connect / connect_tls / connect_ws *(WS = F8)*
- [x] close / free
- [x] ping
- [x] set_event_handler / poll_event
- [x] auto-ping enable/disable
- [x] auto-reconnect + set_resume_seq

### Chat & presence
- [x] send_chat / send_chat_with_media / send_chat_retry
- [x] ack_delivered / ack_read
- [x] subscribe_presence / send_presence

### Groups
- [x] create_group / add_member / remove_member / leave
- [x] send_group
- [x] handle GroupEvent + GroupAckSummary events

### Media
- [x] upload_media / fetch_media
- [x] progress events *(MediaAck / MediaChunk on event bus)*

### E2EE
- [x] generate device / pickle import-export *(account pickle)*
- [x] publish_e2ee_device / fetch_key_bundle(_for_device) / remove_device_keys
- [x] establish_e2ee_session
- [x] send_encrypted_chat / decrypt_chat
- [x] create_group_e2ee_session / distribute_group_session_key
- [x] send_encrypted_group / decrypt_group / try_import_group_key
- [x] safety_number

### Meta
- [x] lane_version / protocol_version / error_string

---

## Packet ID quick reference (client-relevant)

| ID | Packet | Direction |
|----|--------|-----------|
| 0x01 | Login | C→S |
| 0x02 | LoginAck | S→C |
| 0x03 / 0x04 | Ping / Pong | C↔S |
| 0x0F | ProtocolError | S→C |
| 0x10 | Presence | both |
| 0x11 | SubscribePresence | C→S |
| 0x20 | ChatMessage | both |
| 0x21 | ServerAck | S→C |
| 0x22 | DeliveredAck | both |
| 0x23 | ReadAck | both |
| 0x24 | SyncComplete | S→C |
| 0x30–0x33 | Media* | both |
| 0x40 | GroupMessage | both |
| 0x41 | GroupEvent | both |
| 0x42 | GroupAckSummary | S→C |
| 0x60–0x63 | E2EE keys | C↔S |

Do **not** implement 0x50–0x57 in clients.

---

## Suggested project layout

```text
lane_switchboards/
  crates/lane_messenger_ffi/     # Rust FFI crate
    src/lib.rs
    src/session.rs
    src/e2ee.rs
    src/events.rs
    src/lane_messenger.udl
  bindings/
    swift/LaneMessengerFFI/      # SPM
    android/lane-messenger-ffi/  # Gradle AAR
    flutter/lane_messenger/      # plugin
  examples/
    ios_ffi_demo/
    android_ffi_demo/
    flutter_ffi_demo/
  docs/client-ffi/
  todo_client_ffi.md             # this file
  todo_client.md                 # iOS UI (uses FFI)
```

---

## Implementation order

1. **F0 → F1 → F2** — connect, events, ping (unblocks all hosts)  
2. **F3** — chat + presence (MVP product)  
3. **F4** — groups + summaries  
4. **F5** — media  
5. **F6** — E2EE  
6. **F7** — ship Swift / Android / Flutter packages in parallel  
7. **F8** — WS if browsers matter  
8. **F9** — release hardening  

---

## Sharing this doc with mobile teams

Give them:

1. This file (`todo_client_ffi.md`)
2. [`docs/messenger/01_wire_protocol.md`](docs/messenger/01_wire_protocol.md)
3. [`docs/messenger/04_auth.md`](docs/messenger/04_auth.md) (token format —
   **identity service mints tokens**; apps never embed HMAC secret)
4. [`docs/messenger/10_e2ee.md`](docs/messenger/10_e2ee.md)
5. [`proto/messenger.proto`](proto/messenger.proto)
6. When available: generated UniFFI docs + sample apps

Tell UI teams: **do not write a second frame codec**. If FFI is missing an
API, open a PR against `lane_messenger_ffi`, don’t fork the protocol in Dart
or Kotlin.

---

## Status

**Created:** 2026-07-10  
**Server baseline:** messenger plane through Phase 11 (`todo.md`)  
**FFI baseline:** `lane_messenger_ffi` 0.9.2 — F0–F6, F7 (UniFFI/JNI/CI/samples), F8, F9 core  
**Related:** [`todo_client.md`](todo_client.md) (iOS UI on top of this FFI)
