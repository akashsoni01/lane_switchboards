# Production iOS Messenger App — FunXMPP Client

Goal: ship a **production-grade WhatsApp-like iOS app** (Swift / SwiftUI) that
speaks this repo’s **FunXMPP** messenger plane — without re-implementing the
wire codec, E2EE, or framing in Swift.

Legend: `[ ]` pending · `[~]` in progress / partial · `[x]` done

**Status:** I0–I1 in progress (2026-07-10). Kit + auth/Keychain/login shell
under `apps/ios/`; server + FFI remain the foundation.

| Layer | Source of truth | iOS responsibility |
|-------|-----------------|--------------------|
| Wire (FunXMPP frames) | `src/messenger/`, `proto/messenger.proto` | **Do not reimplement** |
| Client session / E2EE | `lane_messenger_ffi` | Link XCFramework / UniFFI |
| UI, local DB, Keychain, push, UX | **this plan** | Own it |

**Related plans**

| Doc | Role |
|-----|------|
| [`todo_client_ffi.md`](todo_client_ffi.md) | FFI API contract (must stay green) |
| [`todo_client.md`](todo_client.md) | Earlier Swift-native wire plan — **superseded for production** by FFI |
| [`todo.md`](todo.md) | Server / gateway roadmap |
| [`docs/client-ffi/`](docs/client-ffi/) | How to build & call the FFI |
| [`docs/messenger/`](docs/messenger/) | Wire semantics the UI must respect |

---

## Architecture decision (non-negotiable)

WhatsApp’s public model matches what we build:

```text
                 LaneMessenger (iOS)
                        │
        ┌───────────────┴───────────────┐
        │                               │
   GRDB / SQLite                  Keychain
 (messages, drafts,              (token, device_id,
  unread, media refs)             E2EE pickle)
        │                               │
        └───────────────┬───────────────┘
                        │
              lane_messenger_ffi (Rust)
                        │
              Persistent TLS TCP socket
                        │
              Binary FunXMPP protocol
                        │
              Messenger gateway (lane_switchboards)
```

### What we use

| Path | Protocol | Purpose |
|------|----------|---------|
| **Realtime** | Persistent TLS + FunXMPP via FFI | Chat, presence, acks, groups, media chunks, heartbeats |
| **HTTP(S)** | REST / future identity service | Login token mint, config, analytics, APNs registration, optional CDN |
| **Local** | SQLite (GRDB) + Keychain | Offline UI, drafts, resume seq, secrets |

### What we do **not** use for core chat

- **gRPC** on the messaging path (no `ChatService.SendMessage` over HTTP/2)
- **WebSocket** as the primary iOS transport (optional FFI `ws` is for
  constrained hosts; production iOS = TCP/TLS FunXMPP)
- A second Swift frame codec (`NWConnection` + hand-rolled protobuf framing)
- Peer packets `0x50`–`0x57` (gateway-to-gateway only)

### Typical send path

```text
User taps Send
    → insert local row (pending)
    → encrypt in Rust (E2EE) when enabled
    → FFI send_chat / send_encrypted_chat
    → FunXMPP ChatMessage on open socket
    → ServerAck → DeliveredAck → ReadAck
    → update ticks in SQLite + UI
```

---

## Product scope

### In scope (MVP → production)

- [ ] Login / logout / session restore
- [ ] Multi-device kick UX (`REPLACED_BY_NEW_SESSION`)
- [ ] Chats list + 1:1 thread with WhatsApp-style ticks
- [ ] Presence (online / offline / last seen when server sends it)
- [ ] Offline catch-up (`resume_after_seq` → `SyncComplete`)
- [ ] Groups: create / add / remove / leave + group thread
- [ ] Media: images, PDFs, files (chunked via FFI; progress UI)
- [ ] E2EE: Olm 1:1 + Megolm groups + safety numbers + account pickle
- [ ] Local search, drafts, unread badges
- [ ] Push-ready (APNs); foreground reconnect
- [ ] TLS in production; ATS-compliant; Keychain for secrets

### Out of scope (do not build on iOS)

- [ ] Hash-ring / home-shard / peer sync logic
- [ ] Server WAL, StorageNode, Prometheus
- [ ] Inventing new packet IDs or proto fields
- [ ] Embedding HMAC gateway secrets in App Store builds

### Platform targets

| Item | Requirement |
|------|-------------|
| Language | Swift 5.9+ |
| UI | SwiftUI (+ UIKit bridges: PhotosPicker, QLPreview, document picker) |
| Min OS | **iOS 16+** (prefer 17+ for Observation); align FFI XCFramework (15+) |
| Architecture | MVVM + actors; **UI never blocks on FFI** |
| Networking | **Only** via `lane_messenger_ffi` (UniFFI preferred) |
| Persistence | GRDB (SQLite) for messages; Keychain for secrets |
| DI | Lightweight (factories / `@Environment`); no heavy frameworks required |
| Package | Xcode app `LaneMessenger` + SPM deps on `LaneMessengerFFI` |

---

## Module map

```text
LaneMessenger (app)
├── App/                    # @main, DI, scene phase, deep links
├── Features/
│   ├── Auth/
│   ├── Inbox/              # chats list
│   ├── Chat/               # 1:1 + group thread
│   ├── Contacts/
│   ├── Groups/
│   ├── Media/
│   ├── Settings/           # devices, safety number, logout
│   └── Debug/              # staging gateway, token paste (DEBUG only)
├── Domain/                 # models, use cases (SendMessage, MarkRead…)
├── Data/
│   ├── FFI/                # SessionActor wrapping UniFFI / C ABI
│   ├── Local/              # GRDB repositories
│   ├── Keychain/
│   └── HTTP/               # identity / config only
└── DesignSystem/           # colors, typography, bubbles, ticks
```

**Rule:** `Features/*` may call Domain + observe session state. Only
`Data/FFI` talks to Rust.

---

## Phase I0 — App foundation & FFI link

Mirror FFI F7 / [`docs/client-ffi/BUILD.md`](docs/client-ffi/BUILD.md).

- [x] Create app sources `apps/ios/` (`LaneMessengerKit` + `App/`).
- [~] Add SPM / binary dependency on `bindings/swift/LaneMessengerFFI`
      (or vendored `LaneMessengerFFI.xcframework` from
      `./scripts/build_xcframework.sh`). — kit builds without it via mock;
      `LaneFFITransport` links when module is present.
- [~] Prefer **UniFFI-generated** Swift (`generated/lane_messenger.swift`)
      for session APIs; keep C ABI wrapper as fallback.
- [x] `SessionActor` (Swift actor):
      - [x] `connect(config)` / `close()`
      - [x] `subscribeEvents()` → `AsyncStream<LaneEvent>` (JSON parse)
      - [x] Never call FFI on the main actor for connect/send/poll
- [x] App config:
      - [x] `Info.plist` ATS local networking; TLS default in Release config
      - [x] Feature flags: `E2EE_ENABLED`, `DEBUG_PLAINTEXT_FALLBACK` (DEBUG)
      - [x] Build settings: `MESSENGER_HOST`, `MESSENGER_PORT` via Info.plist
- [x] Logging: `os.Logger` categories `session`, `chat`, `media`, `e2ee`,
      `ui`, `auth` — **never** log `auth_token`, private keys, or E2EE plaintext.
- [ ] CI (GitHub Actions macOS):
      - [x] Build XCFramework artifact (reuse `ffi-ios-xcframework` job)
      - [ ] `xcodebuild` simulator smoke for `LaneMessenger`

**Tests**
- [~] App launches on simulator (needs Xcode project / XcodeGen).
- [ ] Link succeeds for `iphonesimulator` + `iphoneos` slices.
- [x] Unit: `SessionActor` maps FFI errors to typed `AppError`
      (`swift run lane-messenger-kit-smoke`).

**Docs**
- [x] `docs/client-ios/00_overview.md` — architecture diagram above.
- [x] `docs/client-ios/README.md` — run against local
      `cargo run --example messenger_demo --features messenger`.

**Exit criteria**: kit + mock session green; real FFI link when XCFramework
present. DEBUG Home screen after login with mock transport.

---

## Phase I1 — Auth, identity, Keychain

Wire: `Login` / `LoginAck` via FFI (see `docs/messenger/04_auth.md`).

- [x] Stable `device_id` (UUID) in Keychain
      (`AfterFirstUnlockThisDeviceOnly`).
- [x] Persist `user_id` + `auth_token` in Keychain (not UserDefaults).
- [x] `AuthService` protocol:
      - [x] DEBUG: paste token / mint helper (compile-stripped in Release)
      - [x] Production: HTTPS identity service → token (no HMAC secret in app)
- [x] Splash → restore session if Keychain has credentials.
- [x] Login screen → `SessionActor.connect` with
      `client_version = "ios-<marketing>(<build>)"`.
- [x] Handle `LoginAck.ok == false` and `AUTH_FAILED` with backoff UX.
- [x] Handle `REPLACED_BY_NEW_SESSION`: alert, stop auto-reconnect until
      user confirms.
- [x] Settings → Sign out (close session, clear token; keep `device_id`).

**Tests**
- [x] Keychain round-trip; device_id stable across relaunch.
- [x] Bad token → typed error, no crash loop.
- [x] Kick UX path unit-tested with injected event.

**Docs**
- [x] `docs/client-ios/01_auth.md` — Keychain layout, DEBUG vs Release.

**Exit criteria**: cold start restores session; secrets never in logs or
plist; kick is understandable.

---

## Phase I2 — Session lifecycle & reconnect

Wire: connect → Login → offline replay → `SyncComplete` → Ready; Ping ~30s
(see `docs/messenger/03_sessions.md`, `07_heartbeats.md`).

```text
Disconnected → Connecting → AwaitingLogin → Syncing → Ready
                     ↑_______________|  (backoff reconnect)
```

- [ ] Expose `ConnectionState` to UI (banner: Connecting / Syncing / Ready /
      Offline).
- [ ] Auto-ping via FFI (`ping_interval_secs`); surface RTT in DEBUG only.
- [ ] Foreground: resume connection; Background: expect disconnect.
- [ ] On reconnect: set `resume_after_seq` from local DB high-water mark
      **before** connect (FFI `set_resume_seq` / connect option).
- [ ] Sync progress UI when `LoginAck.pending_messages > 0`.
- [ ] Map `ProtocolError` codes to UX (upgrade, re-auth, rate limit, etc.).
- [ ] Backoff: exponential with jitter; cap; pause on `UNSUPPORTED_VERSION`.

**Tests**
- [ ] Integration (simulator + local gateway): LoginAck → SyncComplete.
- [ ] Kill Wi-Fi / toggle airplane → reconnect + catch-up.
- [ ] Same-device second login kicks first (parity FFI smoke).

**Docs**
- [ ] `docs/client-ios/02_session.md` — state machine + resume rules.

**Exit criteria**: stable Ready; idle reconnect restores inbox without
duplicates (idempotent `message_id`).

---

## Phase I3 — Local message store (SQLite)

Offline-first UI; socket is the sync pipe, not the source of truth for display.

- [ ] Adopt **GRDB** (or equivalent) with migrations.
- [ ] Tables (minimum):
      - [ ] `conversations` (peer or group id, sort_ts, unread, draft)
      - [ ] `messages` (id, conversation_id, direction, body/ciphertext meta,
            media_id, seq, status, created_at)
      - [ ] `contacts` (user_id, display_name, presence, last_seen)
      - [ ] `groups` / `group_members` (versioned membership)
      - [ ] `meta` (`resume_after_seq`, schema_version)
- [ ] Message status enum: `pending → sent(ServerAck) → delivered → read`
      (+ `failed`).
- [ ] Idempotent upsert on `message_id` (replay-safe).
- [ ] Apply inbound FFI events → DB → UI observation (`ValueObservation`
      or `AsyncStream`).
- [ ] Outbound: write pending row **first**, then FFI send; on failure mark
      `failed` + retry affordance.

**Tests**
- [ ] Migration up/down smoke.
- [ ] Duplicate `message_id` does not double-insert.
- [ ] `resume_after_seq` advances only on durable apply.

**Docs**
- [ ] `docs/client-ios/03_storage.md` — schema, retention, wipe-on-logout
      policy.

**Exit criteria**: kill app mid-sync; relaunch shows consistent history;
resume seq correct.

---

## Phase I4 — Inbox + 1:1 chat UI

- [ ] Inbox list: avatar placeholder, title, preview, time, unread badge,
      presence dot.
- [ ] Thread: bubbles, timestamps, day separators, send box, attachment btn.
- [ ] Ticks: clock (pending) → single (ServerAck) → double (Delivered) →
      blue double (Read) — match product copy to server semantics.
- [ ] Send text → Domain `SendMessage` → DB + FFI.
- [ ] On open thread: emit `DeliveredAck` / `ReadAck` via FFI for inbound.
- [ ] Drafts persisted per conversation.
- [ ] Empty / error / offline states.

**Tests**
- [ ] UI snapshot or ViewInspector smoke for bubble + ticks.
- [ ] Integration: A→B chat round-trip against local gateway (two simulators
      or one sim + Rust peer).

**Docs**
- [ ] `docs/client-ios/04_chat.md` — tick mapping table to packet types.

**Exit criteria**: two users exchange messages with correct ticks after
reconnect.

---

## Phase I5 — Presence & contacts

Wire: `Presence`, optional `SubscribePresence`
([`docs/messenger/04_presence.md`](docs/messenger/04_presence.md)).

- [ ] Apply presence events into `contacts` + inbox rows.
- [ ] Contact bootstrap: DEBUG seed list / manual add until server contact
      API exists (HTTP).
- [ ] Privacy: never invent last-seen if server omitted it.
- [ ] Do **not** invent typing packets (not in proto).

**Tests**
- [ ] Presence updates UI without scroll jump.
- [ ] Integration parity with FFI presence smoke.

**Docs**
- [ ] `docs/client-ios/05_presence.md`

**Exit criteria**: online/offline reflects gateway; last-seen only when
provided.

---

## Phase I6 — Groups

Wire: `GroupEvent`, `GroupMessage`, `GroupAckSummary`
([`docs/messenger/08_groups.md`](docs/messenger/08_groups.md)).

- [ ] Create group / add / remove / leave via FFI.
- [ ] Apply membership `version` in order; ignore stale.
- [ ] Group thread UI (sender labels, system lines for membership).
- [ ] Group info screen (members, admin actions).
- [ ] Respect `max_group_members` in UI when known.
- [ ] Document ack-aggregation vs WhatsApp until server summary is complete.

**Tests**
- [ ] Integration: create → add → fan-out message.
- [ ] Non-member send / non-admin add surfaces error.

**Docs**
- [ ] `docs/client-ios/06_groups.md`

**Exit criteria**: group golden path works on single-node gateway.

---

## Phase I7 — Media

Wire: `MediaStart` / `MediaChunk` / `MediaAck` / `MediaFetch`
([`docs/messenger/02_bulk_data.md`](docs/messenger/02_bulk_data.md)).

WhatsApp-style split still applies conceptually: **control + chunks on the
messaging socket** (this repo), not a separate gRPC stream. Optional future
CDN/HTTP for large blobs can sit behind the same UI.

- [ ] Picker: Photos, Files (PDF), Camera.
- [ ] Cap **64 MiB**; chunk ≤ **64 KiB**; SHA-256 before upload (FFI/helpers).
- [ ] Queue: one upload at a time per session (server rule).
- [ ] Progress UI; failure + retry; resume download via `from_offset`.
- [ ] Persist blobs under Application Support; reference from `messages`.
- [ ] Image viewer + Quick Look for PDF.

**Tests**
- [ ] Upload/fetch round-trip; oversized rejected; corrupt SHA fails cleanly.

**Docs**
- [ ] `docs/client-ios/07_media.md`

**Exit criteria**: image + PDF send/receive end-to-end; no partial file shown
as complete.

---

## Phase I8 — E2EE

Wire/crypto in Rust (`docs/messenger/10_e2ee.md`); iOS only stores pickle +
UX.

- [ ] Feature flag `E2EE_ENABLED` default **on** for staging/prod builds.
- [ ] On first login: generate device via FFI; `publish` one-time keys.
- [ ] 1:1: `send_encrypted_chat` / `decrypt_chat`; store ciphertext + meta
      only when possible.
- [ ] Groups: Megolm session create / distribute / decrypt.
- [ ] Safety number screen (compare / screenshot warning).
- [ ] Account pickle export/import to Keychain-backed file
      (passphrase UX).
- [ ] DEBUG-only plaintext fallback behind explicit flag (never in Release).

**Tests**
- [ ] Two-device encrypt/decrypt against gateway (parity FFI E2EE smokes).
- [ ] Pickle round-trip survives reinstall (Keychain + file).

**Docs**
- [ ] `docs/client-ios/08_e2ee.md` — threat model notes for UI
      ([`11_security_review.md`](docs/messenger/11_security_review.md)).

**Exit criteria**: E2EE on by default in Release; safety number reachable
from chat info.

---

## Phase I9 — Push, background, notifications

FunXMPP socket will drop in background — design for it.

- [ ] APNs entitlement + device token → HTTPS registration API (when server
      exists); stub protocol now.
- [ ] Notification service: show sender/preview policy (hide body when E2EE).
- [ ] Tap notification → deep link to conversation.
- [ ] Foreground reconnect + `resume_after_seq` (I2).
- [ ] Badge = sum of local unread.
- [ ] Do **not** abuse VoIP push for chat (App Store risk); use standard
      alert pushes.

**Tests**
- [ ] Cold start from notification opens correct thread (UI test).
- [ ] Badge updates on inbound apply.

**Docs**
- [ ] `docs/client-ios/09_push.md` — payload contract, E2EE preview rules.

**Exit criteria**: background message shows notification; opening app
syncs without gaps/duplicates.

---

## Phase I10 — Polish, accessibility, release hardening

- [ ] Dynamic Type, VoiceOver labels on ticks/send/attachments.
- [ ] Dark mode; reduce motion.
- [ ] Search messages (FTS in SQLite).
- [ ] Block / report placeholders (HTTP when available).
- [ ] Crash reporting (no secret fields in breadcrumbs).
- [ ] Performance: inbox scroll 60 fps with 10k messages (pagination).
- [ ] App Privacy nutrition labels; Keychain / photos usage strings.
- [ ] Release checklist:
      - [ ] ATS strict; no DEBUG mint in binary
      - [ ] E2EE on; plaintext flag absent
      - [ ] Strip symbols; bitcode N/A; size budget vs XCFramework
      - [ ] NOTICE / licenses (vodozemac, rustls, UniFFI) in Settings → Legal

**Tests**
- [ ] Accessibility audit on Login, Inbox, Chat.
- [ ] Instruments: no main-thread FFI; no retain cycles on `SessionActor`.

**Docs**
- [ ] `docs/client-ios/10_release.md` — store submission checklist.

**Exit criteria**: TestFlight build against staging gateway; crash-free
smoke (login → chat → media → background → resume).

---

## Suggested implementation order (follow this)

```text
I0 FFI link
 → I1 Auth/Keychain
 → I2 Session/reconnect
 → I3 SQLite
 → I4 Inbox + 1:1 UI
 → I5 Presence
 → I6 Groups
 → I7 Media
 → I8 E2EE
 → I9 Push
 → I10 Polish / TestFlight
```

Do not start rich UI (I4+) until I0–I3 are green: otherwise you will paint
over a flaky session and fight duplicate messages forever.

---

## HTTP vs FunXMPP split (product reminder)

```text
HTTPS (identity / config / APNs / analytics)
│
├── login / token refresh
├── profile & contacts directory (when exists)
├── push device registration
└── optional media CDN later

FunXMPP TLS socket (via lane_messenger_ffi)
│
├── Login / sync / chat / groups
├── presence / acks / receipts
├── media chunks (current design)
└── ping / reconnect
```

If you later add gRPC, use it **only** for request/response control APIs —
never as a replacement for the realtime FunXMPP session.

---

## Definition of done (production)

- [ ] Release build links stripped XCFramework; no DEBUG auth helpers.
- [ ] Two physical devices: 1:1 E2EE chat, ticks, reconnect, media, group.
- [ ] Keychain + DB wipe paths documented and tested.
- [ ] No peer/server internals leaked into Swift.
- [ ] Docs under `docs/client-ios/` match shipped behaviour.
- [ ] TestFlight (or internal) build signed and distributed.

---

## Quick commands

```bash
# Gateway (repo root)
cargo run --example messenger_demo --features messenger

# XCFramework for the app
./scripts/build_xcframework.sh

# Regenerate UniFFI Swift
./scripts/generate_uniffi_bindings.sh

# FFI smoke (Rust)
cargo test -p lane_messenger_ffi --features uniffi
```

Sample driver: [`examples/ios_ffi_demo/`](examples/ios_ffi_demo/).
