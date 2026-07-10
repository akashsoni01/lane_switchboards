# Lane messenger ecosystem — architecture & beginner guide

This is the **end-to-end** map of the production stack: iOS UI → Swift kit →
FFI bindings → Rust client → FunXMPP over TCP/TLS → messenger gateway.

**Diagrams in this doc**

| Section | Contents |
|---------|----------|
| [§1.1](#11-entities-servers-protocols-mermaid) | Entities, servers, protocols (flowchart) |
| [§1A](#1a-session-connect--login-detailed) | Connect / Login / SyncComplete |
| [§1B](#1b-message-send-flow-maximum-detail) | Full send + ack ladder + offline/cluster |
| [§1C](#1c-message-receive-flow-maximum-detail) | Live receive, offline replay, APNs open |
| [§1D](#1d-ack-ladder--wire-packets-cheat-sheet) | Packet ID cheat sheet |

If you are new here, read this page first, then dive into the linked docs.

| Audience | Start here |
|----------|------------|
| Backend / protocol | [`docs/messenger/`](messenger/README.md) |
| Mobile FFI | [`docs/client-ffi/`](client-ffi/00_overview.md) |
| iOS product | [`docs/client-ios/`](client-ios/00_overview.md) |
| Phase checklists | [`todo.md`](../todo.md), [`todo_ios.md`](../todo_ios.md), [`todo_client_ffi.md`](../todo_client_ffi.md) |
| WA vs Lane gaps | [`gap.md`](../gap.md) — fidelity score + implementation backlog |

---

## 1. Big picture (production)

### 1.1 Entities, servers, protocols (Mermaid)

Who talks to whom, and **which protocol** each link uses.

```mermaid
flowchart TB
  subgraph Device["iOS device (LaneMessenger)"]
    UI["SwiftUI views<br/>RootView / ChatThread"]
    AM["AppModel<br/>MVVM + Observation"]
    SA["SessionActor<br/>reconnect · resume_after_seq"]
    CS["ChatService / GroupService<br/>MediaService / E2eeService"]
    SQL[("SQLite<br/>messages · unread · drafts · seq")]
    KC[("Keychain<br/>auth_token · device_id · E2EE pickle")]
    TR["MessengerTransport"]
    MOCK["MockMessengerTransport<br/>DEBUG / smoke only"]
    FFI_T["LaneFFITransport"]
    BIND["LaneMessengerFFI<br/>LaneSession · LaneE2ee"]
    FFI_LIB["lane_messenger_ffi<br/>SessionHandle · Tokio · E2eeDevice"]
    UN["UNUserNotificationCenter<br/>local + remote alert"]
    BADGE["App icon badge<br/>sum unread"]

    UI --> AM
    AM --> SA
    AM --> CS
    AM --> SQL
    AM --> KC
    AM --> UN
    AM --> BADGE
    CS --> SA
    SA --> TR
    TR -.->|tests / no XCFramework| MOCK
    TR -->|production| FFI_T
    FFI_T --> BIND
    BIND --> FFI_LIB
  end

  subgraph Edge["Edge / control plane (HTTPS)"]
    ID["Identity service<br/>mint / refresh auth token"]
    APNS_REG["Push registration API<br/>POST device token"]
    CDN["Optional CDN / media origin"]
    APNS["Apple APNs<br/>alert pushes only — never VoIP"]
  end

  subgraph GatewayNode["Messenger gateway node<br/>lane_switchboards · feature messenger"]
    ACC["Accept loop<br/>TCP · optional rustls TLS"]
    AUTH["Authenticator<br/>pluggable · HMAC demo / prod verifier"]
    SESS["Session task per connection<br/>Login deadline 10s"]
    ROUTER["Message router"]
    PRES["Presence registry<br/>available · unavailable · last_seen"]
    GROUPS["Group membership<br/>fan-out"]
    MEDIA["Media store<br/>chunked blobs 0x30–0x33"]
    INBOX[("Recipient inbox + seq<br/>durable before ServerAck")]
    WAL[("WAL / journal<br/>crash recovery")]
    KEYDIR["E2EE key directory<br/>identity / one-time keys"]
  end

  subgraph Cluster["Optional multi-node cluster"]
    PEER["Peer mesh<br/>PeerHello 0x50 · PeerPresence 0x51 · PeerSync 0x52"]
    HOME["Home shard<br/>hash-ring by user_id"]
  end

  %% Control-plane HTTPS
  AM -->|"HTTPS JSON login refresh"| ID
  AM -->|"HTTPS JSON push_type alert"| APNS_REG
  APNS_REG -.->|"APNs HTTP2 provider API"| APNS
  APNS -->|"alert payload lane.conversation_id"| UN
  CS -.->|"HTTPS GET optional"| CDN

  %% Realtime FunXMPP
  FFI_LIB -->|"persistent TCP plus TLS FunXMPP frames"| ACC
  ACC --> AUTH
  AUTH --> SESS
  SESS --> ROUTER
  SESS --> PRES
  ROUTER --> INBOX
  ROUTER --> GROUPS
  ROUTER --> MEDIA
  ROUTER --> KEYDIR
  INBOX --> WAL
  ROUTER -.->|"forward if remote home"| PEER
  PEER --> HOME
  HOME --> INBOX
```

**Protocol legend**

| Link | Protocol | Payload |
|------|----------|---------|
| App ↔ Identity / push register | **HTTPS** + JSON | tokens, APNs device token |
| App ↔ APNs (via Apple) | APNs **alert** | `aps` + `lane.*` custom keys |
| FFI ↔ Gateway | **TCP** (+ **TLS** in prod) | FunXMPP: `u8 ver \| u8 pkt_type \| u32 len \| protobuf` |
| Gateway ↔ Gateway (cluster) | Same FunXMPP peer packets | `0x50`–`0x52` only (never exposed to apps) |
| Kit ↔ SQLite / Keychain | Local IPC / OS APIs | rows / secrets — not on the wire |

### 1.2 Stack (ASCII quick view)

```text
┌──────────────────────────────────────────────────────────────────┐
│  LaneMessenger (SwiftUI)                                         │
│  apps/ios/App/                                                   │
│    · screens, deep links, APNs AppDelegate                       │
└────────────────────────────┬─────────────────────────────────────┘
                             │
┌────────────────────────────▼─────────────────────────────────────┐
│  LaneMessengerKit                                                │
│  apps/ios/Sources/LaneMessengerKit/                              │
│    · AppModel, SessionActor, Chat/Group/Media/E2ee services      │
│    · SQLite (messages, unread, resume seq)                       │
│    · Keychain (token, device_id, E2EE pickle)                    │
│    · Push presentation + badge (alert APNs only)                 │
└────────────────────────────┬─────────────────────────────────────┘
                             │ MessengerTransport
              ┌──────────────┴──────────────┐
              │                             │
   MockMessengerTransport          LaneFFITransport
   (smoke / no XCFramework)        (#if canImport(LaneMessengerFFI))
              │                             │
              │                  bindings/swift/LaneMessengerFFI
              │                    LaneSession · LaneE2ee
              │                             │
              │                  lane_messenger_ffi (Rust)
              │                    SessionHandle · E2eeDevice
              │                    Tokio runtime · poll events
              └──────────────┬──────────────┘
                             │  TCP (+ TLS) · FunXMPP frames
                             │  u8 ver | u8 type | u32 len | protobuf
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│  Messenger gateway  (lane_switchboards, feature `messenger`)     │
│  src/messenger/server.rs                                         │
│    accept → auth → session → router → presence / groups / media  │
│    inbox + WAL for offline · optional cluster peer mesh          │
└──────────────────────────────────────────────────────────────────┘
```

### What production uses vs what it does **not**

| Use | Do **not** use for core chat |
|-----|------------------------------|
| Persistent **TCP + TLS** + FunXMPP via FFI | gRPC `SendMessage` over HTTP/2 |
| HTTPS for login / APNs register / CDN | WebSocket as primary iOS transport |
| SQLite + Keychain on device | Hand-rolled Swift frame codec |
| Standard **alert** APNs | VoIP / Push-to-Talk for chat |

---

## 1A. Session connect & login (detailed)

Before any chat, the client must open a FunXMPP session. Login is always the
**first** frame (deadline **10s**).

```mermaid
sequenceDiagram
  autonumber
  actor User
  participant UI as SwiftUI AppModel
  participant Auth as AuthService
  participant KC as Keychain
  participant SA as SessionActor
  participant FFI as lane_messenger_ffi
  participant GW as Gateway
  participant AuthZ as Authenticator
  participant Inbox as Recipient inbox

  User->>UI: Sign in user_id secret
  UI->>Auth: login user_id secret
  alt DEBUG
    Auth-->>UI: mint HMAC demo token
  else Production
    Auth->>Auth: HTTPS POST identity service
    Auth-->>UI: auth_token plus claims
  end
  UI->>KC: save user_id device_id auth_token
  UI->>SA: connect host port use_tls creds resume_after_seq
  SA->>FFI: SessionHandle.connect ConnectOptions
  FFI->>GW: TCP connect optional TLS handshake
  Note over FFI,GW: Frame ver=1 pkt=Login 0x01 protobuf Login

  FFI->>GW: Login user_id device_id auth_token resume_after_seq
  GW->>AuthZ: verify token
  alt AUTH_FAILED or NOT_AUTHENTICATED
    AuthZ-->>GW: reject
    GW-->>FFI: ProtocolError then close
    FFI-->>SA: LaneEvent error disconnect
  else OK
    AuthZ-->>GW: ok
    opt Same device already online
      GW-->>GW: kick old session REPLACED_BY_NEW_SESSION 0x0F
    end
    GW-->>FFI: LoginAck session_id pending_messages ok
    loop Inbox rows with seq greater than resume_after_seq
      Inbox-->>GW: ChatMessage or GroupMessage
      GW-->>FFI: push frame monotonic seq
      FFI-->>SA: LaneEvent then AppModel upsert SQLite
    end
    GW-->>FFI: SyncComplete delivered latest_seq
    FFI-->>SA: SyncComplete
    SA-->>UI: connectionState ready
  end

  loop Every about 30s while connected
    FFI->>GW: Ping 0x03
    GW-->>FFI: Pong 0x04
  end
  Note over FFI,GW: Idle over 90s without traffic server closes
```

**Packet IDs in this phase:** `Login 0x01`, `LoginAck 0x02`, `Ping 0x03`,
`Pong 0x04`, `ProtocolError 0x0F`, `SyncComplete 0x24`, plus any replayed
`ChatMessage 0x20` / `GroupMessage 0x40`.

---

## 1B. Message **send** flow (maximum detail)

Alice (online) sends a 1:1 text to Bob. E2EE optional; ack ladder always applies.

```mermaid
sequenceDiagram
  autonumber
  actor Alice
  participant UI as SwiftUI thread
  participant AM as AppModel
  participant SQL as SQLite
  participant E2EE as E2eeService
  participant Chat as ChatService
  participant SA as SessionActor
  participant FFI as lane_messenger_ffi
  participant GW as Gateway Alice
  participant R as Message router
  participant Inbox as Bob inbox WAL
  participant Pres as Presence registry
  participant BobS as Gateway Bob
  participant BobFFI as Bob FFI client
  participant BobUI as Bob AppModel UI

  Alice->>UI: Tap Send compose text
  UI->>AM: sendChat peer=bob body
  AM->>SQL: upsertMessage pending outbound message_id
  AM->>UI: show pending tick

  alt E2EE enabled
    AM->>E2EE: encryptOutboundChat to=bob plaintext
    E2EE->>E2EE: Olm session encrypt in Rust
    E2EE-->>AM: ciphertext opaque body
  else Plaintext or DEBUG fallback
    AM->>AM: body equals UTF-8 bytes
  end

  AM->>Chat: send peer message_id body
  Chat->>SA: transport sendChat or sendEncryptedChat
  SA->>FFI: send_chat or send_encrypted_chat
  Note over FFI,GW: FunXMPP ChatMessage 0x20 opaque body

  FFI->>GW: ChatMessage from=alice to=bob message_id body
  GW->>R: route alice to bob

  R->>Inbox: durable insert dedup by message_id
  Note over Inbox: Assign monotonic seq then ServerAck
  Inbox-->>R: seq N assigned

  R-->>GW: enqueue ServerAck for Alice
  GW-->>FFI: ServerAck message_id seq N pkt 0x21
  FFI-->>SA: LaneEvent ServerAck
  SA-->>AM: handle ack
  AM->>SQL: updateStatus sent single tick
  AM->>UI: refresh ticks

  R->>Pres: is bob online on this node or cluster
  alt Bob online same node
    R->>BobS: push ChatMessage seq N
    BobS-->>BobFFI: ChatMessage 0x20
    BobFFI-->>BobUI: LaneEvent ChatMessage
    BobUI->>BobUI: E2EE decrypt if needed
    BobUI->>SQL: upsert inbound unread plus one
    opt App in background
      BobUI->>BobUI: local UNNotification and badge
    end
    BobUI->>BobFFI: DeliveredAck 0x22
    BobFFI->>BobS: DeliveredAck message_id
    BobS->>R: fan-in to Alice
    R->>GW: DeliveredAck toward Alice
    GW-->>FFI: DeliveredAck 0x22
    FFI-->>AM: updateStatus delivered double tick

    opt Bob opens thread
      BobUI->>BobFFI: ReadAck 0x23
      BobFFI->>BobS: ReadAck
      BobS->>R: route to Alice
      R->>GW: ReadAck
      GW-->>FFI: ReadAck 0x23
      FFI-->>AM: updateStatus read blue ticks
      AM->>SQL: mark read
    end
  else Bob not on this node
    alt Bob offline
      Note over Inbox: Stays in inbox until Bob Login resume_after_seq
      Note over Alice: Alice already has ServerAck Delivered Read wait
    else Bob on another cluster node
      R->>R: PeerSync forward to Bob home shard 0x52
      Note over R: Home persists seq then deliver via PeerPresence
    end
  end
```

**Send-path guarantees**

| Step | Guarantee |
|------|-----------|
| Before `ServerAck` | Message is **durable** in recipient inbox (WAL) |
| Dedup | Retries with same `message_id` → exactly-once effect per inbox |
| Ordering | Recipient sees monotonic `seq`; replay is gap-free ascending |
| Body | Opaque bytes — plaintext or Olm/Megolm ciphertext; gateway does not need to read E2EE content |
| Ticks | `ServerAck 0x21` → `DeliveredAck 0x22` → `ReadAck 0x23` |

**Group send (delta):** `GroupMessage 0x40` instead of `ChatMessage`; router
fans out to members; receipts may arrive as `GroupAckSummary 0x42`.

**Media send (delta):** `MediaStart 0x30` → many `MediaChunk 0x31` (≤64 KiB) →
`MediaAck 0x32`; then chat/group message carries `media_id` + optional caption.

---

## 1C. Message **receive** flow (maximum detail)

Two cases: Bob is **online** when Alice sends, and Bob was **offline** (catch-up
on login). Also covers background notification + deep link.

```mermaid
sequenceDiagram
  autonumber
  participant Inbox as Bob durable inbox
  participant GW as Gateway Bob
  participant FFI as Bob lane_messenger_ffi
  participant SA as SessionActor
  participant AM as AppModel
  participant E2EE as E2eeService
  participant SQL as SQLite
  participant UN as Notification presenter
  participant UI as SwiftUI
  actor Bob

  Note over Inbox,UI: Case A Bob already online live push
  Inbox->>GW: router delivers ChatMessage seq N
  GW->>FFI: frame ChatMessage 0x20
  FFI->>SA: event stream LaneEvent ChatMessage
  SA->>AM: handle event
  AM->>E2EE: decryptInboundChat from bodyData
  alt decrypt OK
    E2EE-->>AM: plaintext wasEncrypted
  else decrypt fail no fallback
    E2EE-->>AM: placeholder may skip notify
  end
  AM->>SQL: upsertMessage inbound delivered unread plus one
  AM->>SQL: refresh inbox preview sort_ts

  alt selectedPeer is conversation and foreground
    AM->>FFI: ackDelivered message_id
    AM->>FFI: ackRead message_id
    AM->>SQL: status read unread zero
    AM->>UI: reloadThread
  else background or other thread
    AM->>UN: presentLocal PushPayload preview policy
    AM->>UN: setBadge sum unread
    AM->>FFI: ackDelivered when policy allows
  end

  Note over Inbox,UI: Case B Bob was offline login replay
  Bob->>AM: cold start or foreground
  AM->>SA: connect resume_after_seq max stored seq
  SA->>FFI: Login resume_after_seq S
  FFI->>GW: Login 0x01
  GW->>Inbox: select rows seq greater than S order by seq
  loop Each pending message
    Inbox-->>GW: row next seq
    GW->>FFI: ChatMessage or GroupMessage with seq
    FFI->>AM: upsert and decrypt as above
  end
  GW->>FFI: SyncComplete delivered latest_seq
  FFI->>AM: persist latest_seq as resume cursor
  AM->>UI: connectionState ready refreshInbox

  Note over UN,UI: Case C APNs or deep link open
  UN->>AM: handleNotificationOpen or handleDeepLink
  AM->>AM: pendingOpenConversationId bob if not home
  AM->>SA: foreground reconnect and resume
  AM->>UI: openChat peer bob after home
  AM->>SQL: markConversationRead
  AM->>UN: clearNotifications conversation
  AM->>UN: refreshBadge
  AM->>FFI: DeliveredAck and ReadAck for visible msgs
```

**Receive-path state on device**

```mermaid
stateDiagram-v2
  [*] --> Disconnected
  Disconnected --> Connecting: bootstrap signIn foreground
  Connecting --> AwaitingLogin: TCP TLS up
  AwaitingLogin --> Syncing: LoginAck ok
  AwaitingLogin --> Locked: REPLACED_BY_NEW_SESSION
  Syncing --> Ready: SyncComplete
  Ready --> Ready: ChatMessage acks presence Ping
  Ready --> Reconnecting: socket drop background kill
  Reconnecting --> Connecting: auto-reconnect policy
  Locked --> [*]: user acknowledges re-login required
  Ready --> Disconnected: signOut close
```

**Inbound UI / notify decision**

| Condition | Behavior |
|-----------|----------|
| Foreground + thread open | Upsert, show bubble, send Delivered+Read, no banner |
| Foreground + other screen | Upsert, unread++, inbox badge; optional in-app banner |
| Background + socket alive | Upsert, **local** notification, badge++ |
| Background + socket dead | Miss live push; rely on **APNs alert** then resume on open |
| E2EE + `hideBodyWhenE2EE` | Notification body = “New message” (no plaintext on lock screen) |

---

## 1D. Ack ladder & wire packets (cheat sheet)

```mermaid
flowchart LR
  subgraph ClientSend["Sender client"]
    P["pending"] --> S["sent ServerAck"]
    S --> D["delivered DeliveredAck"]
    D --> R["read ReadAck"]
  end

  subgraph Wire["FunXMPP on TCP TLS"]
    CM["0x20 ChatMessage"]
    SA["0x21 ServerAck"]
    DA["0x22 DeliveredAck"]
    RA["0x23 ReadAck"]
  end

  P -.->|send| CM
  CM -.->|after durable inbox| SA
  SA --> S
  DA --> D
  RA --> R
```

| ID | Packet | Direction | Role in send/receive |
|----|--------|-----------|----------------------|
| 0x01 | Login | C→S | Auth + `resume_after_seq` |
| 0x02 | LoginAck | S→C | `session_id`, `pending_messages` |
| 0x03 / 0x04 | Ping / Pong | C↔S | Liveness (~30s / idle 90s) |
| 0x0F | ProtocolError | S→C | Fatal then close |
| 0x10 | Presence | both | Online / offline / last seen |
| 0x11 | SubscribePresence | C→S | Roster filter |
| 0x20 | ChatMessage | both | 1:1 body (opaque) |
| 0x21 | ServerAck | S→C | Persisted (single tick) |
| 0x22 | DeliveredAck | both | Device got it (double tick) |
| 0x23 | ReadAck | both | Opened thread (blue) |
| 0x24 | SyncComplete | S→C | Offline replay done |
| 0x30–0x33 | Media* | both | Chunked blob transfer |
| 0x40 | GroupMessage | both | Group chat |
| 0x41 | GroupEvent | both | Create / add / remove / leave |
| 0x42 | GroupAckSummary | S→C | Aggregated group receipts |
| 0x50–0x52 | Peer* | node↔node | Cluster only — **not** in app FFI |

Frame layout: `u8 ver | u8 pkt_type | u32 length | protobuf` (big-endian).
Schemas: `proto/messenger.proto`.

---

## 2. The three packages beginners mix up

### 2.1 `messenger` (server + reference client)

**What it is:** the FunXMPP messaging plane inside the root crate
`lane_switchboards` (Cargo feature `messenger`).

**Owns:**

- Binary wire codec (`src/messenger/codec.rs`)
- Gateway sessions, routing, acks, presence, groups, media (`server.rs`)
- Offline inbox / journal
- Olm/Megolm E2EE (`e2ee.rs`)
- Reference client used by tests and `messenger_demo` (`client.rs`)
- Optional multi-node cluster (`cluster.rs`)

**Mental model:** WhatsApp-like **backend + a Rust test client**. Mobile apps do
not talk to this crate directly; they talk through the FFI.

**Docs:** [`docs/messenger/README.md`](messenger/README.md)

### 2.2 `lane_messenger_ffi` (mobile bridge)

**What it is:** a separate Cargo crate that wraps the same Rust
`MessengerClient` + `E2eeDevice` behind **opaque handles** for Swift / Android /
Flutter / C.

**Owns:**

- Tokio runtime (hosts must not block the UI thread on connect/send)
- `SessionHandle::connect` / `poll_event` / send / ack / media / groups
- C ABI (`include/lane_messenger_ffi.h`, `c_api.rs`)
- Optional UniFFI + JNI

**Does not own:** UI, Keychain, SQLite, push banners.

**Mental model:** “the chat engine as a native library.”

**Docs:** [`lane_messenger_ffi/README.md`](../lane_messenger_ffi/README.md),
[`docs/client-ffi/`](client-ffi/00_overview.md)

### 2.3 `bindings` (language wrappers)

**What it is:** thin language packages that call the FFI. **No protocol logic.**

| Binding | Path |
|---------|------|
| Swift (hand-written C wrappers) | `bindings/swift/LaneMessengerFFI/` (`LaneSession.swift`, `LaneE2ee.swift`) |
| Swift / Kotlin UniFFI generated | `…/generated/` via `./scripts/generate_uniffi_bindings.sh` |
| Android JNI module | `bindings/android/lane-messenger-ffi/` |

**Mental model:** “Swift/Kotlin types that point at the Rust library.”

---

## 3. Layer-by-layer detail

### 3.1 iOS app (`apps/ios/App/`)

- `@main` SwiftUI entry (`LaneMessengerApp.swift`)
- Info.plist: gateway host/port, `remote-notification`, `lane://` URL scheme
- Entitlements: `aps-environment` (alert APNs only)
- Chooses transport:

```swift
#if canImport(LaneMessengerFFI)
  AppModel(transport: LaneFFITransport())
#else
  AppModel(transport: MockMessengerTransport())
#endif
```

### 3.2 LaneMessengerKit

| Area | Responsibility |
|------|----------------|
| `App/AppModel.swift` | Route, inbox, scene phase, push open, badge |
| `FFI/SessionActor.swift` | Connect / reconnect / resume_after_seq |
| `FFI/MessengerTransport.swift` | Protocol + mock |
| `FFI/LaneFFITransport.swift` | Real FFI |
| `Auth/` | DEBUG HMAC mint vs Release HTTPS identity |
| `Local/` | SQLite messages, drafts, unread, groups |
| `Domain/` | Chat, groups, media, E2EE orchestration |
| `Push/` | Payload parse, preview policy, token register |
| `UI/` | SwiftUI shells |

### 3.3 Wire protocol (FunXMPP)

```text
┌────┬──────────┬──────────┬─────────────────────┐
│ ver│ pkt_type │ length   │ protobuf payload    │
│ u8 │ u8       │ u32 BE   │                     │
└────┴──────────┴──────────┴─────────────────────┘
```

- Schema: `proto/messenger.proto`
- Lifecycle: connect → **Login** (first, ≤10s) → LoginAck → offline replay →
  SyncComplete → steady state (Ping/Pong ~30s; idle >90s closes)
- Docs: [`01_wire_protocol.md`](messenger/01_wire_protocol.md)

### 3.4 Gateway internals

```text
TCP(+TLS) accept
  → Authenticator (HMAC demo or pluggable)
  → Session registry (user → devices; same device → REPLACED_BY_NEW_SESSION)
  → Router
       · 1:1: persist inbox → ServerAck → deliver if online
       · groups: membership + fan-out
       · media: chunked blobs
       · presence: available / unavailable / last_seen
  → Optional cluster: home shard + peer forward (0x50–0x57, not exposed to apps)
```

### 3.5 Typical send path

Short form (full sequence diagrams: [§1B](#1b-message-send-flow-maximum-detail),
[§1C](#1c-message-receive-flow-maximum-detail)):

```text
User taps Send
  → SQLite row (pending)
  → E2EE encrypt in Rust (if enabled)
  → FFI send_chat / send_encrypted_chat
  → FunXMPP ChatMessage
  → ServerAck (✓) → DeliveredAck (✓✓) → ReadAck (blue)
  → update ticks in SQLite + UI
```

### 3.6 Background / push

FunXMPP TCP drops in background. Design:

1. Foreground: reconnect + `resume_after_seq` (I2)
2. Local notification while socket briefly alive
3. Future APNs alert payload → same `PushPayload` → deep link `lane://chat/<id>`
4. Badge = sum of local unread
5. **Never** VoIP push for chat

Docs: [`docs/client-ios/09_push.md`](client-ios/09_push.md)

---

## 4. DEBUG vs production

| Concern | DEBUG / local | Production |
|---------|---------------|------------|
| Auth | `DebugAuthService` — empty secret mints `hex(HMAC-SHA256("demo-secret", "user:device"))` | `HttpAuthService` → identity HTTPS; **no HMAC secret in the app** |
| Transport | Mock (smoke) or FFI with `use_tls: false` | `LaneFFITransport` + TLS |
| Gateway secret | `"demo-secret"` in `messenger_demo` | Server-side only |
| `MESSENGER_USE_TLS` | `NO` | `YES` |
| Push registrar | `StubPushTokenRegistrar` | `HttpPushTokenRegistrar` |

---

## 5. Step-by-step: run the ecosystem (beginner)

Do these in order. Each step proves one layer.

### Prerequisites

- Rust (stable) + Cargo
- For iOS native: macOS, Xcode, rustup iOS targets
- For kit-only smoke: Swift toolchain (Command Line Tools is enough)

```bash
# Optional iOS targets for XCFramework
rustup target add aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios
```

---

### Step A — Run the gateway demo (Rust only)

Proves: codec, auth, chat, acks, offline sync, groups, media on one machine.

```bash
cd /path/to/lane_switchboards
cargo run --example messenger_demo --features messenger
```

You should see a listen address and alice/bob golden-path logs.

Run the integration suite:

```bash
cargo test --test messenger
```

---

### Step B — Run iOS kit smoke (no native FFI required)

Proves: Swift session actor, store, groups, media, E2EE mocks, push helpers.

```bash
cd apps/ios
swift run lane-messenger-kit-smoke
```

Expect: `OK — all smoke checks passed`.

This uses `MockMessengerTransport` — **no gateway needed**.

---

### Step C — Build the FFI library (host)

Proves: the mobile crate compiles.

```bash
cargo build -p lane_messenger_ffi --release
cargo test -p lane_messenger_ffi
```

Header: `lane_messenger_ffi/include/lane_messenger_ffi.h`

---

### Step D — Build the iOS XCFramework

Proves: device + simulator slices for linking into the app.

```bash
./scripts/build_xcframework.sh
# → dist/LaneMessengerFFI.xcframework
```

Optional UniFFI codegen:

```bash
./scripts/generate_uniffi_bindings.sh
```

Details: [`docs/client-ffi/BUILD.md`](client-ffi/BUILD.md)

---

### Step E — Open the iOS app against a local gateway

1. Start a gateway you can point at (demo binds a random port — for a fixed
   port, run your own `MessengerServer::bind("127.0.0.1:9000", …)` or adjust
   Info.plist after noting the demo address).

2. Generate / open the Xcode project:

```bash
cd apps/ios
xcodegen generate          # requires XcodeGen
open LaneMessenger.xcodeproj
```

3. Link `dist/LaneMessengerFFI.xcframework` (and the Swift package
   `bindings/swift/LaneMessengerFFI` as needed).

4. Set Info.plist / scheme:

| Key | Example |
|-----|---------|
| `MESSENGER_HOST` | `127.0.0.1` |
| `MESSENGER_PORT` | `9000` |
| `MESSENGER_USE_TLS` | `NO` for local DEBUG |

5. Run on Simulator. **DEBUG login:**

- User ID: `alice` (or `bob`)
- Secret: leave **empty** to mint the demo HMAC token
- Or paste a pre-minted hex token

Without the XCFramework linked, the app still opens on **mock** transport for
UI work — you will not hit a real gateway.

---

### Step F — Two-device mental test (local)

1. Terminal: gateway listening.
2. Device/sim A: login `alice`, open chat with `bob`, send a message.
3. Device/sim B (or second process / Rust client): login `bob`, confirm
   delivery ticks and offline catch-up after disconnect.

Use `resume_after_seq` persistence in SQLite so reconnect does not duplicate.

---

### Step G — Production checklist (before TestFlight)

- [ ] Identity HTTPS issues tokens; app uses `HttpAuthService` only
- [ ] TLS on (`MESSENGER_USE_TLS=YES`); ATS-compliant
- [ ] No `demo-secret` / HMAC mint in Release
- [ ] XCFramework Release build linked
- [ ] APNs alert entitlement; token → HTTPS register; no VoIP
- [ ] E2EE pickle in Keychain; preview policy hides body when encrypted
- [ ] Multi-device kick UX for `REPLACED_BY_NEW_SESSION`
- [ ] See [`docs/messenger/11_security_review.md`](messenger/11_security_review.md)

---

## 6. How to *use* the pieces in code

### Rust reference client (tests / tools)

```rust
use lane_switchboards::messenger::MessengerClient;

let (mut client, _) = MessengerClient::connect(
    "127.0.0.1:9000", "alice", "phone-1", &token, /* resume_after_seq */ 0,
).await?;
client.send_chat("bob", "m-1", b"hello").await?;
```

### FFI (Rust API inside the crate)

```rust
use lane_messenger_ffi::{ConnectOptions, SessionHandle, LaneEvent};

let session = SessionHandle::connect(ConnectOptions {
    host: "127.0.0.1".into(),
    port: 9000,
    use_tls: false,
    user_id: "alice".into(),
    device_id: "phone-1".into(),
    auth_token: token,
    ..Default::default()
})?;

while let Some(ev) = session.poll_event(100) {
    if matches!(ev, LaneEvent::SyncComplete(_)) { break; }
}
session.send_chat("bob", "m-1", b"hi")?;
```

### Swift kit (app code)

Prefer `AppModel` / `ChatService` — do not call C APIs from views.

```swift
await model.signIn(userId: "alice", secret: "")
await model.openChat(peer: "bob")
// compose + send via ChatService / AppModel send helpers
```

Transport is injected once at `AppModel` init (`LaneFFITransport` or mock).

---

## 7. Repository map (bookmark this)

```text
lane_switchboards/
├── src/messenger/           # Gateway + reference client + codec + E2EE
├── proto/messenger.proto    # Packet schemas
├── examples/messenger_demo.rs
├── tests/messenger.rs
├── lane_messenger_ffi/      # Mobile FFI crate
├── bindings/swift/          # Swift wrappers + UniFFI output
├── bindings/android/        # JNI / AAR inputs
├── apps/ios/                # LaneMessenger + LaneMessengerKit
├── scripts/
│   ├── build_xcframework.sh
│   ├── generate_uniffi_bindings.sh
│   └── build_android_ndk.sh
├── docs/
│   ├── ECOSYSTEM_GUIDE.md   # ← you are here
│   ├── messenger/
│   ├── client-ffi/
│   └── client-ios/
└── deploy/                  # systemd / K8s examples
```

---

## 8. FAQ

**Q: Why isn’t chat just REST or gRPC?**  
Realtime delivery, presence, and ack ladders need a long-lived binary session.
HTTP is reserved for identity, config, APNs registration, and CDN.

**Q: Can I implement FunXMPP in Swift with Network.framework?**  
Not for production. The codec, E2EE, and framing stay in Rust via FFI so all
platforms share one implementation.

**Q: Mock vs FFI — which should I use day to day?**  
UI and kit logic: mock + smoke. Integration and devices: XCFramework + gateway.

**Q: Where do secrets live?**  
Auth token + device id + E2EE pickle → Keychain. Gateway HMAC secret →
**server only**, never in App Store builds.

**Q: What about Android / Flutter?**  
Same FFI crate; see `docs/client-ffi/07_platforms.md` and
`examples/android_ffi_demo/`, `examples/flutter_ffi_demo/`.

---

## 9. Next reading order

1. This guide (done)
2. [`messenger/00_overview.md`](messenger/00_overview.md) — delivery guarantees
3. [`client-ffi/02_session_and_events.md`](client-ffi/02_session_and_events.md) — connect / poll
4. [`client-ios/02_session.md`](client-ios/02_session.md) — SessionActor
5. [`messenger/10_e2ee.md`](messenger/10_e2ee.md) + [`client-ios/08_e2ee.md`](client-ios/08_e2ee.md)
6. [`todo_ios.md`](../todo_ios.md) — remaining I10 polish
