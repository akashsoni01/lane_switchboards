# FunXMPP-Style Messenger on `lane_switchboards` — Production TODO

Goal: build a WhatsApp-like compact binary messaging protocol (login, presence,
routing, acks, offline store, ping, groups, multi-node, E2EE) on top of the
existing actor mesh (`lane_core` actors, `Cluster`/`ServiceMesh`, hash ring,
gRPC data plane, `StorageNode`).

Legend: `[ ]` pending · `[~]` in progress / partial · `[x]` done
Every phase ends with **Tests**, **Docs**, and **Exit criteria** — do not move
to the next phase until all three are checked.

**Status (2026-07-08):** single-node milestone implemented and verified —
`src/messenger/` (codec, auth, server, client), `proto/messenger.proto`,
18 end-to-end tests in `tests/messenger.rs` (all passing), runnable demo
`examples/messenger_demo.rs`, docs in `docs/messenger/`. Next up: multi-node
routing (Phase 9), durable storage via `StorageNode` (Phase 6 hardening),
and E2EE (Phase 10).

---

## Phase 0 — Foundation & Project Hygiene

- [x] Create `messenger/` module (or `lane_messenger` workspace crate) so chat
      semantics stay separate from the actor runtime. → `src/messenger/`
- [x] Add `proto/messenger.proto` (package `lane_switchboard.messenger`) wired
      into `build.rs` — kept independent from `mesh_data.proto`.
- [x] Decide client transport: raw TCP with length-delimited frames for
      mobile-style clients; gRPC stays server-to-server.
      - [ ] `tokio-tungstenite` behind a `ws` feature for browser clients.
- [ ] Add feature flag `messenger` in `Cargo.toml`; CI builds with and without.
- [ ] Set up CI matrix: `cargo fmt --check`, `clippy -D warnings`,
      `cargo test --all-features`, `cargo deny` (licenses/advisories).

**Tests**
- [x] Proto compiles via `tonic-build`; crate + tests build clean.

**Docs**
- [x] `docs/messenger/00_overview.md` — architecture diagram (Gateway →
      Session → Router → Presence/Inbox/Media), scope, non-goals.

**Exit criteria**: ~~met for single-node scope~~ — generated types accessible
via `crate::proto::messenger` (re-exported as `messenger::wire`).

---

## Phase 1 — Compact Binary Wire Protocol

- [x] Frame layout: `| u8 version | u8 packet_type | u32 length | payload |`
      with prost-encoded payload. Max frame size enforced (default 256 KiB,
      rejected before allocation) — `src/messenger/codec.rs`.
- [x] Packet types: `Login`, `LoginAck`, `Ping`, `Pong`, `Presence`,
      `ChatMessage`, `ServerAck`, `DeliveredAck`, `ReadAck`, `SyncComplete`,
      `GroupMessage`, `GroupEvent`, `ProtocolError` + media packets
      (`MediaStart`/`MediaChunk`/`MediaAck`/`MediaFetch`) for bulk data (PDFs).
- [x] Every `ChatMessage` carries a client-generated `message_id` (dedup key)
      + `sent_at` timestamp + server-assigned `seq`.
- [x] `FrameCodec` (`tokio_util::codec::{Encoder, Decoder}`) with strict
      validation: unknown packet type / bad version / oversize → typed error.
- [x] Version byte checked on every frame; unsupported → typed error + close.

**Tests**
- [x] Unit: encode/decode round-trip for every packet type; malformed,
      truncated, oversized, garbage-payload, and pipelined-frame cases
      (7 tests in `codec.rs`).
      - [ ] Upgrade to proptest/cargo-fuzz for the decoder.
- [ ] Bench: add `benches/messenger_codec.rs` (frames/sec vs XML baseline).

**Docs**
- [x] `docs/messenger/01_wire_protocol.md` — byte layout, packet table,
      lifecycle, error codes, delivery semantics.
- [x] `docs/messenger/02_bulk_data.md` — chunked media transfer protocol.

**Exit criteria**: round-trip tests pass. Fuzz run still pending.

---

## Phase 2 — Gateway & Session Actors

- [x] Gateway: `TcpListener` accept loop; each connection → spawned session
      task — `MessengerServer::bind` in `src/messenger/server.rs`.
- [x] Session state machine: `AwaitingLogin → Authenticated → Closed`
      (per-connection task + `SessionHandle` registry entry).
- [~] Pre-auth limits: login deadline (10 s) enforced; per-IP connection
      rate limiting still pending.
- [x] Backpressure: bounded outbound queue per session (`session_buffer`,
      default 256); slow consumers drop frames via `try_send`.
- [ ] Graceful shutdown: gateway drains sessions, flushes pending acks.
- [ ] TLS on client sockets via existing `TlsConfig` (require TLS in prod).

**Tests**
- [x] Integration: clients connect, login, exchange frames (whole suite).
- [x] Failure: disconnect → session removed, presence goes Unavailable.
- [~] Load smoke: 100 concurrent clients chatting pairwise passes; the 10k
      idle-connection soak is pending.

**Docs**
- [ ] `docs/messenger/02_sessions.md` — session lifecycle state machine,
      backpressure policy, shutdown semantics.

**Exit criteria**: partially met (100-client smoke); 10k soak pending.

---

## Phase 3 — Authentication

- [x] `Login { user_id, device_id, auth_token, client_version }`; pluggable
      `trait Authenticator`; `HmacAuthenticator` (HMAC-SHA256, hex tokens) —
      `src/messenger/auth.rs`. `hmac`/`sha2` promoted to real dependencies.
- [x] Multi-device: session key = `(user_id, device_id)`; same-device
      reconnect kicks the old session with typed `REPLACED_BY_NEW_SESSION`.
- [x] Constant-time verification (`Mac::verify_slice`); tokens never logged;
      login success/failure logged.
- [ ] Brute-force protection: exponential backoff per user_id + per IP.

**Tests**
- [x] Unit: token verify (valid / tampered / garbage / wrong user / wrong
      device / wrong secret) — 4 tests in `auth.rs`.
- [x] Integration: same-device kick (`same_device_reconnect_replaces_old_session`).
- [x] Security: unauthenticated socket cannot send `ChatMessage`
      (`unauthenticated_chat_rejected`).

**Docs**
- [ ] `docs/messenger/03_auth.md` — token format, rotation, multi-device
      rules, threat model notes.

**Exit criteria**: met for HMAC scheme; JWT + brute-force backoff pending.

---

## Phase 4 — Presence Registry

- [~] Presence registry: `user_id → Vec<SessionHandle>` on the gateway
      (single node). Multi-node sharding via `HashRing` /
      `Cluster::send_by_key` is Phase 9 work.
- [x] On login/disconnect: registry updated; idle TTL sweep closes dead
      sessions and records last-seen.
- [~] Presence packets: `Available` / `Unavailable` / `LastSeen` implemented;
      currently broadcast to all online users — contact-list subscription
      filtering pending.
- [~] "Last seen" tracked in memory; persistence to `StorageNode` pending.

**Tests**
- [x] Integration: login broadcasts Available; disconnect broadcasts
      Unavailable with last_seen (`presence_broadcast_on_login_and_disconnect`).
- [ ] 2-node cluster lookup test (Phase 9).
- [ ] Property: convergence under arbitrary connect/disconnect interleavings.

**Docs**
- [ ] `docs/messenger/04_presence.md` — data model, sharding, TTL, privacy.

**Exit criteria**: single-node met; cross-node pending.

---

## Phase 5 — Message Routing & Delivery Acks

- [x] Routing on the gateway: `ChatMessage` → persist to recipient inbox →
      deliver to live sessions if online, else stays queued for sync.
      Cross-node routing via `RemoteActorRef` is Phase 9.
- [x] Ack ladder (WhatsApp ticks): `ServerAck` (persisted, single tick) →
      `DeliveredAck` (double tick) → `ReadAck` (blue tick), relayed to sender.
- [x] At-least-once + dedup by `message_id` per recipient inbox (idempotent
      insert; duplicate retries re-acked with `seq = 0` sentinel).
- [~] Client waits for `ServerAck` before considering a send done
      (`MessengerClient::send_chat`); automatic retry with backoff pending.
- [ ] Inter-node hops via `send_with_ack` + hop-vs-message ack mapping (Phase 9).
- [x] Ordering: per-recipient-inbox monotonic `seq` assigned at persist time;
      replay is ascending and gap-free.

**Tests**
- [x] Unit/integration: dedup (`duplicate_send_is_deduplicated`), full tick
      ladder (`online_delivery_with_full_tick_ladder`).
- [x] Integration: offline → stored → replayed
      (`offline_messages_replayed_in_order_on_login`).
- [ ] Cross-node delivery + chaos (kill node mid-delivery) — Phase 9.
- [ ] Bench: `benches/messenger_routing.rs`.

**Docs**
- [~] Delivery guarantees + ack semantics documented in
      `docs/messenger/01_wire_protocol.md`; dedicated failure-matrix doc pending.

**Exit criteria**: single-node met; chaos runs pending multi-node.

---

## Phase 6 — Offline Storage & Sync

- [~] Inbox per user with monotonic seq, dedup set, and tombstone-on-ack —
      currently in-memory (`Inbox` in `server.rs`). Migration to WAL-backed
      `StorageNode` with SERIAL/QUORUM writes pending.
- [x] On login: pending messages with `seq > resume_after_seq` stream in
      order, terminated by `SyncComplete`; `DeliveredAck` tombstones entries
      and contiguous-delivered heads are dropped.
- [x] Quotas: `max_inbox` per user (default 10 000, oldest-drop).
- [x] Compaction: tombstoned head entries removed on ack.

**Tests**
- [x] Integration: send while offline → reconnect → exact ordered replay →
      acks → inbox empty (`offline_messages_replayed_in_order_on_login`).
- [x] Resume: `resume_after_seq` replays only the gap
      (`resume_after_seq_skips_already_seen_messages`).
- [ ] Crash-recovery across storage restart (needs `StorageNode` backend).
- [ ] Property: gap-free/duplicate-free for random offline/online schedules.

**Docs**
- [~] Sync + retention semantics in `01_wire_protocol.md`; dedicated
      `06_offline_store.md` pending the durable backend.

**Exit criteria**: in-memory semantics verified; durability pending.

---

## Phase 7 — Heartbeats & Connection Health

- [x] Client→server `Ping` / server `Pong` implemented
      (`MessengerClient::ping`); server idle timeout 90 s (2 missed 30 s
      intervals) with a 5 s sweep.
- [x] Server-side idle sweep integrated with presence (sweep → session
      removed → Unavailable broadcast + last_seen).
- [~] User-space timer done; TCP keepalive socket option pending.
- [x] Fast reconnect: `Login.resume_after_seq` skips already-seen messages.

**Tests**
- [x] Integration: silent client swept and marked offline
      (`idle_session_swept_after_timeout`), ping round-trips
      (`ping_pong_round_trip`).
- [ ] Simulated packet-loss partition → reconnect + gap-only resume.

**Docs**
- [~] Intervals + resume protocol in `01_wire_protocol.md`; dedicated doc
      pending.

**Exit criteria**: met (detection well under 90 s in test; resume dedups).

---

## Phase 8 — Group Chat

- [~] `Group { members, admins, version }` with monotonic membership version —
      in-memory; `StorageNode` persistence + `HashRing(group_id)` home shard
      pending (Phase 9).
- [x] `GroupEvent`: create / add / remove / leave, versioned; events fan out
      to members.
- [x] Fan-out: membership snapshot at consistent version → per-member inbox
      (per-member dedup key `message_id:member`) → online deliver or offline
      store. Membership cap enforced (`max_group_members`, default 1024).
- [ ] Per-member delivery/read ack aggregation for the sender.
- [x] Authorization: only members send; only admins mutate membership.

**Tests**
- [x] Integration: create/add/fan-out to online members
      (`group_create_add_and_fanout`), offline member gets message on login
      (`offline_group_member_gets_message_on_login`).
- [x] Authz: non-member send rejected, non-admin add rejected
      (`non_member_cannot_send_and_non_admin_cannot_add`).
- [ ] 3-node 50-member mixed online/offline exactly-once test (Phase 9).
- [ ] Bench: fan-out latency vs group size.

**Docs**
- [ ] `docs/messenger/08_groups.md` — membership model, fan-out design,
      ack aggregation, limits.

**Exit criteria**: single-node met; multi-node exactly-once pending.

---

## Phase 9 — Multi-Node Operation & Scale

- [ ] Wire messenger services into `ServiceMesh` (`join_mesh`,
      `MeshRegistry`) so gateways discover router/presence/storage shards.
- [ ] User home shards on `HashRing`; inter-gateway delivery via
      `RemoteActorRef` / `Cluster::send_by_key(user_id)`.
- [ ] Node join/leave rebalance; sessions stay on their gateway, only shard
      ownership moves (presence re-register).
- [ ] Multi-DC: reuse `DcTopology`; user home DC affinity; cross-DC delivery.
- [ ] Observability (production requirement, not optional):
      - [ ] Metrics via existing `metrics` feature: connected sessions,
            msgs in/out/sec, ack latency histograms, inbox depth, fan-out
            duration, dropped frames.
      - [~] Structured logging (`tracing`) in place for session lifecycle;
            message_id correlation + user_id hashing pending.
      - [ ] Health/readiness endpoints for orchestration.
- [ ] Deployment: config via `DistributedConfig` extension, TLS everywhere,
      systemd/k8s manifests example.

**Tests**
- [ ] Integration: 3-node cluster, kill/rejoin a node during traffic → no
      message loss, presence converges.
- [ ] Soak: 1 h sustained load; memory flat, no fd leak.
- [ ] Bench: cross-node p50/p99 delivery latency recorded in docs.

**Docs**
- [ ] `docs/messenger/09_operations.md` — topology, scaling playbook,
      dashboards/alerts list, runbook for node loss.

**Exit criteria**: soak passes; runbook reviewed; all metrics visible in
Prometheus export test.

---

## Phase 10 — End-to-End Encryption

- [ ] Design doc first: X3DH key agreement + Double Ratchet (Signal model).
      Evaluate `vodozemac` or `libsignal-client` crates before hand-rolling
      (do NOT hand-roll primitives).
- [ ] Key server API: publish identity key, signed prekey, one-time prekeys;
      fetch prekey bundle for a peer. Store in `StorageNode`.
- [ ] Client encrypts payload before framing; server treats
      `ChatMessage.body` as opaque bytes end-to-end (wire format already
      uses `bytes body`, so no proto change needed).
- [ ] Group E2EE: sender-keys scheme (encrypt once per group, distribute
      sender key via pairwise sessions).
- [ ] Key rotation, device add/remove re-keying, out-of-order ratchet message
      handling (bounded skipped-key cache).
- [ ] Safety numbers / fingerprint verification API for clients.

**Tests**
- [ ] Unit: session establishment, ratchet forward secrecy, out-of-order
      decryption.
- [ ] Integration: full A→B E2EE flow through 2-node cluster; server-side
      assertion that payloads are never valid plaintext proto.
- [ ] Test vectors from the chosen library validated in CI.
- [ ] External security review checklist before calling this done.

**Docs**
- [ ] `docs/messenger/10_e2ee.md` — protocol choice rationale, key lifecycle,
      what the server can/cannot see, limitations.

**Exit criteria**: forward-secrecy tests pass; design doc reviewed; no
plaintext observable at server in integration test.

---

## Phase 11 — End-to-End Test Suite & Release

- [~] `tests/messenger.rs`: reusable full-protocol client
      (`MessengerClient`) covering the golden path on one node: login →
      presence → 1:1 chat with tick ladder → offline+sync → groups → media.
      18 tests, all passing. 3-node cluster variant pending Phase 9.
- [ ] Chaos suite: random node kills, packet loss/latency injection, clock
      skew — invariants asserted: no loss, no dupes (by effect), presence
      convergence.
- [ ] Fuzz targets in CI (decoder, auth, sync) — scheduled nightly.
- [x] Example app: `examples/messenger_demo.rs` + walkthrough
      `examples/messenger_demo.md` (repo convention) — runs clean.
- [ ] Docs index `docs/messenger/README.md` linking all docs; update root
      `README.md` feature matrix.
- [ ] Version bump, CHANGELOG, release notes per repo convention
      (`READMEvX.Y.Z.md`).

**Exit criteria**: e2e suite green on every run (currently: yes, locally);
CI wiring pending.

---

## Bonus (implemented beyond original plan)

- [x] Bulk data / media transfer (e.g. PDFs): chunked upload with per-chunk
      flow-control acks, SHA-256 integrity verification on both ends,
      resumable download (`MediaFetch.from_offset`), size caps
      (`max_media_bytes`, default 64 MiB). Tests:
      `upload_and_fetch_pdf_blob_round_trip`, `oversized_media_rejected`,
      `corrupted_upload_fails_sha_check`.

---

## Cross-Cutting Production Standards (apply to every phase)

- [x] No `unwrap`/`expect` on network/user input paths; typed errors
      (`MessengerError` via `thiserror`) end-to-end.
- [x] Public messenger types/functions documented.
- [~] All limits configurable with safe defaults (`ServerConfig`: frame size,
      login deadline, idle timeout, session buffer, inbox quota, media size,
      group size); consolidated `docs/messenger/limits.md` table pending.
- [x] Proto evolution rules stated in `proto/messenger.proto` header (append
      only, never reuse field numbers or packet IDs).
- [ ] Benchmarks tracked over time (criterion baselines committed per release).
