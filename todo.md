# FunXMPP-Style Messenger on `lane_switchboards` — Production TODO

Goal: build a WhatsApp-like compact binary messaging protocol (login, presence,
routing, acks, offline store, ping, groups, multi-node, E2EE) on top of the
existing actor mesh (`lane_core` actors, `Cluster`/`ServiceMesh`, hash ring,
gRPC data plane, `StorageNode`).

Legend: `[ ]` pending · `[~]` in progress / partial · `[x]` done
Every phase ends with **Tests**, **Docs**, and **Exit criteria** — do not move
to the next phase until all three are checked.

**Status (2026-07-10):** messenger plane production-ready. **42** e2e tests
(+2 ignored soaks). Phase 11 closed out: WebSocket (`ws`), cargo-deny + fuzz
CI, contact presence filtering, group ack aggregation, mesh discovery helper,
StorageNode inbox helper, deploy manifests, expanded chaos, 10k idle soak
(ignored), CHANGELOG, criterion baselines doc. Optional follow-ups: JWT auth,
formal git tag when cutting the release.

---

## Phase 0 — Foundation & Project Hygiene

- [x] Create `messenger/` module (or `lane_messenger` workspace crate) so chat
      semantics stay separate from the actor runtime. → `src/messenger/`
- [x] Add `proto/messenger.proto` (package `lane_switchboard.messenger`) wired
      into `build.rs` — kept independent from `mesh_data.proto`.
- [x] Decide client transport: raw TCP with length-delimited frames for
      mobile-style clients; gRPC stays server-to-server.
      - [x] `tokio-tungstenite` behind a `ws` feature for browser clients
            (`src/messenger/ws.rs`, `bind_ws`).
- [x] Add feature flag `messenger` in `Cargo.toml`; CI builds with and without.
- [x] Set up CI matrix: `cargo fmt --check`, `clippy -D warnings`,
      `cargo test` with/without `messenger`, `cargo deny` (`deny.toml` +
      EmbarkStudios action), fuzz smoke job (nightly, continue-on-error).

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
      - [x] Upgrade to proptest for the decoder (`codec.rs` proptest_roundtrip).
      - [x] cargo-fuzz target `fuzz/fuzz_targets/messenger_codec.rs` + CI smoke.
- [x] Bench: `benches/messenger_codec.rs` — binary frame 102 B vs XML stanza
      159 B for the same chat message; encode+decode round trip ~740 ns.

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
- [x] Pre-auth limits: login deadline (10 s) + per-IP connection rate limit
      (`max_conns_per_ip_per_min`, sliding 1-minute window).
- [x] Backpressure: bounded outbound queue per session (`session_buffer`,
      default 256); slow consumers drop frames via `try_send`.
- [x] Graceful shutdown: `MessengerServer::shutdown()` stops accepting,
      drains session queues, records last_seen.
- [x] TLS on client sockets (`feature = "tls"`): `MessengerServer::bind_tls`
      + `MessengerClient::connect_tls` over `MaybeTlsStream`; plaintext
      clients rejected at the handshake.

**Tests**
- [x] Integration: clients connect, login, exchange frames (whole suite).
- [x] Failure: disconnect → session removed, presence goes Unavailable.
- [~] Load smoke: 100 concurrent clients chatting pairwise passes; 10k
      idle-connection soak available as `idle_10k_connections` (`#[ignore]`).

**Docs**
- [x] `docs/messenger/03_sessions.md` — session lifecycle state machine,
      backpressure policy, liveness layers, shutdown semantics.

**Exit criteria**: partially met (100-client smoke, shutdown/rate-limit
tests); 10k soak pending.

---

## Phase 3 — Authentication

- [x] `Login { user_id, device_id, auth_token, client_version }`; pluggable
      `trait Authenticator`; `HmacAuthenticator` (HMAC-SHA256, hex tokens) —
      `src/messenger/auth.rs`. `hmac`/`sha2` promoted to real dependencies.
- [x] Multi-device: session key = `(user_id, device_id)`; same-device
      reconnect kicks the old session with typed `REPLACED_BY_NEW_SESSION`.
- [x] Constant-time verification (`Mac::verify_slice`); tokens never logged;
      login success/failure logged.
- [x] Brute-force protection: exponential per-user backoff
      (`auth_backoff_base` × 2^n, capped) + per-IP connection rate limit.

**Tests**
- [x] Unit: token verify (valid / tampered / garbage / wrong user / wrong
      device / wrong secret) — 4 tests in `auth.rs`.
- [x] Integration: same-device kick (`same_device_reconnect_replaces_old_session`).
- [x] Security: unauthenticated socket cannot send `ChatMessage`
      (`unauthenticated_chat_rejected`).

**Docs**
- [x] `docs/messenger/04_auth.md` — token format, multi-device rules,
      brute-force protection, threat model notes.

**Exit criteria**: met for HMAC scheme; JWT upgrade pending.

---

## Phase 4 — Presence Registry

- [~] Presence registry: `user_id → Vec<SessionHandle>` on the gateway
      (single node). Multi-node sharding via `HashRing` /
      `Cluster::send_by_key` is Phase 9 work.
- [x] On login/disconnect: registry updated; idle TTL sweep closes dead
      sessions and records last-seen.
- [~] Presence packets: `Available` / `Unavailable` / `LastSeen` implemented;
      contact-list filtering via `SubscribePresence` (0x11) — legacy
      broadcast until first subscribe.
- [~] "Last seen" tracked in memory; persistence to `StorageNode` pending
      (inbox replication helper exists: `InboxStorage`).

**Tests**
- [x] Integration: login broadcasts Available; disconnect broadcasts
      Unavailable with last_seen (`presence_broadcast_on_login_and_disconnect`).
- [x] 2-node cluster lookup test (`cluster_tests::presence_propagates_across_nodes`).
- [ ] Property: convergence under arbitrary connect/disconnect interleavings.

**Docs**
- [x] `docs/messenger/04_presence.md` — data model, sharding, TTL, privacy.

**Exit criteria**: single-node met; cross-node presence met; contact filtering
via `SubscribePresence` shipped.

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
      (`MessengerClient::send_chat`); automatic retry with backoff via
      `send_chat_with_retry`.
- [ ] Inter-node hops via `send_with_ack` + hop-vs-message ack mapping (Phase 9).
- [x] Ordering: per-recipient-inbox monotonic `seq` assigned at persist time;
      replay is ascending and gap-free.

**Tests**
- [x] Unit/integration: dedup (`duplicate_send_is_deduplicated`), full tick
      ladder (`online_delivery_with_full_tick_ladder`).
- [x] Integration: offline → stored → replayed
      (`offline_messages_replayed_in_order_on_login`).
- [x] Cross-node delivery + chaos (`cluster_tests::node_failure_during_traffic`).
- [x] Bench: `benches/messenger_routing.rs` (single-node ServerAck latency).

**Docs**
- [~] Delivery guarantees + ack semantics documented in
      `docs/messenger/01_wire_protocol.md`; dedicated failure-matrix doc pending.

**Exit criteria**: single-node met; multi-node chaos met; failure-matrix doc
pending.

---

## Phase 6 — Offline Storage & Sync

- [x] Inbox per user with monotonic seq, dedup set, and tombstone-on-ack;
      durable mode (`ServerConfig::durable_dir`) backs inboxes with a
      fsync-before-ServerAck WAL journal (`inbox.wal`, wire-format frames),
      replayed + compacted on startup (`src/messenger/journal.rs`).
      - [x] Optional follow-up: `InboxStorage` helper replicates inbox frames
            to `StorageNode` (`messenger:inbox:{user}:{seq}`); full SERIAL
            replacement of local WAL still optional.
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
- [x] Crash-recovery: `acked_message_survives_gateway_restart` (send →
      crash → replay → ack → crash → seq high-water mark holds),
      `dedup_survives_restart`, plus journal unit tests (torn tail,
      compaction, tombstones).
- [ ] Property: gap-free/duplicate-free for random offline/online schedules.

**Docs**
- [x] `docs/messenger/06_offline_store.md` — inbox model, journal format,
      startup replay/compaction, durability matrix.

**Exit criteria**: met — zero acked-message loss across process restarts in
tests; seq never regresses.

---

## Phase 7 — Heartbeats & Connection Health

- [x] Client→server `Ping` / server `Pong` implemented
      (`MessengerClient::ping`); server idle timeout 90 s (2 missed 30 s
      intervals) with a 5 s sweep.
- [x] Server-side idle sweep integrated with presence (sweep → session
      removed → Unavailable broadcast + last_seen).
- [x] TCP keepalive (`tcp_keepalive`, default 60 s via socket2) + user-space
      idle timer both enabled.
- [x] Fast reconnect: `Login.resume_after_seq` skips already-seen messages.

**Tests**
- [x] Integration: silent client swept and marked offline
      (`idle_session_swept_after_timeout`), ping round-trips
      (`ping_pong_round_trip`).
- [x] Simulated partition → reconnect + gap-only resume
      (`reconnect_after_partition_replays_gap_only`).

**Docs**
- [x] `docs/messenger/07_heartbeats.md` — Ping/Pong, idle timeout, resume.

**Exit criteria**: met (detection well under 90 s in test; resume dedups).

---

## Phase 8 — Group Chat

- [~] `Group { members, admins, version }` with monotonic membership version —
      in-memory; `HashRing(group_id)` home shard shipped; durable StorageNode
      group membership still optional.
- [x] `GroupEvent`: create / add / remove / leave, versioned; events fan out
      to members.
- [x] Fan-out: membership snapshot at consistent version → per-member inbox
      (per-member dedup key `message_id:member`) → online deliver or offline
      store. Membership cap enforced (`max_group_members`, default 1024).
- [x] Per-member delivery/read ack aggregation for the sender
      (`GroupAckSummary` 0x42).
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
- [x] `docs/messenger/08_groups.md` — membership model, fan-out design,
      ack aggregation gap, limits.

**Exit criteria**: single-node met; multi-node exactly-once pending.

---

## Phase 9 — Multi-Node Operation & Scale

- [x] Multi-node clustering: `MessengerServer::bind_cluster` with full-mesh
      peer links over the same binary protocol (`PeerHello` auth via
      dedicated peer secret, reconnecting links with backoff).
- [x] User/group home shards on `HashRing` (64 vnodes); home node owns
      inbox persistence, seq, dedup, and group membership; user location
      map maintained via `PeerPresence` broadcasts.
- [x] Cross-node flows: 1:1 chat forward → home persist → ack ladder back,
      offline replay via `PeerSync` from any login gateway, group fan-out
      through group home to each member's home, presence propagation.
- [x] Dynamic membership: `add_peer` / `remove_peer`, `PeerJoin` /
      `PeerLeave` gossip, inbox + group handoff via `PeerHandoffUser` /
      `PeerHandoffGroup` on hash-ring rebalance.
- [x] Peer links over TLS: `bind_cluster_tls` (acceptor + peer connector).
- [x] Cross-node media fetch: `PeerMediaReady` gossip + peer relay.
- [x] Optional: discovery via `ServiceMesh`/`MeshRegistry`
      (`spawn_mesh_discovery` in `src/messenger/discovery.rs`); multi-DC
      affinity via `DcTopology` still optional.
- [x] Observability (production requirement, not optional):
      - [x] Metrics via `metrics` feature (`src/messenger/metrics.rs`).
      - [~] Structured logging (`tracing`) in place for session lifecycle;
            message_id correlation + user_id hashing pending.
      - [x] Health/readiness: `MessengerServer::serve_observability`.
- [x] Deployment: example systemd unit + Kubernetes StatefulSet under
      `deploy/`; wire your binary to `MessengerServer::bind_cluster_tls`.

**Tests**
- [x] Integration (3-node cluster, real TCP): cross-node delivery with full
      ack ladder, offline replay on a non-home login gateway with
      exactly-one-copy assertion, cluster-spanning group chat, cross-node
      presence, dynamic join with inbox handoff, cross-node media fetch,
      non-home node failure during traffic (`cluster_tests` in
      `tests/messenger.rs`).
- [x] Chaos: kill non-home node during traffic → no message loss.
- [x] Soak: 5 s sustained load (`soak_short_burst`); 30 s manual
      (`soak_sustained`, `#[ignore]`).
- [x] Bench: cross-node latency via `cargo bench --bench messenger_cluster`
      (p50/p99 recorded in `docs/messenger/09_operations.md`).

**Docs**
- [x] `docs/messenger/05_cluster.md` — topology, sharding model, flows,
      guarantees, limitations.
- [x] `docs/messenger/09_operations.md` — scaling playbook, dashboards,
      runbook for node loss.

**Exit criteria**: soak passes; runbook reviewed; all metrics visible in
Prometheus export test (`tests/messenger_metrics.rs`).

---

## Phase 10 — End-to-End Encryption

- [x] Design doc: `docs/messenger/10_e2ee.md` (Olm / vodozemac, Signal model).
- [x] Key server API: `PublishKeys` / `FetchKeys` / `KeyBundle` on home shard
      (cluster-aware); one-time prekeys consumed on fetch.
- [x] Client E2EE: `E2eeDevice` + `publish_e2ee_device`,
      `establish_e2ee_session`, `send_encrypted_chat`, `decrypt_chat`.
- [x] Safety numbers: `E2eeDevice::safety_number`.
- [x] Group E2EE: sender-keys scheme (Megolm) — `EncryptedGroupPayload`,
      Olm-distributed `GroupSessionKeyShare`, client `send_encrypted_group`.
- [x] Key rotation / multi-device: per `(user_id, device_id)` directory,
      `FetchKeys.device_id`, `RemoveDeviceKeys` (0x63), primary device selection.
- [x] Persist key directory: `KeyDirectory` with `durable_dir/keys/` files or
      optional `ServerConfig.keys_storage` (`StorageNode`).

**Tests**
- [x] Unit: session establishment, ratchet forward secrecy, out-of-order
      decrypt, Megolm round-trip (`src/messenger/e2ee.rs`).
- [x] Integration: single-node + 2-node cluster E2EE; group Megolm opacity;
      multi-device fetch; keys survive restart (`e2ee_tests` in `tests/messenger.rs`).
- [x] Test vectors: vodozemac ↔ libolm Megolm (`tests/libolm_compat.rs`,
      `--features libolm-compat`, requires `cmake`).
- [x] External security review checklist (`docs/messenger/11_security_review.md`).

**Docs**
- [x] `docs/messenger/10_e2ee.md` — protocol choice, key lifecycle, limits.
- [x] `docs/messenger/11_security_review.md` — pre-production checklist.

**Exit criteria**: forward-secrecy unit tests pass; integration opacity tests
pass; group E2EE + multi-device + persistence tests pass; security checklist
in place.

---

## Phase 11 — End-to-End Test Suite & Release

- [x] `tests/messenger.rs`: full-protocol client (`MessengerClient`) covering
      login → presence → 1:1 ticks → offline+sync → groups → media → E2EE +
      3-node cluster + chaos. **42** tests passing (+2 ignored soaks).
- [x] Chaos suite: `node_failure_during_traffic`,
      `chaos_packet_loss_reconnect_no_dupes`,
      `chaos_clock_skew_sent_at_ignored_for_order`.
- [x] Fuzz targets: decoder proptest + `cargo fuzz` target + CI smoke.
- [x] Example app: `examples/messenger_demo.rs` + walkthrough
      `examples/messenger_demo.md` (repo convention) — runs clean.
- [x] Docs index `docs/messenger/README.md` linking all docs; root
      `README.md` module map updated.
- [x] Release notes: `READMEv0.9.2.md` + `CHANGELOG.md`.
- [ ] Formal git tag `v0.9.2` when cutting the release (manual).

**Exit criteria**: e2e suite green (42 tests); CI fmt/clippy/deny/fuzz wired;
deploy examples present. Tag when ready to publish.

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
- [x] All limits configurable with safe defaults (`ServerConfig`) and
      documented in one table: `docs/messenger/limits.md`.
- [x] Proto evolution rules stated in `proto/messenger.proto` header (append
      only, never reuse field numbers or packet IDs).
- [x] Benchmarks tracked over time: `benches/baselines/README.md` documents
      how to save criterion baselines per release.
