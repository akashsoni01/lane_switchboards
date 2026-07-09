# lane_switchboards v0.9.2

Release notes for **v0.9.2** — FunXMPP-style messenger plane reaches production
readiness: multi-node clustering, durable inboxes, media transfer, full E2EE
(Olm + Megolm), and 38 end-to-end integration tests.

For the full project overview see [README.md](./README.md).  
Previous release: [v0.9.0](./READMEv0.9.0.md) · Messenger docs: [docs/messenger/README.md](./docs/messenger/README.md)

---

## What's new in v0.9.2

### Summary

| Area | What landed |
|------|-------------|
| **Messenger wire protocol** | Compact binary framing (v1), 20+ client packet types, prost payloads |
| **Gateway & sessions** | TCP accept loop, login deadline, rate limits, backpressure, graceful shutdown |
| **Auth** | HMAC-SHA256 tokens, multi-device, same-device kick, exponential backoff |
| **Routing & acks** | WhatsApp-style tick ladder: ServerAck → DeliveredAck → ReadAck |
| **Offline store** | Per-user inbox, `resume_after_seq`, WAL journal (`durable_dir`) |
| **Groups** | Versioned membership, fan-out, admin authz, offline member sync |
| **Media** | Chunked upload/download (64 KiB chunks, SHA-256, 64 MiB cap) |
| **Cluster** | Hash-ring home shards, peer mesh, cross-node chat/groups/presence/media |
| **E2EE** | vodozemac Olm 1:1 + Megolm group sender-keys, multi-device key directory |
| **Observability** | `/health`, `/ready`, `/metrics` (Prometheus, `metrics` feature) |
| **Client API** | `MessengerClient` reference impl + `send_chat_with_retry` |
| **Tests** | 38 e2e tests (`tests/messenger.rs`), proptest codec, libolm-compat feature |

---

## Messenger architecture

```text
Clients (TCP/TLS, length-prefixed binary frames)
   │
   ▼
Gateway (MessengerServer) ──► Session per connection
   │                              │
   ▼                              ▼
Presence registry          Message router
   │                              │
   ▼                        ┌─────┴─────┐
Online delivery             ▼           ▼
                       Offline inbox  Media store
```

Cluster mode adds a full-mesh peer link between gateways; user/group **home
shards** own inboxes and membership. Clients are unaware of topology.

---

## Key APIs

### Server

```rust
use lane_switchboards::messenger::{MessengerServer, ServerConfig, HmacAuthenticator};

let server = MessengerServer::bind("0.0.0.0:9000", Arc::new(auth), ServerConfig::default()).await?;
// Cluster: MessengerServer::bind_cluster(...)
// TLS:    MessengerServer::bind_tls(...)  // feature = "tls"
```

### Client

```rust
use lane_switchboards::messenger::{MessengerClient, E2eeDevice};

let (mut client, outcome) = MessengerClient::connect(addr, user, device, token, resume_seq).await?;
client.send_chat_with_retry("bob", "m-1", b"hello", 3).await?;
```

### E2EE

```rust
let mut e2ee = E2eeDevice::generate();
client.publish_e2ee_device("d1", &mut e2ee, 10).await?;
client.establish_e2ee_session("bob", &mut e2ee).await?;
client.send_encrypted_chat(&mut e2ee, "bob", "m-1", b"secret").await?;
```

---

## Feature flags

| Feature | Default | Purpose |
|---------|---------|---------|
| `messenger` | yes | Messenger module + vodozemac E2EE |
| `tls` | no | TLS on client sockets + peer cluster links |
| `metrics` | no | Prometheus export for gateway |
| `libolm-compat` | no | vodozemac ↔ libolm cross-validation tests (needs cmake) |

Build without messenger: `cargo build --no-default-features`

---

## Test matrix

```bash
cargo test --test messenger              # 38 integration tests
cargo test --features tls --test messenger  # +2 TLS cluster tests
cargo test --features metrics --test messenger_metrics
cargo test --features libolm-compat --test libolm_compat  # optional
cargo bench --bench messenger_codec
cargo bench --bench messenger_cluster
cargo bench --bench messenger_routing
```

---

## Documentation

| Doc | Topic |
|-----|-------|
| [00_overview.md](docs/messenger/00_overview.md) | Architecture |
| [01_wire_protocol.md](docs/messenger/01_wire_protocol.md) | Frames, packets, errors |
| [04_presence.md](docs/messenger/04_presence.md) | Presence registry |
| [07_heartbeats.md](docs/messenger/07_heartbeats.md) | Ping/Pong, reconnect |
| [08_groups.md](docs/messenger/08_groups.md) | Group chat |
| [10_e2ee.md](docs/messenger/10_e2ee.md) | Olm + Megolm |
| [11_security_review.md](docs/messenger/11_security_review.md) | Pre-production checklist |

Roadmap: [todo.md](todo.md) · iOS client plan: [todo_client.md](todo_client.md)

---

## Known limitations (v0.9.2)

- Presence broadcasts to all online users (no contact-list filter yet)
- Group per-member read-receipt aggregation not implemented
- HMAC tokens do not expire (JWT planned)
- WebSocket transport not implemented (`ws` feature pending)
- ServiceMesh-based peer discovery optional / not wired

---

## Upgrade from v0.9.0

No breaking changes to storage or actor APIs. Messenger is a new module behind
the default `messenger` feature. Disable with `--no-default-features` if you
only need the storage/actor stack.

---

## Contributors / next

Phase 11 (release hardening): CI fmt/clippy, chaos suite expansion, criterion
baselines. iOS Swift client: see `todo_client.md`.
