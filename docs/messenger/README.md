# Messenger Documentation Index

FunXMPP-style (WhatsApp-like) compact binary messaging plane. Start with the
overview, then dive into the layer you care about.

| Doc | Contents |
|-----|----------|
| [00_overview.md](00_overview.md) | Architecture, components, delivery guarantees, scope |
| [01_wire_protocol.md](01_wire_protocol.md) | Frame layout, packet table, connection lifecycle, error codes |
| [02_bulk_data.md](02_bulk_data.md) | Chunked media transfer (PDFs, images) with integrity checks |
| [03_sessions.md](03_sessions.md) | Session state machine, backpressure, liveness, graceful shutdown |
| [04_auth.md](04_auth.md) | Token format, multi-device rules, brute-force protection, threat model |
| [05_cluster.md](05_cluster.md) | Multi-node topology, home shards, peer links, cross-node flows |
| [06_offline_store.md](06_offline_store.md) | Inbox model, durable WAL journal, crash recovery |
| [limits.md](limits.md) | Every configurable limit with defaults and rationale |

Code map:

- `proto/messenger.proto` — packet schemas (append-only field numbers)
- `src/messenger/codec.rs` — binary frame codec
- `src/messenger/auth.rs` — pluggable authentication
- `src/messenger/server.rs` — gateway: sessions, presence, routing, acks, inbox, media, groups
- `src/messenger/client.rs` — reference client (tests + demo)
- `tests/messenger.rs` — 21 end-to-end tests over real TCP
- `benches/messenger_codec.rs` — binary vs XML throughput/size baseline
- `examples/messenger_demo.rs` — runnable golden-path walkthrough

Roadmap and per-phase status: [`todo.md`](../../todo.md) at the repo root.
