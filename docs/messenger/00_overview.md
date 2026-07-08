# Messenger Overview

A FunXMPP-style (WhatsApp-like) messaging plane built on the `lane_switchboards`
runtime. Clients speak a compact binary protocol over persistent TCP
connections instead of verbose XML XMPP.

## Architecture

```text
Clients (TCP, length-prefixed binary frames)
   │
   ▼
Gateway (accept loop)  ──►  Session task per connection
   │                              │
   ▼                              ▼
Presence registry  ◄──────  Message router
   │                              │
   ▼                        ┌─────┴─────┐
Online delivery             ▼           ▼
                       Offline inbox  Media store (bulk data: PDFs etc.)
```

## Components

| Component | Where | Role |
|-----------|-------|------|
| Wire codec | `src/messenger/codec.rs` | `u8 ver \| u8 type \| u32 len \| protobuf` framing |
| Packet schema | `proto/messenger.proto` | All packet payloads (prost-generated) |
| Auth | `src/messenger/auth.rs` | Pluggable `Authenticator`; HMAC-SHA256 default |
| Gateway/server | `src/messenger/server.rs` | Sessions, presence, routing, acks, offline store, media, groups |
| Reference client | `src/messenger/client.rs` | Full-protocol client used by tests and the demo |

## Guarantees

- **Durability before ack**: `ServerAck` is sent only after the message is
  stored in the recipient's inbox.
- **At-least-once + dedup**: clients may retry sends; the server dedups by
  `message_id` per recipient inbox, so the effect is exactly-once.
- **Ordering**: the recipient's inbox assigns a monotonic `seq`; replay on
  login is gap-free and ascending.
- **Ack ladder**: `ServerAck` (single tick) → `DeliveredAck` (double tick) →
  `ReadAck` (blue tick).

## Scope and non-goals (current milestone)

Single-node gateway with in-memory state. Multi-node routing (hash-ring user
shards over `Cluster`/`ServiceMesh`), durable storage via `StorageNode`, and
E2EE are the next phases — see `todo.md` phases 9–10.

## Run the demo

```bash
cargo run --example messenger_demo
cargo test --test messenger
```
