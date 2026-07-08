# Messenger Demo

Runs a full FunXMPP-style (WhatsApp-like) flow against an in-process gateway
over real TCP sockets using the compact binary protocol.

```bash
cargo run --example messenger_demo
```

What it demonstrates, in order:

1. **Gateway boot** — `MessengerServer::bind` with HMAC token auth.
2. **Login + heartbeat** — Alice authenticates and does a `Ping`/`Pong`
   round trip.
3. **Offline delivery** — Alice messages Bob while he is offline; the message
   is persisted (single tick `ServerAck`) into Bob's inbox.
4. **Sync on login** — Bob connects and the pending message replays in order,
   ending with `SyncComplete`.
5. **Ack ladder** — Bob sends `DeliveredAck` (double tick) and `ReadAck`
   (blue tick), both relayed to Alice.
6. **Bulk data (PDF)** — Alice uploads a 200 KiB blob in 64 KiB chunks with
   SHA-256 verification, references it from a chat message, and Bob downloads
   and re-verifies it.
7. **Group chat** — Alice creates a group, adds Bob and Carol, and one send
   fans out to every member.

Related docs:

- `docs/messenger/00_overview.md` — architecture
- `docs/messenger/01_wire_protocol.md` — frame layout and packet table
- `docs/messenger/02_bulk_data.md` — chunked media transfer

Integration tests covering the same paths plus failure cases (bad tokens,
duplicate sends, idle sweep, sha mismatch, group authorization):
`tests/messenger.rs`.
