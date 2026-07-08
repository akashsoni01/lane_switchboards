# Wire Protocol

Compact binary framing, inspired by WhatsApp's FunXMPP: XMPP concepts without
XML overhead.

## Frame layout

All integers are network byte order (big endian).

```text
+---------+--------------+-------------+------------------+
| u8 ver  | u8 pkt_type  | u32 length  | protobuf payload |
+---------+--------------+-------------+------------------+
   1 byte     1 byte        4 bytes        `length` bytes
```

- `ver` — protocol version, currently `1`. Mismatch → `UNSUPPORTED_VERSION`
  error and the connection closes.
- `pkt_type` — one of the identifiers below; unknown types fail fast before
  the payload is read.
- `length` — payload bytes only. Frames above the configured maximum
  (default 256 KiB) are rejected before any allocation.

Comparison: the XML stanza `<message to="user2"><body>Hello</body></message>`
is 52 bytes of markup for 5 bytes of content. The equivalent binary frame is
6 bytes of header plus a ~30-byte protobuf payload including ids/timestamps.

## Packet types

| ID | Packet | Direction | Purpose |
|------|----------------|-----------|---------|
| 0x01 | `Login` | C→S | Authenticate; must be first frame |
| 0x02 | `LoginAck` | S→C | Session id + pending message count |
| 0x03 | `Ping` | C→S | Heartbeat |
| 0x04 | `Pong` | S→C | Heartbeat reply |
| 0x0F | `ProtocolError` | S→C | Typed fatal error, then close |
| 0x10 | `Presence` | both | Available / Unavailable / LastSeen |
| 0x20 | `ChatMessage` | both | 1:1 message (body opaque bytes) |
| 0x21 | `ServerAck` | S→C | Message persisted (single tick) |
| 0x22 | `DeliveredAck` | both | Device received (double tick) |
| 0x23 | `ReadAck` | both | Read (blue tick) |
| 0x24 | `SyncComplete` | S→C | Offline replay finished |
| 0x30 | `MediaStart` | both | Begin blob upload / fetch metadata |
| 0x31 | `MediaChunk` | both | One ≤64 KiB blob chunk |
| 0x32 | `MediaAck` | S→C | Chunk credit / final verification |
| 0x33 | `MediaFetch` | C→S | Request stored blob (resumable) |
| 0x40 | `GroupMessage` | both | Group chat message |
| 0x41 | `GroupEvent` | both | Create / add / remove / leave |

Payload schemas: `proto/messenger.proto`. Field numbers and packet IDs are
append-only — never reuse or renumber.

## Connection lifecycle

```text
connect ──► Login ──► LoginAck ──► [offline replay…] ──► SyncComplete ──► steady state
                                                                    │
                                          Ping/Pong every 30 s ◄────┤
                                          idle > 90 s → closed ◄────┘
```

1. **AwaitingLogin** — the first frame must be `Login` within 10 s
   (`ServerConfig::login_deadline`), otherwise `NOT_AUTHENTICATED` + close.
2. **Replay** — pending inbox messages with `seq > resume_after_seq` are
   streamed in order, ending with `SyncComplete`.
3. **Steady state** — full duplex; server pushes chat/presence/acks at any
   time.
4. Same-device relogin kicks the previous session with
   `REPLACED_BY_NEW_SESSION`.

## Error codes

`ProtocolError.code` values: `UNSUPPORTED_VERSION`, `NOT_AUTHENTICATED`,
`AUTH_FAILED`, `REPLACED_BY_NEW_SESSION`, `FRAME_TOO_LARGE`,
`MALFORMED_FRAME`, `RATE_LIMITED`, `UNKNOWN_RECIPIENT`,
`MEDIA_TRANSFER_FAILED`. All are fatal to the connection except media errors,
which fail only the transfer.

## Delivery semantics

- Sender retries until `ServerAck`; the server dedups by `message_id`, so
  retries are safe. A duplicate is re-acked with `seq = 0`.
- `seq` is per-recipient-inbox and monotonic; it is the resume cursor for
  `Login.resume_after_seq`.
- Inbox entries are tombstoned on `DeliveredAck` and dropped once the head of
  the queue is contiguous-delivered.
