# Heartbeats & Connection Health

How clients and gateways keep connections alive, detect failure, and resume
after partition without duplicating messages.

## Two liveness layers

| Layer | Mechanism | Default interval | Detects |
|-------|-----------|------------------|---------|
| Application | `Ping` (0x03) / `Pong` (0x04) | Client ~30 s | Idle sessions, protocol health |
| Transport | TCP keepalive (`socket2`) | 60 s | Dead NAT paths with no traffic |

Server `idle_timeout` is **90 s** (≈ 2 missed 30 s ping intervals + margin).
The server sweep runs every **5 s** and closes stale sessions, which triggers
`UNAVAILABLE` presence (see `04_presence.md`).

## Client responsibilities

1. Send `Ping { seq }` every ~30 s while authenticated.
2. Verify `Pong` echoes the same `seq` (mismatch → protocol error).
3. Reconnect before 90 s if the socket is silent (proactive).
4. On reconnect, send `Login.resume_after_seq` from the local inbox
   high-water mark — never reset to `0` unless the user clears local state.

Reference: `MessengerClient::ping` in `src/messenger/client.rs`.

## Server responsibilities

1. Reply to every `Ping` with matching `Pong`.
2. Track last activity per session; sweep idle > `idle_timeout`.
3. On sweep: remove session, update presence, record `last_seen`.
4. Apply `tcp_keepalive` on accepted client sockets when configured
   (`ServerConfig.tcp_keepalive`, default 60 s).

## Fast reconnect & gap-only resume

After disconnect (partition, app background, server restart):

```text
1. TCP/TLS reconnect
2. Login { resume_after_seq = latest_seq_seen_locally }
3. Server replays inbox messages with seq > resume_after_seq
4. SyncComplete { delivered, latest_seq }
5. Steady state — only the gap was transferred
```

Duplicate `message_id` inserts are idempotent server-side; replays plus live
delivery never create duplicate inbox entries.

Client retry: `send_chat_with_retry` re-sends until `ServerAck` with
exponential backoff (see `src/messenger/client.rs`).

## Partition scenario

| Event | Expected behaviour |
|-------|-------------------|
| Client silent > 90 s | Server closes; `UNAVAILABLE` |
| Network drop mid-session | Client detects on next send/recv; reconnect |
| Server restart | Unacked messages replay from WAL; acked messages not lost |
| Client crash after ServerAck | Message persisted; no resend needed |
| Client crash before ServerAck | Retry same `message_id` on reconnect → dedup safe |

Integration test: `reconnect_after_partition_replays_gap_only` in
`tests/messenger.rs`.

## Configuration

From `docs/messenger/limits.md`:

| Field | Default | Role |
|-------|---------|------|
| `idle_timeout` | 90 s | Server closes silent sessions |
| `tcp_keepalive` | 60 s | OS-level probe |
| `login_deadline` | 10 s | First frame must be `Login` |

## Related

- Wire packets: `01_wire_protocol.md`
- Session state machine: `03_sessions.md`
- Offline inbox / seq cursor: `06_offline_store.md`
