# Sessions & Connection Health

## Lifecycle state machine

```text
        TCP accept (per-IP rate limit checked first)
              │
              ▼
       AwaitingLogin ──(no Login within login_deadline)──► closed
              │
        Login frame
              │
     token verify (constant time; exponential backoff on failure)
              │
              ▼
       Authenticated ──(same device relogins)──► REPLACED_BY_NEW_SESSION → closed
              │
       offline replay → SyncComplete
              │
              ▼
        Steady state ◄──── Ping/Pong every ~30 s (client-driven)
              │
   idle > idle_timeout (default 90 s) OR socket EOF OR fatal protocol error
              │
              ▼
           Closed → presence Unavailable + last_seen recorded
```

Each connection is one Tokio task plus a `SessionHandle` in the gateway's
session registry (`user_id → Vec<SessionHandle>`, one per device). The
registry holds the only sender for the session's outbound queue, so removing
the handle — kick, idle sweep, or shutdown — closes the queue and the session
task exits and drops the socket.

## Backpressure

Every session has a bounded outbound queue (`session_buffer`, default 256
frames). Routers use non-blocking sends; a slow consumer's frames are dropped
rather than buffered unboundedly. Dropped chat frames are not lost: the
message stays in the recipient's inbox until `DeliveredAck`, so the client
recovers it on the next sync.

## Liveness: two layers

1. **Application Ping/Pong** — clients ping every ~30 s; the server-side
   sweep (every 5 s) closes sessions idle longer than `idle_timeout`
   (default 90 s ≈ 2 missed intervals + margin).
2. **OS TCP keepalive** (`tcp_keepalive`, default 60 s) — catches dead NAT
   paths below the protocol layer where no frames flow at all.

## Graceful shutdown

`MessengerServer::shutdown()`:

1. Stops the accept loop (no new connections).
2. Clears the session registry — each session task drains frames already
   queued to its socket, then exits.
3. Records `last_seen` for every connected user.

In-flight messages are safe: `ServerAck` is only sent after inbox persist, so
anything acked survives shutdown and replays on the next login.

## Connection admission

- Per-IP rate limit: `max_conns_per_ip_per_min` (default 120) over a sliding
  one-minute window; excess connections are dropped before any frame is read.
- Pre-auth deadline: `login_deadline` (default 10 s) bounds how long an
  unauthenticated socket can occupy a task.
- Optional TLS (`feature = "tls"`): `MessengerServer::bind_tls` performs the
  rustls handshake right after admission; failed handshakes never reach the
  session state machine. Socket options (nodelay, keepalive) are applied to
  the raw TCP stream before wrapping.
