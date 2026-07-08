# Configurable Limits (`ServerConfig`)

All limits live in one struct (`src/messenger/server.rs`) with safe defaults.
Every value is per-gateway-node.

| Field | Default | Protects against | Notes |
|-------|---------|------------------|-------|
| `max_frame` | 256 KiB | memory exhaustion via giant frames | Rejected from the 6-byte header before any allocation. Media chunks are 64 KiB, so this leaves headroom. |
| `login_deadline` | 10 s | unauthenticated sockets squatting | Connection closed with `NOT_AUTHENTICATED`. |
| `idle_timeout` | 90 s | dead/ghost connections | ≈ 2 missed 30 s ping intervals; sweep runs every 5 s. |
| `session_buffer` | 256 frames | unbounded buffering for slow clients | Overflow frames are dropped; chat recovers via inbox replay. |
| `max_inbox` | 10 000 msgs | offline-inbox quota abuse | Oldest-drop policy. |
| `max_media_bytes` | 64 MiB | storage exhaustion via blobs | Enforced at `MediaStart` and re-checked per chunk. |
| `max_group_members` | 1024 | fan-out amplification | `AddMember` rejected when full. |
| `max_conns_per_ip_per_min` | 120 | connection floods / auth-backoff dodging | Sliding 1-minute window; `0` disables. |
| `auth_backoff_base` | 250 ms | credential brute force | Doubles per consecutive failure for the same user. |
| `auth_backoff_max` | 30 s | unbounded backoff | Cap for the exponential delay. |
| `tcp_keepalive` | 60 s | dead NAT paths | OS-level; `None` disables. App-level Ping/Pong still applies. |
| `durable_dir` | `None` | message loss on crash | `Some(dir)` enables the fsynced inbox journal (`dir/inbox.wal`); see `06_offline_store.md`. |

Protocol-level constants (in `src/messenger/codec.rs`):

| Constant | Value | Meaning |
|----------|-------|---------|
| `PROTOCOL_VERSION` | 1 | Frame version byte; mismatch is fatal |
| `HEADER_LEN` | 6 bytes | version + type + length prefix |
| Media chunk size | 64 KiB | Client-side chunking granularity |
