# Session lifecycle & events

Hosts must not block the UI thread on `recv`. Prefer **poll** (`poll_event` /
`lane_session_poll_event`) or install a **push** handler via
`SessionHandle::set_event_handler`.

## Connect

```rust
let s = SessionHandle::connect(ConnectOptions {
    host: "127.0.0.1".into(),
    port: 9000,
    use_tls: false,           // production: true (+ system roots)
    user_id: "alice".into(),
    device_id: "phone-1".into(),
    auth_token: token,        // from identity service — never embed HMAC secret
    resume_after_seq: 0,      // from local DB after SyncComplete
    ping_interval_secs: 30,   // 0 = disable auto-ping
    auto_reconnect: true,     // exponential backoff + jitter
    max_reconnect_attempts: 5,
    ..Default::default()
})?;
```

Connect returns after `LoginAck` + offline replay; the FFI emits:

1. `LoginAck`
2. `SyncMessage` (each replayed packet)
3. `SyncComplete` (`latest_seq` — persist this)

## Auto behaviours

| Behaviour | Default | Notes |
|-----------|---------|-------|
| Auto-ping | 30 s | Configurable; `0` off |
| Auto-reconnect | off | Enable with `auto_reconnect`; uses `resume_seq` |
| Same-device kick | emit `ReplacedByNewSession` | **Never** auto-reconnect after this |

Update resume cursor: `set_resume_seq` / `lane_session_set_resume_seq`, or rely
on automatic tracking from inbound chat/group `seq`.

## Event enum

See `todo_client_ffi.md` Phase F2. Peer packets `0x50`–`0x57` are never emitted.

## State machine

Matches [`docs/messenger/03_sessions.md`](../messenger/03_sessions.md):
disconnected → connecting → syncing → ready → (optional reconnect) → closed.
