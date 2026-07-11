# lane_messenger_ffi

Stable FFI for the FunXMPP messenger client used by Swift, Android, and Flutter.

All networking, framing, acks, media chunking, and E2EE stay in Rust. Hosts
hold opaque session / device handles and poll (or later: callback) events.

## Build

```bash
cargo build -p lane_messenger_ffi --release
cargo test -p lane_messenger_ffi
cargo test -p lane_messenger_ffi --features uniffi,jni
```

Features: `c-api` (default), `tls` (default), `ws` (default), optional `uniffi`, optional `jni`.

| Feature | Purpose |
|---------|---------|
| `uniffi` | UDL scaffolding + `uniffi-bindgen` (Swift/Kotlin) |
| `jni` | `Java_com_lane_messenger_*` glue — full session + E2EE JNI |

Platform packages / scripts: see [`docs/client-ffi/BUILD.md`](../docs/client-ffi/BUILD.md).
Java production API: [`docs/client-ffi/10_java.md`](../docs/client-ffi/10_java.md).

Env: `LANE_MESSENGER_WORKER_THREADS` — Tokio worker count (default 2–8).

## Rust API

```rust
use lane_messenger_ffi::{ConnectOptions, SessionHandle, LaneEvent};

let session = SessionHandle::connect(ConnectOptions {
    host: "127.0.0.1".into(),
    port: 9000,
    use_tls: false,
    user_id: "alice".into(),
    device_id: "phone-1".into(),
    auth_token: token,
    ..Default::default()
})?;

while let Some(ev) = session.poll_event(100) {
    match ev {
        LaneEvent::SyncComplete(_) => break,
        _ => {}
    }
}
session.ping()?;
session.send_chat("bob", "m-1", b"hi")?;
```

## C API

See `include/lane_messenger_ffi.h` and `docs/client-ffi/`.

## Docs

- [Overview](../docs/client-ffi/00_overview.md)
- Plan: [`todo_client_ffi.md`](../todo_client_ffi.md)
