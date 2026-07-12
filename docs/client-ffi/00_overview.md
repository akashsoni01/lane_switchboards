# Client FFI overview

Cross-platform hosts (Swift / Kotlin / Flutter / C) call into
`lane_messenger_ffi`, which wraps `MessengerClient` + `E2eeDevice`.

```text
UI  →  language bindings  →  lane_messenger_ffi  →  TCP/TLS gateway
```

## Design rules

1. Opaque handles only (`SessionHandle` / `LaneSession *`).
2. UTF-8 strings; byte buffers for message bodies.
3. Never block the UI thread on connect/send — the crate owns a Tokio runtime;
   hosts call from a background queue or use poll.
4. Secrets (`auth_token`, private keys) must not be logged.
5. Peer packets `0x50`–`0x57` are not exposed.

## Crates

| Artifact | Path |
|----------|------|
| Rust crate | `lane_messenger_ffi/` |
| C header | `lane_messenger_ffi/include/lane_messenger_ffi.h` |
| Plan | [`todo_client_ffi.md`](../../todo_client_ffi.md) |

## Quick start (Rust)

```rust
use lane_messenger_ffi::{ConnectOptions, SessionHandle, LaneEvent};

let s = SessionHandle::connect(ConnectOptions {
    host: "127.0.0.1".into(),
    port: 9000,
    user_id: "alice".into(),
    device_id: "d1".into(),
    auth_token: token,
    ping_interval_secs: 30,
    ..Default::default()
})?;
while let Some(ev) = s.poll_event(100) {
    if matches!(ev, LaneEvent::SyncComplete(_)) { break; }
}
s.ping()?;
```

See [BUILD.md](BUILD.md) for XCFramework / AAR notes.  
Java / Android JNI (production): [10_java.md](10_java.md).  
Swift C ABI (production): [11_swift.md](11_swift.md) ·
[cheatsheet](../../bindings/swift/LaneMessengerFFI/SWIFT_CHEATSHEET.md).
