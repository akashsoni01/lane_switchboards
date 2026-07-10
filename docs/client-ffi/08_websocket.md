# WebSocket transport (F8)

Server: `feature = "ws"`, `bind_ws`. Each WS **binary** message = one full
messenger frame.

## Connect

```rust
use lane_messenger_ffi::{ConnectOptions, SessionHandle, Transport};

let s = SessionHandle::connect(ConnectOptions {
    host: "127.0.0.1".into(),
    port: 9001,
    transport: Transport::WebSocket,
    ws_url: "ws://127.0.0.1:9001/".into(), // or leave empty to auto-build
    use_tls: false, // wss when true
    user_id: "alice".into(),
    device_id: "web-1".into(),
    auth_token: token,
    ..Default::default()
})?;
```

Prefer **WSS** in production. Mobile apps usually keep TCP/TLS; WS is for
browser-adjacent hosts and Flutter web later.

Enable crate feature: `lane_messenger_ffi` / `lane_switchboards` `ws`.
