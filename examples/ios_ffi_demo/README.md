# iOS FFI demo

Minimal Swift driver that connects, waits for `LoginAck` / `SyncComplete`,
sends a chat, and prints polled JSON events.

## Prerequisites

1. Local gateway: `cargo run --example messenger_demo --features messenger`
2. Built library / XCFramework:

```bash
# Device staticlib
cargo build -p lane_messenger_ffi --release --target aarch64-apple-ios --features uniffi

# Or full XCFramework (macOS + Xcode):
./scripts/build_xcframework.sh

# Regenerate UniFFI Swift (optional; checked-in under generated/):
./scripts/generate_uniffi_bindings.sh
```

## C ABI path (hand wrapper)

See `bindings/swift/LaneMessengerFFI/Sources/LaneMessengerFFI/LaneSession.swift`
and `DemoMain.swift` in this folder (copy into an Xcode app target that links
`liblane_messenger_ffi.a` / the XCFramework).

```swift
let session = try LaneSession(
  host: "127.0.0.1", port: 9000, useTls: false,
  userId: "alice", deviceId: "phone-1", authToken: token
)
try session.ping()
while let ev = session.pollEvent(timeoutMs: 100) {
  print(ev)
}
```

## UniFFI path

After `./scripts/generate_uniffi_bindings.sh`, import the generated module
(`lane_messenger.swift` + `lane_messengerFFI.h` from the XCFramework):

```swift
import Foundation
// Generated: LaneSession / ConnectConfig / LaneE2ee

let cfg = ConnectConfig(
  host: "127.0.0.1", port: 9000, useTls: false,
  userId: "alice", deviceId: "phone-1", authToken: token,
  clientVersion: "ios-demo", resumeAfterSeq: 0,
  pingIntervalSecs: 30, autoReconnect: false,
  maxReconnectAttempts: 0, useWebsocket: false, wsUrl: ""
)
let session = try LaneSession(config: cfg)
try session.ping()
if let json = session.pollEventJson(timeoutMs: 500) {
  print(json)
}
```

ATS: production builds must use TLS (`useTls: true`). Staging custom CAs go
through `ca_pem_path` on the Rust `ConnectOptions` (C/UniFFI surface TBD).
