# iOS FFI demo

Build the Rust library, then link from an Xcode app or SwiftPM:

```bash
cargo build -p lane_messenger_ffi --release --target aarch64-apple-ios
# create XCFramework (see docs/client-ffi/BUILD.md)
```

Swift usage (see `bindings/swift/LaneMessengerFFI`):

```swift
let session = try LaneSession(
  host: "127.0.0.1", port: 9000, useTls: false,
  userId: "alice", deviceId: "phone-1", authToken: token
)
try session.ping()
while let ev = session.pollEvent(timeoutMs: 100) {
  print(ev) // JSON event
}
```

Run a local gateway: `cargo run --example messenger_demo --features messenger`.
