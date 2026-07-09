# Building lane_messenger_ffi

## Host (dev)

```bash
cargo build -p lane_messenger_ffi --release
cargo test -p lane_messenger_ffi
```

Produces `target/release/liblane_messenger_ffi.{a,dylib,so}` plus the C header
at `lane_messenger_ffi/include/lane_messenger_ffi.h`.

## iOS XCFramework (outline)

```bash
# Requires rustup targets: aarch64-apple-ios, aarch64-apple-ios-sim, x86_64-apple-ios
cargo build -p lane_messenger_ffi --release --target aarch64-apple-ios
# … sim targets, then `xcodebuild -create-xcframework`
```

Full UniFFI Swift module generation is Phase F7 in `todo_client_ffi.md`.

## Android NDK (outline)

```bash
# cargo-ndk or manual: aarch64-linux-android, armv7, x86_64
cargo build -p lane_messenger_ffi --release --target aarch64-linux-android
```

Ship `.so` inside an AAR with the Kotlin UniFFI bindings (F7).

## Env

| Variable | Meaning |
|----------|---------|
| `LANE_MESSENGER_WORKER_THREADS` | Tokio worker threads (default clamp 2–8) |
