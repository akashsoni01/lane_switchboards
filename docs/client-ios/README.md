# Build & run (iOS client)

## Prerequisites

- Xcode 15+ / iOS 17 SDK
- Local gateway for real FFI:

```bash
cargo run --example messenger_demo --features messenger
# Note the printed listen address; default app expects host:port via Info.plist
```

- Optional XCFramework:

```bash
./scripts/build_xcframework.sh
```

## Kit unit tests (no Xcode UI)

```bash
cd apps/ios
swift run lane-messenger-kit-smoke
# With full Xcode installed:
# swift test
```

Uses `MockMessengerTransport` — does not need the native library.

## Open the app in Xcode

1. `cd apps/ios && swift package generate-xcodeproj` **or** open Package.swift
   and add an iOS App target that depends on `LaneMessengerKit`, **or**
2. Install [XcodeGen](https://github.com/yonaskolb/XcodeGen) and run:

```bash
cd apps/ios
xcodegen generate
open LaneMessenger.xcodeproj
```

3. Link `LaneMessengerFFI` / XCFramework for device builds (see
   [`docs/client-ffi/BUILD.md`](../client-ffi/BUILD.md)). Without it, the app
   uses `MockMessengerTransport` so Login → Home still works for UI work.

## DEBUG login

- User ID: e.g. `alice`
- Secret: leave empty to mint `hex(HMAC-SHA256(demo-secret, "user_id:device_id"))`
  (same as `messenger_demo`), **or** paste a pre-minted hex token.
- Gateway host/port editable on the login form (DEBUG only).

Release builds use `HttpAuthService` (inject a real identity URL) and never
embed the HMAC secret.

## Env / Info.plist

| Key | Meaning |
|-----|---------|
| `MESSENGER_HOST` | Gateway host (default `127.0.0.1`) |
| `MESSENGER_PORT` | Gateway port (default `9000`) |
| `MESSENGER_USE_TLS` | `YES` in Release xcconfig |
