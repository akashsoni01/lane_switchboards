# LaneMessengerFFI (Swift)

Production **Swift** bindings over the Rust C ABI (`lane_messenger_ffi.h`).
Same wire / E2EE engine as Android Java JNI — **no Swift reimplementation** of
FunXMPP or Olm/Megolm.

```text
SwiftUI / LaneMessengerKit
  → LaneMessengerFFI.LaneSession / LaneE2eeDevice   (this package)
  → LaneMessengerC (shim → lane_messenger_ffi.h)
  → liblane_messenger_ffi (Rust MessengerClient + E2eeDevice)
  → TCP/TLS FunXMPP → gateway
```

## Conflict rules (read this)

| Path | Use? |
|------|------|
| `LaneSession.swift` + `LaneE2ee.swift` (hand-written) | **Yes** — default SPM product |
| `generated/lane_messenger.swift` (UniFFI) | **Separate** optional path only |
| Both in one target | **No** — duplicate `LaneSession` / `LaneE2eeDevice` |

`Package.swift` **excludes** `generated/` so UniFFI cannot collide with the C ABI
wrappers or with Rust symbol expectations.

## Build Rust library

```bash
# Host (macOS smoke)
cargo build -p lane_messenger_ffi --release

# iOS XCFramework
./scripts/build_xcframework.sh
# → dist/LaneMessengerFFI.xcframework
```

## Swift package

```bash
cd bindings/swift/LaneMessengerFFI
swift build
```

Linking the `.dylib` / `.a` is the **app** or Xcode target’s job (see cheatsheet).

## Kit without native lib

`apps/ios` uses `MockMessengerTransport` when `LaneMessengerFFI` is not linked:

```bash
cd apps/ios && swift run lane-messenger-kit-smoke
```

## Docs

- Cheatsheet: [`SWIFT_CHEATSHEET.md`](SWIFT_CHEATSHEET.md)
- Client FFI: [`docs/client-ffi/11_swift.md`](../../../docs/client-ffi/11_swift.md)
- Java twin: [`docs/client-ffi/10_java.md`](../../../docs/client-ffi/10_java.md)
- Ecosystem: [`docs/ECOSYSTEM_GUIDE.md`](../../../docs/ECOSYSTEM_GUIDE.md)
