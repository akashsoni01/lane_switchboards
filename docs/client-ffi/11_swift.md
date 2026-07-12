# Swift bindings (C ABI)

Hand-written Swift over `lane_messenger_ffi.h`. Default for `apps/ios` via
`LaneFFITransport` when the XCFramework / library is linked.

## Layout

```text
bindings/swift/LaneMessengerFFI/
  Package.swift                 # excludes UniFFI generated/ (avoids LaneSession clash)
  README.md
  SWIFT_CHEATSHEET.md
  Sources/LaneMessengerC/       # modulemap → lane_messenger_ffi.h
  Sources/LaneMessengerFFI/
    LaneSession.swift
    LaneE2ee.swift
    generated/                  # UniFFI only — NOT in default target
```

## Conflict with Rust / UniFFI

| Risk | Mitigation |
|------|------------|
| Two Swift types named `LaneSession` | `generated/` excluded from SPM target |
| Duplicate `@_silgen_name` vs C header | Wrappers `import LaneMessengerC` only — no silgen shims |
| Kit without native lib | `MockMessengerTransport` / `#if canImport(LaneMessengerFFI)` |
| Reimplementing codec in Swift | Forbidden — see `todo_client.md` superseded by FFI |

## API parity (C header)

Session: connect, free, close, ping, set_resume_seq, poll_event, send_chat,
send_chat_with_media, send_chat_retry, acks, presence subscribe/send, groups,
media upload/fetch, version helpers.

E2EE: generate, import/export pickle, publish, encrypt/decrypt chat & group,
distribute group key, try_import_group_key, safety number.

## Build

```bash
cargo build -p lane_messenger_ffi --release
cd bindings/swift/LaneMessengerFFI && swift build
./scripts/build_xcframework.sh
cd apps/ios && swift run lane-messenger-kit-smoke
```

Cheatsheet: [`../../bindings/swift/LaneMessengerFFI/SWIFT_CHEATSHEET.md`](../../bindings/swift/LaneMessengerFFI/SWIFT_CHEATSHEET.md).
