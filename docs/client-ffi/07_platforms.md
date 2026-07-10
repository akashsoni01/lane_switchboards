# Platform bindings (F7)

Native packages are **not** fully generated yet. Until UniFFI CI is wired:

1. Link `liblane_messenger_ffi` (cdylib/staticlib) from
   `cargo build -p lane_messenger_ffi --release`.
2. Use the C header
   [`lane_messenger_ffi/include/lane_messenger_ffi.h`](../../lane_messenger_ffi/include/lane_messenger_ffi.h).
3. Or call the Rust API from a thin host wrapper.

## Planned layout

```text
bindings/
  swift/LaneMessengerFFI/     # SPM + XCFramework
  android/lane-messenger-ffi/ # AAR
  flutter/lane_messenger/     # plugin
```

UDL scaffold: `lane_messenger_ffi/uniffi/lane_messenger.udl`.

## iOS notes

- Min iOS 15+; ATS requires TLS in production.
- Custom CA for staging: pass via future connect option / pin set (F9).

## Android notes

- Min API 24; ship `arm64-v8a`, `armeabi-v7a`, `x86_64`.
- Keep ProGuard rules for JNI once UniFFI Kotlin is generated.

## Flutter notes

- Prefer Dart FFI to the same `.so` / XCFramework — do not reimplement frames.
