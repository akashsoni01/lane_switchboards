# Platform bindings (F7)

```text
bindings/
  swift/LaneMessengerFFI/     # SPM (C ABI) + UniFFI generated/
  android/lane-messenger-ffi/ # Gradle module: JNI + UniFFI/JNA
  flutter/lane_messenger/     # Dart FFI plugin (same .so / XCFramework)
```

## Generate / build

| Artifact | Command |
|----------|---------|
| UniFFI Swift + Kotlin | `./scripts/generate_uniffi_bindings.sh` |
| XCFramework | `./scripts/build_xcframework.sh` |
| Android `.so` (3 ABIs) | `./scripts/build_android_ndk.sh` |

UDL: `lane_messenger_ffi/src/lane_messenger.udl`.

## iOS notes

- Min iOS 15+; ATS requires TLS in production.
- **Production path (recommended):** hand-written C ABI Swift
  (`bindings/swift/LaneMessengerFFI` — `LaneSession` / `LaneE2eeDevice`).
  See [`11_swift.md`](11_swift.md) + [`SWIFT_CHEATSHEET.md`](../../bindings/swift/LaneMessengerFFI/SWIFT_CHEATSHEET.md).
- **UniFFI path:** `generated/` after XCFramework link — **do not** compile with
  the hand-written wrappers in the same target (duplicate `LaneSession`).
- Sample: `examples/ios_ffi_demo/`.

## Android notes

- Min API 24; ship `arm64-v8a`, `armeabi-v7a`, `x86_64`.
- **Production Java JNI path (recommended):** `com.lane.messenger.LaneSession` /
  `LaneE2eeDevice` — full C ABI parity. See [`10_java.md`](10_java.md).
  Build with `FEATURES="c-api,tls,ws,jni" ./scripts/build_android_ndk.sh`.
- **UniFFI path:** `uniffi.lane_messenger` (JNA) + `--features uniffi`.
- ProGuard rules in `bindings/android/lane-messenger-ffi/proguard-rules.pro`.
- Sample: `examples/android_ffi_demo/`.

## Flutter notes

- Dart FFI to the same `.so` / XCFramework — do not reimplement frames.
- Sample: `examples/flutter_ffi_demo/`.
