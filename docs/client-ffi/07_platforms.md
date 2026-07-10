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
- Prefer UniFFI-generated Swift after linking the XCFramework; C ABI wrapper
  remains for hosts that skip UniFFI.
- Sample: `examples/ios_ffi_demo/`.

## Android notes

- Min API 24; ship `arm64-v8a`, `armeabi-v7a`, `x86_64`.
- **JNI path:** `com.lane.messenger.LaneSession` + `--features jni`.
- **UniFFI path:** `uniffi.lane_messenger` (JNA) + `--features uniffi`.
- ProGuard rules in `bindings/android/lane-messenger-ffi/proguard-rules.pro`.
- Sample: `examples/android_ffi_demo/`.

## Flutter notes

- Dart FFI to the same `.so` / XCFramework — do not reimplement frames.
- Sample: `examples/flutter_ffi_demo/`.
