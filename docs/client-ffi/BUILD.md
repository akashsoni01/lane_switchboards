# Building lane_messenger_ffi

## Host (dev)

```bash
cargo build -p lane_messenger_ffi --release
cargo test -p lane_messenger_ffi

# UniFFI + JNI glue
cargo test -p lane_messenger_ffi --features uniffi,jni
```

Produces `target/release/liblane_messenger_ffi.{a,dylib,so}` plus the C header
at `lane_messenger_ffi/include/lane_messenger_ffi.h`.

## UniFFI codegen

UDL: `lane_messenger_ffi/src/lane_messenger.udl`

```bash
./scripts/generate_uniffi_bindings.sh
```

Writes:

- Swift → `bindings/swift/LaneMessengerFFI/Sources/LaneMessengerFFI/generated/`
- Kotlin (JNA) → `bindings/android/lane-messenger-ffi/src/main/java/uniffi/lane_messenger/`

Hand-written C ABI wrappers remain available:

- Swift: `LaneSession.swift` (links C header)
- Kotlin JNI: `com.lane.messenger.LaneSession` (`--features jni`)

## iOS XCFramework

```bash
# Requires Xcode + rustup targets aarch64-apple-ios{,-sim}, x86_64-apple-ios
./scripts/build_xcframework.sh
# → dist/LaneMessengerFFI.xcframework
```

CI job: `ffi-ios-xcframework` (macOS).

## Android NDK / AAR inputs

```bash
export ANDROID_NDK_HOME=…
./scripts/build_android_ndk.sh
# → bindings/android/lane-messenger-ffi/src/main/jniLibs/<abi>/liblane_messenger_ffi.so
```

Package the module `bindings/android/lane-messenger-ffi/` as an AAR (Gradle).
CI job: `ffi-android-ndk`.

## Env

| Variable | Meaning |
|----------|---------|
| `LANE_MESSENGER_WORKER_THREADS` | Tokio worker threads (default clamp 2–8) |
| `FEATURES` | Override Cargo features for the build scripts |
| `ANDROID_NDK_HOME` | Required by `scripts/build_android_ndk.sh` |
