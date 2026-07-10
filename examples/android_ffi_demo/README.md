# Android FFI demo

Connect → poll `LoginAck` / `SyncComplete` → send chat → print events.

## Build native library

```bash
# Requires ANDROID_NDK_HOME + cargo-ndk
./scripts/build_android_ndk.sh
# → bindings/android/lane-messenger-ffi/src/main/jniLibs/<abi>/liblane_messenger_ffi.so
```

Optional UniFFI Kotlin (JNA) regeneration:

```bash
./scripts/generate_uniffi_bindings.sh
```

## JNI path (`com.lane.messenger.LaneSession`)

Requires `--features jni` when building the `.so` (default in
`scripts/build_android_ndk.sh`).

```kotlin
val session = LaneSession.connect(
  host = "10.0.2.2", // emulator → host loopback
  port = 9000,
  useTls = false,
  userId = "alice",
  deviceId = "pixel-1",
  authToken = token
)
session.ping()
println(session.pollEvent(500))
val seq = session.sendChat("bob", "m1", "hello".toByteArray())
session.close()
session.free()
```

See `DemoActivity.kt` in this folder and the library module
`bindings/android/lane-messenger-ffi/`.

## UniFFI / JNA path (`uniffi.lane_messenger`)

Generated Kotlin lives at
`bindings/android/lane-messenger-ffi/src/main/java/uniffi/lane_messenger/`.
Build the `.so` with `--features uniffi` (and ship JNA AAR dependency from
`build.gradle.kts`).

```kotlin
val cfg = ConnectConfig(
  host = "10.0.2.2", port = 9000u, useTls = false,
  userId = "alice", deviceId = "pixel-1", authToken = token,
  clientVersion = "android-demo", resumeAfterSeq = 0u,
  pingIntervalSecs = 30u, autoReconnect = false,
  maxReconnectAttempts = 0u, useWebsocket = false, wsUrl = ""
)
val session = LaneSession(cfg)
session.ping()
println(session.pollEventJson(500u))
```

Run a local gateway: `cargo run --example messenger_demo --features messenger`.
