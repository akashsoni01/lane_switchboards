# Android FFI demo

Connect → poll events → send chat (production **Java** JNI API).

## Build native library

```bash
# Requires ANDROID_NDK_HOME + cargo-ndk (jni is default)
./scripts/build_android_ndk.sh
# → bindings/android/lane-messenger-ffi/src/main/jniLibs/<abi>/liblane_messenger_ffi.so
```

Docs: [`docs/client-ffi/10_java.md`](../../docs/client-ffi/10_java.md).

## Java JNI path (`com.lane.messenger`)

```kotlin
val opts = ConnectOptions.builder()
  .host("10.0.2.2") // emulator → host loopback
  .port(9000)
  .useTls(false)
  .userId("alice")
  .deviceId("pixel-1")
  .authToken(token)
  .build()
LaneSession.connect(opts).use { session ->
  session.ping()
  println(session.pollEvent(500))
  val seq = session.sendChat("bob", "m1", "hello".toByteArray())
}
```

Optional E2EE: set env `LANE_E2EE=1` in the demo to publish keys.

See `DemoActivity.kt` and module `bindings/android/lane-messenger-ffi/`.

## UniFFI / JNA path (`uniffi.lane_messenger`)

Generated Kotlin lives at
`bindings/android/lane-messenger-ffi/src/main/java/uniffi/lane_messenger/`.
Build the `.so` with `--features uniffi` (and ship JNA AAR dependency from
`build.gradle.kts`).

Run a local gateway: `cargo run --example messenger_demo --features messenger`.
