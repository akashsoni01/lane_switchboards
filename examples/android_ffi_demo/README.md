# Android FFI demo

```bash
# Build .so for device ABI (requires NDK + cargo-ndk)
cargo ndk -t arm64-v8a -o bindings/android/lane-messenger-ffi/src/main/jniLibs \
  build -p lane_messenger_ffi --release
```

Kotlin:

```kotlin
val session = LaneSession.connect(
  host = "10.0.2.2", port = 9000, useTls = false,
  userId = "alice", deviceId = "pixel-1", authToken = token
)
session.ping()
println(session.pollEvent(100))
```

See `bindings/android/lane-messenger-ffi/` for the library module scaffold.
JNI glue that maps `nativeConnect` → `lane_session_connect` still needs a
small C shim or UniFFI codegen (F7 completion).
