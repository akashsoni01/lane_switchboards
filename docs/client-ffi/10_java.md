# Java / Android JNI bindings

Production **Java** surface over `liblane_messenger_ffi` (C ABI + `feature = "jni"`).
Parity with Swift `LaneSession` / `LaneE2eeDevice`: session, chat, presence, groups,
media, and E2EE.

```text
App (Java / Kotlin)
  → com.lane.messenger.LaneSession / LaneE2eeDevice
  → JNI (lane_messenger_ffi/src/jni.rs)
  → C ABI → MessengerClient + E2eeDevice (Rust)
  → TCP/TLS FunXMPP → gateway
```

## Artifacts

| Path | Role |
|------|------|
| `bindings/android/lane-messenger-ffi/src/main/java/com/lane/messenger/*.java` | Production Java API |
| `lane_messenger_ffi/src/jni.rs` | JNI glue (`--features jni`) |
| `bindings/android/lane-messenger-ffi/` | Android library module (AAR) |
| `uniffi.lane_messenger` (generated) | Optional UniFFI/JNA path |

## Build native library

```bash
# Desktop / JVM smoke
cargo build -p lane_messenger_ffi --release --features jni,tls

# Android ABIs → jniLibs
export ANDROID_NDK_HOME=…
./scripts/build_android_ndk.sh
# FEATURES should include jni (default script uses c-api,tls,ws — set FEATURES)
FEATURES="c-api,tls,ws,jni" ./scripts/build_android_ndk.sh
```

## API overview

```java
try (LaneSession session = LaneSession.connect(
        ConnectOptions.builder()
            .host("127.0.0.1")
            .port(9000)
            .useTls(true)
            .userId("alice")
            .deviceId("phone-1")
            .authToken(token)
            .resumeAfterSeq(lastSeq)
            .build())) {
    session.ping();
    String ev = session.pollEvent(100);
    long seq = session.sendChat("bob", "m-1", "hello".getBytes(UTF_8));
    session.ackDelivered("m-1");
    session.ackRead("m-1");
    session.subscribePresence(List.of("bob", "carol"));
    session.createGroup("g1");
    session.addMember("g1", "bob");
    long n = session.uploadMedia("pdf-1", "a.pdf", "application/pdf", bytes);
    MediaBlob blob = session.fetchMedia("pdf-1");

    try (LaneE2eeDevice e2ee = LaneE2eeDevice.generate()) {
        e2ee.publish(session, "phone-1");
        e2ee.sendEncryptedChat(session, "bob", "m-2", "secret".getBytes(UTF_8));
        byte[] plain = e2ee.decryptChat("bob", ciphertext);
        byte[] pickle = e2ee.exportPickle(passphrase);
    }
}
```

### Classes

| Class | Purpose |
|-------|---------|
| `LaneNative` | `loadLibrary`, version, lastError |
| `ConnectOptions` | Builder for connect |
| `LaneSession` | AutoCloseable session |
| `LaneE2eeDevice` | AutoCloseable Olm/Megolm device |
| `MediaBlob` | fetchMedia result |
| `LaneException` | Checked FFI failures |

## Production rules

1. **Never** call connect / send / poll / E2EE on the Android main thread.
2. Prefer `Dispatchers.IO` / a dedicated executor; one session per connection.
3. Persist `resumeAfterSeq` after `SyncComplete` (from poll JSON).
4. Store E2EE pickle in Keystore-backed storage; never log tokens or plaintext.
5. Production: `useTls(true)`; never embed HMAC `demo-secret` in Release.
6. ProGuard: keep `com.lane.messenger.**` native methods (see `proguard-rules.pro`).

## Sample

[`examples/android_ffi_demo/`](../../examples/android_ffi_demo/)

## Related

- C header: [`lane_messenger_ffi/include/lane_messenger_ffi.h`](../../lane_messenger_ffi/include/lane_messenger_ffi.h)
- Build: [`BUILD.md`](BUILD.md)
- Platforms: [`07_platforms.md`](07_platforms.md)
