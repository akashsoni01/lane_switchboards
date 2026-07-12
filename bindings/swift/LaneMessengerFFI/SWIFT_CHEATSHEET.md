# Swift FFI cheatsheet

Quick reference for Lane messenger on iOS / macOS. Wire + E2EE stay in **Rust**;
Swift owns UI, Keychain, SQLite, push.

## Two integration paths (pick one)

| Path | When | Conflict risk |
|------|------|----------------|
| **A. C ABI wrappers** (default) `LaneSession` / `LaneE2eeDevice` in this package | App links `liblane_messenger_ffi` or XCFramework; SPM depends on `LaneMessengerFFI` | Safe — `generated/` excluded |
| **B. UniFFI** `generated/lane_messenger.swift` | Prefer UniFFI codegen + JNA/Swift runtime | **Do not** also compile path A in the same target |

## Link Rust (path A)

```bash
cargo build -p lane_messenger_ffi --release
# or
./scripts/build_xcframework.sh
```

Xcode / SPM linker (host example):

```swift
// Package.swift linkerSettings on the app target:
.linkedLibrary("lane_messenger_ffi"),
.linkedLibrary("resolv"), // if needed on Apple platforms
// librarySearchPaths: ["…/target/release"]
```

## Connect + send

```swift
import LaneMessengerFFI

let session = try LaneSession(
    host: "127.0.0.1",
    port: 9000,
    useTls: false,           // true in production
    userId: "alice",
    deviceId: "iphone-1",
    authToken: token,
    resumeAfterSeq: lastSeq,
    pingIntervalSecs: 30
)
try session.ping()
while let ev = session.pollEvent(timeoutMs: 100) {
    // JSON LaneEvent — same schema as Java / C
    print(ev)
}
let seq = try session.sendChat(
    to: "bob",
    messageId: UUID().uuidString,
    body: Data("hello".utf8)
)
try session.ackDelivered(messageId: mid)
try session.ackRead(messageId: mid)
try session.setResumeSeq(seq)
try session.close()
```

## Presence / groups / media

```swift
try session.subscribePresence(contactIds: ["bob", "carol"])
try session.sendPresence(kind: 0) // available

let v = try session.createGroup(groupId: "g1")
_ = try session.addMember(groupId: "g1", user: "bob")
try session.sendGroup(groupId: "g1", messageId: "m1", body: Data("hi".utf8))

_ = try session.uploadMedia(
    mediaId: "pdf-1", fileName: "a.pdf",
    mimeType: "application/pdf", data: bytes
)
let blob = try session.fetchMedia(mediaId: "pdf-1")
```

## E2EE (on device — not server)

```swift
let e2ee = LaneE2eeDevice()
try e2ee.publish(session: session, deviceId: "iphone-1")
let seq = try e2ee.sendEncryptedChat(
    session: session, to: "bob",
    messageId: "m2", plaintext: Data("secret".utf8)
)
let plain = try e2ee.decryptChat(from: "bob", body: ciphertext)
let pickle = try e2ee.exportPickle(passphrase: pass)
let restored = try LaneE2eeDevice(pickle: pickle, passphrase: pass)
let sn = try LaneE2eeDevice.safetyNumber(
    localIdentityB64: try e2ee.identityKey(),
    remoteIdentityB64: peerIk
)
```

## Kit layer (`apps/ios`)

| Piece | Role |
|-------|------|
| `LaneFFITransport` | `#if canImport(LaneMessengerFFI)` → real socket |
| `MockMessengerTransport` | Smoke / UI without Rust lib |
| `NativeE2eeBackend` | Wraps `LaneE2eeDevice` when FFI linked |
| `MockE2eeBackend` | Kit smoke tests |

```bash
cd apps/ios && swift run lane-messenger-kit-smoke   # mock path, no Rust required
```

## Must not conflict with Rust

1. **Never** reimplement FunXMPP frames or Olm in Swift for production.
2. **Never** compile UniFFI `generated/` + hand-written wrappers together.
3. One Tokio runtime lives inside the Rust `.dylib` / XCFramework — do not
   spawn a second messenger client stack in Swift.
4. Secrets (`authToken`, pickle passphrase, plaintext) must not be logged.
5. Call connect / send / poll off the main actor (background queue).

## Version

```swift
print(LaneFFI.version, LaneFFI.protocolVersion)
```

## Related

- Package README: [`README.md`](README.md)
- Full guide: [`docs/client-ffi/11_swift.md`](../../../docs/client-ffi/11_swift.md)
- C header: [`lane_messenger_ffi/include/lane_messenger_ffi.h`](../../../lane_messenger_ffi/include/lane_messenger_ffi.h)
