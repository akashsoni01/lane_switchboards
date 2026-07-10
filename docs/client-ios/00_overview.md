# Client iOS overview

Production SwiftUI app for the FunXMPP messenger. **Wire / E2EE stay in Rust**
(`lane_messenger_ffi`); iOS owns UI, Keychain, and (later) SQLite.

```text
SwiftUI (LaneMessenger)
        │
LaneMessengerKit (SessionActor, Auth, Keychain)
        │
LaneMessengerFFI / XCFramework
        │
TLS + FunXMPP → gateway
```

## Layout

| Path | Role |
|------|------|
| `apps/ios/Sources/LaneMessengerKit/` | Shared kit (auth, session, UI shells) |
| `apps/ios/App/` | `@main` app entry + Info.plist |
| `apps/ios/Tests/` | Unit tests (`swift test`) |
| [`todo_ios.md`](../../todo_ios.md) | Phase checklist |

## Phases

- **I0** — kit + `SessionActor` + mock/FFI transport
- **I1** — Keychain credentials, login/splash/home, kick alert
- **I2** — reconnect / ping / protocol errors / scene phase
- **I3** — SQLite local store + resume seq
- **I4** — inbox + 1:1 thread, ticks, drafts, send path
- **I5** — presence dots + contacts / subscribe
- **I6** — groups (create/add/remove/leave, version gate, ack summary)
- **I7** — media (upload/fetch, picker, progress, viewers)
- **I8** — E2EE (Olm/Megolm, safety numbers, pickle)
- I9+ — push, polish, … (see `todo_ios.md`)
