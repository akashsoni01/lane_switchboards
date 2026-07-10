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
- I2+ — reconnect, GRDB, chat UI, … (see `todo_ios.md`)
