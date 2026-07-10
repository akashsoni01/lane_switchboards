# Auth & Keychain (I1)

## Keychain layout

Service: `com.lane.messenger` (override in tests).

| Account | Survives logout | Notes |
|---------|-----------------|-------|
| `device_id` | yes | UUID, created once |
| `user_id` | no | Cleared on sign-out |
| `auth_token` | no | Cleared on sign-out |

Accessibility: `kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly`.

Never store tokens in UserDefaults or logs.

## AuthService

| Build | Implementation |
|-------|----------------|
| DEBUG | `DebugAuthService` — mint HMAC or accept pasted hex token |
| Release | `HttpAuthService` — POST JSON to identity service |

HMAC mint (DEBUG only), parity with Rust `HmacAuthenticator`:

```text
token = hex(HMAC-SHA256(secret, "user_id:device_id"))
demo secret = "demo-secret"  // messenger_demo
```

## Session UX

1. Splash → `CredentialStore.load()` → connect or Login.
2. Login → mint/fetch token → Keychain save → `SessionActor.connect`.
3. `LoginAck.ok=false` / transport auth error → banner, stay on Login.
4. `ReplacedByNewSession` → alert; reconnect blocked until user confirms;
   token cleared; `device_id` kept.
5. Settings / Home → Sign out closes session and clears user/token only.

## Client version

`Login.client_version` = `ios-<CFBundleShortVersionString>(<CFBundleVersion>)`
via `AppConfig.clientVersion`.
