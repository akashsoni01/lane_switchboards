# Push, background, notifications (iOS)

FunXMPP TCP drops in background. The app reconnects on foreground with
`resume_after_seq` (I2). Push is **alert APNs only** — never VoIP / Push to
Talk for chat (App Store risk).

## Architecture

```text
Background inbound (socket still up briefly)
  → local UNNotification (preview policy)
  → badge = sum(conversation.unread)

APNs (future gateway)
  → device token → HTTPS register (stub / HttpPushTokenRegistrar)
  → payload → same PushPayload parser
  → tap → deep link open conversation
  → cold start → pendingOpenConversationId until home
```

## Payload contract

Nested under `lane` (also accepts flat keys):

| Field | Type | Notes |
|-------|------|-------|
| `conversation_id` | string | Required. 1:1 peer id or `group:<id>` |
| `message_id` | string | Dedup / notification id |
| `sender_id` / `from_user` | string | Title when shown |
| `preview` / `body` | string | May be omitted when E2EE |
| `encrypted` / `e2ee` | bool | Forces hide-body when policy says so |

Example:

```json
{
  "aps": { "alert": { "title": "bob", "body": "New message" }, "badge": 3 },
  "lane": {
    "conversation_id": "bob",
    "message_id": "m-1",
    "sender_id": "bob",
    "encrypted": true
  }
}
```

Server **must not** put E2EE plaintext in `aps.alert` when E2EE is on. Prefer
generic “New message” and set `encrypted: true`.

## Preview policy

| Policy | Behavior |
|--------|----------|
| `showPreview` | Show `preview` text |
| `hideBodyWhenE2EE` (**default** when `E2EE_ENABLED`) | Body → “New message” if encrypted or E2EE on |
| `hideAlways` | Always “New message” |

## Deep links

| URL | Opens |
|-----|-------|
| `lane://chat/<conversationId>` | Thread |
| `https://lane.app/chat/<conversationId>` | Thread |

Registered via `CFBundleURLSchemes` = `lane`.

## Registration

`PushTokenRegistering`:

- `StubPushTokenRegistrar` — tests / DEBUG without backend
- `HttpPushTokenRegistrar` — `POST` JSON `{ platform, token, user_id, device_id, push_type: "alert" }`

Entitlement: `aps-environment` in `App/LaneMessenger.entitlements`.
`UIBackgroundModes` includes `remote-notification` only (not `voip`).

## Badge

`UnreadBadge.total` = sum of local conversation `unread`. Updated on inbox
refresh, open thread, inbound apply, and foreground.

## Foreground reconnect

`handleScenePhase("active")` → `SessionActor.setAppInForeground(true)` →
reconnect + resume seq (I2). Opening a notification also clears delivered
notifications for that conversation.

## Tests

```bash
cd apps/ios && swift run lane-messenger-kit-smoke
```

Covers payload/deep-link parse, preview policy, badge sum, background local
notify, and pending open after login.
