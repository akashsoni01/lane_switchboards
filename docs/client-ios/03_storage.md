# Local storage (I3)

Offline-first SQLite store (`SQLiteLocalStore`). Path:

```text
Application Support/LaneMessenger/messenger.sqlite
```

Tests use `InMemoryLocalStore`.

## Tables

| Table | Purpose |
|-------|---------|
| `meta` | `resume_after_seq`, schema keys |
| `conversations` | inbox rows (sort_ts, unread, draft, preview) |
| `messages` | idempotent by `message_id` |
| `contacts` / `groups` / `group_members` | reserved for I5/I6 |

## Message status

`pending → sent → delivered → read` (+ `failed`).

Status updates never downgrade past `read`.

## Resume seq

- Advanced when upserting a message with `seq > current`.
- Advanced on `ServerAck` / `SyncComplete.latest_seq` when higher.
- Cleared on `wipeUserData()` (sign-out).

## Idempotency

`INSERT … ON CONFLICT(message_id) DO UPDATE` — replay / reconnect must not
duplicate bubbles.

## Wipe policy

Sign-out clears messages + conversations + resume seq. Keychain `device_id`
is retained (see `01_auth.md`).
