# Session lifecycle (I2)

```text
Disconnected → Connecting → AwaitingLogin → Syncing → Ready
       ↑____________ offline / ping fail _____________|
                    (exponential backoff + jitter)
```

## Rules

1. First frame after connect is `Login` (handled inside FFI) with
   `resume_after_seq` from `LocalStore.resumeAfterSeq()`.
2. `LoginAck.ok` → Syncing (show `pending_messages` when &gt; 0).
3. `SyncComplete` → Ready; start client ping loop (~`ping_interval_secs`).
4. `Disconnected` / ping failure → Offline → reconnect if:
   - auto-reconnect on
   - not kicked (`REPLACED_BY_NEW_SESSION`)
   - not paused (`UNSUPPORTED_VERSION`, auth failures)
   - app in foreground
5. Background: stop pings; expect socket drop. Foreground: resume reconnect.
6. Backoff: base 250 ms, double each attempt, cap 30 s, ± ±20%.

## Protocol errors → UX

| Code | Behaviour |
|------|-----------|
| `UNSUPPORTED_VERSION` | Pause reconnect; upgrade alert |
| `AUTH_FAILED` / `NOT_AUTHENTICATED` | Pause; return to login |
| `REPLACED_BY_NEW_SESSION` | Lock device; alert |
| `RATE_LIMITED` | Reconnect with backoff |
| Others | Offline + reconnect |

## Resume

`SessionActor.setResumeSeqProvider` reads SQLite `meta.resume_after_seq`
before every connect/reconnect. Advanced when durable messages / sync apply
(see `03_storage.md`).
