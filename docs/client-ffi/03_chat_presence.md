# Chat & presence (FFI)

## Send / ack

| API | Notes |
|-----|-------|
| `send_chat` / `lane_send_chat` | Waits `ServerAck`; returns inbox `seq` |
| `send_chat_with_media` | Caption + `media_id` after upload |
| `send_chat_with_retry` | Same `message_id` is dedup-safe; duplicate → seq `0` |
| `ack_delivered` / `ack_read` | Double / blue ticks |

Generate `message_id` (UUID) **before** send; reuse on retry.

## Presence roster

After `subscribe_presence(contact_ids)`, only listed contacts exchange
presence (see [`docs/messenger/04_presence.md`](../messenger/04_presence.md)).
Until subscribe, the gateway uses legacy broadcast.

`send_presence(kind)`: `1=Available`, `2=Unavailable`, `3=LastSeen`.

## Host flow

1. Persist inbound `ChatMessage` → `ack_delivered`
2. When UI shows the message → `ack_read`
3. Persist `latest_seq` from `SyncComplete` / inbound messages for resume
