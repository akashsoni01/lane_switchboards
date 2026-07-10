# Media (iOS)

Wire: `MediaStart` / `MediaChunk` / `MediaAck` / `MediaFetch`
([`docs/messenger/02_bulk_data.md`](../messenger/02_bulk_data.md)).

Blobs travel on the **same FunXMPP socket** via FFI — not a separate gRPC or
HTTP upload for the MVP path.

## Limits

| Limit | Value |
|-------|-------|
| Max blob | **64 MiB** (`AppLimits.maxMediaBytes`) |
| Chunk size | ≤ **64 KiB** (enforced inside Rust FFI) |
| Concurrent uploads | **1** per session (serial queue in `MediaService`) |

## Send path

```text
Picker / file importer
  → reject if > 64 MiB
  → SHA-256 (CryptoKit) + write Application Support/LaneMessenger/media/<id>/
  → insert pending message (media_id set)
  → queue uploadMedia (FFI chunks + sha verify on server)
  → sendChatWithMedia / sendGroupWithMedia
  → ServerAck → ticks
```

Local blob is marked `complete=true` only after a successful integrity write.
Failed uploads leave the message `failed` with Retry (re-uploads from local
copy).

## Receive path

```text
ChatMessage / GroupMessage with media_id
  → upsert message
  → ensureDownloaded (fetchMedia via FFI)
  → verify sha256 before marking complete
  → UI shows thumbnail / file chip only when complete
```

Partial downloads are never shown as openable files.

## Storage layout

```text
Application Support/LaneMessenger/media/<media_id>/
  meta.json   { fileName, mimeType, sha256, size, complete }
  blob        raw bytes
```

## UI

- Paperclip → Photos (PhotosPicker) + Files (PDF/images)
- Progress bar while transferring
- Image full-screen viewer; PDF via Quick Look (iOS)
- Caption optional in the compose field at send time

## FFI

| C API | Swift |
|-------|-------|
| `lane_upload_media` | `uploadMedia` |
| `lane_fetch_media` | `fetchMedia` |
| `lane_send_chat_with_media` | `sendChat(..., mediaId:)` |
| `lane_send_group_with_media` | `sendGroup(..., mediaId:)` |

## Tests

```bash
cd apps/ios && swift run lane-messenger-kit-smoke
```

Covers size rejection, corrupt SHA, and upload→send→fetch round-trip on
`MockMessengerTransport`.
