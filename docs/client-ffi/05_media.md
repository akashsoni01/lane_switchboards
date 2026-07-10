# Media (FFI)

Contract: [`docs/messenger/02_bulk_data.md`](../messenger/02_bulk_data.md).

| API | Notes |
|-----|-------|
| `upload_media` | SHA-256 in Rust; 64 KiB chunks; credit/ack flow |
| `fetch_media` | Verifies sha256; returns `DownloadedMediaInfo` |

Cap pickers at **64 MiB** (`DEFAULT_MAX_MEDIA`). One upload at a time per
session. After upload, send chat/group with `media_id` + caption.

Sanitize server `file_name` before writing to disk (path traversal).
