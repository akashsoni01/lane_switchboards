# Bulk Data Transfer (Media: PDFs, images, files)

Large payloads never travel as a single frame. Blobs are chunked, integrity
checked, stored server-side, and referenced from chat messages by `media_id`.

## Upload

```text
Sender                              Server
  │ MediaStart{media_id, name,        │
  │   mime, total_size, sha256}  ───► │  allocate; enforce max size
  │ ◄─── MediaAck{ok, complete=false} │
  │ MediaChunk{offset=0, data}   ───► │  append; offsets must be contiguous
  │ ◄─── MediaAck{received_bytes}     │  (per-chunk flow-control credit)
  │ …                                 │
  │ MediaChunk{…, last=true}     ───► │  verify size + sha256
  │ ◄─── MediaAck{ok, complete=true}  │  blob is now fetchable
```

Rules:

- Chunks are ≤ 64 KiB and strictly sequential (`offset` must equal bytes
  received so far); out-of-order chunks are rejected.
- The final chunk triggers verification: declared `total_size` must match and
  the SHA-256 hex digest must equal `MediaStart.sha256`. Any mismatch discards
  the blob and returns `MediaAck{ok=false, error}`.
- Blobs above `ServerConfig::max_media_bytes` (default 64 MiB) are rejected at
  `MediaStart`.
- One upload at a time per connection.

## Referencing from chat

After `complete=true`, put the `media_id` into `ChatMessage.media_id` (or
`GroupMessage.media_id`). The message body can carry a caption. The message
flows through normal routing/acks; the blob does not.

## Download

```text
Receiver                            Server
  │ MediaFetch{media_id, from_offset} ───►
  │ ◄─── MediaStart{metadata + sha256}
  │ ◄─── MediaChunk{offset, data}
  │ ◄─── …
  │ ◄─── MediaChunk{last=true}
```

- `from_offset` supports resuming interrupted downloads.
- The client re-verifies the sha256 digest after reassembly
  (`MessengerClient::fetch_media` does this automatically).

## Example (reference client)

```rust
let bytes = std::fs::read("report.pdf")?;
alice.upload_media("pdf-1", "report.pdf", "application/pdf", &bytes).await?;
alice.send_chat_with_media("bob", "m-1", b"see attached", "pdf-1").await?;

// Bob's side, after receiving the ChatMessage with media_id = "pdf-1":
let blob = bob.fetch_media("pdf-1").await?;   // sha256-verified
std::fs::write(&blob.file_name, &blob.data)?;
```

## Current storage note

Blobs live in gateway memory for this milestone. The production path is
content-addressed storage in `StorageNode` (or object storage) with the same
wire protocol — the client contract will not change.
