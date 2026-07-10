# Chat UI & ticks (I4)

## Tick mapping

| UI | `MessageStatus` | Wire |
|----|-----------------|------|
| Clock | `pending` | Local only (pre-`ServerAck`) |
| Single check | `sent` | `ServerAck` |
| Double check | `delivered` | `DeliveredAck` |
| Blue double | `read` | `ReadAck` |
| Error + Retry | `failed` | Transport / send error |

Outbound path (production rule):

```text
compose → upsert pending row → FFI sendChat → ServerAck → … → ticks
```

Never send without a durable local row first.

## Thread open

Opening a conversation:

1. `markConversationRead`
2. For each inbound message not `read`: `ackDelivered` + `ackRead` via FFI
3. Reload bubbles

## Drafts

`conversations.draft` updated with debounce from the composer. Cleared on
successful send.

## Body encoding

C JSON field `body_hex` is **base64** (`lane_messenger_ffi` `c_api::b64`).
Swift decodes base64 first, hex fallback.
