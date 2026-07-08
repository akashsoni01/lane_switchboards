# Offline Store & Durability

## Inbox model

Each user has one inbox on their home node (single node: the only node):

- `next_seq` — monotonic per-inbox sequence, assigned at persist time.
- `pending` — undelivered messages, ordered by seq.
- `seen` — dedup keys (`message_id` for 1:1, `message_id:member` for group
  copies); retries are idempotent.

Entries are tombstoned by `DeliveredAck` and dropped once the head of the
queue is contiguous-delivered. Quota: `max_inbox` (default 10 000,
oldest-drop).

## Durable mode (`ServerConfig::durable_dir`)

With `durable_dir: Some(dir)` the inbox state is backed by an append-only
journal at `dir/inbox.wal`. Entries are ordinary protocol frames (the same
binary codec as the wire — no second serialization format):

| Journal entry | Meaning |
|---------------|---------|
| `ChatMessage` / `GroupMessage` | message persisted (seq assigned) |
| `DeliveredAck` | tombstone for one recipient's copy |
| `SyncComplete{user_id, latest_seq}` | seq high-water mark (compaction) |

Ordering guarantee: **journal fsync happens before `ServerAck` leaves the
node**, so any message the sender saw acked survives a crash.

### Startup

1. Read and fold the journal into per-user inbox state. Duplicate appends
   are dropped (dedup keys), tombstones remove pending entries, and a torn
   final frame (crash mid-write) is tolerated and discarded.
2. Compact: rewrite the file with only high-water marks + pending messages
   (atomic tmp-file + rename), then reopen for appends.

The high-water mark entry is what keeps `seq` monotonic even when a user's
inbox is fully delivered and compacted away — sequence numbers never
regress across restarts, so client resume cursors stay valid.

### Failure policy

A journal append failure is logged at error level and the message is still
delivered (degraded durability rather than an outage). Operators should
alert on `inbox journal append failed`.

### What is durable and what is not

| State | Durable? |
|-------|----------|
| Inbox messages + seq + dedup | yes (journal) |
| Delivered tombstones | yes (journal) |
| Media blobs | no — in-memory (Phase 6 follow-up) |
| Group membership | no — in-memory (Phase 6 follow-up) |
| Presence / last-seen | no — rebuilt from live traffic |

## Tests

- Unit (`src/messenger/journal.rs`): replay restores pending and drops
  delivered; seq high-water mark survives double restart + compaction; torn
  tails are tolerated.
- Integration (`tests/messenger.rs`): `acked_message_survives_gateway_restart`
  (send → crash → reboot → replay → ack → crash → seq continues at 2) and
  `dedup_survives_restart`.
