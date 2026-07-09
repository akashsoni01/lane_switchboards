# Group Chat

Membership model, fan-out routing, authorization, and known limitations.

## Group state

Each group is stored on its **home shard** (hash of `group_id`):

```rust
Group {
    members: HashSet<String>,
    admins: HashSet<String>,
    version: u64,  // monotonic, bumped on every membership change
}
```

In-memory today; durable `StorageNode` persistence is a follow-up (Phase 6
optional item in `todo.md`).

## Operations (`GroupEvent`, pkt `0x41`)

| `GroupOp` | Who may invoke | Effect |
|-----------|----------------|--------|
| `CREATE` | Any authenticated user | Creator becomes member + admin; `version = 1` |
| `ADD_MEMBER` | Admin only | Add `subject_user`; bump `version` |
| `REMOVE_MEMBER` | Admin only | Remove `subject_user`; bump `version` |
| `LEAVE` | Member (self) | Remove actor; bump `version` |

Every successful op returns an updated `GroupEvent` with the new `version`.
Events fan out to all members so clients can update local membership views.

## Group messages (`GroupMessage`, pkt `0x40`)

Fan-out flow:

```text
Sender → group home shard
  → snapshot members at consistent version
  → for each member:
        persist to member's inbox (dedup key: message_id + member)
        deliver online OR queue for offline sync
  → ServerAck to sender (single tick, like 1:1)
```

- `body` is opaque bytes (plaintext or E2EE `EncryptedGroupPayload`).
- Optional `media_id` references a blob uploaded via the media protocol.
- Per-member `seq` is assigned on each member's home inbox (cluster mode).

## Authorization

| Action | Rule |
|--------|------|
| Send `GroupMessage` | Sender must be in `members` |
| `ADD_MEMBER` / `REMOVE_MEMBER` | Actor must be in `admins` |
| `LEAVE` | Actor must be in `members` |

Violations return `ProtocolError` with detail; connection stays open.

## Limits

| Limit | Default | Source |
|-------|---------|--------|
| Max members | 1024 | `ServerConfig.max_group_members` |
| Max frame | 256 KiB | Same as 1:1 |
| Max media | 64 MiB | `ServerConfig.max_media_bytes` |

## Ack ladder (current vs planned)

**Implemented (per sender, like 1:1):**

- `ServerAck` when the group home persists and accepts the fan-out job.

**Aggregation (implemented):**

- On fan-out the group home registers a `GroupAckTracker` for `message_id`.
- Member `DeliveredAck` / `ReadAck` update the tracker and push
  `GroupAckSummary` (0x42) to the sender with `delivered_by`, `read_by`,
  and `member_count`.
- Clients show single tick on `ServerAck`, then progress from summaries
  (e.g. "read by 2 of 5").

## E2EE groups

Megolm sender-keys: see `10_e2ee.md`. Session keys are distributed via
Olm-encrypted 1:1 side channels; `GroupMessage.body` carries
`EncryptedGroupPayload`.

## Cluster behaviour

- Group home shard owns membership and fan-out orchestration.
- Each member's inbox lives on that member's **user home shard**.
- Cross-node: group home forwards per-member copies to member home nodes
  (see `05_cluster.md`).
- Dynamic join/leave: `PeerHandoffGroup` migrates group state on ring
  rebalance.

## Tests

| Test | Coverage |
|------|----------|
| `group_create_add_and_fanout` | Create, add, online delivery |
| `offline_group_member_gets_message_on_login` | Offline member sync |
| `non_member_cannot_send_and_non_admin_cannot_add` | Authz |
| `cluster_tests::group_chat_spans_nodes` | 3-node cluster fan-out |
| `e2ee_tests::e2ee_group_megolm_single_node` | Encrypted group opacity |

## Related

- Wire format: `01_wire_protocol.md`
- Offline replay: `06_offline_store.md`
- Cluster fan-out: `05_cluster.md`
- E2EE: `10_e2ee.md`
