# Groups (iOS)

Wire: `GroupEvent` (`0x41`), `GroupMessage` (`0x40`), `GroupAckSummary` (`0x42`).
Spec: [`docs/messenger/08_groups.md`](../messenger/08_groups.md).

## Conversation id

Group threads use conversation id `group:<group_id>` so they never collide with
1:1 peer user ids in the inbox.

## Membership

| Op | Who | Local effect |
|----|-----|--------------|
| `CREATE` (1) | Any user | Creator = member + admin; `version = 1` |
| `ADD_MEMBER` (2) | Admin | Add subject; bump version |
| `REMOVE_MEMBER` (3) | Admin | Remove subject; bump version |
| `LEAVE` (4) | Member | Remove self; bump version |

Apply rules:

1. Persist only when `version` is **strictly greater** than the stored version.
2. Ignore stale / duplicate events (no second system line).
3. Cap UI adds at `AppLimits.maxGroupMembers` (**1024**).

FFI entry points (C ABI / `LaneSession`):

- `lane_create_group` / `createGroup`
- `lane_add_member` / `addMember`
- `lane_remove_member` / `removeMember`
- `lane_leave_group` / `leaveGroup`
- `lane_send_group` / `sendGroup`

## Send path

Same offline-first rule as 1:1:

```text
compose → upsert pending (conversation = group:<id>)
       → FFI sendGroup
       → ServerAck → ticks
       → GroupAckSummary → delivered/read counts
```

Non-members are rejected locally when membership is known; the gateway also
returns `ProtocolError` for authz violations.

## Ack aggregation vs WhatsApp

| Stage | WhatsApp UX | Lane today |
|-------|-------------|------------|
| Accepted by server | single grey tick | `ServerAck` → `sent` |
| Delivered to devices | double grey | `GroupAckSummary.delivered_count` |
| Read | double blue | `GroupAckSummary.read_count` |

The UI shows `delivered N/M` / `read N/M` under outbound group bubbles when
`member_count > 0`. Per-member identity lists (`delivered_by` / `read_by`) are
available on the wire but not yet rendered in the kit (counts only).

## UI surfaces

- Inbox: group rows (no presence dot).
- Thread: sender labels on inbound; centered system lines for membership.
- Group info: members, admin add/remove, leave.
- Create group sheet: title + contact multi-select.

## Tests

```bash
cd apps/ios && swift run lane-messenger-kit-smoke
```

Covers parse, version gating, create→add→send, and non-member send rejection.
