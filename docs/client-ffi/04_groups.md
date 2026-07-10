# Groups (FFI)

| API | Notes |
|-----|-------|
| `create_group` / `add_member` / `remove_member` / `leave_group` | Returns membership `version` |
| `send_group` | Waits `ServerAck` |

## GroupAckSummary

After a group send, members' delivered/read acks are aggregated into
`GroupAckSummary` events (not per-member `DeliveredAck` to the sender).

Display “read by N of M” using `read_by.len` vs `member_count`.

See [`docs/messenger/08_groups.md`](../messenger/08_groups.md).
