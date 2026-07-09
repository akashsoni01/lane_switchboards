# Presence Registry

How the messenger gateway tracks who is online, broadcasts presence updates,
and propagates state across a cluster.

## Data model

| Field | Location | Meaning |
|-------|----------|---------|
| `sessions` | `State.sessions: RwLock<HashMap<String, Vec<SessionHandle>>>` | Live connections keyed by `user_id`; one handle per `(user_id, device_id)` |
| `last_seen` | `State.last_seen: RwLock<HashMap<String, u64>>` | Unix seconds when user last went offline |
| `user_locations` | cluster only | `user_id → node_id` of the gateway holding the user's session |

Presence is **not** persisted to `StorageNode` in this milestone. Restarting a
gateway clears in-memory last-seen for users who were not connected at crash
time (connected users reconnect and republish `Available`).

## Presence packets

`Presence { user_id, kind, last_seen }` (`pkt_type = 0x10`):

| `PresenceKind` | When sent | `last_seen` |
|----------------|-----------|-------------|
| `AVAILABLE` | Login, reconnect after sync | `0` |
| `UNAVAILABLE` | Disconnect, idle sweep, graceful shutdown | unix secs at transition |
| `LAST_SEEN` | Reserved for explicit updates | meaningful timestamp |

Clients may send `Presence` after login (e.g. going invisible). The server
broadcasts the update using the same fan-out rules as server-initiated
presence.

## Single-node fan-out

On login/disconnect/idle sweep, `broadcast_presence_local` delivers a
`Presence` packet to **every other online user** on the same gateway:

```text
user A logs in
  → all sessions except A receive Presence{ A, AVAILABLE }
user A disconnects
  → all sessions except A receive Presence{ A, UNAVAILABLE, last_seen }
```

**Contact filtering:** clients send `SubscribePresence { contact_ids }`
(`pkt_type = 0x11`). Until the first subscribe, behaviour is legacy
broadcast-to-all. After subscribe:

- Subject with a roster: only listed contacts receive that user's presence.
- Observer with a roster: only receives presence for their contacts.

Empty roster = visible to nobody / see nobody (privacy lockdown).

## Multi-node propagation

When `ClusterRuntime` is active:

1. Local sessions receive `Presence` as above.
2. `PeerPresence { user_id, online, node_id, last_seen }` is gossiped to all
   peer gateways on login/logout.
3. Peers update `user_locations` and may rebroadcast `Presence` locally so
   clients on other nodes see remote users go online/offline.

Cross-node delivery of chat uses `user_locations` to forward live packets to
the gateway holding the recipient's session (see `05_cluster.md`).

## Idle sweep integration

The 5 s sweep closes sessions silent longer than `idle_timeout` (default 90 s).
For each closed session:

1. Remove `SessionHandle` from registry.
2. If no other device sessions remain for that user, record `last_seen`.
3. Broadcast `UNAVAILABLE` (+ cluster `PeerPresence`).

This ties application-level heartbeats (`Ping`/`Pong`) to presence; see
`07_heartbeats.md`.

## Privacy notes

- Server knows online/offline and last-seen timestamps for all users in the
  broadcast model.
- Production deployments should hash `user_id` in logs (Phase 9 observability
  follow-up).
- "Last seen" privacy settings (hide from non-contacts) require contact
  filtering — not implemented.

## Tests

| Test | What it proves |
|------|----------------|
| `presence_broadcast_on_login_and_disconnect` | Single-node Available/Unavailable |
| `cluster_tests::presence_propagates_across_nodes` | Cross-node presence after login on different gateways |

## Related

- Session lifecycle: `03_sessions.md`
- Cluster location map: `05_cluster.md`
- Wire format: `01_wire_protocol.md` (packet `0x10`)
