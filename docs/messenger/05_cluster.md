# Multi-Node Clustering

Multiple gateways form a full mesh of peer links speaking the same binary
protocol as clients. Configuration is `ClusterConfig { node_id, peers,
peer_secret }` passed to `MessengerServer::bind_cluster`.

## Topology

```text
              clients                clients                clients
                 │                      │                      │
            ┌────▼────┐            ┌────▼────┐            ┌────▼────┐
            │ node-0  │◄──────────►│ node-1  │◄──────────►│ node-2  │
            └────┬────┘  peer      └─────────┘   peer     └────┬────┘
                 └────────────────── links ────────────────────┘
```

- **Peer links**: every ordered node pair has one outbound link (connect →
  `PeerHello` with an HMAC token over `peer_secret` → frames). Links
  reconnect with exponential backoff; queued frames survive reconnects.
  Inbound links only receive; replies travel over the receiver's own
  outbound link.
- **Peer auth**: `PeerHello.auth_token = hex(HMAC-SHA256(peer_secret,
  "node_id:peer"))` — same authenticator machinery as clients, separate
  secret.

## Sharding model

Every user and group has a **home node** chosen by consistent hashing
(`HashRing`, 64 virtual nodes) over all node ids:

| Owned by home node | Meaning |
|--------------------|---------|
| Inbox | persistence, `seq` assignment, dedup, tombstoning |
| Group membership | create/add/remove/leave, versioning, fan-out |

Sessions live wherever the client connected; a **location map**
(`user → node`) is maintained on every node via `PeerPresence` broadcasts on
login/logout.

## Message flows

**1:1 chat** (Alice on node-0 → Bob homed on node-1, connected to node-2):

```text
Alice ── ChatMessage ──► node-0
node-0 ─ forward ──────► node-1 (Bob's home)   persist, assign seq
node-1 ─ ServerAck{to_user=alice} ─► node-0 ─► Alice   (single tick)
node-1 ─ ChatMessage{seq} ─► node-2 ─► Bob             (delivery)
Bob ── DeliveredAck ─► node-2 ─ broadcast ─► all peers
node-1 tombstones; Alice's node relays the double tick to her session
```

**Offline + sync**: messages wait in the home node's inbox. When the user
logs in on any gateway, that gateway sends `PeerSync { user, after_seq }` to
the home node, which streams the pending messages followed by
`SyncComplete{user_id}` through the mesh to the user's session.

**Groups**: the sender's gateway forwards `GroupMessage` to the group's home
shard, which validates membership at a consistent version, acks the sender
(`ServerAck{to_user}` routed via the location map), and fans out one copy per
member to that member's home node (persist, dedup key `message_id:member`)
and on to their live session.

## Dynamic membership (join / leave)

The hash ring can change at runtime via `MessengerServer::add_peer` /
`remove_peer` or inbound gossip:

| Packet | Direction | Purpose |
|--------|-----------|---------|
| `PeerJoin { node_id, addr }` | gossip | announce a new gateway; every node updates the ring and opens an outbound link |
| `PeerLeave { node_id }` | gossip | remove a gateway from the ring and drop its link |
| `PeerHandoffUser` | point-to-point | migrate an inbox (pending messages, `next_seq`, dedup set) to the new home shard |
| `PeerHandoffGroup` | point-to-point | migrate group membership state (members, admins, version) |

Rebalance steps when the ring changes:

1. Snapshot users/groups this node currently owns.
2. Add or remove the node from the ring.
3. For every snapshot entry that is **no longer** homed here, send a handoff
   packet to the new home node and delete the local copy.

Handoffs are idempotent: the receiver merges via the existing `seen` set and
keeps the highest group version.

## Delivery guarantees (cluster)

- `ServerAck` still means "persisted on the recipient's home node".
- Exactly one stored copy per recipient cluster-wide (dedup at the home).
- Peer-link frame drops are safe post-persist: inbox replay recovers them.
- Acks (`DeliveredAck`/`ReadAck`) are broadcast to all nodes; each node
  relays to local sessions and the home node tombstones. Broadcast is O(n)
  in cluster size — acceptable for small meshes, a routing-table follow-up
  for large ones.

## Current limitations

- Media blobs are stored in memory on the uploader's gateway; cross-node
  `MediaFetch` relays over the peer mesh but blobs are not replicated.
- Group-operation errors (e.g. non-admin add) are logged on the home node
  but not relayed to remote actors; the client times out instead of getting
  a typed error.
- Client sockets support TLS (`bind_tls`); peer links support TLS via
  `bind_cluster_tls`. Plain TCP remains the default for local development.

## Tests

`tests/messenger.rs` (`cluster_tests`): cross-node delivery with the full
ack ladder, offline replay when the user logs in on a non-home node with
exactly-one-copy assertion, a 3-node group chat, cross-node presence, and
dynamic join with inbox handoff + replay on the new node.
