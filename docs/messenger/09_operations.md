# Operations — Multi-Node Messenger

Scaling playbook, observability, and runbooks for messenger gateway clusters.

## Scaling playbook

1. **Bootstrap** — start N gateways with `MessengerServer::bind_cluster` (or
   `bind_cluster_tls` in production). Each node needs every *other* node's
   `node_id` and `host:port` in `ClusterConfig.peers`.
2. **Grow** — call `add_peer` on existing nodes with the new gateway's
   `PeerAddr`. Gossip (`PeerJoin`) updates every ring; inbox/group handoffs
   run automatically.
3. **Shrink** — `remove_peer(node_id)` gossips `PeerLeave`, hands off shards,
   and drops the outbound link.
4. **TLS** — use `bind_cluster_tls` with the same `TlsAcceptor` for client
   sockets and `TlsConnector` for outbound peer links.

## Observability

Build with `feature = "metrics"` and expose HTTP on a sidecar port:

```rust
let server = MessengerServer::bind_cluster(...).await?;
tokio::spawn(async move {
    server.serve_observability("127.0.0.1:9091").await.ok();
});
```

| Endpoint | Meaning |
|----------|---------|
| `GET /health` | Liveness — process is up |
| `GET /ready` | Readiness — accept loop running and all peer links connected |
| `GET /metrics` | Prometheus text (lane actor + messenger series) |

### Messenger metric series

| Metric | Type | Description |
|--------|------|-------------|
| `lane_messenger_sessions_connected` | gauge | Live client sessions |
| `lane_messenger_peer_links_connected` | gauge | Outbound peer links |
| `lane_messenger_messages_in_total` | counter | Frames received |
| `lane_messenger_messages_out_total` | counter | Frames sent |
| `lane_messenger_ack_latency_seconds` | histogram | Home-shard persist → ServerAck |
| `lane_messenger_inbox_pending` | gauge | Undelivered offline messages (hashed user label) |
| `lane_messenger_fanout_duration_seconds` | histogram | Group fan-out on home shard |
| `lane_messenger_dropped_frames_total` | counter | Peer-link backpressure drops |

## Cross-node latency (bench)

Run `cargo bench --bench messenger_cluster` on localhost. Debug builds
typically show **p50 ~2–4 ms** and **p99 ~8–15 ms** for cross-node offline
ack (2-node mesh). Re-record after hardware or release-mode changes.

## Runbook: node loss

**Symptom** — one gateway stops; clients on that node disconnect.

1. Confirm `/ready` fails on the lost node; surviving nodes still show
   `peer_links_connected` one less than `cluster_size - 1`.
2. On a surviving node, `remove_peer("<lost-node-id>")` to rebalance shards.
3. Clients reconnect to any surviving gateway; offline inboxes replay via
   `PeerSync` from the user's home shard.
4. Replace the node: boot a new gateway, then `add_peer` from all survivors.

**Data safety** — messages with `ServerAck` are durable on the recipient's
home shard. In-flight frames before persist may be retried by the client.

## Soak / chaos tests

| Test | Command | Purpose |
|------|---------|---------|
| Chaos | `cargo test node_failure_during_traffic` | Kill node mid-traffic; no ack'd loss |
| Soak (CI) | `cargo test soak_short_burst` | 5 s sustained send; no errors |
| Soak (manual) | `cargo test soak_sustained -- --ignored` | 30 s burst; watch memory/fds |

Manual 1 h soak: run `soak_sustained` with a longer duration locally and
confirm RSS flat in `top` / Prometheus.
