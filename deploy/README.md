# Messenger deployment

Examples for running the FunXMPP gateway in production.

| Path | Purpose |
|------|---------|
| [systemd/lane-messenger.service](systemd/lane-messenger.service) | Single-node systemd unit |
| [k8s/messenger.yaml](k8s/messenger.yaml) | 3-replica StatefulSet + headless + LB |

## Environment variables (convention)

| Variable | Meaning |
|----------|---------|
| `LISTEN_ADDR` | Client TCP/TLS bind (`0.0.0.0:9000`) |
| `AUTH_SECRET` | HMAC secret for client tokens |
| `PEER_SECRET` | HMAC secret for inter-node `PeerHello` |
| `NODE_ID` | Stable cluster node id |
| `DURABLE_DIR` | Inbox WAL + key directory path |
| `OBSERVABILITY_ADDR` | `/health` `/ready` `/metrics` bind |
| `MESH_REGISTRY` | Optional ServiceMesh registry URL |

## Peer discovery

- **Static:** pass peer list at boot (`ClusterConfig.peers`).
- **Dynamic:** `spawn_mesh_discovery` (`src/messenger/discovery.rs`) polls
  `MeshRegistry` for `lane.messenger` instances and calls `add_peer` /
  `remove_peer`.

## TLS

Always enable `feature = "tls"` in production. Mount cert/key as in the
systemd unit or via a cert-manager Secret in Kubernetes.

## Notes

The container image / CLI binary (`lane-messenger`) is illustrative — wire
your own binary that calls `MessengerServer::bind_cluster_tls` (or
`bind_ws` for browser clients) using the env vars above.
