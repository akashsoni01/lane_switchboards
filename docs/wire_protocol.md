# Wire protocol (gRPC / protobuf)

lane_switchboards **0.7+** uses Protocol Buffers over gRPC (tonic). There is no length-prefixed JSON framing on the wire.

## Planes

| Plane | Proto package | Service | Purpose |
|-------|---------------|---------|---------|
| Control | `lane_switchboard.control` | `MeshRegistry` | Service discovery: register, deregister, list, ping, watch |
| Data | `lane_switchboard.data` | `ActorMessaging` | Bidirectional `Deliver` stream: encode actor payloads as `bytes` |
| Consensus | `lane_switchboard.paxos` | `PaxosAcceptor` | prepare / propose / commit for linearizable keys |

Definitions live under `proto/` and are compiled in `build.rs` into `crate::proto`.

## Data plane: `ActorMessaging.Deliver`

- Client opens a **bidirectional** gRPC stream to the peer’s data-plane listener.
- Each `DeliverRequest` carries:
  - `target` — actor name on the remote node (e.g. microservice `instance_id`)
  - `payload` — `prost` encoding of the service’s `RemoteMessage` type
  - `frame_id` — correlates optional `DeliverReply` when `expect_ack` is true
  - `expect_ack` — quorum / consistency paths wait for `DeliverReply.ok`
- Server decodes `payload` with `M::decode`, dispatches to a registered actor or mailbox, and optionally replies on the same stream.

`RemoteMessage` is `prost::Message + Default + Send + Sync + Clone + Debug`. Application enums must be protobuf messages (not `serde` JSON).

## Control plane: `MeshRegistry`

- `Register` / `Deregister` — `ServiceRecord` (service name, instance id, listen address, deliver target)
- `List` — snapshot of live records
- `Watch` — server stream of `ServiceEvent` (registered / deregistered)
- Background eviction uses TTL heartbeats (`mesh::EVICTION_INTERVAL`)

Clients use `MeshRegistryClient::connect` (optional `TlsConfig` with `feature = "tls"`).

## TLS (`feature = "tls"`)

- `TlsConfig` holds PEM cert, key, optional CA.
- `DistributedConfig.tls` — data-plane `Node` / `RemoteActorRef`
- `MeshRegistryHandle::bind_with_tls` / `MeshRegistryClient::connect_with_tls`

## Observability

Tracing spans (when `tracing` is enabled):

| Span | Handler |
|------|---------|
| `grpc.deliver` | `ActorMessagingService::deliver` |
| `grpc.register` / `grpc.deregister` / `grpc.list` / `grpc.ping` / `grpc.watch` | `MeshRegistryService` |
| `grpc.paxos.prepare` / `grpc.paxos.propose` / `grpc.paxos.commit` | `PaxosGrpcService` |
| `consistency.invoke_consistent` / `consistency.read_consistent` | `mesh.rs` (see `docs/consistency.md`) |

`Node::connected_channels()` reports active bidi deliver streams on that node.

## Migration from 0.6 JSON/TCP

- Remove `serde` on wire types; derive `prost::Message` instead.
- Replace `MeshRegistryServer` TCP with `MeshRegistryHandle::bind`.
- Replace `RemoteActorRef::with_tls(connector)` with `DistributedConfig { tls: Some(...), .. }`.
- See `docs/GRPC_MIGRATION_TODO.md` for the full checklist.
