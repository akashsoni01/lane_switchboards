# lane_switchboards v0.0.5

Release notes for **v0.0.5** — optional TLS/SSL for distributed and mesh TCP (rustls), plus the `tls_distributed` example and docs.

For the full project overview see [README.md](./README.md).  
Previous release notes: [READMEv0.0.4.md](./READMEv0.0.4.md) · [READMEv0.0.3.md](./READMEv0.0.3.md) · [Ideas blog post](./docs/lane_switchboards_blog.md)

---

## What's new in v0.0.5

### 1) New `src/tls.rs` — rustls + tokio-rustls

Optional TLS wraps the existing length-prefixed frame protocol. Plain TCP remains the default when no acceptor/connector is supplied.

| Type / fn | Role |
|-----------|------|
| `MaybeTlsStream` | `Plain(TcpStream)` or `Tls(TlsStream<TcpStream>)` — used internally for read/write |
| `tls_connect` / `tls_accept` | Upgrade a connected `TcpStream` |
| `server_config_from_pem` | Server cert + key; optional client CA for mTLS |
| `client_config_from_pem` | Custom CA or webpki-roots; optional client cert |
| `build_acceptor` / `build_connector` | `Arc`-wrapped Tokio TLS wrappers |
| `load_certs`, `load_private_key`, `load_ca_store` | PEM file helpers |
| `host_from_addr` | Parse `"host:port"` for SNI / name validation |

Crypto provider (`aws-lc-rs`) is installed automatically on first config build.

**Dependencies added:**

```toml
rustls = "=0.23.26"
tokio-rustls = "=0.26.1"
rustls-pemfile = "=2.2.0"
webpki-roots = "=0.26.8"
rcgen = "0.13"          # used by tls_distributed example for ephemeral certs
```

---

### 2) Distributed data plane — TLS hooks

`src/distributed.rs` — same framing and reconnect loop; TLS negotiated per connection:

| API | When |
|-----|------|
| `Node::bind_tls_on_runtime(..., Arc<TlsAcceptor>)` | TLS listener |
| `RemoteActorRef::with_tls(..., Arc<TlsConnector>)` | TLS outbound persistent writer |
| `serve_actor_tls_on_runtime(..., Arc<TlsAcceptor>)` | Bind + bridge actor over TLS |
| `Cluster::set_tls_connector` | TLS for all `join()` remote refs |
| `ClusterMember::remote_ref_with(config, tls)` | Per-member TLS ref |

`serve_actor_on_runtime` gains an optional final parameter `tls: Option<Arc<TlsAcceptor>>` (pass `None` for plain TCP — existing call sites unchanged via wrapper functions).

---

### 3) Service mesh — TLS on control + data plane

`src/mesh.rs`:

| API | Layer |
|-----|-------|
| `MeshRegistryServer::bind_tls(addr, acceptor)` | Control plane registry |
| `MeshRegistryClient::with_tls(addr, connector)` | Persistent registry client |
| `MeshRouter::with_registry_tls(addr, connector)` | Router sync over TLS |
| `serve_microservice_tls(..., acceptor)` | Data-plane microservice instance |

Control-plane read/write uses the same 64 KiB frame cap and 30s read timeout as v0.0.4.

---

### 4) Example + documentation

| Artifact | Description |
|----------|-------------|
| [`examples/tls_distributed.rs`](examples/tls_distributed.rs) | Two-node ping over TLS; ephemeral localhost certs |
| [`examples/tls_distributed.md`](examples/tls_distributed.md) | Architecture + sequence mermaid, cert notes, troubleshooting |

```bash
cargo run --example tls_distributed
```

---

### 5) Library exports

New module and re-exports from [`src/lib.rs`](src/lib.rs):

```rust
pub mod tls;

pub use distributed::{serve_actor_tls_on_runtime, TlsAcceptor, /* ... */};
pub use mesh::{serve_microservice_tls, /* ... */};
pub use tls::{
    build_acceptor, build_connector, client_config_from_pem, server_config_from_pem,
    tls_connect, tls_accept, MaybeTlsStream, TlsConnector,
    load_certs, load_private_key, load_ca_store, host_from_addr,
};
```

---

## Migration notes from v0.0.4

### No action required for plain TCP

All v0.0.4 APIs continue to work without TLS. `Node::bind`, `RemoteActorRef::new`, `serve_actor`, `MeshRegistryServer::bind`, and `MeshRegistryClient::new` default to plain TCP.

### Enabling TLS on an existing deployment

1. Provision server cert/key (and CA for clients if not using public roots).
2. Replace bind/connect pairs:

```rust
// Server
let acceptor = Arc::new(build_acceptor(server_config_from_pem("server.crt", "server.key", None)?));
let node = Node::bind_tls_on_runtime(&handle, "worker", "0.0.0.0:9001", &config, acceptor).await?;

// Client
let connector = Arc::new(build_connector(client_config_from_pem(Some("ca.crt"), None, None)?));
let remote = RemoteActorRef::with_tls(&addr, "worker", &config, connector);
```

3. **Both ends must use TLS** — a TLS client cannot talk to a plain TCP listener and vice versa.

### SNI and certificate names

The TLS client uses the **host** from `"host:port"` as the server name. Certificates must include matching DNS or IP SANs. Connecting to `127.0.0.1:9001` requires an IP SAN; `localhost:9001` requires a DNS SAN for `localhost`.

### Cluster TLS

Set the connector once on the roster:

```rust
let mut cluster = Cluster::new();
cluster.set_tls_connector(Some(connector));
cluster.join(member);  // remote refs use TLS
```

---

## Tests and checks

Validated on v0.0.5 changes:

- `cargo test` ✅ (includes `tls::tests::tls_round_trip`)
- `cargo run --example tls_distributed` ✅

Test count: **23** (unit + integration + supervisor + TLS round-trip).

---

## Quick TLS reference

```rust
use lane_switchboards::tls::{build_acceptor, build_connector, client_config_from_pem, server_config_from_pem};
use lane_switchboards::distributed::{Node, RemoteActorRef};
use lane_switchboards::config::DistributedConfig;
use std::sync::Arc;

let acceptor = Arc::new(build_acceptor(server_config_from_pem("cert.pem", "key.pem", None)?));
let connector = Arc::new(build_connector(client_config_from_pem(Some("ca.pem"), None, None)?));
let config = DistributedConfig::default();

let node = Node::bind_tls_on_runtime(&handle, "n", "127.0.0.1:0", &config, acceptor).await?;
let remote = RemoteActorRef::with_tls(node.address(), "target", &config, connector);
```

---

## File map (v0.0.5 touch points)

| File | Changes |
|------|---------|
| `src/tls.rs` | **New** — `MaybeTlsStream`, PEM loaders, connect/accept |
| `src/distributed.rs` | TLS on bind, accept, remote write loop, cluster connector |
| `src/mesh.rs` | TLS registry server/client, `serve_microservice_tls` |
| `src/lib.rs` | `pub mod tls` + re-exports |
| `Cargo.toml` | rustls stack, `rcgen`, `tls_distributed` example |
| `examples/tls_distributed.rs` | **New** example |
| `examples/tls_distributed.md` | **New** docs with mermaid |
