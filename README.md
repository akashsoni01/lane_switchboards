# lane_switchboards

OTP-style actor runtime in Rust: actors, supervision, linking, monitoring, distributed messaging, and hot code upgrade.

See **[architecture.md](./architecture.md)** for Mermaid diagrams and module breakdown.

## Library (`src/`)

| Module | Capability |
|--------|------------|
| `actor.rs` | Actors, linking, monitoring, hot upgrade |
| `supervisor.rs` | OneForOne / OneForAll / RestForOne |
| `registry.rs` | Global `DashMap` actor index |
| `distributed.rs` | TCP-framed remote actors |

## Examples

| Example | Command |
|---------|---------|
| Hot code upgrade | `cargo run --example hot_upgrade` |
| Distributed messaging | `cargo run --example distributed_demo` |

Gateway (after `cargo run --example gateway`):

- `GET http://127.0.0.1:8080/health`
- `GET http://127.0.0.1:8080/fetch` — single request (pooled connection reuse)
- `GET http://127.0.0.1:8080/fetch-parallel` — parallel GETs to multiple httpbin endpoints

```rust
// Parallel calls from code
handle.get_parallel(["https://api.a.com/x", "https://api.b.com/y"]).await?;
handle.execute_parallel(vec![/* RestRequest */]).await?;
```

## Tests

```bash
cargo test
```
