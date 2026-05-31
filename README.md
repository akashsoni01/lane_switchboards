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
| Actix gateway + Ractor RestTemplate | `cargo run --example gateway` |
| Hot code upgrade | `cargo run --example hot_upgrade` |
| Envelope variants (link, monitor, upgrade, …) | `cargo run --example envelope_demo` — see [envelope_demo.md](examples/envelope_demo.md) |
| Calculator (add, sub, mul, div) | `cargo run --example calculator` — see [calculator.md](examples/calculator.md) |
| Resilient calculator (survives panic) | `cargo run --example resilient_calculator` — see [resilient_calculator.md](examples/resilient_calculator.md) |
| Resilient calculator + last-result timer | `cargo run --example resilient_calculator_timer` |
| Recoverable calculator + journal timer | `cargo run --example recoverable_timer_calc` |
| Distributed messaging | `cargo run --example distributed_demo` |
| ONDC signing (testecom → dummy server) | `cargo run --example ondc_demo` — see [ondc.md](examples/gateway/ondc.md) |

Gateway (after `cargo run --example gateway`):

- `GET http://127.0.0.1:8080/health`
- `GET http://127.0.0.1:8080/fetch` — single request (pooled connection reuse)
- `GET http://127.0.0.1:8080/fetch-parallel` — parallel GETs to multiple httpbin endpoints

```rust
// Parallel calls from code
handle.get_parallel(["https://api.a.com/x", "https://api.b.com/y"]).await?;
handle.execute_parallel(vec![/* RestRequest */]).await?;
```

RestTemplate lives under `examples/gateway/` — see **[rest_template.md](examples/gateway/rest_template.md)** (architecture, connection pooling, **security / signing**).

## Tests

```bash
cargo test
```
