# Criterion baselines (messenger)

Commit these numbers after each release so regressions are visible in review.

## How to refresh

```bash
cargo bench --bench messenger_codec -- --save-baseline v0.9.2
cargo bench --bench messenger_routing -- --save-baseline v0.9.2
cargo bench --bench messenger_cluster -- --save-baseline v0.9.2
```

Criterion writes under `target/criterion/`. Copy summary lines into the tables
below (do not commit the full `target/` tree).

## messenger_codec (localhost, Jul 2026)

| Metric | v0.9.2 (approx) |
|--------|-----------------|
| encode+decode ChatMessage | ~740 ns |
| binary frame size (hello) | ~102 B vs ~159 B XML |

## messenger_routing (single-node ServerAck)

| Metric | v0.9.2 (approx) |
|--------|-----------------|
| online chat → ServerAck p50 | record after first CI bench run |
| online chat → ServerAck p99 | record after first CI bench run |

## messenger_cluster (2-node)

| Metric | v0.9.2 (approx) |
|--------|-----------------|
| cross-node offline ack p50 | ~2–4 ms |
| cross-node offline ack p99 | ~8–15 ms |

Source: `docs/messenger/09_operations.md` and bench module docs.
