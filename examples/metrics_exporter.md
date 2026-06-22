# metrics_exporter — Prometheus `/metrics` for Grafana

Runnable demo of the `metrics` feature: supervised actor + HTTP scrape endpoint.

```bash
cargo run --example metrics_exporter --features metrics
```

## Scrape

```bash
curl -s http://127.0.0.1:9090/metrics | head
curl -s http://127.0.0.1:9090/health
```

Set process labels once at startup:

```rust
init_metrics(MetricsConfig {
    node: Some("my-node".into()),
    dc: Some("us-east-1".into()),
    ..Default::default()
});
```

Tag actors via [`ActorConfig::monitor_meta`](../lane_core/src/config.rs).

## Prometheus → Grafana

1. Run the exporter: `cargo run --example metrics_exporter --features metrics` (host port **9090**).
2. Start the observability stack:

```bash
docker compose -f docs/grafana/docker-compose.yml up
```

3. Grafana: http://localhost:3000 (admin / admin) — add Prometheus datasource `http://prometheus:9090`.
4. Import [`docs/grafana/actor-runtime.json`](../docs/grafana/actor-runtime.json).
5. Prometheus UI: http://localhost:9091 (alert rules in [`alerts.yml`](../docs/grafana/alerts.yml)).

Useful PromQL:
   - `rate(lane_actor_messages_handled_total[5m])`
   - `histogram_quantile(0.95, sum(rate(lane_actor_handle_duration_seconds_bucket[5m])) by (le))`
   - `lane_actor_mailbox_depth / lane_actor_mailbox_capacity`
   - `rate(lane_supervisor_restarts_total[15m])`

## Storage / consistency (optional)

From application code before scrape:

```rust
sync_storage_stats(&StorageMetricsSnapshot {
    node: "node-1".into(),
    puts_total: stats.puts_total,
    // ...
    ..Default::default()
});
```

Mesh consistency ops are recorded automatically when `features = ["metrics"]` (see `emit_metrics` in `consistency.rs`).

## Roadmap

See [`docs/todo.md`](../docs/todo.md) for remaining panels, alerts, and docker-compose.
