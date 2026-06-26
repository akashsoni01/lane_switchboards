# metrics_exporter — Prometheus `/metrics` for Grafana

Runnable demo of the `metrics` feature: supervised actor, HTTP scrape endpoint on **9090**, and steady ping traffic until you press Ctrl-C.

```bash
cargo run --example metrics_exporter --features metrics
```

---

## Run with Docker (Prometheus + Grafana)

Use two terminals. Only **one** example should bind port **9090** at a time.

### Terminal 1 — start the exporter on the host

```bash
cargo run --example metrics_exporter --features metrics
```

Leave it running. You should see:

```
metrics_exporter listening on http://127.0.0.1:9090/metrics
```

Verify:

```bash
curl -s http://127.0.0.1:9090/metrics | grep lane_actor_messages_handled
curl -s http://127.0.0.1:9090/health
```

### Terminal 2 — start Prometheus + Grafana

```bash
docker compose -f docs/grafana/docker-compose.yml up
```

| Service | URL | Notes |
|---------|-----|-------|
| Grafana | http://localhost:3000 | login `admin` / `admin` |
| Prometheus UI | http://localhost:9091 | scrapes `host.docker.internal:9090` |
| Your exporter | http://127.0.0.1:9090/metrics | runs on the host, not in Docker |

The compose file auto-provisions:

- Prometheus datasource in Grafana
- [`actor-runtime.json`](../docs/grafana/actor-runtime.json) dashboard under **Lane → Lane Actor Runtime**

Open Grafana → **Lane → Lane Actor Runtime**. Panels should show message rate and mailbox stats within ~30s (scrape interval 10s).

### Optional — run the monitor tour against the same stack

In a **third** terminal (stop `metrics_exporter` first — both use port 9090):

```bash
cargo run --example resilient_monitor --features metrics
```

For **all** log types, alert demos, and built-in Prometheus verification, use [`observability_demo`](./observability_demo.md) instead.

---

## Manual setup (no Docker)

### 1. Run the exporter

```bash
cargo run --example metrics_exporter --features metrics
```

### 2. Install Prometheus

**macOS (Homebrew):**

```bash
brew install prometheus
```

**Linux:** download from [prometheus.io](https://prometheus.io/download/) or your package manager.

Create `prometheus.yml` (or append a job to your existing config):

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: lane_metrics_exporter
    static_configs:
      - targets: ["127.0.0.1:9090"]
        labels:
          env: local
```

Start Prometheus (default UI port **9090** conflicts with the exporter — use a different port for Prometheus):

```bash
prometheus --config.file=prometheus.yml --web.listen-address=:9091
```

Open http://localhost:9091 → **Status → Targets** → job should be **UP**.

Query: `rate(lane_actor_messages_handled_total[1m])`

### 3. Install Grafana

**macOS:**

```bash
brew install grafana
brew services start grafana
```

**Linux:** see [grafana.com/docs](https://grafana.com/docs/grafana/latest/setup-grafana/installation/).

Open http://localhost:3000 (default `admin` / `admin` on first login).

### 4. Add Prometheus datasource

1. **Connections → Data sources → Add data source → Prometheus**
2. URL: `http://localhost:9091` (your Prometheus listen address)
3. **Save & test**

### 5. Import dashboard

1. **Dashboards → New → Import**
2. Upload [`docs/grafana/actor-runtime.json`](../docs/grafana/actor-runtime.json)
3. Select the Prometheus datasource → **Import**

---

## Application integration

Set process labels once at startup:

```rust
init_metrics(MetricsConfig {
    node: Some("my-node".into()),
    dc: Some("us-east-1".into()),
    ..Default::default()
});
```

Tag actors via [`ActorConfig::monitor_meta`](../lane_core/src/config.rs).

## Useful PromQL

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

## Related

- [`resilient_monitor.md`](./resilient_monitor.md) — in-process `ActorMonitor` tour (+ optional `--features metrics`)
- [`lane_core/monitor.md`](../lane_core/monitor.md) — connect `lane_core` to your existing Prometheus/Grafana
- [`docs/todo.md`](../docs/todo.md) — roadmap for alerts and extra panels
