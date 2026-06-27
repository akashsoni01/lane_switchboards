# Actor monitoring & Prometheus (`lane_core`)

Guide for **`lane_core` only** — connect to an **existing** Prometheus + Grafana stack, keep hot actors fast, and migrate from the [`resilient_monitor`](../examples/resilient_monitor.rs) in-process demo to exported metrics.

---

## Feature flags (quick reference)

| Cargo feature | What runs on the message hot path |
|---------------|-------------------------------------|
| *(none)* | Nothing — fastest default |
| `monitor` | In-process `ActorMonitor` counters |
| `metrics` | `monitor` + Prometheus series (`lane_*`) |

| Per-actor (`ActorConfig`) | Effect (requires `monitor` feature) |
|---------------------------|-------------------------------------|
| `monitor_enabled: true` (default) | Stats + enqueue timestamps for this actor |
| `monitor_enabled: false` / `.without_monitor()` | No stats, plain mailbox envelopes |

| Env / config | Default | Shared Prometheus / Grafana impact |
|--------------|---------|-------------------------------------|
| *(unset)* | `lane_*` metric names | **None** — same as before; works with `docs/grafana/actor-runtime.json` |
| `LANE_METRICS_PREFIX` / `metric_prefix` | opt-in | **This process only** — other scrape targets unchanged; update *your* dashboards if you set a custom prefix |
| `LANE_NODE` / `MetricsConfig.node` | `unknown` | Label on your series only; filter in Grafana with `node=` |
| `METRICS_ADDR` (examples) | `127.0.0.1:9090` | Listen port for **this binary** only; does not affect other exporters |

**Do not** set `LANE_METRICS_PREFIX` in a shared cluster ConfigMap unless every lane app in that namespace should drop `lane_*` names. For multiple lane services on one Prometheus, prefer **labels** (`service`, `node`) on the default `lane_*` names, or set a **per-deployment** prefix (e.g. `orders_lane`, `billing_lane`) so names stay unique without colliding with non-lane metrics.

---

## Part 1 — Send metrics to your existing Prometheus / Grafana

You already have Prometheus scraping and Grafana dashboards. You only need your **`lane_core` binary to expose a `/metrics` endpoint** (or hook into one you already serve).

### Step 1 — Enable the feature in `Cargo.toml`

```toml
[dependencies]
lane_core = { path = "../lane_core", features = ["metrics"] }
tokio = { version = "1", features = ["full"] }
async-trait = "0.1"
# optional: tracing-subscriber for logs
```

`metrics` turns on `monitor` automatically — you do not need both features.

### Step 2 — Set process labels once at startup

Labels must match what your Grafana dashboards filter on (often `node`, `env`, `dc`).

```rust
use lane_core::metrics::{init_metrics, MetricsConfig};

#[tokio::main]
async fn main() {
    init_metrics(MetricsConfig {
        node: Some("my-service-pod-1".into()),
        dc: Some("us-east-1".into()),
        environment: Some("production".into()),
        ..Default::default()
    });

    // ... spawn actors, run your app ...
}
```

You can also set `LANE_NODE=my-host` in the environment; it is used when `MetricsConfig.node` is unset.

### Step 3 — Tag each actor you care about in Grafana

Prometheus labels come from [`ActorConfig::monitor_meta`](src/config.rs):

```rust
use lane_core::config::ActorConfig;
use lane_core::monitor::ActorMeta;

let actor_config = ActorConfig {
    monitor_meta: ActorMeta::default()
        .with_name("worker")           // → actor_name label
        .with_actor_type("Worker"),    // → actor_type label
    ..Default::default()
};
```

Pass this config when spawning (see [`spawn_with_config`](src/actor.rs)) or via `Supervisor::with_actor_config`.

### Step 4 — Expose metrics for Prometheus to scrape

**Option A — built-in HTTP server (simplest)**

```rust
use lane_core::metrics::serve_metrics_http;
use std::net::SocketAddr;

let addr: SocketAddr = "0.0.0.0:9090".parse().unwrap();
tokio::spawn(async move {
    serve_metrics_http(addr).await.expect("metrics server");
});
```

**Option B — embed in your existing HTTP server**

If you already run axum, actix, hyper, etc., call this on your metrics route:

```rust
use lane_core::metrics::render_prometheus_text;

async fn metrics_handler() -> Result<String, prometheus::Error> {
    render_prometheus_text()
}
```

Return `Content-Type: text/plain; version=0.0.4; charset=utf-8`.

**Option C — push via callback (no scrape port)**

```rust
use lane_core::metrics::{init_metrics, MetricsConfig, render_prometheus_text};
use std::sync::Arc;

init_metrics(MetricsConfig {
    on_scrape: Some(Arc::new(|body| {
        // e.g. POST to your gateway, or write to a file your agent picks up
        let _ = body;
    })),
    ..Default::default()
});

// Periodically:
let _ = render_prometheus_text();
```

### Step 5 — Add a scrape job in Prometheus

Point at your process (host + port from Step 4):

```yaml
scrape_configs:
  - job_name: lane_core_my_service
    scrape_interval: 15s
    static_configs:
      - targets: ["my-service:9090"]   # or host.docker.internal:9090 from Docker
        labels:
          service: my-service
          env: production
```

Reload Prometheus config. In **Status → Targets**, the job should be **UP**.

### Step 6 — Verify data before Grafana

```bash
curl -s http://my-service:9090/metrics | grep lane_actor
```

In Prometheus **Graph**, try:

```promql
rate(lane_actor_messages_handled_total[5m])
```

You should see non-zero rates after your actors handle messages.

### Step 7 — Wire Grafana panels

Import or copy queries from [`docs/grafana/actor-runtime.json`](../docs/grafana/actor-runtime.json). Common panels:

| Panel | PromQL |
|-------|--------|
| Message rate | `sum(rate(lane_actor_messages_handled_total[5m]))` |
| Handle p95 | `histogram_quantile(0.95, sum(rate(lane_actor_handle_duration_seconds_bucket[5m])) by (le, actor_name))` |
| Timeouts | `sum(rate(lane_actor_handle_timeouts_total[5m]))` |
| Restarts | `sum(rate(lane_supervisor_restarts_total[15m])) by (child)` |
| Mailbox pressure | `lane_actor_mailbox_depth / clamp_min(lane_actor_mailbox_capacity, 1)` |

Match your dashboard variables (`node`, `env`, etc.) to the labels you set in Steps 2–3.

### Step 8 — Supervisor metrics (if you use supervisors)

Supervisor restart counters are recorded automatically when the `metrics` feature is enabled and you use [`Supervisor`](src/supervisor.rs). Named children get `child` labels from the registry name.

No extra code beyond spawning supervised actors with `monitor_meta` on `ActorConfig`.

---

## Part 2 — Beginner checklist (copy/paste order)

1. Add `features = ["metrics"]` to `lane_core` in `Cargo.toml`.
2. Call `init_metrics(MetricsConfig { node: Some(...), .. })` at the start of `main`.
3. Set `ActorConfig::monitor_meta` for each actor type you want in Grafana.
4. Spawn actors with `spawn_with_config` or `Supervisor::with_actor_config`.
5. Start `serve_metrics_http("0.0.0.0:9090")` **or** mount `render_prometheus_text()` on your app’s `/metrics`.
6. Add Prometheus scrape target → confirm **UP**.
7. `curl /metrics` → see `lane_actor_*` lines.
8. Prometheus query → see time series.
9. Grafana panel → import JSON or paste PromQL from Part 1.

Minimal runnable reference (full repo example):

```bash
cargo run --example metrics_exporter --features metrics -p lane_switchboards
curl -s http://127.0.0.1:9090/metrics | head
```

For **`lane_core` only**, copy the pattern from [`examples/metrics_exporter.rs`](../examples/metrics_exporter.rs) but use `lane_core::` imports instead of `lane_switchboards::`.

---

## Part 3 — Keep hot actors fast (avoid monitoring overhead)

Three levels — pick the strictest you need.

### Level 1 — Default build (no Cargo features)

```toml
lane_core = { path = "lane_core" }   # no "monitor", no "metrics"
```

Zero monitor code on the hot path. Best for high-throughput pingers, routers, or fan-out workers.

### Level 2 — Monitor globally, opt out per actor

Enable stats for most actors, disable for hot ones:

```rust
let hot_config = ActorConfig::default().without_monitor();
let (pinger, _) = spawn_with_config(Pinger, None, &hot_config).await?;

let worker_config = ActorConfig {
    monitor_meta: ActorMeta::default().with_name("worker"),
    ..Default::default()
};
let (worker, _) = spawn_with_config(Worker, None, &worker_config).await?;
```

With `monitor_enabled: false`:

- No `ActorMonitor` registration
- No atomic counter updates per message
- No `Instant::now()` on enqueue (plain `Envelope` in the mailbox)

### Level 3 — Prometheus without monitoring every actor

Use `metrics` feature but set `.without_monitor()` on latency-sensitive actors. They will **not** appear in Prometheus actor series (no labels allocated at register time).

Reserve `monitor_enabled: true` + `monitor_meta` for actors you plot in Grafana (workers, calculators, supervisors).

### Summary table

| Goal | Cargo.toml | `ActorConfig` |
|------|------------|---------------|
| Maximum throughput | no features | `monitor_enabled: false` (ignored if no feature) |
| Debug one slow actor | `features = ["monitor"]` | `monitor_enabled: true` only on that actor |
| Grafana for some actors | `features = ["metrics"]` | meta + `monitor_enabled: true` on monitored set only |
| Grafana + fast side actors | `features = ["metrics"]` | `.without_monitor()` on hot paths |

---

## Part 4 — Migrate `resilient_monitor` → Prometheus metrics

The [`resilient_monitor`](../examples/resilient_monitor.rs) example uses **`ActorMonitor::global().get()`** and `println!` for phases 1–5. To export the same behaviour to Grafana, change the following.

### 4.1 — `Cargo.toml`

**Before** (monitor only, via full crate):

```bash
cargo run --example resilient_monitor --features monitor
```

**After** (`lane_core` only):

```toml
[dependencies]
lane_core = { path = "../lane_core", features = ["metrics"] }
```

Or with `lane_switchboards`:

```toml
lane_switchboards = { path = "..", features = ["metrics"] }
```

### 4.2 — Imports

Add:

```rust
use lane_core::metrics::{init_metrics, serve_metrics_http, MetricsConfig};
use lane_core::monitor::ActorMeta;
```

Keep existing monitor imports if you still want local `print_stats` during development:

```rust
use lane_core::monitor::{ActorMonitor, ActorStats};
```

### 4.3 — `main()` — init metrics + HTTP scrape

At the top of `main()` (before spawning):

```rust
init_metrics(MetricsConfig {
    node: Some("resilient_monitor".into()),
    ..Default::default()
});

let metrics_addr = "127.0.0.1:9090".parse().unwrap();
tokio::spawn(async move {
    serve_metrics_http(metrics_addr).await.expect("metrics http");
});
```

### 4.4 — `WorkerApp::start` — add `monitor_meta`

**Before** ([`resilient_monitor.rs`](../examples/resilient_monitor.rs) ~L144):

```rust
let actor_config = ActorConfig {
    handle_timeout: Some(Duration::from_millis(80)),
    slow_handle_threshold: Some(Duration::from_millis(15)),
    ..Default::default()
};
```

**After**:

```rust
let actor_config = ActorConfig {
    handle_timeout: Some(Duration::from_millis(80)),
    slow_handle_threshold: Some(Duration::from_millis(15)),
    monitor_meta: ActorMeta::default()
        .with_name("worker")
        .with_actor_type("MonitoredWorker"),
    ..Default::default()
};
```

Supervisor child name is already set in `spawn_child_spec` (`with_name` from registry) — that feeds Prometheus `child` labels for restarts.

### 4.5 — Phase behaviour → Grafana (instead of only `print_stats`)

| Demo phase | In-process (`ActorMonitor`) | Prometheus / Grafana |
|------------|----------------------------|----------------------|
| 1 Normal work | `messages_handled`, `mean_handle_ms` | `rate(lane_actor_messages_handled_total[5m])`, handle duration histogram |
| 2 Slow handle | `slow_handles++` | `rate(lane_actor_slow_handles_total[5m])` |
| 3 Panic | `panics++`, post-mortem snapshot | `rate(lane_actor_panics_total[5m])`, `lane_actor_exits_total{reason="panic"}` |
| 4 Handle timeout | `handle_timeouts++` | `rate(lane_actor_handle_timeouts_total[5m])`, `lane_actor_exits_total{reason="handle_timeout"}` |
| 5 Global snapshot | `ActorMonitor::global().all()` | Grafana table: `lane_actor_in_flight`, `lane_actor_mailbox_depth` by `actor_name` |

You can **keep** `print_stats` for local debugging; Grafana reads the same counters via scrape.

After phase 1, verify:

```bash
curl -s http://127.0.0.1:9090/metrics | grep lane_actor_messages_handled
```

### 4.6 — Optional: drop in-process printing in production

Replace phase-5 `all()` loop with dashboard panels, or keep it behind a CLI flag / `#[cfg(debug_assertions)]`.

### 4.7 — Run command

```bash
# lane_switchboards workspace
cargo run --example resilient_monitor --features metrics

# lane_core-only binary
cargo run --features metrics
```

Point your existing Prometheus at `host:9090/metrics`.

### 4.8 — Mapping cheat sheet

Full table lives in [`README.md`](README.md#prometheus-series-mapping). Key fields from `resilient_monitor`:

| `print_stats` field | Prometheus series |
|---------------------|-------------------|
| `messages_handled` | `lane_actor_messages_handled_total` |
| `panics` | `lane_actor_panics_total` |
| `handle_timeouts` | `lane_actor_handle_timeouts_total` |
| `slow_handles` | `lane_actor_slow_handles_total` |
| `in_flight` | `lane_actor_in_flight` |
| `last_handle_ms` / `max_handle_ms` | `lane_actor_last_handle_seconds`, `lane_actor_max_handle_seconds` |

Use **histogram quantiles** for latency SLOs — not `mean_handle_ms`.

---

## Troubleshooting

| Symptom | Check |
|---------|--------|
| Empty `/metrics` | Actors spawned with `monitor_enabled: false`? Feature `metrics` enabled at compile time? |
| Prometheus target DOWN | Firewall, bind address (`0.0.0.0` vs `127.0.0.1`), correct port |
| Grafana flat lines | Scrape interval passed? Actors handling messages? Query uses `rate(...[5m])` on counters? |
| Too many labels | Use stable `monitor_meta.name` / `actor_type` — not per-actor UUIDs |
| Hot path slow | `monitor_enabled: false` on throughput actors; or drop `monitor` feature entirely |

---

## Related docs

- [`README.md`](README.md) — API tables, series mapping
- [`examples/resilient_monitor.md`](../examples/resilient_monitor.md) — in-process monitor tour
- [`examples/metrics_exporter.md`](../examples/metrics_exporter.md) — minimal exporter example
- [`docs/grafana/`](../docs/grafana/) — dashboard JSON, alerts, docker-compose reference
