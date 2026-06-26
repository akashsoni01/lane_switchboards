# observability_demo — logs, metrics, Grafana panels, and alerts

Single example that exercises **every** `ActorMonitor` counter, **tracing** log levels, **Prometheus** series used by [`actor-runtime.json`](../docs/grafana/actor-runtime.json), and **alert rules** in [`alerts.yml`](../docs/grafana/alerts.yml) / [`alerts-demo.yml`](../docs/grafana/alerts-demo.yml).

```bash
# Terminal 1
docker compose -f docs/grafana/docker-compose.yml up

# Terminal 2
cargo run --example observability_demo --features metrics

# Non-interactive (CI / scripted verify)
OBSERVABILITY_AUTO_EXIT=1 OBSERVABILITY_KEEP_ALIVE_SECS=12 \
  cargo run --example observability_demo --features metrics
```

---

## What it runs (9 phases)

| Phase | Triggers | Tracing / logs | Prometheus |
|-------|----------|----------------|------------|
| 1 Normal work | 5 × `AddOnly` | `INFO` phase + worker start | `lane_actor_messages_handled_total` |
| 2 Slow handle | 25ms > 15ms threshold | `WARN` slow threshold (monitor) | `lane_actor_slow_handles_total` |
| 3 Handle error | `Fail` → `Err` | supervisor restart `INFO` | `lane_actor_handle_errors_total` |
| 4 Panic | `CrashNow` | panic + `post_stop` | `lane_actor_panics_total`, `exits{reason="panic"}` |
| 5 Handle timeout | `HangForever` @ 80ms | `ERROR` timeout, `WARN` stuck | `lane_actor_handle_timeouts_total`, `exits{reason="handle_timeout"}` |
| 6 Mailbox pressure | capacity 4 + busy slow op | blocked async sends | `lane_actor_mailbox_send_blocked_total` |
| 7 In-flight hold | 16s sleep, no timeout | `in_flight=1` each second | `lane_actor_in_flight` (alert demo) |
| 8 Domain metrics | synthetic export | `INFO` | mesh / storage / remote series |
| 9 Snapshot | `ActorMonitor::all()` | stdout table | scrape + verification block |

### Phase 5 post-mortem (matches Grafana / stdout)

After `add(100,1)` then `HangForever` with an 80ms `handle_timeout`:

```
messages_handled : 2    # 1 successful add + 1 timed-out handle (record_timeout counts both)
handle_timeouts  : 1
last_handle_ms   : 81
mean_handle_ms   : 40
```

This is **expected**: `record_timeout` increments `messages_handled` for the timed-out handle (see `lane_core/src/monitor/live.rs`).

---

## Verification built into the example

After a scrape wait (`OBSERVABILITY_KEEP_ALIVE_SECS`, default 25), the example prints:

1. **Local `/metrics` checks** — compares Prometheus text to minimum expected counters.
2. **Post-mortem cross-check** — phase 5 `ActorMonitor` vs cumulative Prometheus counters.
3. **Remote Prometheus** — queries `http://127.0.0.1:9091` when Docker is running.
4. **Alert rules** — lists whether each `Lane*` rule is loaded.

All checks should show `OK` except `in_flight_peak` at scrape time (gauge is 0 after phase 7 completes; phase 7 stdout shows `in_flight=1` while the hold runs).

---

## Grafana panels covered

| Dashboard panel | Series |
|-----------------|--------|
| Actor message rate | `lane_actor_messages_handled_total` |
| Handle error ratio | errors + panics + timeouts / handled |
| Handle latency percentiles | `lane_actor_handle_duration_seconds_bucket` |
| In-flight handles | `lane_actor_in_flight` |
| Mailbox depth % | `lane_actor_mailbox_depth` / capacity |
| Mailbox wait p95 | `lane_actor_mailbox_wait_seconds_bucket` |
| Failures & slow handles | timeouts, panics, slow_handles rates |
| Supervisor restarts | `lane_supervisor_restarts_total` |
| Supervisor health | intensity + children alive |
| Exit reasons | `lane_actor_exits_total` |
| Mesh dispatch | `lane_mesh_dispatches_total` |
| Consistency latency | `lane_consistency_duration_seconds_bucket` |
| Storage quorum & WAL | `lane_storage_quorum_failures_total`, WAL bytes |

---

## Alerts

Production rules: [`docs/grafana/alerts.yml`](../docs/grafana/alerts.yml)

Demo rules (shorter `for` windows): [`docs/grafana/alerts-demo.yml`](../docs/grafana/alerts-demo.yml) — loaded automatically via [`prometheus.yml`](../docs/grafana/prometheus.yml).

| Alert | Demo trigger |
|-------|----------------|
| `LaneActorInFlightStuck` | Phase 7 — 16s hold, `in_flight=1` |
| `LaneActorHandleTimeouts` | Phase 5 — `HangForever` |
| `LaneSupervisorRestartStorm` | Phases 3–5 restarts |
| `LaneMailboxBackpressure` | Phase 6 — mailbox > 75% |
| `LaneCounterSaturated` | Not triggered (requires `usize::MAX`) |

Alerts need their `for` duration to elapse before state becomes **firing**. Check Prometheus → **Alerts** or Grafana dashboard annotations (**Lane alerts**).

---

## Run with Docker

```bash
docker compose -f docs/grafana/docker-compose.yml up
cargo run --example observability_demo --features metrics
```

| URL | Purpose |
|-----|---------|
| http://127.0.0.1:9090/metrics | Example exporter (host) |
| http://localhost:9091 | Prometheus UI |
| http://localhost:3000 | Grafana (`admin` / `admin`) |

Grafana: **Lane → Lane Actor Runtime**. Set time range to **Last 15 minutes**.

---

## Manual setup (no Docker)

Same as [`metrics_exporter.md`](./metrics_exporter.md#manual-setup-no-docker):

1. Run the example (`cargo run --example observability_demo --features metrics`).
2. Point Prometheus at `127.0.0.1:9090` (use a non-conflicting port for Prometheus itself, e.g. 9091).
3. Import [`docs/grafana/actor-runtime.json`](../docs/grafana/actor-runtime.json).
4. Copy [`alerts.yml`](../docs/grafana/alerts.yml) and [`alerts-demo.yml`](../docs/grafana/alerts-demo.yml) into your Prometheus `rule_files`.

---

## Grafana: panics/sec looks flat?

### 1. Port 9090 conflict (most common)

Prometheus scrapes `host.docker.internal:9090`. If **`metrics_exporter`** or an old demo is still running, Grafana shows **that** process (often `node="metrics_exporter"`, `panics=0`) while your new demo prints correct stats locally.

```bash
lsof -i :9090          # find the PID
kill <pid>             # stop the stale exporter
```

`observability_demo` now **fails at startup** if port 9090 is busy. Or use another port:

```bash
METRICS_ADDR=127.0.0.1:9092 cargo run --example observability_demo --features metrics
# add host.docker.internal:9092 to docs/grafana/prometheus.yml and restart prometheus
```

### 2. `rate()` hides one-shot events

The demo panics **once** in ~30s. `rate(lane_actor_panics_total[5m])` averages over 5 minutes → **~0 panics/sec** on the graph.

The dashboard **Failures & slow handles** panel now includes:

| Line | Query | What you should see |
|------|-------|---------------------|
| **panics (range)** | `increase(...[$__range])` | Spikes to **1** when you run the demo |
| panics/sec | `rate(...[$__rate_interval])` | Near zero for a single panic (expected) |

**Grafana checks:**

1. Time range: **Last 5 minutes** (not Last 15m from an old run)
2. Dashboard: **Lane → Lane Actor Runtime**
3. Prometheus → **Explore**: `sum(increase(lane_actor_panics_total[15m]))` → should be ≥ 1

### 3. Process exited before scrape

With `OBSERVABILITY_AUTO_EXIT=1`, the `/metrics` server stops when the binary exits. Prometheus needs at least one scrape **while the process is still up** (default wait 30s). Use `OBSERVABILITY_KEEP_ALIVE_SECS=30` or higher.

### 4. Recommended run order

```bash
# Terminal 1 — start FIRST, leave running
docker compose -f docs/grafana/docker-compose.yml up

# Terminal 2 — ensure 9090 is free, then run (no AUTO_EXIT while viewing Grafana)
lsof -i :9090
cargo run --example observability_demo --features metrics
# leave running until you finish viewing Grafana, then Ctrl-C
```

---

## Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `OBSERVABILITY_KEEP_ALIVE_SECS` | `30` | Wait before scrape verification (min 20) |
| `METRICS_ADDR` | `127.0.0.1:9090` | Scrape listen address |
| `OBSERVABILITY_AUTO_EXIT` | off | Set `1` to exit after verification (no Ctrl-C) |

---

## Related examples

- [`resilient_monitor.md`](./resilient_monitor.md) — focused `ActorMonitor` stdout tour (subset of phases 1–5)
- [`metrics_exporter.md`](./metrics_exporter.md) — steady ping traffic for dashboard soak tests
- [`lane_core/monitor.md`](../lane_core/monitor.md) — production integration guide
