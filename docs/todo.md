# Grafana / Prometheus metrics — implementation plan

Roadmap for exposing `ActorMonitor` (and related runtime stats) to **Grafana** via **Prometheus** (or OpenTelemetry). Work through phases in order; later phases depend on metadata and export plumbing from earlier ones.

**Related code today**

| Area | Location | Notes |
|------|----------|-------|
| Actor counters | `lane_core/src/monitor.rs` | `ActorStats`, `usize`, saturating atomics |
| Actor lifecycle | `lane_core/src/actor.rs` | `begin_handle`, `finish_handle`, `register` / `unregister` |
| Supervisor | `lane_core/src/supervisor.rs` | restarts, `ChildRegistry`, generation |
| Consistency callbacks | `src/consistency.rs` | `ConsistencyMetrics` behind `metrics` feature |
| Storage stats | `src/storage/mod.rs` | `StorageStats` snapshot |
| Existing feature flag | `Cargo.toml` | `metrics = []` (consistency only today) |

**Target stack**

```text
Hot path (atomics in monitor)  →  scrape / push (5–15s)  →  Prometheus  →  Grafana
```

---

## Design principles

- [ ] **Hot path stays cheap** — atomics + CAS only; no locks, HTTP, or string formatting per message.
- [ ] **Scrape-time aggregation** — histogram bucket updates are O(1); label strings built only when exporting.
- [ ] **Stable labels** — prefer `actor_name`, `actor_type`, `supervisor` over raw `ActorId` (ids change on restart).
- [ ] **Bounded cardinality** — no unbounded message-type or request-id labels; use enums or top-N recording rules.
- [ ] **Feature-gated** — extend existing `metrics` feature (or add `metrics-prometheus` sub-feature) so default builds stay lean.
- [ ] **Mirror existing patterns** — follow `ConsistencyMetrics` + callback style where a push model fits; prefer pull `/metrics` for long-running nodes.

---

## Phase 0 — Decisions & scaffolding

- [ ] **Choose export mechanism**
  - [ ] Option A (recommended): Prometheus text exposition via `prometheus` crate + optional HTTP server (`hyper` / `axum` / reuse `actix` in examples only).
  - [ ] Option B: OpenTelemetry metrics (`opentelemetry` + OTLP exporter) for shops already on Grafana Cloud / Tempo stack.
  - [ ] Document choice in `lane_core/README.md` and root `README.md`.
- [ ] **Extend Cargo features**
  - [ ] `lane_core/Cargo.toml`: optional `metrics` dep on `prometheus` (and `http` helper if needed).
  - [ ] Root `Cargo.toml`: wire `metrics = ["lane_core/metrics", …]` and document in feature table.
- [ ] **Metric naming convention** — prefix all series with `lane_`:
  - [ ] Counters: `lane_*_total`
  - [ ] Gauges: `lane_*` (no suffix)
  - [ ] Histograms: `lane_*_seconds` with `_bucket`, `_sum`, `_count`
- [ ] **Create module skeleton**
  - [ ] `lane_core/src/metrics.rs` (registry, export helpers) — or `lane_core/src/monitor/export.rs`
  - [ ] Re-export from `lane_core/src/lib.rs` behind `#[cfg(feature = "metrics")]`
  - [ ] Re-export from `src/lib.rs` when feature enabled

---

## Phase 1 — Actor metadata & labels

Today stats are keyed only by `ActorId`. Grafana needs human-readable, restart-stable labels.

- [ ] **Define `ActorMeta` (or `MonitorLabels`)**
  ```rust
  pub struct ActorMeta {
      pub name: Option<String>,       // e.g. "calculator" from ChildRegistry
      pub actor_type: Option<String>, // e.g. "Calculator"
      pub supervisor_id: Option<ActorId>,
      pub node: Option<String>,       // env: LANE_NODE, k8s pod, DC
      pub service: Option<String>,    // mesh service name if applicable
  }
  ```
- [ ] **Store metadata in `ActorMonitor`**
  - [ ] `register(id, meta)` or `register(id)` + `set_meta(id, meta)`
  - [ ] Keep metadata in `RwLock<HashMap<ActorId, ActorMeta>>` (write on spawn only)
  - [ ] Copy meta into post-mortem snapshot on `unregister`
- [ ] **Wire spawn paths**
  - [ ] `lane_core/src/actor.rs` — accept optional meta from `spawn` / `spawn_with_config`
  - [ ] `lane_core/src/supervisor.rs` — pass child name + strategy into meta on `spawn_child_spec`
  - [ ] `ChildRegistry` / `ChildSlot` — helper `monitor_meta(name)` for examples
- [ ] **Global static labels**
  - [ ] `MetricsConfig { node, dc, environment }` set once at process start
  - [ ] Applied to every exported series in scrape handler
- [ ] **Tests**
  - [ ] Meta survives live → post-mortem transition
  - [ ] Re-register same id clears old post-mortem (existing behavior preserved)

---

## Phase 2 — Core actor metrics (extend `monitor.rs`)

### 2a — Counters (map existing fields + new)

| Task | Prometheus name | Source |
|------|-----------------|--------|
| [ ] | `lane_actor_messages_handled_total` | existing `messages_handled` |
| [ ] | `lane_actor_handle_errors_total` | existing `handle_errors` |
| [ ] | `lane_actor_panics_total` | existing `panics` |
| [ ] | `lane_actor_handle_timeouts_total` | existing `handle_timeouts` |
| [ ] | `lane_actor_slow_handles_total` | existing `slow_handles` |
| [ ] | `lane_actor_counter_saturated_total{field}` | new — on `usize::MAX` clamp |
| [ ] | `lane_actor_exits_total{reason}` | new — on `unregister`, map `ExitReason` |
| [ ] | `lane_actor_mailbox_send_rejected_total` | new — failed `send` / full mailbox |

- [ ] **Pass `ExitReason` into `unregister`**
  - [ ] Thread reason from actor loop exit through `ActorMonitor::unregister(id, reason)`
  - [ ] Normalize reason label: `normal`, `shutdown`, `killed`, `handle_timeout`, `panic`, `error`, `linked`
- [ ] **Dual-write strategy**
  - [ ] Keep `ActorStats` / `StatsCell` for in-process API (`get`, `all`, examples)
  - [ ] When `metrics` feature on, also increment Prometheus `IntCounterVec` with labels from `ActorMeta`

### 2b — Gauges

| Task | Prometheus name | Source |
|------|-----------------|--------|
| [ ] | `lane_actor_in_flight` | existing |
| [ ] | `lane_actor_last_handle_ms` | existing (or seconds gauge) |
| [ ] | `lane_actor_max_handle_ms` | existing |
| [ ] | `lane_actor_alive` | 1 while in `cells`, 0 in post-mortem |
| [ ] | `lane_actor_uptime_seconds` | new — `Instant` at register, exported at scrape |
| [ ] | `lane_actor_mailbox_depth` | new — see Phase 4 |
| [ ] | `lane_actor_mailbox_capacity` | new — from `ActorConfig` |
| [ ] | `lane_actor_idle_seconds` | new — time since last `finish_handle` |

- [ ] **Register time per actor** — `registered_at: Instant` in cell or side map

### 2c — Histograms (latency — required for Grafana percentiles)

- [ ] **`lane_actor_handle_duration_seconds`**
  - [ ] Observe in `finish_handle` and `record_timeout` (wall time including stuck handler)
  - [ ] Default buckets: `0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10`
  - [ ] Configurable via `MetricsConfig::handle_duration_buckets`
- [ ] **Optional: `lane_actor_mailbox_wait_seconds`**
  - [ ] Time from message enqueue to `begin_handle` (Phase 4)

- [ ] **Deprecate mean for dashboards** — keep `mean_handle_ms` on `ActorStats` for debug; document that Grafana should use histogram quantiles

### 2d — `ActorStats` API updates

- [ ] Add optional fields to snapshot (feature-gated or always cheap):
  - [ ] `registered_at`, `last_handle_at`, `exit_reason`
- [ ] Update `lane_core/README.md` field table
- [ ] Update `examples/resilient_monitor.md`

---

## Phase 3 — Supervisor metrics

New `SupervisorMonitor` or extend `ActorMonitor` with supervisor-scoped counters.

- [ ] **`lane_supervisor_restarts_total{child, strategy}`**
  - [ ] Hook `RestartSignal` handling in `supervisor.rs`
- [ ] **`lane_supervisor_child_generation{child}`** — gauge from `ChildRegistry`
- [ ] **`lane_supervisor_children_alive`** — gauge per supervisor
- [ ] **`lane_supervisor_restart_intensity_remaining`** — gauge: `max_restarts - restarts_in_window`
- [ ] **`lane_supervisor_intensity_exceeded_total`** — counter when `IntensityAction` fires
- [ ] **Info metric (optional)** — `lane_supervisor_info{strategy="rest_for_one", max_restarts="3"}`

- [ ] **Tests** — restart bumps counter and generation; intensity window resets

---

## Phase 4 — Mailbox & queue metrics

Requires access to `mpsc` channel depth (not exposed by Tokio today).

- [ ] **Research / implement queue depth**
  - [ ] Option A: wrap `mpsc::Sender` with atomic enqueue counter (increment on send, decrement on recv in actor loop)
  - [ ] Option B: periodic `Sender::capacity()` if using bounded channel API that exposes len (Tokio 1.x: track manually)
- [ ] **`lane_actor_mailbox_depth`** gauge — current queued messages
- [ ] **`lane_actor_mailbox_send_blocked_total`** — if using async `send().await` wait (optional)
- [ ] **Mailbox wait histogram** — timestamp on enqueue in envelope, observe at `begin_handle`

- [ ] **Update `lane_core/src/actor.rs`**
  - [ ] Envelope carries `enqueued_at: Instant` (behind `metrics` cfg to avoid size cost when disabled)

---

## Phase 5 — Prometheus export layer

- [ ] **`MetricsRegistry` singleton** (process-global, like `ActorMonitor::global()`)
  - [ ] Register all `IntCounterVec`, `GaugeVec`, `HistogramVec` at startup
  - [ ] `fn gather() -> String` — Prometheus text format
- [ ] **Scrape handler**
  - [ ] `lane_core`: `metrics::render_text()` for embedding in any HTTP server
  - [ ] `lane_switchboards`: optional `serve_metrics(addr)` example helper using `hyper` or document integration with existing gRPC server (secondary port `:9090`)
- [ ] **Scrape loop integration**
  - [ ] Walk `ActorMonitor::all()` + meta map each scrape to sync gauge values (alive, uptime, idle)
  - [ ] Counters/histograms updated on hot path — scrape only reads
- [ ] **Push model (optional, lower priority)**
  - [ ] Callback `MetricsConfig::on_scrape` for custom sinks
  - [ ] Bridge `ConsistencyMetrics` → same registry

- [ ] **Tests**
  - [ ] Render output contains expected `# HELP` / `# TYPE`
  - [ ] Labels present after spawn with meta
  - [ ] Integration test: spawn actor, handle messages, scrape, assert counters > 0

---

## Phase 6 — Mesh, distributed & storage metrics

Unify under `lane_` prefix; reuse existing stats where possible.

### 6a — Consistency (已有 `ConsistencyMetrics`)

- [ ] **`lane_consistency_operations_total{service, level, result}`**
- [ ] **`lane_consistency_duration_seconds`** histogram
- [ ] **`lane_consistency_acks_required`** / **`lane_consistency_acks_received`** gauges or histogram
- [ ] Default exporter: register callback in `ConsistencyConfig::on_metrics` when feature enabled
- [ ] Document migration from custom callback to built-in Prometheus series

### 6b — Storage (`StorageStats`)

- [ ] Map `StorageStats` fields to counters/gauges on scrape:
  - [ ] `lane_storage_puts_total`, `gets_total`, `deletes_total`
  - [ ] `lane_storage_read_repairs_total`, `paxos_writes_total`, `quorum_failures_total`
  - [ ] `lane_storage_wal_bytes_written_total`
  - [ ] `lane_storage_live_records`, `tombstone_count` gauges
- [ ] Hook in `StorageNode::stats()` scrape path

### 6c — Mesh / distributed (later)

- [ ] `lane_mesh_dispatches_total{service}`
- [ ] `lane_remote_send_total` / `lane_remote_ack_timeouts_total`
- [ ] `lane_grpc_requests_total` (if not covered by tonic middleware)

---

## Phase 7 — Grafana dashboards & alerts

Deliver as JSON in repo (e.g. `docs/grafana/actor-runtime.json`).

### Dashboard: Actor health

- [ ] Panel — message rate: `rate(lane_actor_messages_handled_total[5m])`
- [ ] Panel — error ratio: `(errors + panics + timeouts) / handled`
- [ ] Panel — p50 / p95 / p99 handle latency from histogram
- [ ] Panel — in-flight handles (table of actors > 0)
- [ ] Panel — slow handle rate
- [ ] Panel — mailbox depth % of capacity

### Dashboard: Supervisor & resilience

- [ ] Panel — restarts per child over time
- [ ] Panel — generation (step chart)
- [ ] Panel — exit reasons (pie / bar)
- [ ] Panel — intensity remaining

### Dashboard: Storage & consistency

- [ ] Panel — quorum failure rate
- [ ] Panel — consistency op latency by service
- [ ] Panel — WAL growth

### Alert rules (PrometheusRule or Grafana)

- [ ] `lane_actor_in_flight > 0` for 30s → possible deadlock
- [ ] `rate(lane_actor_handle_timeouts_total[5m]) > 0` → sustained timeouts
- [ ] `rate(lane_supervisor_restarts_total[15m]) > threshold` → restart storm
- [ ] `lane_actor_mailbox_depth / lane_actor_mailbox_capacity > 0.9` → backpressure
- [ ] `rate(lane_actor_counter_saturated_total[1h]) > 0` → counter overflow (investigate)

- [ ] **Runbook links** in dashboard annotations → `examples/resilient_monitor.md`, `handle_timeout_calculator_timer.md`

---

## Phase 8 — Examples & documentation

- [ ] **New example**: `examples/metrics_exporter.rs`
  - [ ] Spawn supervised actors from `resilient_monitor` pattern
  - [ ] Serve `/metrics` on `:9090`
  - [ ] Print scrape URL and sample PromQL queries
- [ ] **New example doc**: `examples/metrics_exporter.md`
  - [ ] docker-compose snippet: Prometheus + Grafana
  - [ ] Import dashboard JSON steps
- [ ] **Update existing docs**
  - [ ] `lane_core/README.md` — metrics feature, export API, label guide
  - [ ] `README.md` — feature flag, Grafana section link
  - [ ] `examples/resilient_monitor.md` — point to Prometheus series mapping table
  - [ ] `READMEv0.9.0.md` or next release notes — metrics milestone
- [ ] **Prometheus series mapping table** (add to `lane_core/README.md`)

| `ActorStats` field | Prometheus series |
|--------------------|-------------------|
| `messages_handled` | `lane_actor_messages_handled_total` |
| `handle_errors` | `lane_actor_handle_errors_total` |
| … | … |

---

## Phase 9 — Tests, benchmarks & CI

- [ ] Unit tests in `lane_core/src/monitor.rs` / `metrics.rs` (saturate, histogram observe, labels)
- [ ] Integration test: `tests/metrics_export.rs` behind `metrics` feature
- [ ] Benchmark: monitor overhead with Prometheus counters enabled vs disabled
  - [ ] Extend or mirror `handle_timeout_calculator_timer_latency` methodology
  - [ ] Target: < 10% overhead vs current monitor-only path
- [ ] CI job: `cargo test --features metrics` and `cargo check --examples --features metrics`
- [ ] Optional: promtool check rules on `docs/grafana/alerts.yml`

---

## File checklist (expected touches)

| File | Phase |
|------|-------|
| `lane_core/Cargo.toml` | 0 |
| `lane_core/src/lib.rs` | 0, 1 |
| `lane_core/src/monitor.rs` | 2, 4 |
| `lane_core/src/metrics.rs` (new) | 0, 5 |
| `lane_core/src/actor.rs` | 1, 2, 4 |
| `lane_core/src/supervisor.rs` | 1, 3 |
| `lane_core/src/config.rs` | 0, 2 (`MetricsConfig`) |
| `Cargo.toml` (root) | 0 |
| `src/lib.rs` | 0, 5 |
| `src/consistency.rs` | 6a |
| `src/storage/mod.rs` | 6b |
| `examples/metrics_exporter.rs` (new) | 8 |
| `examples/metrics_exporter.md` (new) | 8 |
| `docs/grafana/*.json` (new) | 7 |
| `docs/todo.md` | — (this file; check off as you go) |

---

## Non-goals (explicitly out of scope for v1)

- [ ] Per-message-type labels with unbounded cardinality
- [ ] Using raw `ActorId` as the primary Grafana label
- [ ] Storing full post-mortem history in Prometheus (keep in-process post-mortem + logs)
- [ ] Replacing `tracing` — logs for events, metrics for aggregates
- [ ] Grafana Cloud account provisioning / Terraform (document only; optional follow-up)

---

## Suggested implementation order (summary)

1. **Phase 0** — feature flag + `metrics` module skeleton  
2. **Phase 1** — `ActorMeta` + supervisor wiring  
3. **Phase 2c** — handle duration histogram (biggest Grafana win)  
4. **Phase 2a/2b** — counters + gauges + exit reason  
5. **Phase 5** — `/metrics` render + test  
6. **Phase 3** — supervisor counters  
7. **Phase 4** — mailbox depth  
8. **Phase 6** — storage + consistency  
9. **Phase 7–8** — dashboards, example, docs  
10. **Phase 9** — CI + benchmark  

---

## Progress log

| Date | Phase | Notes |
|------|-------|-------|
| 2026-06-11 | 0–2, 5 (partial) | `metrics` feature, `lane_core/src/metrics.rs`, `ActorMeta`, Prometheus counters/histogram/gauges, `render_prometheus_text()`, `examples/metrics_exporter.rs` |
| 2026-06-11 | 3, 4, 6a/6b (partial), 7 (starter) | Supervisor metrics, mailbox depth, storage/consistency export, `docs/grafana/actor-runtime.json` |
| 2026-06-11 | 4, 5, 6c, 7, 8, 9 (partial) | Mailbox wait histogram, `serve_metrics_http`, remote metrics, docker-compose, alerts, `tests/metrics_export.rs` |
