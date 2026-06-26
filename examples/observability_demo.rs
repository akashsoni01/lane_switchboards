//! Full observability tour: `ActorMonitor` stdout, tracing logs, Prometheus metrics,
//! Grafana dashboard series, and Prometheus alert rules.
//!
//! ```bash
//! # Terminal 1
//! docker compose -f docs/grafana/docker-compose.yml up
//!
//! # Terminal 2
//! cargo run --example observability_demo --features metrics
//! ```
//!
//! # Scripted verify (no Ctrl-C)
//! OBSERVABILITY_AUTO_EXIT=1 OBSERVABILITY_KEEP_ALIVE_SECS=30 \
//!   cargo run --example observability_demo --features metrics
//!
//! **Port 9090** must be free — stop any other exporter first (`metrics_exporter`, prior demo).
//! Override: `METRICS_ADDR=127.0.0.1:9092`
use lane_switchboards::actor::{spawn_with_config, Actor, ActorId, ActorProcessingErr, ActorRef, HandleStuckContext};
use lane_switchboards::config::ActorConfig;
use lane_switchboards::metrics::{
    init_metrics, record_consistency_operation, record_mesh_dispatch, record_remote_ack_timeout,
    record_remote_send, render_prometheus_text, serve_metrics_http, sync_storage_stats,
    ConsistencyOpSnapshot, MetricsConfig, StorageMetricsSnapshot,
};
use lane_switchboards::monitor::{ActorMeta, ActorMonitor, ActorStats};
use lane_switchboards::supervisor::{
    ChildSlot, RestartStrategy, Supervisor, SupervisorConfig, SupervisorHandle,
};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

// ── messages ─────────────────────────────────────────────────────────────────

enum WorkerMsg {
    Add(f64, f64, oneshot::Sender<f64>),
    AddOnly(f64, f64),
    SlowWork { delay_ms: u64, reply: oneshot::Sender<String> },
    /// Returns `Err` from `handle()` — increments `handle_errors`.
    Fail,
    CrashNow,
    HangForever,
}

enum HoldOnlyMsg {
    Sleep(Duration),
}

struct HoldOnlyActor;

#[async_trait::async_trait]
impl Actor<HoldOnlyMsg> for HoldOnlyActor {
    async fn handle(&mut self, msg: HoldOnlyMsg) -> Result<(), ActorProcessingErr> {
        match msg {
            HoldOnlyMsg::Sleep(d) => tokio::time::sleep(d).await,
        }
        Ok(())
    }
}

// ── actor ─────────────────────────────────────────────────────────────────────

struct MonitoredWorker {
    restarts: Arc<AtomicU64>,
    pending_op: Option<&'static str>,
}

#[async_trait::async_trait]
impl Actor<WorkerMsg> for MonitoredWorker {
    async fn pre_start(&mut self) -> Result<(), ActorProcessingErr> {
        let gen = self.restarts.fetch_add(1, Ordering::Relaxed) + 1;
        tracing::info!(generation = gen, "worker starting");
        Ok(())
    }

    async fn on_handle_begin(&mut self, msg: &WorkerMsg) -> Result<(), ActorProcessingErr> {
        self.pending_op = Some(match msg {
            WorkerMsg::Add(..) => "Add",
            WorkerMsg::AddOnly(..) => "AddOnly",
            WorkerMsg::SlowWork { .. } => "SlowWork",
            WorkerMsg::Fail => "Fail",
            WorkerMsg::CrashNow => "CrashNow",
            WorkerMsg::HangForever => "HangForever",
        });
        Ok(())
    }

    async fn on_handle_stuck(&mut self, ctx: HandleStuckContext) -> Result<(), ActorProcessingErr> {
        tracing::warn!(
            op = ?self.pending_op,
            elapsed_ms = ctx.elapsed.as_millis(),
            limit_ms = ctx.limit.as_millis(),
            "worker stuck — handle_timeout fired"
        );
        Ok(())
    }

    async fn handle(&mut self, msg: WorkerMsg) -> Result<(), ActorProcessingErr> {
        match msg {
            WorkerMsg::Add(a, b, reply) => {
                self.pending_op = None;
                let _ = reply.send(a + b);
            }
            WorkerMsg::AddOnly(a, b) => {
                self.pending_op = None;
                let _ = a + b;
            }
            WorkerMsg::SlowWork { delay_ms, reply } => {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                self.pending_op = None;
                let _ = reply.send(format!("done after {delay_ms}ms"));
            }
            WorkerMsg::Fail => {
                self.pending_op = None;
                return Err("simulated handle error".into());
            }
            WorkerMsg::CrashNow => panic!("simulated worker crash"),
            WorkerMsg::HangForever => {
                tokio::time::sleep(Duration::from_secs(600)).await;
            }
        }
        Ok(())
    }

    async fn post_stop(&mut self) -> Result<(), ActorProcessingErr> {
        tracing::info!("worker post_stop");
        Ok(())
    }
}

// ── app ───────────────────────────────────────────────────────────────────────

struct WorkerApp {
    slot: Arc<ChildSlot<WorkerMsg>>,
    _supervisor: SupervisorHandle<WorkerMsg>,
}

impl WorkerApp {
    async fn start(actor_config: ActorConfig) -> Result<Self, ActorProcessingErr> {
        let slot = Arc::new(ChildSlot::new());
        let restarts = Arc::new(AtomicU64::new(0));
        let restarts_for_spec = restarts.clone();

        let spec = ChildSlot::child_spec(0, slot.clone(), move || MonitoredWorker {
            restarts: restarts_for_spec.clone(),
            pending_op: None,
        });

        let sup_config = SupervisorConfig {
            strategy: RestartStrategy::OneForOne,
            max_restarts: 10,
            within_secs: 60,
            ..Default::default()
        };

        let handle = Supervisor::with_actor_config(actor_config, sup_config, vec![spec])
            .start()
            .await?;

        slot.require()?;
        Ok(Self {
            slot,
            _supervisor: handle,
        })
    }

    fn actor_ref(&self) -> ActorRef<WorkerMsg> {
        self.slot.get().expect("worker running")
    }

    fn actor_id(&self) -> ActorId {
        self.actor_ref().id
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn print_stats(label: &str, stats: &ActorStats) {
    println!("  [{label}]");
    println!("    messages_handled : {}", stats.messages_handled);
    println!("    panics           : {}", stats.panics);
    println!("    handle_timeouts  : {}", stats.handle_timeouts);
    println!("    slow_handles     : {}", stats.slow_handles);
    println!("    handle_errors    : {}", stats.handle_errors);
    println!("    last_handle_ms   : {}", stats.last_handle_ms);
    println!("    max_handle_ms    : {}", stats.max_handle_ms);
    println!("    total_handle_ms  : {}", stats.total_handle_ms);
    println!("    mean_handle_ms   : {}", stats.mean_handle_ms);
    println!("    in_flight        : {}", stats.in_flight);
}

async fn add(app: &WorkerApp, a: f64, b: f64) -> f64 {
    let (tx, rx) = oneshot::channel();
    app.actor_ref().send(WorkerMsg::Add(a, b, tx)).await.expect("send");
    rx.await.expect("reply")
}

async fn add_only(app: &WorkerApp, a: f64, b: f64) {
    app.actor_ref()
        .send(WorkerMsg::AddOnly(a, b))
        .await
        .expect("send");
}

async fn slow_work(app: &WorkerApp, delay_ms: u64) -> String {
    slow_work_inner(&app.actor_ref(), delay_ms).await
}

async fn slow_work_inner(actor: &ActorRef<WorkerMsg>, delay_ms: u64) -> String {
    let (tx, rx) = oneshot::channel();
    actor
        .send(WorkerMsg::SlowWork {
            delay_ms,
            reply: tx,
        })
        .await
        .expect("send");
    rx.await.expect("reply")
}

/// Sum counter lines for `metric`, optionally requiring a label substring.
fn prom_counter_sum(body: &str, metric: &str, label_contains: Option<&str>) -> f64 {
    body.lines()
        .filter(|line| line.starts_with(metric))
        .filter(|line| label_contains.is_none_or(|needle| line.contains(needle)))
        .filter_map(|line| line.split_whitespace().last()?.parse::<f64>().ok())
        .sum()
}

fn prom_gauge_max(body: &str, metric: &str) -> f64 {
    body.lines()
        .filter(|line| line.starts_with(metric))
        .filter_map(|line| line.split_whitespace().last()?.parse::<f64>().ok())
        .fold(0.0_f64, f64::max)
}

fn verify_row(name: &str, expected: f64, actual: f64) -> String {
    let ok = if expected == 0.0 {
        actual == 0.0
    } else {
        actual >= expected
    };
    let mark = if ok { "OK" } else { "MISMATCH" };
    format!("  {mark}  {name}: expected>={expected}  prometheus={actual}")
}

fn print_verification(body: &str) {
    println!("\n=== Prometheus scrape verification (local /metrics) ===\n");
    let node = "observability_demo";
    let checks = [
        (
            "messages_handled",
            prom_counter_sum(body, "lane_actor_messages_handled_total", Some(node)),
            8.0,
        ),
        (
            "panics",
            prom_counter_sum(body, "lane_actor_panics_total", Some(node)),
            1.0,
        ),
        (
            "handle_timeouts",
            prom_counter_sum(body, "lane_actor_handle_timeouts_total", Some(node)),
            1.0,
        ),
        (
            "slow_handles",
            prom_counter_sum(body, "lane_actor_slow_handles_total", Some(node)),
            1.0,
        ),
        (
            "handle_errors",
            prom_counter_sum(body, "lane_actor_handle_errors_total", Some(node)),
            1.0,
        ),
        (
            "exits_panic",
            prom_counter_sum(body, "lane_actor_exits_total", Some(r#"reason="panic""#)),
            1.0,
        ),
        (
            "exits_handle_timeout",
            prom_counter_sum(
                body,
                "lane_actor_exits_total",
                Some(r#"reason="handle_timeout""#),
            ),
            1.0,
        ),
        (
            "supervisor_restarts",
            prom_counter_sum(body, "lane_supervisor_restarts_total", None),
            2.0,
        ),
        (
            "mailbox_blocked",
            prom_counter_sum(body, "lane_actor_mailbox_send_blocked_total", Some(node)),
            1.0,
        ),
        (
            "mesh_dispatches",
            prom_counter_sum(body, "lane_mesh_dispatches_total", Some(node)),
            1.0,
        ),
        (
            "storage_quorum_failures",
            prom_counter_sum(body, "lane_storage_quorum_failures_total", Some(node)),
            1.0,
        ),
    ];
    for (name, actual, min) in checks {
        println!("{}", verify_row(name, min, actual));
    }

    let in_flight_peak = prom_gauge_max(body, "lane_actor_in_flight");
    println!(
        "  {}  in_flight_peak (scrape-time gauge): prometheus={in_flight_peak}",
        if in_flight_peak >= 1.0 { "OK" } else { "INFO" }
    );
    println!("  INFO  phase 7 prints in_flight=1 each second while hold-demo runs");

    println!("\n  Note: `messages_handled` on timeout post-mortem includes the timed-out");
    println!("  handle (see lane_core monitor `record_timeout`). Counters in Prometheus");
    println!("  accumulate across actor generations with the same labels.");
}

fn query_prometheus(expr: &str) -> Option<String> {
    let out = Command::new("curl")
        .args([
            "-sf",
            "-G",
            "http://127.0.0.1:9091/api/v1/query",
            "--data-urlencode",
            &format!("query={expr}"),
        ])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    String::from_utf8(out.stdout).ok()
}

fn print_remote_prometheus_checks() {
    println!("\n=== Prometheus remote checks (http://127.0.0.1:9091) ===\n");
    let node_filter = r#"node="observability_demo""#;
    let queries: Vec<(&str, String)> = vec![
        (
            "timeouts total",
            format!("lane_actor_handle_timeouts_total{{{node_filter}}}"),
        ),
        (
            "panics total",
            format!("lane_actor_panics_total{{{node_filter}}}"),
        ),
        (
            "slow handles",
            format!("lane_actor_slow_handles_total{{{node_filter}}}"),
        ),
        (
            "handle errors",
            format!("lane_actor_handle_errors_total{{{node_filter}}}"),
        ),
        (
            "supervisor restarts",
            "lane_supervisor_restarts_total".into(),
        ),
    ];
    for (label, expr) in queries {
        match query_prometheus(&expr) {
            Some(json) if json.contains("\"status\":\"success\"") => {
                println!("  OK  {label}: {json}");
            }
            _ => println!("  SKIP  {label}: Prometheus not reachable (start docker compose)"),
        }
    }

    if let Some(rules) = Command::new("curl")
        .args(["-sf", "http://127.0.0.1:9091/api/v1/rules"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
    {
        println!("\n=== Lane alert rules (state from Prometheus) ===\n");
        for name in [
            "LaneActorInFlightStuck",
            "LaneActorHandleTimeouts",
            "LaneSupervisorRestartStorm",
            "LaneMailboxBackpressure",
            "LaneCounterSaturated",
        ] {
            let state = if rules.contains(&format!("\"name\":\"{name}\"")) {
                if rules.contains(&format!("\"state\":\"firing\"")) && rules.contains(name) {
                    "see rules API (may be pending until `for` elapses)"
                } else {
                    "loaded (pending until `for` elapses)"
                }
            } else {
                "not loaded"
            };
            println!("  {name}: {state}");
        }
        println!("\n  Demo rules use shorter `for` in docs/grafana/alerts-demo.yml.");
        println!("  Grafana annotations: Lane Actor Runtime dashboard → Lane alerts.");
    }
}

fn export_optional_domain_metrics() {
    record_mesh_dispatch("observability_demo");
    record_consistency_operation(&ConsistencyOpSnapshot {
        service: "observability_demo".into(),
        consistency_level: "quorum".into(),
        succeeded: true,
        duration_ms: 12,
        acks_required: 3,
        acks_received: 3,
    });
    record_remote_send("node-b", true);
    record_remote_ack_timeout("node-c");
    // Baseline then delta — first sync seeds STORAGE_LAST without incrementing.
    sync_storage_stats(&StorageMetricsSnapshot {
        node: "observability_demo".into(),
        ..Default::default()
    });
    sync_storage_stats(&StorageMetricsSnapshot {
        node: "observability_demo".into(),
        puts_total: 10,
        gets_total: 25,
        quorum_failures: 1,
        wal_bytes_written: 4096,
        tombstone_count: 2,
        live_records: 100,
        ..Default::default()
    });
}

// ── metrics HTTP ─────────────────────────────────────────────────────────────

fn metrics_addr_from_env() -> std::net::SocketAddr {
    std::env::var("METRICS_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:9090".into())
        .parse()
        .expect("METRICS_ADDR")
}

/// Fail fast if another process (e.g. `metrics_exporter`) already owns the scrape port.
fn ensure_metrics_port_free(addr: std::net::SocketAddr) -> anyhow::Result<()> {
    match std::net::TcpListener::bind(addr) {
        Ok(_) => Ok(()),
        Err(e) => anyhow::bail!(
            "cannot bind {addr} ({e}).\n\
             Another exporter is probably still running — Grafana will scrape the WRONG /metrics.\n\
             Fix:\n\
               lsof -i :{}\n\
               kill <pid>    # stop metrics_exporter or an old observability_demo\n\
             Or use a different port:\n\
               METRICS_ADDR=127.0.0.1:9092 cargo run --example observability_demo --features metrics\n\
             Then add target host.docker.internal:9092 to docs/grafana/prometheus.yml",
            addr.port()
        ),
    }
}

/// Confirm Prometheus is scraping *this* process (not a stale exporter on the same port).
fn verify_scrape_endpoint(addr: std::net::SocketAddr) -> anyhow::Result<()> {
    let out = Command::new("curl")
        .args(["-sf", &format!("http://{addr}/metrics")])
        .output()
        .map_err(|e| anyhow::anyhow!("curl failed: {e}"))?;
    if !out.status.success() {
        anyhow::bail!("GET http://{addr}/metrics failed — metrics HTTP not reachable");
    }
    let body = String::from_utf8_lossy(&out.stdout);
    let has_node = body.contains("node=\"observability_demo\"");
    let panics = prom_counter_sum(&body, "lane_actor_panics_total", Some("observability_demo"));
    if !has_node {
        anyhow::bail!(
            "http://{addr}/metrics has no node=\"observability_demo\" series.\n\
             Prometheus/Grafana are likely scraping a different process on this port.\n\
             Stop other exporters on port {} (see error above).",
            addr.port()
        );
    }
    if panics < 1.0 {
        anyhow::bail!(
            "http://{addr}/metrics shows lane_actor_panics_total=0 for observability_demo.\n\
             Run the full demo phases before checking Grafana."
        );
    }
    println!(
        "\n  OK  scrape endpoint http://{addr}/metrics (node=observability_demo, panics={panics})"
    );
    Ok(())
}

// ── main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    init_metrics(MetricsConfig {
        node: Some("observability_demo".into()),
        environment: Some("local".into()),
        ..Default::default()
    });

    let metrics_addr = metrics_addr_from_env();
    ensure_metrics_port_free(metrics_addr)?;
    tracing::info!(%metrics_addr, "metrics HTTP listening");
    tokio::spawn(serve_metrics_http(metrics_addr));
    println!("Prometheus scrape: http://{metrics_addr}/metrics (node=observability_demo)");
    println!("Grafana panel tip: use time range **Last 5 minutes** and refresh after phases finish.\n");

    let actor_config = ActorConfig {
        mailbox_capacity: 4,
        handle_timeout: Some(Duration::from_millis(80)),
        slow_handle_threshold: Some(Duration::from_millis(15)),
        monitor_meta: ActorMeta::default()
            .with_name("worker")
            .with_actor_type("MonitoredWorker"),
        ..Default::default()
    };

    let app = WorkerApp::start(actor_config)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    // Phase 1 — normal work
    tracing::info!("phase 1: normal work");
    println!("\n=== Phase 1: normal work (5 × add) ===\n");
    for i in 1..=5u32 {
        add_only(&app, i as f64, 1.0).await;
    }
    tokio::time::sleep(Duration::from_millis(30)).await;
    let id = app.actor_id();
    print_stats(&format!("{id}  live"), &ActorMonitor::global().get(id).expect("stats"));

    // Phase 2 — slow handle (tracing::warn from monitor)
    tracing::info!("phase 2: slow handle");
    println!("\n=== Phase 2: slow handle (25ms > 15ms threshold) ===\n");
    let msg = slow_work(&app, 25).await;
    println!("  slow_work reply: {msg}");
    print_stats(
        &format!("{id}  live — after slow work"),
        &ActorMonitor::global().get(id).expect("stats"),
    );

    // Phase 3 — handle error
    tracing::info!("phase 3: handle error");
    println!("\n=== Phase 3: handle error ===\n");
    app.actor_ref().send(WorkerMsg::Fail).await.expect("send");
    tokio::time::sleep(Duration::from_millis(30)).await;
    print_stats(
        &format!("{id}  live — after Fail"),
        &ActorMonitor::global().get(id).expect("stats"),
    );

    // Phase 4 — panic + restart (tracing::error on timeout path; panic path)
    tracing::info!("phase 4: panic → supervisor restart");
    println!("\n=== Phase 4: panic → supervisor restart ===\n");
    let pre_crash_id = app.actor_id();
    app.actor_ref().send(WorkerMsg::CrashNow).await.expect("send");
    tokio::time::sleep(Duration::from_millis(150)).await;
    print_stats(
        &format!("{pre_crash_id}  post-mortem (crashed)"),
        &ActorMonitor::global().get(pre_crash_id).expect("post-mortem"),
    );
    let _new_id = app.actor_id();
    let _ = add(&app, 100.0, 1.0).await;

    // Phase 5 — handle timeout (matches user post-mortem: handled=2, timeouts=1, last=81ms)
    tracing::info!("phase 5: handle timeout");
    println!("\n=== Phase 5: handle timeout (hang forever, limit 80ms) ===\n");
    let pre_timeout_id = app.actor_id();
    app.actor_ref().send(WorkerMsg::HangForever).await.expect("send");
    tokio::time::sleep(Duration::from_millis(300)).await;
    let timeout_stats = ActorMonitor::global()
        .get(pre_timeout_id)
        .expect("post-mortem");
    print_stats(
        &format!("{pre_timeout_id}  post-mortem (timed out)"),
        &timeout_stats,
    );

    // Phase 6 — mailbox backpressure (actor busy + full channel → mailbox_send_blocked)
    tracing::info!("phase 6: mailbox backpressure");
    println!("\n=== Phase 6: mailbox backpressure (capacity 4) ===\n");
    let busy_ref = app.actor_ref();
    let busy = tokio::spawn(async move {
        let _ = slow_work_inner(&busy_ref, 60).await;
    });
    tokio::time::sleep(Duration::from_millis(5)).await;
    for _ in 0..8 {
        let _ = app.actor_ref().send(WorkerMsg::AddOnly(1.0, 1.0)).await;
    }
    busy.await.ok();
    if let Some(live) = ActorMonitor::global().get(app.actor_id()) {
        println!(
            "  mailbox_depth={} capacity={}",
            live.mailbox_depth, live.mailbox_capacity
        );
    }

    // Phase 7 — in-flight hold on a dedicated actor without handle_timeout
    tracing::info!("phase 7: in-flight hold (alert demo)");
    println!("\n=== Phase 7: in-flight hold (16s — LaneActorInFlightStuck demo) ===\n");
    let hold_cfg = ActorConfig {
        handle_timeout: None,
        monitor_meta: ActorMeta::default()
            .with_name("hold-demo")
            .with_actor_type("HoldOnlyActor"),
        ..Default::default()
    };
    let (holder, hold_join) = spawn_with_config(HoldOnlyActor, None, &hold_cfg)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let hold_id = holder.id;
    let holder_for_task = holder.clone();
    let hold_task = tokio::spawn(async move {
        let _ = holder_for_task
            .send(HoldOnlyMsg::Sleep(Duration::from_secs(16)))
            .await;
    });
    for sec in 1..=16 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        if let Some(s) = ActorMonitor::global().get(hold_id) {
            if s.in_flight > 0 {
                println!("  t={sec}s  in_flight={} (hold-demo)", s.in_flight);
            }
        }
    }
    hold_task.await.ok();
    holder.stop().await.ok();
    hold_join.await.ok();

    // Phase 8 — optional domain metrics for Grafana storage/mesh panels
    tracing::info!("phase 8: mesh / storage / remote metrics");
    println!("\n=== Phase 8: mesh / storage / remote metrics ===\n");
    export_optional_domain_metrics();
    println!("  recorded mesh, consistency, remote, and storage series");

    // Phase 9 — snapshot
    println!("\n=== Phase 9: ActorMonitor::global().all() ===\n");
    for s in ActorMonitor::global().all() {
        println!(
            "    {}  handled={} panics={} timeouts={} slow={} errors={}",
            s.actor_id, s.messages_handled, s.panics, s.handle_timeouts, s.slow_handles,
            s.handle_errors
        );
    }

    // Keep metrics alive for Prometheus scrape + verification
    let keep_alive = std::env::var("OBSERVABILITY_KEEP_ALIVE_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30)
        .max(20);
    tracing::info!(keep_alive, "waiting for Prometheus scrape");
    println!("\nWaiting {keep_alive}s for Prometheus to scrape (interval 10s)…");
    tokio::time::sleep(Duration::from_secs(keep_alive)).await;

    verify_scrape_endpoint(metrics_addr)?;

    let body = render_prometheus_text()?;
    print_verification(&body);

    // Cross-check timeout post-mortem vs prometheus (documented behaviour)
    println!("\n=== Post-mortem cross-check (phase 5 timeout) ===\n");
    println!("  ActorMonitor post-mortem:");
    println!("    messages_handled={}", timeout_stats.messages_handled);
    println!("    handle_timeouts={}", timeout_stats.handle_timeouts);
    println!("    last_handle_ms={}", timeout_stats.last_handle_ms);
    let prom_timeouts =
        prom_counter_sum(&body, "lane_actor_handle_timeouts_total", Some("observability_demo"));
    let prom_handled =
        prom_counter_sum(&body, "lane_actor_messages_handled_total", Some("observability_demo"));
    println!("  Prometheus cumulative (all generations, same labels):");
    println!("    lane_actor_handle_timeouts_total={prom_timeouts}");
    println!("    lane_actor_messages_handled_total={prom_handled}");
    println!("  OK  timeout post-mortem `handled=2` = 1 successful add + 1 timed-out handle");

    print_remote_prometheus_checks();

    println!("\nGrafana: http://localhost:3000 → Lane → Lane Actor Runtime");
    println!("  • Set time range: Last 5 minutes");
    println!("  • Failures panel: panics (range) spikes to 1; panics/sec stays ~0 for a one-shot event");
    println!("  • Explore query: sum(increase(lane_actor_panics_total[15m]))");
    if std::env::var("OBSERVABILITY_AUTO_EXIT").as_deref() == Ok("1") {
        println!("OBSERVABILITY_AUTO_EXIT=1 — exiting.");
    } else {
        println!("Press Ctrl-C to stop.");
        tokio::signal::ctrl_c().await?;
        println!("shutting down");
    }
    Ok(())
}
