//! Actor runtime monitoring with `ActorMonitor` — live stats, post-mortem snapshots,
//! panic and timeout counters, slow-handle detection, and mean latency.
//!
//! Run: `cargo run --example resilient_monitor`
//! See: `examples/resilient_monitor.md`
//!
//! Demo phases:
//!   1. Normal work       — `messages_handled` and `total_handle_ms` accumulate
//!   2. Slow handle       — `slow_handles` incremented when `slow_handle_threshold` exceeded
//!   3. Panic             — `panics` counter; post-mortem snapshot survives actor exit
//!   4. Handle timeout    — `handle_timeouts` counter; post-mortem on timeout exit
//!   5. Global snapshot   — `ActorMonitor::global().all()` lists every live actor

use lane_switchboards::actor::{Actor, ActorId, ActorProcessingErr, ActorRef, HandleStuckContext};
use lane_switchboards::config::ActorConfig;
use lane_switchboards::monitor::{ActorMonitor, ActorStats};
use lane_switchboards::supervisor::{
    ChildSlot, RestartStrategy, Supervisor, SupervisorConfig, SupervisorHandle,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

// ── messages ─────────────────────────────────────────────────────────────────

enum WorkerMsg {
    /// Fast add — completes in microseconds.
    Add(f64, f64, oneshot::Sender<f64>),
    AddOnly(f64, f64),
    /// Sleeps for `delay_ms` before replying — triggers `slow_handles` when above threshold.
    SlowWork {
        delay_ms: u64,
        reply: oneshot::Sender<String>,
    },
    /// Panics unconditionally — triggers supervisor restart.
    CrashNow,
    /// Sleeps forever — triggers `handle_timeout` and supervisor restart.
    HangForever,
}

// ── actor ─────────────────────────────────────────────────────────────────────

struct MonitoredWorker {
    /// Shared restart counter so main can report the total generation.
    restarts: Arc<AtomicU64>,
    /// Set in `on_handle_begin`; used in `on_handle_stuck` to log what got stuck.
    pending_op: Option<&'static str>,
}

#[async_trait::async_trait]
impl Actor<WorkerMsg> for MonitoredWorker {
    async fn pre_start(&mut self) -> Result<(), ActorProcessingErr> {
        let gen = self.restarts.fetch_add(1, Ordering::Relaxed) + 1;
        println!("[worker] generation {gen} starting");
        Ok(())
    }

    /// Journal which operation is in-flight so `on_handle_stuck` can report it.
    async fn on_handle_begin(&mut self, msg: &WorkerMsg) -> Result<(), ActorProcessingErr> {
        self.pending_op = Some(match msg {
            WorkerMsg::Add(..) => "Add",
            WorkerMsg::AddOnly(..) => "AddOnly",
            WorkerMsg::SlowWork { .. } => "SlowWork",
            WorkerMsg::CrashNow => "CrashNow",
            WorkerMsg::HangForever => "HangForever",
        });
        Ok(())
    }

    /// Called when `handle_timeout` fires; the actor is about to exit.
    async fn on_handle_stuck(&mut self, ctx: HandleStuckContext) -> Result<(), ActorProcessingErr> {
        println!(
            "[worker] stuck on {:?} — elapsed {}ms (limit {}ms)",
            self.pending_op,
            ctx.elapsed.as_millis(),
            ctx.limit.as_millis(),
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
                let res = a + b;
            }

            WorkerMsg::SlowWork { delay_ms, reply } => {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                self.pending_op = None;
                let _ = reply.send(format!("done after {delay_ms}ms"));
            }
            WorkerMsg::CrashNow => {
                panic!("simulated worker crash");
            }
            WorkerMsg::HangForever => {
                tokio::time::sleep(Duration::from_secs(600)).await;
            }
        }
        Ok(())
    }

    async fn post_stop(&mut self) -> Result<(), ActorProcessingErr> {
        println!("[worker] post_stop");
        Ok(())
    }
}

// ── application handle ────────────────────────────────────────────────────────

struct WorkerApp {
    slot: Arc<ChildSlot<WorkerMsg>>,
    _supervisor: SupervisorHandle<WorkerMsg>,
}

impl WorkerApp {
    async fn start(restarts: Arc<AtomicU64>) -> Result<Self, ActorProcessingErr> {
        let slot = Arc::new(ChildSlot::new());
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

        // handle_timeout: actor exits if handle() takes more than 80ms.
        // slow_handle_threshold: slow_handles++ when handle() takes more than 15ms (but < 80ms).
        let actor_config = ActorConfig {
            handle_timeout: Some(Duration::from_millis(80)),
            slow_handle_threshold: Some(Duration::from_millis(15)),
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
    app.actor_ref()
        .send(WorkerMsg::Add(a, b, tx))
        .await
        .expect("send");
    rx.await.expect("reply")
}


async fn add_only(app: &WorkerApp, a: f64, b: f64) {
    app.actor_ref()
        .send(WorkerMsg::AddOnly(a, b))
        .await
        .expect("send");
}

async fn slow_work(app: &WorkerApp, delay_ms: u64) -> String {
    let (tx, rx) = oneshot::channel();
    app.actor_ref()
        .send(WorkerMsg::SlowWork {
            delay_ms,
            reply: tx,
        })
        .await
        .expect("send");
    rx.await.expect("reply")
}

// ── main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .init();

    let restarts = Arc::new(AtomicU64::new(0));
    let app = WorkerApp::start(restarts.clone())
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    // ── phase 1: normal work ──────────────────────────────────────────────────
    println!("\n=== Phase 1: normal work (5 × add) ===\n");
    for i in 1..=5000000u32 {
        let result = add_only(&app, i as f64, 1.0).await;
        // println!("  add({i}, 1) = {result}");
    }

    let id = app.actor_id();
    let stats = ActorMonitor::global().get(id).expect("stats present");
    print_stats(&format!("{id}  live"), &stats);

    // ── phase 2: slow handle ──────────────────────────────────────────────────
    println!("\n=== Phase 2: slow handle (delay 25ms > threshold 15ms) ===\n");

    let msg = slow_work(&app, 25).await;
    println!("  slow_work reply: {msg}");

    let stats = ActorMonitor::global().get(id).expect("stats present");
    print_stats(&format!("{id}  live — after slow work"), &stats);

    // ── phase 3: panic → post-mortem ─────────────────────────────────────────
    println!("\n=== Phase 3: panic → supervisor restart ===\n");

    let pre_crash_id = app.actor_id();
    app.actor_ref()
        .send(WorkerMsg::CrashNow)
        .await
        .expect("send");
    // Give the supervisor time to detect the exit and restart the child.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // The old actor is gone; its final stats are preserved as a post-mortem snapshot.
    let post_mortem = ActorMonitor::global()
        .get(pre_crash_id)
        .expect("post-mortem present");
    print_stats(&format!("{pre_crash_id}  post-mortem (crashed)"), &post_mortem);

    let new_id = app.actor_id();
    println!("\n  new actor after restart: {new_id}");

    let result = add(&app, 100.0, 1.0).await;
    println!("  add(100, 1) = {result}");

    let stats = ActorMonitor::global().get(new_id).expect("stats present");
    print_stats(&format!("{new_id}  live — fresh generation"), &stats);

    // ── phase 4: handle timeout → post-mortem ────────────────────────────────
    println!("\n=== Phase 4: handle timeout (hang forever, limit 80ms) ===\n");

    let pre_timeout_id = app.actor_id();
    // Fire-and-forget — the reply channel is dropped; we only care about the timeout.
    app.actor_ref()
        .send(WorkerMsg::HangForever)
        .await
        .expect("send");
    // Wait for timeout (80ms) + restart.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let post_mortem = ActorMonitor::global()
        .get(pre_timeout_id)
        .expect("post-mortem present");
    print_stats(&format!("{pre_timeout_id}  post-mortem (timed out)"), &post_mortem);

    let final_id = app.actor_id();
    println!("\n  new actor after timeout restart: {final_id}");

    let result = add(&app, 7.0, 3.0).await;
    println!("  add(7, 3) = {result}");

    // ── phase 5: global snapshot ──────────────────────────────────────────────
    println!("\n=== Phase 5: ActorMonitor::global().all() ===\n");

    let all = ActorMonitor::global().all();
    println!("  {} live actor(s):", all.len());
    for s in &all {
        println!(
            "    {}  handled={} panics={} timeouts={} slow={} mean_ms={}",
            s.actor_id,
            s.messages_handled,
            s.panics,
            s.handle_timeouts,
            s.slow_handles,
            s.mean_handle_ms,
        );
    }

    println!(
        "\n  total actor generations (includes initial): {}",
        restarts.load(Ordering::Relaxed)
    );

    println!("\nDone.");
    Ok(())
}
