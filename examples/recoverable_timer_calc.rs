//! Supervised calculator with an operation journal (`HashMap`) that survives panic/restart.
//!
//! Successful ops are stored in the journal. After a panic the supervisor spawns a fresh
//! actor that replays the journal in `pre_start`, restoring `last_result`.
//!
//! Run: `cargo run --example recoverable_timer_calc`
//! See: `examples/recoverable_timer_calc.md`

use lane_switchboards::actor::{spawn, Actor, ActorProcessingErr, ActorRef};
use lane_switchboards::supervisor::{
    child_spec, RestartStrategy, Supervisor, SupervisorConfig, SupervisorHandle,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, Mutex};

#[derive(Clone, Debug)]
enum OpKind {
    Add(f64, f64),
    Sub(f64, f64),
    Mul(f64, f64),
    Div(f64, f64),
}

impl OpKind {
    fn label(&self) -> &'static str {
        match self {
            OpKind::Add(_, _) => "add",
            OpKind::Sub(_, _) => "sub",
            OpKind::Mul(_, _) => "mul",
            OpKind::Div(_, _) => "div",
        }
    }

    fn operands(&self) -> (f64, f64) {
        match self {
            OpKind::Add(a, b)
            | OpKind::Sub(a, b)
            | OpKind::Mul(a, b)
            | OpKind::Div(a, b) => (*a, *b),
        }
    }
}

#[derive(Clone, Debug)]
enum OpStatus {
    Pending,
    Ok(f64),
}

#[derive(Clone, Debug)]
struct JournalEntry {
    kind: OpKind,
    status: OpStatus,
}

#[derive(Default)]
struct OpJournal {
    ops: HashMap<u64, JournalEntry>,
    next_id: u64,
}

impl OpJournal {
    fn start_op(&mut self, kind: OpKind) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.ops.insert(
            id,
            JournalEntry {
                kind,
                status: OpStatus::Pending,
            },
        );
        id
    }

    fn complete_op(&mut self, id: u64, result: f64) {
        if let Some(entry) = self.ops.get_mut(&id) {
            entry.status = OpStatus::Ok(result);
        }
    }

    fn replay_last_result(&self) -> Option<f64> {
        let mut last = None;
        let mut ids: Vec<_> = self.ops.keys().copied().collect();
        ids.sort_unstable();
        for id in ids {
            if let Some(JournalEntry {
                status: OpStatus::Ok(v),
                ..
            }) = self.ops.get(&id)
            {
                last = Some(*v);
            }
        }
        last
    }

    fn counts(&self) -> (usize, usize) {
        let mut ok = 0;
        let mut pending = 0;
        for entry in self.ops.values() {
            match entry.status {
                OpStatus::Ok(_) => ok += 1,
                OpStatus::Pending => pending += 1,
            }
        }
        (ok, pending)
    }
}

type SharedJournal = Arc<Mutex<OpJournal>>;

enum CalcMsg {
    Add(f64, f64, oneshot::Sender<Result<f64, String>>),
    Sub(f64, f64, oneshot::Sender<Result<f64, String>>),
    Mul(f64, f64, oneshot::Sender<Result<f64, String>>),
    Div(f64, f64, oneshot::Sender<Result<f64, String>>),
    LastResult(oneshot::Sender<Option<f64>>),
    JournalSummary(oneshot::Sender<(Option<f64>, usize, usize)>),
}

struct RecoverableCalculator {
    journal: SharedJournal,
    last_result: Option<f64>,
}

impl RecoverableCalculator {
    fn new(journal: SharedJournal) -> Self {
        Self {
            journal,
            last_result: None,
        }
    }

    fn compute(kind: &OpKind) -> Result<f64, String> {
        let (a, b) = kind.operands();
        match kind {
            OpKind::Add(_, _) => Ok(a + b),
            OpKind::Sub(_, _) => Ok(a - b),
            OpKind::Mul(_, _) => Ok(a * b),
            OpKind::Div(_, _) if b == 0.0 => panic!("division by zero"),
            OpKind::Div(_, _) => Ok(a / b),
        }
    }

    async fn replay_journal(&mut self) {
        let last = self.journal.lock().await.replay_last_result();
        self.last_result = last;
        if let Some(v) = last {
            tracing::info!(%v, "replayed journal into last_result");
        }
    }

    async fn run_op(
        &mut self,
        kind: OpKind,
        reply: oneshot::Sender<Result<f64, String>>,
    ) {
        let op_id = self.journal.lock().await.start_op(kind.clone());
        let result = Self::compute(&kind);
        match result {
            Ok(value) => {
                self.journal.lock().await.complete_op(op_id, value);
                self.last_result = Some(value);
                let _ = reply.send(Ok(value));
            }
            Err(e) => {
                let _ = reply.send(Err(e));
            }
        }
    }
}

#[async_trait::async_trait]
impl Actor<CalcMsg> for RecoverableCalculator {
    async fn pre_start(&mut self) -> Result<(), ActorProcessingErr> {
        println!("replaying journal into last_result on pre_start===========");
        self.replay_journal().await;
        Ok(())
    }

    async fn handle(&mut self, msg: CalcMsg) -> Result<(), ActorProcessingErr> {
        match msg {
            CalcMsg::LastResult(reply) => {
                let _ = reply.send(self.last_result);
            }
            CalcMsg::JournalSummary(reply) => {
                let journal = self.journal.lock().await;
                let (ok, pending) = journal.counts();
                let _ = reply.send((self.last_result, ok, pending));
            }
            CalcMsg::Add(a, b, reply) => {
                self.run_op(OpKind::Add(a, b), reply).await;
            }
            CalcMsg::Sub(a, b, reply) => {
                self.run_op(OpKind::Sub(a, b), reply).await;
            }
            CalcMsg::Mul(a, b, reply) => {
                self.run_op(OpKind::Mul(a, b), reply).await;
            }
            CalcMsg::Div(a, b, reply) => {
                self.run_op(OpKind::Div(a, b), reply).await;
            }
        }
        Ok(())
    }
}

struct CalcHandle {
    current: Arc<Mutex<Option<ActorRef<CalcMsg>>>>,
    journal: SharedJournal,
    _supervisor: SupervisorHandle<CalcMsg>,
}

impl CalcHandle {
    async fn start(journal: SharedJournal) -> Result<Self, ActorProcessingErr> {
        let current = Arc::new(Mutex::new(None));
        let slot_for_spec = current.clone();
        let journal_for_spec = journal.clone();

        let spec = child_spec(0, move |sup_tx| {
            let slot = slot_for_spec.clone();
            let journal = journal_for_spec.clone();
            Box::pin(async move {
                let (actor_ref, _) =
                    spawn(RecoverableCalculator::new(journal), Some(sup_tx)).await?;
                *slot.lock().await = Some(actor_ref.clone());
                Ok(actor_ref)
            })
        });

        let config = SupervisorConfig {
            strategy: RestartStrategy::OneForOne,
            max_restarts: 10,
            within_secs: 60,
            ..Default::default()
        };

        let supervisor = Supervisor::new(config, vec![spec]);
        let sup_handle = supervisor.start().await?;

        if current.lock().await.is_none() {
            return Err("supervised calculator not started".into());
        }

        Ok(Self {
            current,
            journal,
            _supervisor: sup_handle,
        })
    }

    async fn actor(&self) -> ActorRef<CalcMsg> {
        self.current
            .lock()
            .await
            .clone()
            .expect("supervised calculator running")
    }

    async fn pending_ops(&self) -> Vec<(u64, OpKind)> {
        let journal = self.journal.lock().await;
        let mut ids: Vec<_> = journal.ops.keys().copied().collect();
        ids.sort_unstable();
        ids.into_iter()
            .filter_map(|id| {
                journal.ops.get(&id).and_then(|entry| {
                    if matches!(entry.status, OpStatus::Pending) {
                        Some((id, entry.kind.clone()))
                    } else {
                        None
                    }
                })
            })
            .collect()
    }
}

enum TimerMsg {
    Start(ActorRef<TimerMsg>),
    Tick,
}

struct JournalTimer {
    calc: Arc<CalcHandle>,
    self_ref: Option<ActorRef<TimerMsg>>,
    interval: Duration,
    running: bool,
}

impl JournalTimer {
    fn new(calc: Arc<CalcHandle>, interval: Duration) -> Self {
        Self {
            calc,
            self_ref: None,
            interval,
            running: false,
        }
    }

    fn schedule_next(&self) {
        let Some(self_ref) = self.self_ref.clone() else {
            return;
        };
        let delay = self.interval;
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = self_ref.send(TimerMsg::Tick).await;
        });
    }
}

#[async_trait::async_trait]
impl Actor<TimerMsg> for JournalTimer {
    async fn handle(&mut self, msg: TimerMsg) -> Result<(), ActorProcessingErr> {
        match msg {
            TimerMsg::Start(self_ref) => {
                self.self_ref = Some(self_ref);
                self.running = true;
                self.schedule_next();
            }
            TimerMsg::Tick if self.running => {
                match query_summary(&self.calc).await {
                    Ok((last, ok, pending)) => {
                        let last = last
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "(none)".into());
                        println!(
                            "[timer] last_result = {last}, journal: {ok} ok, {pending} pending"
                        );
                    }
                    Err(e) => println!("[timer] query failed: {e}"),
                }
                self.schedule_next();
            }
            TimerMsg::Tick => {}
        }
        Ok(())
    }
}

fn actor_err(e: ActorProcessingErr) -> anyhow::Error {
    anyhow::anyhow!("{e}")
}

async fn query_summary(handle: &CalcHandle) -> anyhow::Result<(Option<f64>, usize, usize)> {
    let (tx, rx) = oneshot::channel();
    let calc = handle.actor().await;
    calc.send(CalcMsg::JournalSummary(tx)).await.map_err(actor_err)?;
    rx.await
        .map_err(|_| anyhow::anyhow!("calculator dropped journal summary reply"))
}

async fn request(
    handle: &CalcHandle,
    build: impl FnOnce(oneshot::Sender<Result<f64, String>>) -> CalcMsg,
) -> anyhow::Result<Result<f64, String>> {
    let (tx, rx) = oneshot::channel();
    let calc = handle.actor().await;
    calc.send(build(tx)).await.map_err(actor_err)?;
    match rx.await {
        Ok(result) => Ok(result),
        Err(_) => Err(anyhow::anyhow!(
            "calculator crashed before reply (supervisor will restart and replay journal)"
        )),
    }
}

async fn add(handle: &CalcHandle, a: f64, b: f64) -> anyhow::Result<Result<f64, String>> {
    request(handle, |reply| CalcMsg::Add(a, b, reply)).await
}

async fn div(handle: &CalcHandle, a: f64, b: f64) -> anyhow::Result<Result<f64, String>> {
    request(handle, |reply| CalcMsg::Div(a, b, reply)).await
}

fn print_op(op: &str, a: f64, b: f64, outcome: anyhow::Result<Result<f64, String>>) {
    match outcome {
        Ok(Ok(value)) => println!("[calc] {op}: {a} and {b} = {value}"),
        Ok(Err(e)) => println!("[calc] {op}: {a} and {b} -> error: {e}"),
        Err(e) => println!("[calc] {op}: {a} and {b} -> {e}"),
    }
}

fn print_pending(pending: &[(u64, OpKind)]) {
    if pending.is_empty() {
        println!("[recover] no pending ops in journal");
        return;
    }
    for (id, kind) in pending {
        let (a, b) = kind.operands();
        println!(
            "[recover] pending op #{id}: {} {a} and {b} (never completed — panic interrupted it)",
            kind.label()
        );
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let journal = Arc::new(Mutex::new(OpJournal::default()));
    let calc = Arc::new(CalcHandle::start(journal).await.map_err(actor_err)?);
    let interval = Duration::from_millis(800);

    let (timer, timer_join) = spawn(JournalTimer::new(calc.clone(), interval), None)
        .await
        .map_err(actor_err)?;

    timer
        .send(TimerMsg::Start(timer.clone()))
        .await
        .map_err(actor_err)?;

    println!("Recoverable calculator + journal timer started (every {}ms)\n", interval.as_millis());

    tokio::time::sleep(Duration::from_millis(400)).await;
    print_op("add", 10.0, 4.0, add(&calc, 10.0, 4.0).await);

    tokio::time::sleep(Duration::from_millis(1200)).await;
    print_op("add", 5.0, 3.0, add(&calc, 5.0, 3.0).await);

    tokio::time::sleep(Duration::from_millis(400)).await;
    println!("\n--- panic: divide by zero (stored as pending in journal) ---");
    print_op("div", 10.0, 0.0, div(&calc, 10.0, 0.0).await);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let pending = calc.pending_ops().await;
    print_pending(&pending);

    tokio::time::sleep(Duration::from_millis(1000)).await;
    println!("\n--- after supervisor restart: last_result restored from journal ---");
    print_op("add", 1.0, 1.0, add(&calc, 1.0, 1.0).await);

    tokio::time::sleep(Duration::from_millis(1200)).await;

    timer.stop().await.map_err(actor_err)?;
    timer_join.await?;

    let actor = calc.actor().await;
    actor.stop().await.map_err(actor_err)?;

    println!("\nTimer and calculator stopped.");
    Ok(())
}
