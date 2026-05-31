//! Supervisor strategies: OneForOne, OneForAll, RestForOne, and intensity limits.
//!
//! Run: `cargo run --example supervisor_strategies`
//! See: `examples/supervisor_strategies.md`

use lane_switchboards::actor::{spawn, Actor, ActorProcessingErr, ActorRef};
use lane_switchboards::supervisor::{
    child_spec, IntensityAction, RestartStrategy, Supervisor, SupervisorConfig,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

enum SupMsg {
    Ping,
    Fail,
}

#[derive(Clone)]
struct NamedWorker {
    name: String,
    generations: Arc<Mutex<HashMap<String, u64>>>,
}

#[async_trait::async_trait]
impl Actor<SupMsg> for NamedWorker {
    async fn pre_start(&mut self) -> Result<(), ActorProcessingErr> {
        let mut gens = self.generations.lock().await;
        *gens.entry(self.name.clone()).or_insert(0) += 1;
        println!("[spawn] {} generation {}", self.name, gens[&self.name]);
        Ok(())
    }

    async fn handle(&mut self, msg: SupMsg) -> Result<(), ActorProcessingErr> {
        match msg {
            SupMsg::Ping => {
                println!("[ping] {}", self.name);
                Ok(())
            }
            SupMsg::Fail => Err(format!("{} failed", self.name).into()),
        }
    }

    async fn post_stop(&mut self) -> Result<(), ActorProcessingErr> {
        println!("[stop] {}", self.name);
        Ok(())
    }
}

struct ChildRefs {
    by_name: Arc<Mutex<HashMap<String, ActorRef<SupMsg>>>>,
    generations: Arc<Mutex<HashMap<String, u64>>>,
}

impl ChildRefs {
    fn new() -> Self {
        Self {
            by_name: Arc::new(Mutex::new(HashMap::new())),
            generations: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn get(&self, name: &str) -> Option<ActorRef<SupMsg>> {
        self.by_name.lock().await.get(name).cloned()
    }

    async fn snapshot_generations(&self) -> HashMap<String, u64> {
        self.generations.lock().await.clone()
    }
}

fn actor_err(e: ActorProcessingErr) -> anyhow::Error {
    anyhow::anyhow!("{e}")
}

fn make_spec(order: usize, name: &str, refs: &ChildRefs) -> Box<dyn lane_switchboards::supervisor::ChildSpec<SupMsg>> {
    let name = name.to_string();
    let slot = refs.by_name.clone();
    let gens = refs.generations.clone();
    child_spec(order, move |sup_tx| {
        let name = name.clone();
        let slot = slot.clone();
        let gens = gens.clone();
        Box::pin(async move {
            let worker = NamedWorker {
                name: name.clone(),
                generations: gens,
            };
            let (actor_ref, _) = spawn(worker, Some(sup_tx)).await?;
            slot.lock().await.insert(name, actor_ref.clone());
            Ok(actor_ref)
        })
    })
}

async fn start_supervisor(
    config: SupervisorConfig,
    names: &[&str],
    refs: &ChildRefs,
) -> Result<lane_switchboards::supervisor::SupervisorHandle<SupMsg>, ActorProcessingErr> {
    let children: Vec<_> = names
        .iter()
        .enumerate()
        .map(|(order, name)| make_spec(order, name, refs))
        .collect();
    let handle = Supervisor::new(config, children).start().await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok(handle)
}

async fn fail_worker(refs: &ChildRefs, name: &str) -> anyhow::Result<()> {
    let actor = refs
        .get(name)
        .await
        .ok_or_else(|| anyhow::anyhow!("worker {name} not found"))?;
    actor.send(SupMsg::Fail).await.map_err(actor_err)?;
    tokio::time::sleep(Duration::from_millis(150)).await;
    Ok(())
}

async fn ping_worker(refs: &ChildRefs, name: &str) -> anyhow::Result<()> {
    let actor = refs
        .get(name)
        .await
        .ok_or_else(|| anyhow::anyhow!("worker {name} not found"))?;
    actor.send(SupMsg::Ping).await.map_err(actor_err)?;
    Ok(())
}

fn print_generations(label: &str, before: &HashMap<String, u64>, after: &HashMap<String, u64>) {
    println!("{label}");
    for name in ["alpha", "beta", "gamma"] {
        let b = before.get(name).copied().unwrap_or(0);
        let a = after.get(name).copied().unwrap_or(0);
        let delta = a.saturating_sub(b);
        println!("  {name}: {b} -> {a} (+{delta})");
    }
}

fn section(title: &str) {
    println!("\n========== {title} ==========\n");
}

async fn demo_one_for_one() -> anyhow::Result<()> {
    section("OneForOne — only the failed child restarts");
    let refs = ChildRefs::new();
    let config = SupervisorConfig {
        strategy: RestartStrategy::OneForOne,
        max_restarts: 10,
        within_secs: 60,
        ..Default::default()
    };
    let _sup = start_supervisor(config, &["alpha", "beta", "gamma"], &refs)
        .await
        .map_err(actor_err)?;

    let before = refs.snapshot_generations().await;
    fail_worker(&refs, "beta").await?;
    let after = refs.snapshot_generations().await;
    print_generations("[generations after beta fails]", &before, &after);

    ping_worker(&refs, "alpha").await?;
    ping_worker(&refs, "beta").await?;
    ping_worker(&refs, "gamma").await?;
    Ok(())
}

async fn demo_one_for_all() -> anyhow::Result<()> {
    section("OneForAll — one failure restarts every child");
    let refs = ChildRefs::new();
    let config = SupervisorConfig {
        strategy: RestartStrategy::OneForAll,
        max_restarts: 10,
        within_secs: 60,
        ..Default::default()
    };
    let _sup = start_supervisor(config, &["alpha", "beta", "gamma"], &refs)
        .await
        .map_err(actor_err)?;

    let before = refs.snapshot_generations().await;
    fail_worker(&refs, "beta").await?;
    let after = refs.snapshot_generations().await;
    print_generations("[generations after beta fails]", &before, &after);

    ping_worker(&refs, "alpha").await?;
    ping_worker(&refs, "beta").await?;
    ping_worker(&refs, "gamma").await?;
    Ok(())
}

async fn demo_rest_for_one() -> anyhow::Result<()> {
    section("RestForOne — failed child and all with higher order restart");
    let refs = ChildRefs::new();
    let config = SupervisorConfig {
        strategy: RestartStrategy::RestForOne,
        max_restarts: 10,
        within_secs: 60,
        ..Default::default()
    };
    let _sup = start_supervisor(config, &["alpha", "beta", "gamma"], &refs)
        .await
        .map_err(actor_err)?;

    let before = refs.snapshot_generations().await;
    fail_worker(&refs, "beta").await?;
    let after = refs.snapshot_generations().await;
    print_generations("[generations after beta (order=1) fails]", &before, &after);
    println!("  alpha order=0 should be unchanged, beta and gamma should restart");

    ping_worker(&refs, "alpha").await?;
    ping_worker(&refs, "beta").await?;
    ping_worker(&refs, "gamma").await?;
    Ok(())
}

async fn demo_intensity_shutdown() -> anyhow::Result<()> {
    section("Intensity limit — ShutdownSupervisor after max_restarts");
    let refs = ChildRefs::new();
    let config = SupervisorConfig {
        strategy: RestartStrategy::OneForOne,
        max_restarts: 3,
        within_secs: 10,
        intensity_action: IntensityAction::ShutdownSupervisor,
    };
    let _sup = start_supervisor(config, &["alpha", "beta", "gamma"], &refs)
        .await
        .map_err(actor_err)?;

    for i in 1..=4 {
        println!("--- failure attempt {i} ---");
        if fail_worker(&refs, "gamma").await.is_err() {
            println!("gamma already gone");
            break;
        }
    }

    let gens = refs.snapshot_generations().await;
    println!(
        "[intensity] gamma generation = {} (supervisor stops after limit)",
        gens.get("gamma").copied().unwrap_or(0)
    );
    Ok(())
}

async fn demo_intensity_abandon() -> anyhow::Result<()> {
    section("Intensity limit — AbandonChild keeps supervisor alive");
    let refs = ChildRefs::new();
    let config = SupervisorConfig {
        strategy: RestartStrategy::OneForOne,
        max_restarts: 2,
        within_secs: 10,
        intensity_action: IntensityAction::AbandonChild,
    };
    let _sup = start_supervisor(config, &["alpha", "beta", "gamma"], &refs)
        .await
        .map_err(actor_err)?;

    for i in 1..=5 {
        println!("--- failure attempt {i} ---");
        let _ = fail_worker(&refs, "beta").await;
    }

    println!("--- other children should still respond ---");
    ping_worker(&refs, "alpha").await?;
    ping_worker(&refs, "gamma").await?;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    demo_one_for_one().await?;
    demo_one_for_all().await?;
    demo_rest_for_one().await?;
    demo_intensity_shutdown().await?;
    demo_intensity_abandon().await?;

    println!("\nAll supervisor strategy demos complete.");
    Ok(())
}
