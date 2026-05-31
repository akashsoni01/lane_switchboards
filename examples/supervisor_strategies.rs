//! Supervisor strategies: OneForOne, OneForAll, RestForOne, and intensity limits.
//!
//! Run: `cargo run --example supervisor_strategies`
//! See: `examples/supervisor_strategies.md`

use lane_switchboards::actor::{Actor, ActorProcessingErr};
use lane_switchboards::supervisor::{
    spawn_child_spec, ChildRegistry, IntensityAction, RestartStrategy, Supervisor,
    SupervisorConfig,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

enum SupMsg {
    Ping,
    Fail,
}

#[derive(Clone)]
struct NamedWorker {
    name: String,
    registry: Arc<ChildRegistry<SupMsg>>,
}

#[async_trait::async_trait]
impl Actor<SupMsg> for NamedWorker {
    async fn pre_start(&mut self) -> Result<(), ActorProcessingErr> {
        self.registry.bump_generation(&self.name).await;
        println!(
            "[spawn] {} generation {}",
            self.name,
            self.registry.generation(&self.name).await
        );
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

fn actor_err(e: ActorProcessingErr) -> anyhow::Error {
    anyhow::anyhow!("{e}")
}

async fn start_supervisor(
    config: SupervisorConfig,
    names: &[&str],
    registry: Arc<ChildRegistry<SupMsg>>,
) -> Result<lane_switchboards::supervisor::SupervisorHandle<SupMsg>, ActorProcessingErr> {
    let children: Vec<_> = names
        .iter()
        .enumerate()
        .map(|(order, name)| {
            let name = (*name).to_string();
            let registry = registry.clone();
            spawn_child_spec(order, name.clone(), registry.clone(), {
                let name = name.clone();
                let registry = registry.clone();
                move || NamedWorker {
                    name: name.clone(),
                    registry: registry.clone(),
                }
            })
        })
        .collect();
    Supervisor::new(config, children)
        .start_settled(Duration::from_millis(50))
        .await
}

async fn fail_worker(registry: &ChildRegistry<SupMsg>, name: &str) -> anyhow::Result<()> {
    let actor = registry
        .get(name)
        .await
        .ok_or_else(|| anyhow::anyhow!("worker {name} not found"))?;
    actor.send(SupMsg::Fail).await.map_err(actor_err)?;
    tokio::time::sleep(Duration::from_millis(150)).await;
    Ok(())
}

async fn ping_worker(registry: &ChildRegistry<SupMsg>, name: &str) -> anyhow::Result<()> {
    let actor = registry
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
    let registry = Arc::new(ChildRegistry::new());
    let config = SupervisorConfig {
        strategy: RestartStrategy::OneForOne,
        max_restarts: 10,
        within_secs: 60,
        ..Default::default()
    };
    let _sup = start_supervisor(config, &["alpha", "beta", "gamma"], registry.clone())
        .await
        .map_err(actor_err)?;

    let before = registry.generations().await;
    fail_worker(&registry, "beta").await?;
    let after = registry.generations().await;
    print_generations("[generations after beta fails]", &before, &after);

    ping_worker(&registry, "alpha").await?;
    ping_worker(&registry, "beta").await?;
    ping_worker(&registry, "gamma").await?;
    Ok(())
}

async fn demo_one_for_all() -> anyhow::Result<()> {
    section("OneForAll — one failure restarts every child");
    let registry = Arc::new(ChildRegistry::new());
    let config = SupervisorConfig {
        strategy: RestartStrategy::OneForAll,
        max_restarts: 10,
        within_secs: 60,
        ..Default::default()
    };
    let _sup = start_supervisor(config, &["alpha", "beta", "gamma"], registry.clone())
        .await
        .map_err(actor_err)?;

    let before = registry.generations().await;
    fail_worker(&registry, "beta").await?;
    let after = registry.generations().await;
    print_generations("[generations after beta fails]", &before, &after);

    ping_worker(&registry, "alpha").await?;
    ping_worker(&registry, "beta").await?;
    ping_worker(&registry, "gamma").await?;
    Ok(())
}

async fn demo_rest_for_one() -> anyhow::Result<()> {
    section("RestForOne — failed child and all with higher order restart");
    let registry = Arc::new(ChildRegistry::new());
    let config = SupervisorConfig {
        strategy: RestartStrategy::RestForOne,
        max_restarts: 10,
        within_secs: 60,
        ..Default::default()
    };
    let _sup = start_supervisor(config, &["alpha", "beta", "gamma"], registry.clone())
        .await
        .map_err(actor_err)?;

    let before = registry.generations().await;
    fail_worker(&registry, "beta").await?;
    let after = registry.generations().await;
    print_generations("[generations after beta (order=1) fails]", &before, &after);
    println!("  alpha order=0 should be unchanged, beta and gamma should restart");

    ping_worker(&registry, "alpha").await?;
    ping_worker(&registry, "beta").await?;
    ping_worker(&registry, "gamma").await?;
    Ok(())
}

async fn demo_intensity_shutdown() -> anyhow::Result<()> {
    section("Intensity limit — ShutdownSupervisor after max_restarts");
    let registry = Arc::new(ChildRegistry::new());
    let config = SupervisorConfig {
        strategy: RestartStrategy::OneForOne,
        max_restarts: 3,
        within_secs: 10,
        intensity_action: IntensityAction::ShutdownSupervisor,
    };
    let _sup = start_supervisor(config, &["alpha", "beta", "gamma"], registry.clone())
        .await
        .map_err(actor_err)?;

    for i in 1..=4 {
        println!("--- failure attempt {i} ---");
        if fail_worker(&registry, "gamma").await.is_err() {
            println!("gamma already gone");
            break;
        }
    }

    let gens = registry.generations().await;
    println!(
        "[intensity] gamma generation = {} (supervisor stops after limit)",
        gens.get("gamma").copied().unwrap_or(0)
    );
    Ok(())
}

async fn demo_intensity_abandon() -> anyhow::Result<()> {
    section("Intensity limit — AbandonChild keeps supervisor alive");
    let registry = Arc::new(ChildRegistry::new());
    let config = SupervisorConfig {
        strategy: RestartStrategy::OneForOne,
        max_restarts: 2,
        within_secs: 10,
        intensity_action: IntensityAction::AbandonChild,
    };
    let _sup = start_supervisor(config, &["alpha", "beta", "gamma"], registry.clone())
        .await
        .map_err(actor_err)?;

    for i in 1..=5 {
        println!("--- failure attempt {i} ---");
        let _ = fail_worker(&registry, "beta").await;
    }

    println!("--- other children should still respond ---");
    ping_worker(&registry, "alpha").await?;
    ping_worker(&registry, "gamma").await?;
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
