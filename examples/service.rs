//! Two separate supervisors — `ServiceASupervisor` and `ServiceBSupervisor` —
//! each supervising two DAO actors with `OneForOne` (restart only the child that crashed).
//!
//! Each DAO has its own message type: `DaoAMsg`, `DaoBMsg`, `DaoCMsg`.
//!
//! - **ServiceASupervisor** → `DaoAActor` (`DaoAMsg`), `DaoBActor` (`DaoBMsg`)
//! - **ServiceBSupervisor** → `DaoBActor` (`DaoBMsg`), `DaoCActor` (`DaoCMsg`)
//!
//! `ServiceASupervisor` / `ServiceBSupervisor` are coordinators in `main`, not supervised
//! actors. DAO failures restart only that child; Service A vs B are isolated. See
//! `examples/service.md` — “What happens on crash / panic?”.
//!
//! Run: `cargo run --example service`
//! See: `examples/service.md`

use lane_switchboards::actor::{Actor, ActorProcessingErr};
use lane_switchboards::supervisor::{
    spawn_child_spec, ChildRegistry, RestartStrategy, Supervisor, SupervisorConfig,
    SupervisorHandle,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

const SERVICE_A: &str = "ServiceASupervisor";
const SERVICE_B: &str = "ServiceBSupervisor";

enum DaoAMsg {
    Ping,
    Fail,
}

enum DaoBMsg {
    Ping,
    Fail,
}

enum DaoCMsg {
    Ping,
    Fail,
}

struct DaoAActor {
    supervisor: &'static str,
    registry: Arc<ChildRegistry<DaoAMsg, &'static str>>,
}

struct DaoBActor {
    supervisor: &'static str,
    registry: Arc<ChildRegistry<DaoBMsg, &'static str>>,
}

struct DaoCActor {
    supervisor: &'static str,
    registry: Arc<ChildRegistry<DaoCMsg, &'static str>>,
}

#[async_trait::async_trait]
impl Actor<DaoAMsg> for DaoAActor {
    async fn pre_start(&mut self) -> Result<(), ActorProcessingErr> {
        self.registry.bump_generation("dao-a").await;
        println!(
            "[{}] DaoA started (gen {})",
            self.supervisor,
            self.registry.generation("dao-a").await
        );
        Ok(())
    }

    async fn handle(&mut self, msg: DaoAMsg) -> Result<(), ActorProcessingErr> {
        match msg {
            DaoAMsg::Ping => {
                println!("[{}] DaoA ping", self.supervisor);
                Ok(())
            }
            DaoAMsg::Fail => Err(format!("{} DaoA failed", self.supervisor).into()),
        }
    }

    async fn post_stop(&mut self) -> Result<(), ActorProcessingErr> {
        println!("[{}] DaoA stopped", self.supervisor);
        Ok(())
    }
}

#[async_trait::async_trait]
impl Actor<DaoBMsg> for DaoBActor {
    async fn pre_start(&mut self) -> Result<(), ActorProcessingErr> {
        self.registry.bump_generation("dao-b").await;
        println!(
            "[{}] DaoB started (gen {})",
            self.supervisor,
            self.registry.generation("dao-b").await
        );
        Ok(())
    }

    async fn handle(&mut self, msg: DaoBMsg) -> Result<(), ActorProcessingErr> {
        match msg {
            DaoBMsg::Ping => {
                println!("[{}] DaoB ping", self.supervisor);
                Ok(())
            }
            DaoBMsg::Fail => Err(format!("{} DaoB failed", self.supervisor).into()),
        }
    }

    async fn post_stop(&mut self) -> Result<(), ActorProcessingErr> {
        println!("[{}] DaoB stopped", self.supervisor);
        Ok(())
    }
}

#[async_trait::async_trait]
impl Actor<DaoCMsg> for DaoCActor {
    async fn pre_start(&mut self) -> Result<(), ActorProcessingErr> {
        self.registry.bump_generation("dao-c").await;
        println!(
            "[{}] DaoC started (gen {})",
            self.supervisor,
            self.registry.generation("dao-c").await
        );
        Ok(())
    }

    async fn handle(&mut self, msg: DaoCMsg) -> Result<(), ActorProcessingErr> {
        match msg {
            DaoCMsg::Ping => {
                println!("[{}] DaoC ping", self.supervisor);
                Ok(())
            }
            DaoCMsg::Fail => Err(format!("{} DaoC failed", self.supervisor).into()),
        }
    }

    async fn post_stop(&mut self) -> Result<(), ActorProcessingErr> {
        println!("[{}] DaoC stopped", self.supervisor);
        Ok(())
    }
}

fn one_for_one_config() -> SupervisorConfig {
    SupervisorConfig {
        strategy: RestartStrategy::OneForOne,
        max_restarts: 10,
        within_secs: 60,
        ..Default::default()
    }
}

fn actor_err(e: ActorProcessingErr) -> anyhow::Error {
    anyhow::anyhow!("{e}")
}

async fn start_one_child<M, B, F>(
    name: &'static str,
    registry: Arc<ChildRegistry<M, &'static str>>,
    build: F,
) -> Result<SupervisorHandle<M>, ActorProcessingErr>
where
    M: Send + Sync + 'static,
    B: Actor<M> + Send + Sync + 'static,
    F: Fn() -> B + Send + Sync + 'static,
{
    let children = vec![spawn_child_spec(0, name, registry, build)];
    Supervisor::new(one_for_one_config(), children)
        .start_settled(Duration::from_millis(50))
        .await
}

async fn send_msg<M>(
    registry: &ChildRegistry<M, &'static str>,
    name: &'static str,
    msg: M,
) -> anyhow::Result<()>
where
    M: Send + Sync + 'static,
{
    let actor = registry
        .get(name)
        .await
        .ok_or_else(|| anyhow::anyhow!("child not in registry"))?;
    actor.send(msg).await.map_err(actor_err)?;
    Ok(())
}

fn print_gens(
    supervisor: &str,
    label: &str,
    keys: &[&'static str],
    before: &HashMap<&'static str, u64>,
    after: &HashMap<&'static str, u64>,
) {
    println!("{label} ({supervisor})");
    for key in keys {
        let b = before.get(key).copied().unwrap_or(0);
        let a = after.get(key).copied().unwrap_or(0);
        let delta = a.saturating_sub(b);
        println!("  {key}: {b} -> {a} (+{delta})");
    }
}

/// Supervises `DaoAActor` + `DaoBActor` (each child has its own `Supervisor` and message type).
struct ServiceASupervisor {
    dao_a_registry: Arc<ChildRegistry<DaoAMsg, &'static str>>,
    _dao_a_sup: SupervisorHandle<DaoAMsg>,
    dao_b_registry: Arc<ChildRegistry<DaoBMsg, &'static str>>,
    _dao_b_sup: SupervisorHandle<DaoBMsg>,
}

impl ServiceASupervisor {
    async fn start() -> Result<Self, ActorProcessingErr> {
        let dao_a_registry = Arc::new(ChildRegistry::new());
        let dao_a_sup = start_one_child("dao-a", dao_a_registry.clone(), {
            let registry = dao_a_registry.clone();
            move || DaoAActor {
                supervisor: SERVICE_A,
                registry: registry.clone(),
            }
        })
        .await?;

        let dao_b_registry = Arc::new(ChildRegistry::new());
        let dao_b_sup = start_one_child("dao-b", dao_b_registry.clone(), {
            let registry = dao_b_registry.clone();
            move || DaoBActor {
                supervisor: SERVICE_A,
                registry: registry.clone(),
            }
        })
        .await?;

        println!("[{SERVICE_A}] started (DaoA + DaoB)");
        Ok(Self {
            dao_a_registry,
            _dao_a_sup: dao_a_sup,
            dao_b_registry,
            _dao_b_sup: dao_b_sup,
        })
    }

    async fn ping_all(&self) -> anyhow::Result<()> {
        send_msg(&self.dao_a_registry, "dao-a", DaoAMsg::Ping).await?;
        send_msg(&self.dao_b_registry, "dao-b", DaoBMsg::Ping).await?;
        Ok(())
    }

    async fn fail_dao_b(&self) -> anyhow::Result<()> {
        send_msg(&self.dao_b_registry, "dao-b", DaoBMsg::Fail).await?;
        tokio::time::sleep(Duration::from_millis(150)).await;
        Ok(())
    }

    async fn generations(&self) -> HashMap<&'static str, u64> {
        let mut gens = HashMap::new();
        gens.insert("dao-a", self.dao_a_registry.generation("dao-a").await);
        gens.insert("dao-b", self.dao_b_registry.generation("dao-b").await);
        gens
    }
}

/// Supervises `DaoBActor` + `DaoCActor` (separate message types per child).
struct ServiceBSupervisor {
    dao_b_registry: Arc<ChildRegistry<DaoBMsg, &'static str>>,
    _dao_b_sup: SupervisorHandle<DaoBMsg>,
    dao_c_registry: Arc<ChildRegistry<DaoCMsg, &'static str>>,
    _dao_c_sup: SupervisorHandle<DaoCMsg>,
}

impl ServiceBSupervisor {
    async fn start() -> Result<Self, ActorProcessingErr> {
        let dao_b_registry = Arc::new(ChildRegistry::new());
        let dao_b_sup = start_one_child("dao-b", dao_b_registry.clone(), {
            let registry = dao_b_registry.clone();
            move || DaoBActor {
                supervisor: SERVICE_B,
                registry: registry.clone(),
            }
        })
        .await?;

        let dao_c_registry = Arc::new(ChildRegistry::new());
        let dao_c_sup = start_one_child("dao-c", dao_c_registry.clone(), {
            let registry = dao_c_registry.clone();
            move || DaoCActor {
                supervisor: SERVICE_B,
                registry: registry.clone(),
            }
        })
        .await?;

        println!("[{SERVICE_B}] started (DaoB + DaoC)");
        Ok(Self {
            dao_b_registry,
            _dao_b_sup: dao_b_sup,
            dao_c_registry,
            _dao_c_sup: dao_c_sup,
        })
    }

    async fn ping_all(&self) -> anyhow::Result<()> {
        send_msg(&self.dao_b_registry, "dao-b", DaoBMsg::Ping).await?;
        send_msg(&self.dao_c_registry, "dao-c", DaoCMsg::Ping).await?;
        Ok(())
    }

    async fn fail_dao_c(&self) -> anyhow::Result<()> {
        send_msg(&self.dao_c_registry, "dao-c", DaoCMsg::Fail).await?;
        tokio::time::sleep(Duration::from_millis(150)).await;
        Ok(())
    }

    async fn generations(&self) -> HashMap<&'static str, u64> {
        let mut gens = HashMap::new();
        gens.insert("dao-b", self.dao_b_registry.generation("dao-b").await);
        gens.insert("dao-c", self.dao_c_registry.generation("dao-c").await);
        gens
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    println!("=== ServiceASupervisor + ServiceBSupervisor (OneForOne per DAO) ===\n");

    let service_a = ServiceASupervisor::start().await.map_err(actor_err)?;
    let service_b = ServiceBSupervisor::start().await.map_err(actor_err)?;

    println!("\n--- Initial ping ---");
    service_a.ping_all().await?;
    service_b.ping_all().await?;

    println!("\n--- Crash DaoB under ServiceASupervisor only ---");
    let before = service_a.generations().await;
    service_a.fail_dao_b().await?;
    let after = service_a.generations().await;
    print_gens(SERVICE_A, "[generations]", &["dao-a", "dao-b"], &before, &after);

    let b_before = service_b.generations().await;
    service_b.ping_all().await?;
    let b_after = service_b.generations().await;
    print_gens(
        SERVICE_B,
        "[ServiceBSupervisor unchanged]",
        &["dao-b", "dao-c"],
        &b_before,
        &b_after,
    );

    println!("\n--- Crash DaoC under ServiceBSupervisor only ---");
    let before = service_b.generations().await;
    service_b.fail_dao_c().await?;
    let after = service_b.generations().await;
    print_gens(SERVICE_B, "[generations]", &["dao-b", "dao-c"], &before, &after);

    let a_before = service_a.generations().await;
    service_a.ping_all().await?;
    let a_after = service_a.generations().await;
    print_gens(
        SERVICE_A,
        "[ServiceASupervisor unchanged]",
        &["dao-a", "dao-b"],
        &a_before,
        &a_after,
    );

    println!("\n--- Done: each supervisor restarts only its own failed DAO ===");
    Ok(())
}
