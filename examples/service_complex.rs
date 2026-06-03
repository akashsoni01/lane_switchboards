//! Same DAO layout as [`service`](./service.rs), but **`ServiceASupervisor` and
//! `ServiceBSupervisor` are supervised actors** (each wrapped in `supervise_actor`).
//!
//! Inner DAOs still use one-child supervisors via [`supervise_named_child!`] because each
//! DAO has a distinct message type.
//!
//! Run: `cargo run --example service_complex`
//! See: `examples/service_complex.md`

use lane_switchboards::actor::{Actor, ActorProcessingErr, ActorRef};
use lane_switchboards::supervisor::{
    supervise_actor, ChildRegistry, RestartStrategy, SupervisorConfig, SupervisorHandle,
};
use lane_switchboards::supervise_named_child;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, Mutex};

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

enum ServiceAMsg {
    PingAll,
    FailDaoB,
    Generations(oneshot::Sender<HashMap<String, u64>>),
}

enum ServiceBMsg {
    PingAll,
    FailDaoC,
    Generations(oneshot::Sender<HashMap<String, u64>>),
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

struct DaoSupervisors<M1, M2>
where
    M1: Send + Sync + 'static,
    M2: Send + Sync + 'static,
{
    _a: SupervisorHandle<M1>,
    _b: SupervisorHandle<M2>,
}

#[derive(Clone)]
struct ServiceASupervisorActor {
    label: &'static str,
    dao_a_registry: Arc<ChildRegistry<DaoAMsg, &'static str>>,
    dao_b_registry: Arc<ChildRegistry<DaoBMsg, &'static str>>,
    inner: Arc<Mutex<Option<DaoSupervisors<DaoAMsg, DaoBMsg>>>>,
}

impl ServiceASupervisorActor {
    fn new() -> Self {
        Self {
            label: SERVICE_A,
            dao_a_registry: Arc::new(ChildRegistry::new()),
            dao_b_registry: Arc::new(ChildRegistry::new()),
            inner: Arc::new(Mutex::new(None)),
        }
    }
}

#[async_trait::async_trait]
impl Actor<ServiceAMsg> for ServiceASupervisorActor {
    async fn pre_start(&mut self) -> Result<(), ActorProcessingErr> {
        let label = self.label;
        let dao_a_reg = self.dao_a_registry.clone();
        let dao_b_reg = self.dao_b_registry.clone();

        let dao_a_sup = supervise_named_child!(
            "dao-a",
            dao_a_reg.clone(),
            one_for_one_config(),
            Duration::from_millis(50),
            DaoAActor {
                supervisor: label,
                registry: dao_a_reg.clone(),
            }
        )
        .await?;

        let dao_b_sup = supervise_named_child!(
            "dao-b",
            dao_b_reg.clone(),
            one_for_one_config(),
            Duration::from_millis(50),
            DaoBActor {
                supervisor: label,
                registry: dao_b_reg.clone(),
            }
        )
        .await?;

        println!("[{label}] actor started (DaoA + DaoB)");
        *self.inner.lock().await = Some(DaoSupervisors {
            _a: dao_a_sup,
            _b: dao_b_sup,
        });
        Ok(())
    }

    async fn handle(&mut self, msg: ServiceAMsg) -> Result<(), ActorProcessingErr> {
        match msg {
            ServiceAMsg::PingAll => {
                send_dao(&self.dao_a_registry, "dao-a", DaoAMsg::Ping).await?;
                send_dao(&self.dao_b_registry, "dao-b", DaoBMsg::Ping).await?;
                Ok(())
            }
            ServiceAMsg::FailDaoB => {
                send_dao(&self.dao_b_registry, "dao-b", DaoBMsg::Fail).await?;
                tokio::time::sleep(Duration::from_millis(150)).await;
                Ok(())
            }
            ServiceAMsg::Generations(reply) => {
                let mut gens = HashMap::new();
                gens.insert(
                    "dao-a".into(),
                    self.dao_a_registry.generation("dao-a").await,
                );
                gens.insert(
                    "dao-b".into(),
                    self.dao_b_registry.generation("dao-b").await,
                );
                let _ = reply.send(gens);
                Ok(())
            }
        }
    }

    async fn post_stop(&mut self) -> Result<(), ActorProcessingErr> {
        println!("[{}] actor stopping — shutting down DAO supervisors", self.label);
        if let Some(inner) = self.inner.lock().await.take() {
            inner._a.stop().await;
            inner._b.stop().await;
        }
        Ok(())
    }
}

#[derive(Clone)]
struct ServiceBSupervisorActor {
    label: &'static str,
    dao_b_registry: Arc<ChildRegistry<DaoBMsg, &'static str>>,
    dao_c_registry: Arc<ChildRegistry<DaoCMsg, &'static str>>,
    inner: Arc<Mutex<Option<DaoSupervisors<DaoBMsg, DaoCMsg>>>>,
}

impl ServiceBSupervisorActor {
    fn new() -> Self {
        Self {
            label: SERVICE_B,
            dao_b_registry: Arc::new(ChildRegistry::new()),
            dao_c_registry: Arc::new(ChildRegistry::new()),
            inner: Arc::new(Mutex::new(None)),
        }
    }
}

#[async_trait::async_trait]
impl Actor<ServiceBMsg> for ServiceBSupervisorActor {
    async fn pre_start(&mut self) -> Result<(), ActorProcessingErr> {
        let label = self.label;
        let dao_b_reg = self.dao_b_registry.clone();
        let dao_c_reg = self.dao_c_registry.clone();

        let dao_b_sup = supervise_named_child!(
            "dao-b",
            dao_b_reg.clone(),
            one_for_one_config(),
            Duration::from_millis(50),
            DaoBActor {
                supervisor: label,
                registry: dao_b_reg.clone(),
            }
        )
        .await?;

        let dao_c_sup = supervise_named_child!(
            "dao-c",
            dao_c_reg.clone(),
            one_for_one_config(),
            Duration::from_millis(50),
            DaoCActor {
                supervisor: label,
                registry: dao_c_reg.clone(),
            }
        )
        .await?;

        println!("[{label}] actor started (DaoB + DaoC)");
        *self.inner.lock().await = Some(DaoSupervisors {
            _a: dao_b_sup,
            _b: dao_c_sup,
        });
        Ok(())
    }

    async fn handle(&mut self, msg: ServiceBMsg) -> Result<(), ActorProcessingErr> {
        match msg {
            ServiceBMsg::PingAll => {
                send_dao(&self.dao_b_registry, "dao-b", DaoBMsg::Ping).await?;
                send_dao(&self.dao_c_registry, "dao-c", DaoCMsg::Ping).await?;
                Ok(())
            }
            ServiceBMsg::FailDaoC => {
                send_dao(&self.dao_c_registry, "dao-c", DaoCMsg::Fail).await?;
                tokio::time::sleep(Duration::from_millis(150)).await;
                Ok(())
            }
            ServiceBMsg::Generations(reply) => {
                let mut gens = HashMap::new();
                gens.insert(
                    "dao-b".into(),
                    self.dao_b_registry.generation("dao-b").await,
                );
                gens.insert(
                    "dao-c".into(),
                    self.dao_c_registry.generation("dao-c").await,
                );
                let _ = reply.send(gens);
                Ok(())
            }
        }
    }

    async fn post_stop(&mut self) -> Result<(), ActorProcessingErr> {
        println!("[{}] actor stopping — shutting down DAO supervisors", self.label);
        if let Some(inner) = self.inner.lock().await.take() {
            inner._a.stop().await;
            inner._b.stop().await;
        }
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

async fn send_dao<M>(
    registry: &ChildRegistry<M, &'static str>,
    name: &'static str,
    msg: M,
) -> Result<(), ActorProcessingErr>
where
    M: Send + Sync + 'static,
{
    let actor = registry
        .get(name)
        .await
        .ok_or_else(|| -> ActorProcessingErr { "child not in registry".into() })?;
    actor.send(msg).await?;
    Ok(())
}

async fn service_generations(
    service: &ActorRef<ServiceAMsg>,
) -> anyhow::Result<HashMap<String, u64>> {
    let (tx, rx) = oneshot::channel();
    service
        .send(ServiceAMsg::Generations(tx))
        .await
        .map_err(actor_err)?;
    rx.await
        .map_err(|_| anyhow::anyhow!("service dropped generations reply"))
}

async fn service_b_generations(
    service: &ActorRef<ServiceBMsg>,
) -> anyhow::Result<HashMap<String, u64>> {
    let (tx, rx) = oneshot::channel();
    service
        .send(ServiceBMsg::Generations(tx))
        .await
        .map_err(actor_err)?;
    rx.await
        .map_err(|_| anyhow::anyhow!("service dropped generations reply"))
}

fn print_gens(
    supervisor: &str,
    label: &str,
    keys: &[&str],
    before: &HashMap<String, u64>,
    after: &HashMap<String, u64>,
) {
    println!("{label} ({supervisor})");
    for key in keys {
        let b = before.get(*key).copied().unwrap_or(0);
        let a = after.get(*key).copied().unwrap_or(0);
        let delta = a.saturating_sub(b);
        println!("  {key}: {b} -> {a} (+{delta})");
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    println!("=== service_complex: supervised ServiceA / ServiceB actors ===\n");

    let (service_a, _sup_a) = supervise_actor(ServiceASupervisorActor::new(), one_for_one_config())
        .await
        .map_err(actor_err)?;
    let (service_b, _sup_b) = supervise_actor(ServiceBSupervisorActor::new(), one_for_one_config())
        .await
        .map_err(actor_err)?;

    println!("\n--- Initial ping ---");
    service_a
        .send(ServiceAMsg::PingAll)
        .await
        .map_err(actor_err)?;
    service_b
        .send(ServiceBMsg::PingAll)
        .await
        .map_err(actor_err)?;

    println!("\n--- Crash DaoB under ServiceASupervisor only ---");
    let before = service_generations(&service_a).await?;
    service_a
        .send(ServiceAMsg::FailDaoB)
        .await
        .map_err(actor_err)?;
    let after = service_generations(&service_a).await?;
    print_gens(
        SERVICE_A,
        "[generations]",
        &["dao-a", "dao-b"],
        &before,
        &after,
    );

    let b_before = service_b_generations(&service_b).await?;
    service_b
        .send(ServiceBMsg::PingAll)
        .await
        .map_err(actor_err)?;
    let b_after = service_b_generations(&service_b).await?;
    print_gens(
        SERVICE_B,
        "[ServiceBSupervisor unchanged]",
        &["dao-b", "dao-c"],
        &b_before,
        &b_after,
    );

    println!("\n--- Crash DaoC under ServiceBSupervisor only ---");
    let before = service_b_generations(&service_b).await?;
    service_b
        .send(ServiceBMsg::FailDaoC)
        .await
        .map_err(actor_err)?;
    let after = service_b_generations(&service_b).await?;
    print_gens(
        SERVICE_B,
        "[generations]",
        &["dao-b", "dao-c"],
        &before,
        &after,
    );

    let a_before = service_generations(&service_a).await?;
    service_a
        .send(ServiceAMsg::PingAll)
        .await
        .map_err(actor_err)?;
    let a_after = service_generations(&service_a).await?;
    print_gens(
        SERVICE_A,
        "[ServiceASupervisor unchanged]",
        &["dao-a", "dao-b"],
        &a_before,
        &a_after,
    );

    println!("\n--- Done: DAO crash = inner sup; service actor has outer sup ---");
    Ok(())
}
