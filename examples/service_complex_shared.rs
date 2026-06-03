//! Shared actors for [`service_complex`](./service_complex.rs) and
//! [`service_complex_cluster`](./service_complex_cluster.rs).

use lane_switchboards::actor::{Actor, ActorProcessingErr};
use lane_switchboards::supervisor::{
    supervise_actor, ChildRegistry, RestartStrategy, SupervisorConfig, SupervisorHandle,
};
use lane_switchboards::supervise_named_child;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

pub const SERVICE_A: &str = "ServiceASupervisor";
pub const SERVICE_B: &str = "ServiceBSupervisor";
pub const SERVICE_TARGET: &str = "service";

pub const CLUSTER_REPLICAS: usize = 10;

pub(crate) enum DaoAMsg {
    Ping,
    Fail,
}

pub(crate) enum DaoBMsg {
    Ping,
    Fail,
}

pub(crate) enum DaoCMsg {
    Ping,
    Fail,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServiceACommand {
    PingAll,
    FailDaoB,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServiceBCommand {
    PingAll,
    FailDaoC,
}

struct DaoAActor {
    supervisor: String,
    registry: Arc<ChildRegistry<DaoAMsg, &'static str>>,
}

struct DaoBActor {
    supervisor: String,
    registry: Arc<ChildRegistry<DaoBMsg, &'static str>>,
}

struct DaoCActor {
    supervisor: String,
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
pub struct ServiceASupervisorActor {
    label: String,
    dao_a_registry: Arc<ChildRegistry<DaoAMsg, &'static str>>,
    dao_b_registry: Arc<ChildRegistry<DaoBMsg, &'static str>>,
    inner: Arc<Mutex<Option<DaoSupervisors<DaoAMsg, DaoBMsg>>>>,
}

impl ServiceASupervisorActor {
    pub fn new_local() -> Self {
        Self::with_label(SERVICE_A.to_string())
    }

    pub fn new_replica(replica: usize) -> Self {
        Self::with_label(format!("{SERVICE_A}-replica-{replica}"))
    }

    pub fn with_registries(
        label: String,
        dao_a_registry: Arc<ChildRegistry<DaoAMsg, &'static str>>,
        dao_b_registry: Arc<ChildRegistry<DaoBMsg, &'static str>>,
    ) -> Self {
        Self {
            label,
            dao_a_registry,
            dao_b_registry,
            inner: Arc::new(Mutex::new(None)),
        }
    }

    fn with_label(label: String) -> Self {
        Self::with_registries(
            label,
            Arc::new(ChildRegistry::new()),
            Arc::new(ChildRegistry::new()),
        )
    }

    pub fn dao_a_registry(&self) -> Arc<ChildRegistry<DaoAMsg, &'static str>> {
        self.dao_a_registry.clone()
    }

    pub fn dao_b_registry(&self) -> Arc<ChildRegistry<DaoBMsg, &'static str>> {
        self.dao_b_registry.clone()
    }
}

#[async_trait::async_trait]
impl Actor<ServiceACommand> for ServiceASupervisorActor {
    async fn pre_start(&mut self) -> Result<(), ActorProcessingErr> {
        let label = self.label.clone();
        let dao_a_reg = self.dao_a_registry.clone();
        let dao_b_reg = self.dao_b_registry.clone();
        let sup_a = label.clone();
        let sup_b = label.clone();

        let dao_a_sup = supervise_named_child!(
            "dao-a",
            dao_a_reg.clone(),
            one_for_one_config(),
            Duration::from_millis(50),
            DaoAActor {
                supervisor: sup_a.clone(),
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
                supervisor: sup_b.clone(),
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

    async fn handle(&mut self, msg: ServiceACommand) -> Result<(), ActorProcessingErr> {
        match msg {
            ServiceACommand::PingAll => {
                send_dao(&self.dao_a_registry, "dao-a", DaoAMsg::Ping).await?;
                send_dao(&self.dao_b_registry, "dao-b", DaoBMsg::Ping).await?;
                Ok(())
            }
            ServiceACommand::FailDaoB => {
                send_dao(&self.dao_b_registry, "dao-b", DaoBMsg::Fail).await?;
                tokio::time::sleep(Duration::from_millis(150)).await;
                Ok(())
            }
        }
    }

    async fn post_stop(&mut self) -> Result<(), ActorProcessingErr> {
        println!(
            "[{}] actor stopping — shutting down DAO supervisors",
            self.label
        );
        if let Some(inner) = self.inner.lock().await.take() {
            inner._a.stop().await;
            inner._b.stop().await;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct ServiceBSupervisorActor {
    label: String,
    dao_b_registry: Arc<ChildRegistry<DaoBMsg, &'static str>>,
    dao_c_registry: Arc<ChildRegistry<DaoCMsg, &'static str>>,
    inner: Arc<Mutex<Option<DaoSupervisors<DaoBMsg, DaoCMsg>>>>,
}

impl ServiceBSupervisorActor {
    pub fn new_local() -> Self {
        Self::with_label(SERVICE_B.to_string())
    }

    pub fn new_replica(replica: usize) -> Self {
        Self::with_label(format!("{SERVICE_B}-replica-{replica}"))
    }

    pub fn with_registries(
        label: String,
        dao_b_registry: Arc<ChildRegistry<DaoBMsg, &'static str>>,
        dao_c_registry: Arc<ChildRegistry<DaoCMsg, &'static str>>,
    ) -> Self {
        Self {
            label,
            dao_b_registry,
            dao_c_registry,
            inner: Arc::new(Mutex::new(None)),
        }
    }

    fn with_label(label: String) -> Self {
        Self::with_registries(
            label,
            Arc::new(ChildRegistry::new()),
            Arc::new(ChildRegistry::new()),
        )
    }

    pub fn dao_b_registry(&self) -> Arc<ChildRegistry<DaoBMsg, &'static str>> {
        self.dao_b_registry.clone()
    }

    pub fn dao_c_registry(&self) -> Arc<ChildRegistry<DaoCMsg, &'static str>> {
        self.dao_c_registry.clone()
    }
}

#[async_trait::async_trait]
impl Actor<ServiceBCommand> for ServiceBSupervisorActor {
    async fn pre_start(&mut self) -> Result<(), ActorProcessingErr> {
        let label = self.label.clone();
        let dao_b_reg = self.dao_b_registry.clone();
        let dao_c_reg = self.dao_c_registry.clone();
        let sup_b = label.clone();
        let sup_c = label.clone();

        let dao_b_sup = supervise_named_child!(
            "dao-b",
            dao_b_reg.clone(),
            one_for_one_config(),
            Duration::from_millis(50),
            DaoBActor {
                supervisor: sup_b.clone(),
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
                supervisor: sup_c.clone(),
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

    async fn handle(&mut self, msg: ServiceBCommand) -> Result<(), ActorProcessingErr> {
        match msg {
            ServiceBCommand::PingAll => {
                send_dao(&self.dao_b_registry, "dao-b", DaoBMsg::Ping).await?;
                send_dao(&self.dao_c_registry, "dao-c", DaoCMsg::Ping).await?;
                Ok(())
            }
            ServiceBCommand::FailDaoC => {
                send_dao(&self.dao_c_registry, "dao-c", DaoCMsg::Fail).await?;
                tokio::time::sleep(Duration::from_millis(150)).await;
                Ok(())
            }
        }
    }

    async fn post_stop(&mut self) -> Result<(), ActorProcessingErr> {
        println!(
            "[{}] actor stopping — shutting down DAO supervisors",
            self.label
        );
        if let Some(inner) = self.inner.lock().await.take() {
            inner._a.stop().await;
            inner._b.stop().await;
        }
        Ok(())
    }
}

pub fn one_for_one_config() -> SupervisorConfig {
    SupervisorConfig {
        strategy: RestartStrategy::OneForOne,
        max_restarts: 10,
        within_secs: 60,
        ..Default::default()
    }
}

pub fn actor_err(e: ActorProcessingErr) -> anyhow::Error {
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

pub async fn service_a_generations(
    dao_a: &ChildRegistry<DaoAMsg, &'static str>,
    dao_b: &ChildRegistry<DaoBMsg, &'static str>,
) -> HashMap<String, u64> {
    let mut gens = HashMap::new();
    gens.insert("dao-a".into(), dao_a.generation("dao-a").await);
    gens.insert("dao-b".into(), dao_b.generation("dao-b").await);
    gens
}

pub async fn service_b_generations(
    dao_b: &ChildRegistry<DaoBMsg, &'static str>,
    dao_c: &ChildRegistry<DaoCMsg, &'static str>,
) -> HashMap<String, u64> {
    let mut gens = HashMap::new();
    gens.insert("dao-b".into(), dao_b.generation("dao-b").await);
    gens.insert("dao-c".into(), dao_c.generation("dao-c").await);
    gens
}

pub fn print_gens(
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

pub async fn start_supervised_service_a(
) -> Result<
    (
        lane_switchboards::actor::ActorRef<ServiceACommand>,
        SupervisorHandle<ServiceACommand>,
        Arc<ChildRegistry<DaoAMsg, &'static str>>,
        Arc<ChildRegistry<DaoBMsg, &'static str>>,
    ),
    ActorProcessingErr,
> {
    let dao_a = Arc::new(ChildRegistry::new());
    let dao_b = Arc::new(ChildRegistry::new());
    let actor = ServiceASupervisorActor::with_registries(
        SERVICE_A.to_string(),
        dao_a.clone(),
        dao_b.clone(),
    );
    let (service_ref, sup) = supervise_actor(actor, one_for_one_config()).await?;
    Ok((service_ref, sup, dao_a, dao_b))
}

pub async fn start_supervised_service_b(
) -> Result<
    (
        lane_switchboards::actor::ActorRef<ServiceBCommand>,
        SupervisorHandle<ServiceBCommand>,
        Arc<ChildRegistry<DaoBMsg, &'static str>>,
        Arc<ChildRegistry<DaoCMsg, &'static str>>,
    ),
    ActorProcessingErr,
> {
    let dao_b = Arc::new(ChildRegistry::new());
    let dao_c = Arc::new(ChildRegistry::new());
    let actor = ServiceBSupervisorActor::with_registries(
        SERVICE_B.to_string(),
        dao_b.clone(),
        dao_c.clone(),
    );
    let (service_ref, sup) = supervise_actor(actor, one_for_one_config()).await?;
    Ok((service_ref, sup, dao_b, dao_c))
}
