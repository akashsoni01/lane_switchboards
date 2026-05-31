//! OTP-style supervisor with OneForOne, OneForAll, and RestForOne strategies.

use crate::actor::{spawn_on_runtime, Actor, ActorId, ActorProcessingErr, ActorRef};
use crate::config::ActorConfig;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;

/// Notification sent to supervisor when a child fails.
#[derive(Debug, Clone)]
pub struct RestartSignal {
    pub child_id: ActorId,
    pub reason: String,
}

/// Restart strategy (mirrors Erlang).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestartStrategy {
    OneForOne,
    OneForAll,
    RestForOne,
}

/// What to do when restart intensity is exceeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntensityAction {
    ShutdownSupervisor,
    AbandonChild,
}

/// Supervisor configuration.
#[derive(Debug, Clone)]
pub struct SupervisorConfig {
    pub strategy: RestartStrategy,
    pub max_restarts: usize,
    pub within_secs: u64,
    pub intensity_action: IntensityAction,
    /// Restart-signal queue capacity.
    pub mailbox_capacity: usize,
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            strategy: RestartStrategy::OneForOne,
            max_restarts: 5,
            within_secs: 10,
            intensity_action: IntensityAction::ShutdownSupervisor,
            mailbox_capacity: 32,
        }
    }
}

/// Factory trait for supervised children.
pub trait ChildSpec<M: Send + Sync + 'static>: Send + Sync {
    fn id(&self) -> ActorId;
    fn order(&self) -> usize;
    fn restart(
        &self,
        supervisor_tx: mpsc::Sender<RestartSignal>,
        actor_config: &ActorConfig,
    ) -> Pin<Box<dyn Future<Output = Result<ActorRef<M>, ActorProcessingErr>> + Send>>;
    fn set_id(&mut self, id: ActorId);
}

struct FnChildSpec<M, F> {
    id: ActorId,
    order: usize,
    factory: F,
    _marker: std::marker::PhantomData<M>,
}

impl<M, F> ChildSpec<M> for FnChildSpec<M, F>
where
    M: Send + Sync + 'static,
    F: Fn(mpsc::Sender<RestartSignal>, &ActorConfig) -> Pin<Box<dyn Future<Output = Result<ActorRef<M>, ActorProcessingErr>> + Send>>
        + Send
        + Sync,
{
    fn id(&self) -> ActorId {
        self.id
    }

    fn order(&self) -> usize {
        self.order
    }

    fn restart(
        &self,
        supervisor_tx: mpsc::Sender<RestartSignal>,
        actor_config: &ActorConfig,
    ) -> Pin<Box<dyn Future<Output = Result<ActorRef<M>, ActorProcessingErr>> + Send>> {
        (self.factory)(supervisor_tx, actor_config)
    }

    fn set_id(&mut self, id: ActorId) {
        self.id = id;
    }
}

/// Build a child spec from a spawn factory closure.
pub fn child_spec<M, F>(order: usize, factory: F) -> Box<dyn ChildSpec<M>>
where
    M: Send + Sync + 'static,
    F: Fn(mpsc::Sender<RestartSignal>, &ActorConfig) -> Pin<Box<dyn Future<Output = Result<ActorRef<M>, ActorProcessingErr>> + Send>>
        + Send
        + Sync
        + 'static,
{
    Box::new(FnChildSpec::<M, F> {
        id: ActorId::new(),
        order,
        factory,
        _marker: std::marker::PhantomData,
    })
}

/// Named child refs updated on every spawn/restart — share with actors and main.
#[derive(Clone)]
pub struct ChildRegistry<M: Send + Sync + 'static> {
    by_name: Arc<Mutex<HashMap<String, ActorRef<M>>>>,
    generations: Arc<Mutex<HashMap<String, u64>>>,
}

impl<M: Send + Sync + 'static> Default for ChildRegistry<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M: Send + Sync + 'static> ChildRegistry<M> {
    pub fn new() -> Self {
        Self {
            by_name: Arc::new(Mutex::new(HashMap::new())),
            generations: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn get(&self, name: &str) -> Option<ActorRef<M>> {
        self.by_name.lock().await.get(name).cloned()
    }

    pub async fn track(&self, name: impl Into<String>, actor_ref: ActorRef<M>) {
        self.by_name.lock().await.insert(name.into(), actor_ref);
    }

    pub async fn bump_generation(&self, name: &str) {
        let mut gens = self.generations.lock().await;
        *gens.entry(name.to_string()).or_insert(0) += 1;
    }

    pub async fn generation(&self, name: &str) -> u64 {
        self.generations.lock().await.get(name).copied().unwrap_or(0)
    }

    pub async fn generations(&self) -> HashMap<String, u64> {
        self.generations.lock().await.clone()
    }
}

/// Single supervised child slot — use for OneForOne with one actor.
#[derive(Clone)]
pub struct ChildSlot<M: Send + Sync + 'static> {
    current: Arc<Mutex<Option<ActorRef<M>>>>,
}

impl<M: Send + Sync + 'static> Default for ChildSlot<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M: Send + Sync + 'static> ChildSlot<M> {
    pub fn new() -> Self {
        Self {
            current: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn get(&self) -> Option<ActorRef<M>> {
        self.current.lock().await.clone()
    }

    pub async fn require(&self) -> Result<ActorRef<M>, ActorProcessingErr> {
        self.get()
            .await
            .ok_or_else(|| "supervised child not running".into())
    }

    /// Build a child spec that spawns `build()` and keeps `slot` current.
    pub fn child_spec<B, F>(order: usize, slot: Arc<Self>, build: F) -> Box<dyn ChildSpec<M>>
    where
        B: Actor<M> + Send + Sync + 'static,
        F: Fn() -> B + Send + Sync + 'static,
    {
        let build = Arc::new(build);
        child_spec(order, move |sup_tx, actor_config| {
            let slot = slot.clone();
            let build = build.clone();
            let actor_config = *actor_config;
            Box::pin(async move {
                let (actor_ref, _) = spawn_on_runtime(&Handle::current(), build(), Some(sup_tx), &actor_config).await?;
                *slot.current.lock().await = Some(actor_ref.clone());
                Ok(actor_ref)
            })
        })
    }
}

/// Build a child spec: spawn `build()`, register under `name`, at supervisor `order`.
pub fn spawn_child_spec<M, B, F>(
    order: usize,
    name: impl Into<String>,
    registry: Arc<ChildRegistry<M>>,
    build: F,
) -> Box<dyn ChildSpec<M>>
where
    M: Send + Sync + 'static,
    B: Actor<M> + Send + Sync + 'static,
    F: Fn() -> B + Send + Sync + 'static,
{
    let name = name.into();
    let build = Arc::new(build);
    child_spec(order, move |sup_tx, actor_config| {
        let registry = registry.clone();
        let name = name.clone();
        let build = build.clone();
        let actor_config = *actor_config;
        Box::pin(async move {
            let (actor_ref, _) =
                spawn_on_runtime(&Handle::current(), build(), Some(sup_tx), &actor_config).await?;
            registry.track(name, actor_ref.clone()).await;
            Ok(actor_ref)
        })
    })
}

/// Handle to a running supervisor.
pub struct SupervisorHandle<M: Send + Sync + 'static> {
    pub id: ActorId,
    _join: JoinHandle<()>,
    _marker: std::marker::PhantomData<M>,
}

/// OTP supervisor task.
pub struct Supervisor<M: Send + Sync + 'static> {
    config: SupervisorConfig,
    actor_config: ActorConfig,
    children: Arc<Mutex<Vec<Box<dyn ChildSpec<M>>>>>,
}

impl<M: Send + Sync + 'static> Supervisor<M> {
    pub fn new(config: SupervisorConfig, children: Vec<Box<dyn ChildSpec<M>>>) -> Self {
        Self::with_actor_config(ActorConfig::default(), config, children)
    }

    pub fn with_actor_config(
        actor_config: ActorConfig,
        config: SupervisorConfig,
        children: Vec<Box<dyn ChildSpec<M>>>,
    ) -> Self {
        Self {
            config,
            actor_config,
            children: Arc::new(Mutex::new(children)),
        }
    }

    pub async fn start(self) -> Result<SupervisorHandle<M>, ActorProcessingErr> {
        self.start_settled(Duration::ZERO).await
    }

    /// Start the supervisor and optionally wait for initial spawns to settle.
    pub async fn start_settled(
        self,
        settle: Duration,
    ) -> Result<SupervisorHandle<M>, ActorProcessingErr> {
        let children = self.children.clone();
        let config = self.config.clone();
        let actor_config = self.actor_config;
        let (tx, mut rx) = mpsc::channel::<RestartSignal>(config.mailbox_capacity);

        // Initial spawn of all children
        {
            let mut slots = children.lock().await;
            for spec in slots.iter_mut() {
                let sup_tx = tx.clone();
                let actor_ref = spec.restart(sup_tx, &actor_config).await?;
                spec.set_id(actor_ref.id);
            }
        }

        if !settle.is_zero() {
            tokio::time::sleep(settle).await;
        }

        let join = tokio::spawn(async move {
            let mut restart_log: Vec<Instant> = Vec::new();

            while let Some(signal) = rx.recv().await {
                let now = Instant::now();
                restart_log.retain(|t| now.duration_since(*t) < Duration::from_secs(config.within_secs));
                restart_log.push(now);

                if restart_log.len() > config.max_restarts {
                    match config.intensity_action {
                        IntensityAction::ShutdownSupervisor => {
                            tracing::error!("supervisor restart intensity exceeded — shutting down");
                            break;
                        }
                        IntensityAction::AbandonChild => {
                            tracing::warn!(child = %signal.child_id, "abandoning child after intensity breach");
                            continue;
                        }
                    }
                }

                let mut slots = children.lock().await;
                let failed_order = slots
                    .iter()
                    .find(|s| s.id() == signal.child_id)
                    .map(|s| s.order());

                let indices: Vec<usize> = match config.strategy {
                    RestartStrategy::OneForOne => slots
                        .iter()
                        .enumerate()
                        .filter(|(_, s)| s.id() == signal.child_id)
                        .map(|(i, _)| i)
                        .collect(),
                    RestartStrategy::OneForAll => (0..slots.len()).collect(),
                    RestartStrategy::RestForOne => {
                        let order = failed_order.unwrap_or(0);
                        slots
                            .iter()
                            .enumerate()
                            .filter(|(_, s)| s.order() >= order)
                            .map(|(i, _)| i)
                            .collect()
                    }
                };

                for idx in indices {
                    let sup_tx = tx.clone();
                    if let Ok(actor_ref) = slots[idx].restart(sup_tx, &actor_config).await {
                        slots[idx].set_id(actor_ref.id);
                        tracing::info!(child = %actor_ref.id, "supervisor restarted child");
                    }
                }
            }
        });

        Ok(SupervisorHandle {
            id: ActorId::new(),
            _join: join,
            _marker: std::marker::PhantomData,
        })
    }
}

/// Convenience: supervise a single typed actor with OneForOne.
pub async fn supervise_actor<M, A>(
    actor: A,
    config: SupervisorConfig,
) -> Result<(ActorRef<M>, SupervisorHandle<M>), ActorProcessingErr>
where
    M: Send + Sync + 'static,
    A: Actor<M> + Send + Sync + Clone + 'static,
{
    supervise_actor_with_config(actor, config, &ActorConfig::default()).await
}

/// Supervise a single typed actor with explicit child mailbox sizing.
pub async fn supervise_actor_with_config<M, A>(
    actor: A,
    config: SupervisorConfig,
    actor_config: &ActorConfig,
) -> Result<(ActorRef<M>, SupervisorHandle<M>), ActorProcessingErr>
where
    M: Send + Sync + 'static,
    A: Actor<M> + Send + Sync + Clone + 'static,
{
    let actor_prototype = actor.clone();
    let child_config = *actor_config;
    let spec = child_spec(0, move |sup_tx, actor_config| {
        let a = actor_prototype.clone();
        let actor_config = *actor_config;
        Box::pin(async move {
            spawn_on_runtime(&Handle::current(), a, Some(sup_tx), &actor_config)
                .await
                .map(|(r, _)| r)
        })
    });

    let sup = Supervisor::with_actor_config(child_config, config, vec![spec]);
    let handle = sup.start().await?;
    let (child_ref, _) = spawn_on_runtime(&Handle::current(), actor, None, actor_config).await?;
    Ok((child_ref, handle))
}
