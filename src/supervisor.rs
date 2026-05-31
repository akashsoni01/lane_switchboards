//! OTP-style supervisor with OneForOne, OneForAll, and RestForOne strategies.

use crate::actor::{spawn, Actor, ActorId, ActorProcessingErr, ActorRef};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
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
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            strategy: RestartStrategy::OneForOne,
            max_restarts: 5,
            within_secs: 10,
            intensity_action: IntensityAction::ShutdownSupervisor,
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
    F: Fn(mpsc::Sender<RestartSignal>) -> Pin<Box<dyn Future<Output = Result<ActorRef<M>, ActorProcessingErr>> + Send>>
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
    ) -> Pin<Box<dyn Future<Output = Result<ActorRef<M>, ActorProcessingErr>> + Send>> {
        (self.factory)(supervisor_tx)
    }

    fn set_id(&mut self, id: ActorId) {
        self.id = id;
    }
}

/// Build a child spec from a spawn factory closure.
pub fn child_spec<M, F>(order: usize, factory: F) -> Box<dyn ChildSpec<M>>
where
    M: Send + Sync + 'static,
    F: Fn(mpsc::Sender<RestartSignal>) -> Pin<Box<dyn Future<Output = Result<ActorRef<M>, ActorProcessingErr>> + Send>>
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

/// Handle to a running supervisor.
pub struct SupervisorHandle<M: Send + Sync + 'static> {
    pub id: ActorId,
    _join: JoinHandle<()>,
    _marker: std::marker::PhantomData<M>,
}

/// OTP supervisor task.
pub struct Supervisor<M: Send + Sync + 'static> {
    config: SupervisorConfig,
    children: Arc<Mutex<Vec<Box<dyn ChildSpec<M>>>>>,
}

impl<M: Send + Sync + 'static> Supervisor<M> {
    pub fn new(config: SupervisorConfig, children: Vec<Box<dyn ChildSpec<M>>>) -> Self {
        Self {
            config,
            children: Arc::new(Mutex::new(children)),
        }
    }

    pub async fn start(self) -> Result<SupervisorHandle<M>, ActorProcessingErr> {
        let (tx, mut rx) = mpsc::channel::<RestartSignal>(32);
        let children = self.children.clone();
        let config = self.config.clone();

        // Initial spawn of all children
        {
            let mut slots = children.lock().await;
            for spec in slots.iter_mut() {
                let sup_tx = tx.clone();
                let actor_ref = spec.restart(sup_tx).await?;
                spec.set_id(actor_ref.id);
            }
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
                    if let Ok(actor_ref) = slots[idx].restart(sup_tx).await {
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
    let actor_prototype = actor.clone();
    let spec = child_spec(0, move |sup_tx| {
        let a = actor_prototype.clone();
        Box::pin(async move { spawn(a, Some(sup_tx)).await.map(|(r, _)| r) })
    });

    let sup = Supervisor::new(config, vec![spec]);
    let handle = sup.start().await?;
    // Child ref is re-fetched from registry on first restart; caller should keep initial spawn ref.
    let (child_ref, _) = spawn(actor, None).await?;
    Ok((child_ref, handle))
}
