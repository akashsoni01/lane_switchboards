//! Custom actor runtime: `run_actor` loop, linking, monitoring, hot upgrade.

use crate::config::{spawn_on, ActorConfig};
use crate::registry::{get_actor_sender, register_actor, register_supervisor, unregister_actor};
use crate::supervisor::RestartSignal;
use async_trait::async_trait;
use futures_util::FutureExt;
use std::any::Any;
use std::fmt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot, Mutex, Semaphore};
use tokio::task::{JoinHandle, JoinSet};

static ACTOR_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Unique actor identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ActorId(pub u64);

impl ActorId {
    pub fn new() -> Self {
        Self(ACTOR_COUNTER.fetch_add(1, Ordering::Relaxed))
    }
}

impl fmt::Display for ActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "actor#{}", self.0)
    }
}

/// Why an actor exited.
#[derive(Debug, Clone)]
pub enum ExitReason {
    Normal,
    Shutdown,
    Error(String),
    Linked(ActorId, Box<ExitReason>),
    Killed,
}

/// Messages delivered to the actor mailbox.
pub enum Envelope<M: Send + Sync + 'static> {
    Msg(M),
    Link(ActorId),
    Unlink(ActorId),
    Monitor {
        observer: ActorId,
        notify: oneshot::Sender<ExitReason>,
    },
    Demonitor(ActorId),
    LinkedExit(ActorId, ExitReason),
    Kill,
    Stop,
    Upgrade(Box<dyn DynActor<M>>),
}

/// Processing error from actor callbacks.
pub type ActorProcessingErr = Box<dyn std::error::Error + Send + Sync>;

/// Object-safe actor trait for hot upgrade (swap implementation in-place).
pub trait DynActor<M: Send + Sync + 'static>: Send + Sync {
    fn dyn_pre_start(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), ActorProcessingErr>> + Send + '_>>;
    fn dyn_handle(
        &mut self,
        msg: M,
    ) -> Pin<Box<dyn Future<Output = Result<(), ActorProcessingErr>> + Send + '_>>;
    fn dyn_post_stop(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), ActorProcessingErr>> + Send + '_>>;
    /// If `true`, linked exits from this actor are trapped (not propagated).
    fn trap_exit(&self) -> bool {
        false
    }
}

/// User-facing actor trait.
#[async_trait]
pub trait Actor<M: Send + Sync + 'static>: Send + Sync {
    async fn pre_start(&mut self) -> Result<(), ActorProcessingErr> {
        Ok(())
    }
    async fn handle(&mut self, _msg: M) -> Result<(), ActorProcessingErr> {
        Ok(())
    }
    async fn post_stop(&mut self) -> Result<(), ActorProcessingErr> {
        Ok(())
    }
    fn trap_exit(&self) -> bool {
        false
    }
}

struct ActorWrapper<A, M> {
    inner: A,
    _marker: std::marker::PhantomData<M>,
}

impl<A: Actor<M> + Send + Sync, M: Send + Sync + 'static> DynActor<M> for ActorWrapper<A, M> {
    fn dyn_pre_start(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), ActorProcessingErr>> + Send + '_>> {
        Box::pin(async move { self.inner.pre_start().await })
    }

    fn dyn_handle(
        &mut self,
        msg: M,
    ) -> Pin<Box<dyn Future<Output = Result<(), ActorProcessingErr>> + Send + '_>> {
        Box::pin(async move { self.inner.handle(msg).await })
    }

    fn dyn_post_stop(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), ActorProcessingErr>> + Send + '_>> {
        Box::pin(async move { self.inner.post_stop().await })
    }

    fn trap_exit(&self) -> bool {
        self.inner.trap_exit()
    }
}

fn into_dyn_actor<A, M>(actor: A) -> Box<dyn DynActor<M>>
where
    A: Actor<M> + Send + Sync + 'static,
    M: Send + Sync + 'static,
{
    Box::new(ActorWrapper {
        inner: actor,
        _marker: std::marker::PhantomData,
    })
}

/// Handle to a running actor.
pub struct ActorRef<M: Send + Sync + 'static> {
    pub id: ActorId,
    tx: mpsc::Sender<Envelope<M>>,
}

impl<M: Send + Sync + 'static> Clone for ActorRef<M> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            tx: self.tx.clone(),
        }
    }
}

impl<M: Send + Sync + 'static> ActorRef<M> {
    pub async fn send(&self, msg: M) -> Result<(), ActorProcessingErr> {
        self.tx
            .send(Envelope::Msg(msg))
            .await
            .map_err(|e| Box::new(e) as ActorProcessingErr)
    }

    pub async fn stop(&self) -> Result<(), ActorProcessingErr> {
        self.tx
            .send(Envelope::Stop)
            .await
            .map_err(|e| Box::new(e) as ActorProcessingErr)
    }

    pub async fn kill(&self) -> Result<(), ActorProcessingErr> {
        self.tx
            .send(Envelope::Kill)
            .await
            .map_err(|e| Box::new(e) as ActorProcessingErr)
    }

    pub async fn link(&self, other: ActorId) -> Result<(), ActorProcessingErr> {
        self.tx
            .send(Envelope::Link(other))
            .await
            .map_err(|e| Box::new(e) as ActorProcessingErr)
    }

    pub async fn unlink(&self, other: ActorId) -> Result<(), ActorProcessingErr> {
        self.tx
            .send(Envelope::Unlink(other))
            .await
            .map_err(|e| Box::new(e) as ActorProcessingErr)
    }

    pub async fn upgrade(&self, new_impl: impl Actor<M> + Send + Sync + 'static) -> Result<(), ActorProcessingErr> {
        let boxed = into_dyn_actor(new_impl);
        self.tx
            .send(Envelope::Upgrade(boxed))
            .await
            .map_err(|e| Box::new(e) as ActorProcessingErr)
    }

    pub async fn monitor(&self, observer_id: ActorId) -> oneshot::Receiver<ExitReason> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(Envelope::Monitor {
                observer: observer_id,
                notify: tx,
            })
            .await;
        rx
    }

    pub async fn demonitor(&self, observer_id: ActorId) -> Result<(), ActorProcessingErr> {
        self.tx
            .send(Envelope::Demonitor(observer_id))
            .await
            .map_err(|e| Box::new(e) as ActorProcessingErr)
    }
}

/// Spawn an actor on the current Tokio runtime.
pub async fn spawn<M, A>(
    actor: A,
    supervisor_tx: Option<mpsc::Sender<RestartSignal>>,
) -> Result<(ActorRef<M>, JoinHandle<()>), ActorProcessingErr>
where
    M: Send + Sync + 'static,
    A: Actor<M> + Send + Sync + 'static,
{
    spawn_on_current_runtime(actor, supervisor_tx, &ActorConfig::default()).await
}

/// Spawn an actor on the current Tokio runtime with explicit config.
pub async fn spawn_with_config<M, A>(
    actor: A,
    supervisor_tx: Option<mpsc::Sender<RestartSignal>>,
    config: &ActorConfig,
) -> Result<(ActorRef<M>, JoinHandle<()>), ActorProcessingErr>
where
    M: Send + Sync + 'static,
    A: Actor<M> + Send + Sync + 'static,
{
    spawn_on_current_runtime(actor, supervisor_tx, config).await
}

/// Spawn an actor on the current Tokio runtime.
pub async fn spawn_on_current_runtime<M, A>(
    actor: A,
    supervisor_tx: Option<mpsc::Sender<RestartSignal>>,
    config: &ActorConfig,
) -> Result<(ActorRef<M>, JoinHandle<()>), ActorProcessingErr>
where
    M: Send + Sync + 'static,
    A: Actor<M> + Send + Sync + 'static,
{
    spawn_on_runtime(&Handle::current(), actor, supervisor_tx, config).await
}

/// Spawn an actor on a specific Tokio runtime handle.
pub async fn spawn_on_runtime<M, A>(
    runtime: &Handle,
    actor: A,
    supervisor_tx: Option<mpsc::Sender<RestartSignal>>,
    config: &ActorConfig,
) -> Result<(ActorRef<M>, JoinHandle<()>), ActorProcessingErr>
where
    M: Send + Sync + 'static,
    A: Actor<M> + Send + Sync + 'static,
{
    let id = ActorId::new();
    let (tx, rx) = mpsc::channel::<Envelope<M>>(config.mailbox_capacity);
    let actor_ref = ActorRef { id, tx: tx.clone() };

    if let Some(sup_tx) = supervisor_tx {
        register_supervisor(id, sup_tx);
    }

    let erased_tx = erase_sender(tx.clone());
    register_actor(id, erased_tx);

    let boxed = into_dyn_actor(actor);
    let config = *config;
    let runtime = runtime.clone();
    let actor_runtime = runtime.clone();
    let join = spawn_on(Some(&runtime), async move {
        run_actor(id, rx, boxed, config, actor_runtime).await;
    });

    Ok((actor_ref, join))
}

fn erase_sender<M: Send + Sync + 'static>(
    tx: mpsc::Sender<Envelope<M>>,
) -> mpsc::Sender<Envelope<Box<dyn Any + Send + Sync>>> {
    // SAFETY: LinkedExit only carries (ActorId, ExitReason) across actor types — no M payload.
    unsafe { std::mem::transmute(tx) }
}

async fn run_actor<M: Send + Sync + 'static>(
    id: ActorId,
    rx: mpsc::Receiver<Envelope<M>>,
    actor: Box<dyn DynActor<M>>,
    config: ActorConfig,
    _runtime: Handle,
) {
    if config.max_in_flight <= 1 {
        run_actor_sequential(id, rx, actor).await;
    } else {
        run_actor_concurrent(id, rx, actor, config).await;
    }
}

async fn run_actor_sequential<M: Send + Sync + 'static>(
    id: ActorId,
    mut rx: mpsc::Receiver<Envelope<M>>,
    mut actor: Box<dyn DynActor<M>>,
) {
    let mut links: Vec<ActorId> = Vec::new();
    let mut monitors: Vec<(ActorId, oneshot::Sender<ExitReason>)> = Vec::new();
    let mut exit_reason = ExitReason::Normal;

    if let Err(e) = actor.dyn_pre_start().await {
        exit_reason = ExitReason::Error(e.to_string());
        unregister_actor(id);
        return;
    }

    'actor_loop: while let Some(envelope) = rx.recv().await {
        match envelope {
            Envelope::Msg(m) => {
                if let Some(reason) = handle_message(id, &mut actor, m, &links).await {
                    exit_reason = reason;
                    break 'actor_loop;
                }
            }
            Envelope::Link(peer) => {
                if !links.contains(&peer) {
                    links.push(peer);
                }
            }
            Envelope::Unlink(peer) => {
                links.retain(|&x| x != peer);
            }
            Envelope::Monitor { observer: _, notify } => {
                monitors.push((ActorId::new(), notify));
            }
            Envelope::Demonitor(_) => {}
            Envelope::LinkedExit(peer, reason) => {
                if actor.trap_exit() {
                    tracing::debug!(%id, %peer, ?reason, "trapped linked exit");
                    continue;
                }
                exit_reason = ExitReason::Linked(peer, Box::new(reason));
                break 'actor_loop;
            }
            Envelope::Upgrade(new_impl) => {
                actor = new_impl;
                if let Err(e) = actor.dyn_pre_start().await {
                    exit_reason = ExitReason::Error(e.to_string());
                    break 'actor_loop;
                }
                tracing::info!(%id, "hot code upgrade applied");
            }
            Envelope::Stop => {
                exit_reason = ExitReason::Shutdown;
                break 'actor_loop;
            }
            Envelope::Kill => {
                exit_reason = ExitReason::Killed;
                break 'actor_loop;
            }
        }
    }

    finish_actor(id, actor, exit_reason, &links, monitors).await;
}

async fn run_actor_concurrent<M: Send + Sync + 'static>(
    id: ActorId,
    mut rx: mpsc::Receiver<Envelope<M>>,
    actor: Box<dyn DynActor<M>>,
    config: ActorConfig,
) {
    let actor: Arc<Mutex<Option<Box<dyn DynActor<M>>>>> = Arc::new(Mutex::new(Some(actor)));
    let in_flight = Arc::new(Semaphore::new(config.max_in_flight));
    let links: Arc<Mutex<Vec<ActorId>>> = Arc::new(Mutex::new(Vec::new()));
    let mut monitors: Vec<(ActorId, oneshot::Sender<ExitReason>)> = Vec::new();
    let mut exit_reason = ExitReason::Normal;
    let mut handlers = JoinSet::new();
    let (fail_tx, mut fail_rx) = mpsc::channel::<ExitReason>(1);

    {
        let mut guard = actor.lock().await;
        if let Some(inner) = guard.as_mut() {
            if let Err(e) = inner.dyn_pre_start().await {
                exit_reason = ExitReason::Error(e.to_string());
                unregister_actor(id);
                return;
            }
        }
    }

    loop {
        tokio::select! {
            biased;
            reason = fail_rx.recv() => {
                if let Some(reason) = reason {
                    exit_reason = reason;
                    break;
                }
            }
            join = handlers.join_next(), if !handlers.is_empty() => {
                if let Some(Err(e)) = join {
                    tracing::warn!(%id, error = %e, "actor handler task failed");
                }
            }
            envelope = rx.recv() => {
                let Some(envelope) = envelope else { break };
                match envelope {
                    Envelope::Msg(m) => {
                        let permit = match in_flight.clone().acquire_owned().await {
                            Ok(permit) => permit,
                            Err(_) => break,
                        };
                        let actor = Arc::clone(&actor);
                        let links_snapshot = links.lock().await.clone();
                        let fail_tx = fail_tx.clone();
                        let task_id = id;
                        handlers.spawn(async move {
                            let _permit = permit;
                            let mut guard = actor.lock().await;
                            let Some(inner) = guard.as_mut() else {
                                return;
                            };
                            if let Some(reason) = handle_message(task_id, inner, m, &links_snapshot).await {
                                let _ = fail_tx.send(reason).await;
                            }
                        });
                    }
                    Envelope::Link(peer) => {
                        let mut guard = links.lock().await;
                        if !guard.contains(&peer) {
                            guard.push(peer);
                        }
                    }
                    Envelope::Unlink(peer) => {
                        links.lock().await.retain(|&x| x != peer);
                    }
                    Envelope::Monitor { observer: _, notify } => {
                        monitors.push((ActorId::new(), notify));
                    }
                    Envelope::Demonitor(_) => {}
                    Envelope::LinkedExit(peer, reason) => {
                        let trap = actor
                            .lock()
                            .await
                            .as_ref()
                            .map(|a| a.trap_exit())
                            .unwrap_or(false);
                        if trap {
                            tracing::debug!(%id, %peer, ?reason, "trapped linked exit");
                            continue;
                        }
                        exit_reason = ExitReason::Linked(peer, Box::new(reason));
                        break;
                    }
                    Envelope::Upgrade(new_impl) => {
                        let mut guard = actor.lock().await;
                        if let Some(inner) = guard.as_mut() {
                            *inner = new_impl;
                            if let Err(e) = inner.dyn_pre_start().await {
                                exit_reason = ExitReason::Error(e.to_string());
                                break;
                            }
                            tracing::info!(%id, "hot code upgrade applied");
                        }
                    }
                    Envelope::Stop => {
                        exit_reason = ExitReason::Shutdown;
                        break;
                    }
                    Envelope::Kill => {
                        exit_reason = ExitReason::Killed;
                        break;
                    }
                }
            }
        }
    }

    handlers.abort_all();
    while handlers.join_next().await.is_some() {}

    let actor = actor.lock().await.take();
    let links = links.lock().await.clone();

    if let Some(actor) = actor {
        finish_actor(id, actor, exit_reason, &links, monitors).await;
    } else {
        unregister_actor(id);
    }
}

async fn handle_message<M: Send + Sync + 'static>(
    id: ActorId,
    actor: &mut Box<dyn DynActor<M>>,
    msg: M,
    links: &[ActorId],
) -> Option<ExitReason> {
    match AssertUnwindSafe(actor.dyn_handle(msg)).catch_unwind().await {
        Ok(Ok(())) => None,
        Ok(Err(e)) => {
            let reason = ExitReason::Error(e.to_string());
            notify_supervisor(id, &reason).await;
            propagate_linked_exit(id, &reason, links).await;
            Some(reason)
        }
        Err(_) => {
            let reason = ExitReason::Error("panic in handle".into());
            notify_supervisor(id, &reason).await;
            propagate_linked_exit(id, &reason, links).await;
            Some(reason)
        }
    }
}

async fn finish_actor<M: Send + Sync + 'static>(
    id: ActorId,
    mut actor: Box<dyn DynActor<M>>,
    exit_reason: ExitReason,
    links: &[ActorId],
    monitors: Vec<(ActorId, oneshot::Sender<ExitReason>)>,
) {
    let _ = actor.dyn_post_stop().await;
    for (_, notify) in monitors {
        let _ = notify.send(exit_reason.clone());
    }
    propagate_linked_exit(id, &exit_reason, links).await;
    unregister_actor(id);
}

async fn notify_supervisor(id: ActorId, reason: &ExitReason) {
    if let Some(tx) = crate::registry::get_supervisor_sender(id) {
        let _ = tx
            .send(RestartSignal {
                child_id: id,
                reason: format!("{:?}", reason),
            })
            .await;
    }
}

async fn propagate_linked_exit(id: ActorId, reason: &ExitReason, links: &[ActorId]) {
    for peer in links {
        if let Some(tx) = get_actor_sender(*peer) {
            let _ = tx
                .send(Envelope::LinkedExit(id, reason.clone()))
                .await;
        }
    }
}
