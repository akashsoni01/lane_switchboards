//! Custom actor runtime: `run_actor` loop, linking, monitoring, hot upgrade.

use crate::config::{spawn_on, ActorConfig};
#[cfg(feature = "monitor")]
use crate::monitor::ActorMonitor;
use crate::registry::{get_control_sender, register_actor, unregister_actor};
use crate::supervisor::RestartSignal;
use async_trait::async_trait;
use futures_util::FutureExt;
use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

/// Compile-time no-op when the `monitor` feature is disabled.
macro_rules! if_monitor {
    ($($body:tt)*) => {
        #[cfg(feature = "monitor")]
        { $($body)* }
    };
}

#[cfg(feature = "monitor")]
pub(crate) struct MailboxItem<M: Send + Sync + 'static> {
    pub envelope: Envelope<M>,
    pub enqueued_at: Instant,
}

#[cfg(feature = "monitor")]
impl<M: Send + Sync + 'static> MailboxItem<M> {
    fn new(envelope: Envelope<M>) -> Self {
        Self {
            envelope,
            enqueued_at: Instant::now(),
        }
    }
}

#[cfg(feature = "monitor")]
type QueueItem<M> = MailboxItem<M>;
#[cfg(not(feature = "monitor"))]
type QueueItem<M> = Envelope<M>;

#[cfg(not(feature = "monitor"))]
#[inline(always)]
fn queue_item<M: Send + Sync + 'static>(envelope: Envelope<M>) -> QueueItem<M> {
    envelope
}

#[cfg(feature = "monitor")]
#[inline(always)]
fn queue_item<M: Send + Sync + 'static>(envelope: Envelope<M>) -> QueueItem<M> {
    MailboxItem::new(envelope)
}

static ACTOR_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Unique actor identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ActorId(pub u64);

impl ActorId {
    /// Sentinel id for a child slot whose last restart attempt failed.
    pub const DEAD: Self = Self(0);

    pub fn new() -> Self {
        Self(ACTOR_COUNTER.fetch_add(1, Ordering::Relaxed))
    }

    pub fn is_dead(self) -> bool {
        self == Self::DEAD
    }
}

impl Default for ActorId {
    fn default() -> Self {
        Self::new()
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
    /// `handle()` exceeded [`ActorConfig::handle_timeout`].
    HandleTimeout { elapsed_ms: u64, limit_ms: u64 },
    Linked(ActorId, Box<ExitReason>),
    Killed,
}

/// Cross-type control signals routed through the global registry.
#[derive(Debug, Clone)]
pub(crate) enum ControlMsg {
    Link(ActorId),
    LinkedExit(ActorId, ExitReason),
}

/// Context passed to [`Actor::on_handle_stuck`] when a handle times out.
#[derive(Debug, Clone)]
pub struct HandleStuckContext {
    pub actor_id: ActorId,
    pub elapsed: Duration,
    pub limit: Duration,
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
    fn dyn_on_upgrade(
        &mut self,
        old_version: u32,
    ) -> Pin<Box<dyn Future<Output = Result<(), ActorProcessingErr>> + Send + '_>>;
    fn dyn_on_handle_begin<'a>(
        &'a mut self,
        msg: &'a M,
    ) -> Pin<Box<dyn Future<Output = Result<(), ActorProcessingErr>> + Send + 'a>>;
    fn dyn_handle(
        &mut self,
        msg: M,
    ) -> Pin<Box<dyn Future<Output = Result<(), ActorProcessingErr>> + Send + '_>>;
    fn dyn_on_handle_stuck(
        &mut self,
        ctx: HandleStuckContext,
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
    /// Called when the implementation is hot-swapped via [`ActorRef::upgrade`].
    async fn on_upgrade(&mut self, _old_version: u32) -> Result<(), ActorProcessingErr> {
        Ok(())
    }
    /// Called before each `handle()` — store pending work here for recovery on timeout.
    async fn on_handle_begin(&mut self, _msg: &M) -> Result<(), ActorProcessingErr> {
        Ok(())
    }
    async fn handle(&mut self, _msg: M) -> Result<(), ActorProcessingErr> {
        Ok(())
    }
    /// Called when `handle()` exceeds [`ActorConfig::handle_timeout`].
    /// Persist `on_handle_begin` state or journal the stuck action.
    async fn on_handle_stuck(&mut self, _ctx: HandleStuckContext) -> Result<(), ActorProcessingErr> {
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

    fn dyn_on_upgrade(
        &mut self,
        old_version: u32,
    ) -> Pin<Box<dyn Future<Output = Result<(), ActorProcessingErr>> + Send + '_>> {
        Box::pin(async move { self.inner.on_upgrade(old_version).await })
    }

    fn dyn_on_handle_begin<'a>(
        &'a mut self,
        msg: &'a M,
    ) -> Pin<Box<dyn Future<Output = Result<(), ActorProcessingErr>> + Send + 'a>> {
        Box::pin(async move { self.inner.on_handle_begin(msg).await })
    }

    fn dyn_handle(
        &mut self,
        msg: M,
    ) -> Pin<Box<dyn Future<Output = Result<(), ActorProcessingErr>> + Send + '_>> {
        Box::pin(async move { self.inner.handle(msg).await })
    }

    fn dyn_on_handle_stuck(
        &mut self,
        ctx: HandleStuckContext,
    ) -> Pin<Box<dyn Future<Output = Result<(), ActorProcessingErr>> + Send + '_>> {
        Box::pin(async move { self.inner.on_handle_stuck(ctx).await })
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
    tx: mpsc::Sender<QueueItem<M>>,
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
    fn on_mailbox_enqueue_ok(&self) {
        if_monitor! {
            ActorMonitor::global().record_mailbox_enqueue(self.id);
        }
    }

    fn on_mailbox_send_failed(&self) {
        if_monitor! {
            ActorMonitor::global().record_mailbox_send_rejected(self.id);
        }
    }

    async fn mailbox_send(&self, envelope: Envelope<M>) -> Result<(), ActorProcessingErr> {
        #[cfg(feature = "metrics")]
        if self.tx.capacity() == 0 {
            if_monitor! {
                ActorMonitor::global().record_mailbox_send_blocked(self.id);
            }
        }
        match self.tx.send(queue_item(envelope)).await {
            Ok(()) => {
                self.on_mailbox_enqueue_ok();
                Ok(())
            }
            Err(e) => {
                self.on_mailbox_send_failed();
                Err(Box::new(e) as ActorProcessingErr)
            }
        }
    }

    fn mailbox_try_send(&self, envelope: Envelope<M>) -> Result<(), ActorProcessingErr> {
        match self.tx.try_send(queue_item(envelope)) {
            Ok(()) => {
                self.on_mailbox_enqueue_ok();
                Ok(())
            }
            Err(e) => {
                self.on_mailbox_send_failed();
                Err(Box::new(e) as ActorProcessingErr)
            }
        }
    }

    pub async fn send(&self, msg: M) -> Result<(), ActorProcessingErr> {
        self.mailbox_send(Envelope::Msg(msg)).await
    }

    /// Enqueue a message without `.await` — for sync callers.
    ///
    /// Uses [`mpsc::Sender::try_send`]: returns immediately, or with an error if
    /// the mailbox is full or the actor has exited.
    pub fn send_sync(&self, msg: M) -> Result<(), ActorProcessingErr> {
        self.mailbox_try_send(Envelope::Msg(msg))
    }

    pub async fn stop(&self) -> Result<(), ActorProcessingErr> {
        self.mailbox_send(Envelope::Stop).await
    }

    pub async fn kill(&self) -> Result<(), ActorProcessingErr> {
        self.mailbox_send(Envelope::Kill).await
    }

    pub async fn link(&self, other: ActorId) -> Result<(), ActorProcessingErr> {
        self.mailbox_send(Envelope::Link(other)).await
    }

    pub async fn unlink(&self, other: ActorId) -> Result<(), ActorProcessingErr> {
        self.mailbox_send(Envelope::Unlink(other)).await
    }

    pub async fn upgrade(
        &self,
        new_impl: impl Actor<M> + 'static,
    ) -> Result<(), ActorProcessingErr> {
        let boxed = into_dyn_actor(new_impl);
        self.mailbox_send(Envelope::Upgrade(boxed)).await
    }

    pub async fn monitor(&self, observer_id: ActorId) -> oneshot::Receiver<ExitReason> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .mailbox_send(Envelope::Monitor {
                observer: observer_id,
                notify: tx,
            })
            .await;
        rx
    }

    pub async fn demonitor(&self, observer_id: ActorId) -> Result<(), ActorProcessingErr> {
        self.mailbox_send(Envelope::Demonitor(observer_id)).await
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
pub(crate) async fn spawn_on_current_runtime<M, A>(
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
    let (tx, rx) = mpsc::channel::<QueueItem<M>>(config.mailbox_capacity);
    let (control_tx, control_rx) = mpsc::channel::<ControlMsg>(config.mailbox_capacity);
    let actor_ref = ActorRef { id, tx: tx.clone() };

    register_actor(id, control_tx, supervisor_tx);
    if_monitor! {
        ActorMonitor::global().register(id, config.monitor_meta.clone(), config.mailbox_capacity);
    }

    let boxed = into_dyn_actor(actor);
    let config = config.clone();
    let runtime = runtime.clone();
    let join = spawn_on(Some(&runtime), async move {
        run_actor(id, rx, control_rx, boxed, config).await;
    });

    Ok((actor_ref, join))
}

async fn dispatch_control_envelope<M: Send + Sync + 'static>(
    id: ActorId,
    actor: &mut Box<dyn DynActor<M>>,
    envelope: Envelope<M>,
    links: &mut HashSet<ActorId>,
    monitors: &mut Vec<(ActorId, oneshot::Sender<ExitReason>)>,
    actor_version: &mut u32,
    exit_reason: &mut ExitReason,
) -> Option<ExitReason> {
    match envelope {
        Envelope::Link(peer) => {
            if links.insert(peer) {
                send_reverse_link(id, peer).await;
            }
            None
        }
        Envelope::Unlink(peer) => {
            links.remove(&peer);
            None
        }
        Envelope::Monitor { observer, notify } => {
            monitors.push((observer, notify));
            None
        }
        Envelope::Demonitor(observer_id) => {
            monitors.retain(|(mid, _)| *mid != observer_id);
            None
        }
        Envelope::Upgrade(new_impl) => {
            *actor = new_impl;
            if let Err(e) = actor.dyn_on_upgrade(*actor_version).await {
                return Some(ExitReason::Error(e.to_string()));
            }
            *actor_version += 1;
            tracing::info!(%id, version = *actor_version, "hot code upgrade applied");
            None
        }
        Envelope::Stop => {
            *exit_reason = ExitReason::Shutdown;
            Some(ExitReason::Shutdown)
        }
        Envelope::Kill => {
            *exit_reason = ExitReason::Killed;
            Some(ExitReason::Killed)
        }
        Envelope::Msg(_) => None,
    }
}

async fn run_actor<M: Send + Sync + 'static>(
    id: ActorId,
    mut rx: mpsc::Receiver<QueueItem<M>>,
    mut control_rx: mpsc::Receiver<ControlMsg>,
    mut actor: Box<dyn DynActor<M>>,
    config: ActorConfig,
) {
    let mut links: HashSet<ActorId> = HashSet::new();
    let mut monitors: Vec<(ActorId, oneshot::Sender<ExitReason>)> = Vec::new();
    let mut exit_reason = ExitReason::Normal;
    let mut actor_version: u32 = 0;

    if let Err(e) = actor.dyn_pre_start().await {
        tracing::error!(%id, error = %e, "pre_start failed");
        let reason = ExitReason::Error(e.to_string());
        notify_supervisor(id, &reason).await;
        unregister_actor(id);
        if_monitor! {
            ActorMonitor::global().unregister(id, Some(&reason));
        }
        return;
    }

    'actor_loop: loop {
        tokio::select! {
            biased;
            ctrl = control_rx.recv() => {
                let Some(ctrl) = ctrl else {
                    exit_reason = ExitReason::Error("control channel closed unexpectedly".into());
                    break 'actor_loop;
                };
                match ctrl {
                    ControlMsg::Link(peer) => {
                        if links.insert(peer) {
                            send_reverse_link(id, peer).await;
                        }
                    }
                    ControlMsg::LinkedExit(peer, reason) => {
                        if let Some(reason) = linked_exit_reason(id, peer, reason, actor.trap_exit()) {
                            exit_reason = reason;
                            break 'actor_loop;
                        }
                    }
                }
            }
            envelope = rx.recv() => {
                let Some(item) = envelope else { break 'actor_loop };
                if_monitor! {
                    ActorMonitor::global().record_mailbox_dequeue(id);
                }
                #[cfg(feature = "monitor")]
                {
                    let mailbox_wait = item.enqueued_at.elapsed();
                    match item.envelope {
                        Envelope::Msg(m) => {
                            if let Some(reason) =
                                handle_message(id, &mut actor, m, &config, mailbox_wait).await
                            {
                                exit_reason = reason;
                                break 'actor_loop;
                            }
                        }
                        other => {
                            if let Some(reason) = dispatch_control_envelope(
                                id, &mut actor, other, &mut links, &mut monitors,
                                &mut actor_version, &mut exit_reason,
                            ).await {
                                exit_reason = reason;
                                break 'actor_loop;
                            }
                        }
                    }
                }
                #[cfg(not(feature = "monitor"))]
                {
                    match item {
                        Envelope::Msg(m) => {
                            if let Some(reason) = handle_message(
                                id, &mut actor, m, &config, Duration::ZERO,
                            ).await {
                                exit_reason = reason;
                                break 'actor_loop;
                            }
                        }
                        other => {
                            if let Some(reason) = dispatch_control_envelope(
                                id, &mut actor, other, &mut links, &mut monitors,
                                &mut actor_version, &mut exit_reason,
                            ).await {
                                exit_reason = reason;
                                break 'actor_loop;
                            }
                        }
                    }
                }
            }
        }
    }

    finish_actor(id, actor, exit_reason, &links, monitors).await;
}

async fn handle_message<M: Send + Sync + 'static>(
    id: ActorId,
    actor: &mut Box<dyn DynActor<M>>,
    msg: M,
    config: &ActorConfig,
    #[cfg_attr(not(feature = "monitor"), allow(unused_variables))]
    mailbox_wait: Duration,
) -> Option<ExitReason> {
    let started = Instant::now();
    if_monitor! {
        ActorMonitor::global().begin_handle(id, mailbox_wait);
    }

    // NOTE: on_handle_begin failure is fatal — it terminates the actor.
    // If you want non-fatal journaling, return Ok(()) and log internally.
    if let Err(e) = actor.dyn_on_handle_begin(&msg).await {
        if_monitor! {
            ActorMonitor::global().record_error(id);
        }
        let reason = ExitReason::Error(e.to_string());
        notify_supervisor(id, &reason).await;
        return Some(reason);
    }

    let handle_fut = AssertUnwindSafe(async {
        actor.dyn_handle(msg).await
    })
    .catch_unwind();

    let handle_result = if let Some(limit) = config.handle_timeout {
        match tokio::time::timeout(limit, handle_fut).await {
            Ok(inner) => inner,
            Err(_) => {
                let elapsed = started.elapsed();
                if_monitor! {
                    ActorMonitor::global().record_timeout(id, elapsed);
                }
                let ctx = HandleStuckContext {
                    actor_id: id,
                    elapsed,
                    limit,
                };
                if let Err(e) = actor.dyn_on_handle_stuck(ctx).await {
                    tracing::warn!(%id, error = %e, "on_handle_stuck failed");
                }
                let reason = ExitReason::HandleTimeout {
                    elapsed_ms: u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
                    limit_ms: u64::try_from(limit.as_millis()).unwrap_or(u64::MAX),
                };
                notify_supervisor(id, &reason).await;
                return Some(reason);
            }
        }
    } else {
        handle_fut.await
    };

    match handle_result {
        Ok(Ok(())) => {
            if_monitor! {
                ActorMonitor::global().finish_handle(
                    id,
                    started.elapsed(),
                    config.effective_slow_threshold(),
                );
            }
            None
        }
        Ok(Err(e)) => {
            if_monitor! {
                ActorMonitor::global().record_error(id);
            }
            let reason = ExitReason::Error(e.to_string());
            notify_supervisor(id, &reason).await;
            Some(reason)
        }
        Err(_) => {
            if_monitor! {
                ActorMonitor::global().record_panic(id);
            }
            let reason = ExitReason::Error("panic in handle".into());
            notify_supervisor(id, &reason).await;
            Some(reason)
        }
    }
}

async fn finish_actor<M: Send + Sync + 'static>(
    id: ActorId,
    mut actor: Box<dyn DynActor<M>>,
    exit_reason: ExitReason,
    links: &HashSet<ActorId>,
    monitors: Vec<(ActorId, oneshot::Sender<ExitReason>)>,
) {
    let _ = actor.dyn_post_stop().await;
    for (_, notify) in monitors {
        let _ = notify.send(exit_reason.clone());
    }
    if should_propagate_linked_exit(&exit_reason) {
        propagate_linked_exit(id, &exit_reason, links).await;
    }
    unregister_actor(id);
    if_monitor! {
        ActorMonitor::global().unregister(id, Some(&exit_reason));
    }
}

fn should_propagate_linked_exit(reason: &ExitReason) -> bool {
    !matches!(reason, ExitReason::Normal | ExitReason::Shutdown)
}

fn linked_exit_reason(
    id: ActorId,
    peer: ActorId,
    reason: ExitReason,
    trap: bool,
) -> Option<ExitReason> {
    if trap {
        tracing::debug!(%id, %peer, ?reason, "trapped linked exit");
        None
    } else {
        Some(ExitReason::Linked(peer, Box::new(reason)))
    }
}

async fn send_reverse_link(self_id: ActorId, peer: ActorId) {
    if let Some(tx) = get_control_sender(peer) {
        let _ = tx.send(ControlMsg::Link(self_id)).await;
    }
}

async fn notify_supervisor(id: ActorId, reason: &ExitReason) {
    if let Some(tx) = crate::registry::get_supervisor_sender(id) {
        let _ = tx
            .send(RestartSignal {
                child_id: id,
                reason: reason.clone(),
            })
            .await;
    }
}

async fn propagate_linked_exit(id: ActorId, reason: &ExitReason, links: &HashSet<ActorId>) {
    for peer in links {
        if let Some(tx) = get_control_sender(*peer) {
            let _ = tx
                .send(ControlMsg::LinkedExit(id, reason.clone()))
                .await;
        }
    }
}
