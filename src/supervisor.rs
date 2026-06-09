//! OTP-style supervisor with OneForOne, OneForAll, and RestForOne strategies.

use crate::actor::{spawn_on_runtime, Actor, ActorId, ActorProcessingErr, ActorRef, ExitReason};
use crate::config::ActorConfig;
use arc_swap::ArcSwap;
use std::borrow::Borrow;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;

/// Notification sent to supervisor when a child fails.
#[derive(Debug, Clone)]
pub struct RestartSignal {
    pub child_id: ActorId,
    pub reason: ExitReason,
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
        actor_config: ActorConfig,
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
    F: Fn(
            mpsc::Sender<RestartSignal>,
            ActorConfig,
        ) -> Pin<Box<dyn Future<Output = Result<ActorRef<M>, ActorProcessingErr>> + Send>>
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
        actor_config: ActorConfig,
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
    F: Fn(
            mpsc::Sender<RestartSignal>,
            ActorConfig,
        ) -> Pin<Box<dyn Future<Output = Result<ActorRef<M>, ActorProcessingErr>> + Send>>
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

struct RegistrySnapshot<M: Send + Sync + 'static, K: Eq + Hash + Clone + Send + Sync + 'static> {
    refs: HashMap<K, ActorRef<M>>,
    generations: HashMap<K, u64>,
}

impl<M: Send + Sync + 'static, K: Eq + Hash + Clone + Send + Sync + 'static> Clone
    for RegistrySnapshot<M, K>
{
    fn clone(&self) -> Self {
        Self {
            refs: self.refs.clone(),
            generations: self.generations.clone(),
        }
    }
}

/// Named child refs updated on every spawn/restart — share with actors and main.
///
/// Reads (`get`, `generation`, …) load an [`ArcSwap`] snapshot without locking.
/// Writes clone-on-write via `rcu` so concurrent lookups stay wait-free.
#[derive(Clone)]
pub struct ChildRegistry<
    M: Send + Sync + 'static,
    K: Eq + Hash + Clone + Send + Sync + 'static = String,
> {
    snapshot: Arc<ArcSwap<RegistrySnapshot<M, K>>>,
}

impl<M: Send + Sync + 'static, K: Eq + Hash + Clone + Send + Sync + 'static> Default
    for ChildRegistry<M, K>
{
    fn default() -> Self {
        Self::new()
    }
}

impl<M: Send + Sync + 'static, K: Eq + Hash + Clone + Send + Sync + 'static> ChildRegistry<M, K> {
    pub fn new() -> Self {
        Self {
            snapshot: Arc::new(ArcSwap::from_pointee(RegistrySnapshot {
                refs: HashMap::new(),
                generations: HashMap::new(),
            })),
        }
    }

    /// Lock-free lookup — loads the current snapshot from [`ArcSwap`].
    pub async fn get<Q>(&self, name: &Q) -> Option<ActorRef<M>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.snapshot.load().refs.get(name).cloned()
    }

    /// Same as [`Self::get`] but synchronous for hot paths (no await point).
    pub fn get_sync<Q>(&self, name: &Q) -> Option<ActorRef<M>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.snapshot.load().refs.get(name).cloned()
    }

    fn update_snapshot<F>(&self, mut f: F)
    where
        F: FnMut(&RegistrySnapshot<M, K>) -> RegistrySnapshot<M, K>,
    {
        self.snapshot.rcu(|snap| Arc::new(f(snap.as_ref())));
    }

    pub async fn track(&self, name: impl Into<K>, actor_ref: ActorRef<M>) {
        let name = name.into();
        self.update_snapshot(|snap| {
            let mut next = snap.clone();
            next.refs.insert(name.clone(), actor_ref.clone());
            next
        });
    }

    pub async fn bump_generation(&self, name: impl Into<K>) {
        let name = name.into();
        self.update_snapshot(|snap| {
            let mut next = snap.clone();
            *next.generations.entry(name.clone()).or_insert(0) += 1;
            next
        });
    }

    /// Atomically register a ref and bump its generation counter.
    pub async fn track_and_bump(&self, name: impl Into<K>, actor_ref: ActorRef<M>) {
        let name = name.into();
        self.update_snapshot(|snap| {
            let mut next = snap.clone();
            next.refs.insert(name.clone(), actor_ref.clone());
            *next.generations.entry(name.clone()).or_insert(0) += 1;
            next
        });
    }

    pub async fn generation<Q>(&self, name: &Q) -> u64
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.snapshot
            .load()
            .generations
            .get(name)
            .copied()
            .unwrap_or(0)
    }

    pub async fn get_with_generation<Q>(&self, name: &Q) -> Option<(ActorRef<M>, u64)>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let snap = self.snapshot.load();
        snap.refs.get(name).cloned().map(|actor_ref| {
            let generation = snap.generations.get(name).copied().unwrap_or(0);
            (actor_ref, generation)
        })
    }

    pub async fn generations(&self) -> HashMap<K, u64> {
        self.snapshot.load().generations.clone()
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
        let handle = Handle::current();
        child_spec(order, move |sup_tx, actor_config| {
            let slot = slot.clone();
            let build = build.clone();
            let handle = handle.clone();
            Box::pin(async move {
                let (actor_ref, _) =
                    spawn_on_runtime(&handle, build(), Some(sup_tx), &actor_config).await?;
                *slot.current.lock().await = Some(actor_ref.clone());
                Ok(actor_ref)
            })
        })
    }
}

/// Build a child spec: spawn `build()`, register under `name`, at supervisor `order`.
pub fn spawn_child_spec<M, K, B, F>(
    order: usize,
    name: impl Into<K>,
    registry: Arc<ChildRegistry<M, K>>,
    build: F,
) -> Box<dyn ChildSpec<M>>
where
    M: Send + Sync + 'static,
    K: Eq + Hash + Clone + Send + Sync + 'static,
    B: Actor<M> + Send + Sync + 'static,
    F: Fn() -> B + Send + Sync + 'static,
{
    let name = name.into();
    let build = Arc::new(build);
    let handle = Handle::current();
    child_spec(order, move |sup_tx, actor_config| {
        let registry = registry.clone();
        let name = name.clone();
        let build = build.clone();
        let handle = handle.clone();
        Box::pin(async move {
            let (actor_ref, _) =
                spawn_on_runtime(&handle, build(), Some(sup_tx), &actor_config).await?;
            registry.track_and_bump(name, actor_ref.clone()).await;
            Ok(actor_ref)
        })
    })
}

/// Handle to a running supervisor.
pub struct SupervisorHandle<M: Send + Sync + 'static> {
    initial_refs: Vec<ActorRef<M>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    _join: JoinHandle<()>,
    _marker: std::marker::PhantomData<M>,
}

impl<M: Send + Sync + 'static> SupervisorHandle<M> {
    /// First child spawned during supervisor startup (convenience for single-child trees).
    pub fn initial_ref(&self) -> Option<&ActorRef<M>> {
        self.initial_refs.first()
    }

    /// All child refs from the initial spawn pass.
    pub fn initial_refs(&self) -> &[ActorRef<M>] {
        &self.initial_refs
    }

    /// Stop children in reverse start order, then shut down the supervisor task.
    pub async fn stop(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        let _ = self._join.await;
    }
}

/// OTP supervisor task.
pub struct Supervisor<M: Send + Sync + 'static> {
    config: SupervisorConfig,
    actor_config: ActorConfig,
    children: Vec<Box<dyn ChildSpec<M>>>,
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
            children,
        }
    }

    pub async fn start(self) -> Result<SupervisorHandle<M>, ActorProcessingErr> {
        self.start_settled(Duration::ZERO).await
    }

    /// Start the supervisor and optionally wait for initial spawns to settle.
    ///
    /// `settle` adds a fixed delay after all children are spawned before the supervisor
    /// begins processing restart signals. Use a non-zero value when children need time
    /// to run `pre_start` or register themselves before you send traffic (for example
    /// mesh join or sibling lookups via [`ChildRegistry`]).
    pub async fn start_settled(
        self,
        settle: Duration,
    ) -> Result<SupervisorHandle<M>, ActorProcessingErr> {
        let Self {
            config,
            actor_config,
            children: mut slots,
        } = self;

        slots.sort_by_key(|s| s.order());

        let (tx, mut rx) = mpsc::channel::<RestartSignal>(config.mailbox_capacity);
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

        let mut initial_refs = Vec::with_capacity(slots.len());
        for spec in slots.iter_mut() {
            let sup_tx = tx.clone();
            let actor_ref = spec.restart(sup_tx, actor_config).await?;
            spec.set_id(actor_ref.id);
            initial_refs.push(actor_ref);
        }

        if !settle.is_zero() {
            tokio::time::sleep(settle).await;
        }

        let config_clone = config.clone();
        let mut current_refs = initial_refs.clone();

        let join = tokio::spawn(async move {
            let mut restart_log: VecDeque<Instant> = VecDeque::new();

            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => {
                        for actor_ref in current_refs.iter().rev() {
                            let _ = actor_ref.stop().await;
                        }
                        break;
                    }
                    signal = rx.recv() => {
                        let Some(signal) = signal else { break; };

                        let now = Instant::now();
                        let cutoff = now - Duration::from_secs(config_clone.within_secs);
                        while restart_log.front().is_some_and(|t| *t < cutoff) {
                            restart_log.pop_front();
                        }
                        restart_log.push_back(now);

                        if restart_log.len() > config_clone.max_restarts {
                            match config_clone.intensity_action {
                                IntensityAction::ShutdownSupervisor => {
                                    tracing::error!("supervisor restart intensity exceeded — shutting down");
                                    for actor_ref in current_refs.iter().rev() {
                                        let _ = actor_ref.stop().await;
                                    }
                                    break;
                                }
                                IntensityAction::AbandonChild => {
                                    tracing::warn!(child = %signal.child_id, "abandoning child after intensity breach");
                                    if let Some(slot) = slots.iter_mut().find(|s| s.id() == signal.child_id) {
                                        slot.set_id(ActorId::DEAD);
                                    }
                                    continue;
                                }
                            }
                        }

                        match config_clone.strategy {
                            RestartStrategy::OneForOne => {
                                if let Some(idx) = slots.iter().position(|s| s.id() == signal.child_id) {
                                    restart_child(
                                        &mut slots,
                                        &mut current_refs,
                                        idx,
                                        &tx,
                                        actor_config,
                                    )
                                    .await;
                                }
                            }
                            RestartStrategy::OneForAll => {
                                for idx in 0..slots.len() {
                                    restart_child(
                                        &mut slots,
                                        &mut current_refs,
                                        idx,
                                        &tx,
                                        actor_config,
                                    )
                                    .await;
                                }
                            }
                            RestartStrategy::RestForOne => {
                                let failed_order = slots
                                    .iter()
                                    .find(|s| s.id() == signal.child_id)
                                    .map(|s| s.order())
                                    .unwrap_or(0);
                                for idx in 0..slots.len() {
                                    if slots[idx].order() >= failed_order {
                                        restart_child(
                                            &mut slots,
                                            &mut current_refs,
                                            idx,
                                            &tx,
                                            actor_config,
                                        )
                                        .await;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        Ok(SupervisorHandle {
            initial_refs,
            shutdown_tx: Some(shutdown_tx),
            _join: join,
            _marker: std::marker::PhantomData,
        })
    }
}

async fn restart_child<M: Send + Sync + 'static>(
    slots: &mut [Box<dyn ChildSpec<M>>],
    current_refs: &mut [ActorRef<M>],
    idx: usize,
    tx: &mpsc::Sender<RestartSignal>,
    actor_config: ActorConfig,
) {
    let child_id = slots[idx].id();
    let sup_tx = tx.clone();
    match slots[idx].restart(sup_tx, actor_config).await {
        Ok(actor_ref) => {
            slots[idx].set_id(actor_ref.id);
            current_refs[idx] = actor_ref.clone();
            tracing::info!(child = %actor_ref.id, "supervisor restarted child");
        }
        Err(e) => {
            tracing::error!(
                child = %child_id,
                error = %e,
                "supervisor failed to restart child"
            );
            slots[idx].set_id(ActorId::DEAD);
        }
    }
}

/// Start a **one-child** supervisor that registers the actor under `name` in `registry`.
///
/// Same as `Supervisor::new(config, vec![spawn_child_spec(0, name, registry, build)])`
/// followed by [`Supervisor::start_settled`]. Prefer [`supervise_named_child!`] in examples
/// to avoid closure boilerplate.
pub async fn supervise_named_child<M, K, B, F>(
    name: impl Into<K>,
    registry: Arc<ChildRegistry<M, K>>,
    config: SupervisorConfig,
    build: F,
) -> Result<SupervisorHandle<M>, ActorProcessingErr>
where
    M: Send + Sync + 'static,
    K: Eq + Hash + Clone + Send + Sync + 'static,
    B: Actor<M> + Send + Sync + 'static,
    F: Fn() -> B + Send + Sync + 'static,
{
    supervise_named_child_settled(name, registry, config, Duration::ZERO, build).await
}

/// Like [`supervise_named_child`] with a post-spawn settle delay before restart signals.
pub async fn supervise_named_child_settled<M, K, B, F>(
    name: impl Into<K>,
    registry: Arc<ChildRegistry<M, K>>,
    config: SupervisorConfig,
    settle: Duration,
    build: F,
) -> Result<SupervisorHandle<M>, ActorProcessingErr>
where
    M: Send + Sync + 'static,
    K: Eq + Hash + Clone + Send + Sync + 'static,
    B: Actor<M> + Send + Sync + 'static,
    F: Fn() -> B + Send + Sync + 'static,
{
    let children = vec![spawn_child_spec(0, name, registry, build)];
    Supervisor::new(config, children)
        .start_settled(settle)
        .await
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
    let handle = Handle::current();
    let spec = child_spec(0, move |sup_tx, actor_config| {
        let a = actor_prototype.clone();
        let handle = handle.clone();
        Box::pin(async move {
            spawn_on_runtime(&handle, a, Some(sup_tx), &actor_config)
                .await
                .map(|(r, _)| r)
        })
    });

    let sup = Supervisor::with_actor_config(child_config, config, vec![spec]);
    let handle = sup.start().await?;
    let child_ref = handle
        .initial_ref()
        .cloned()
        .ok_or_else(|| -> ActorProcessingErr { "supervised child failed to spawn".into() })?;
    Ok((child_ref, handle))
}
