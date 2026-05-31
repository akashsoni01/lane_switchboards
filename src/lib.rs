//! Lane Switchboards — OTP-style actor runtime.
//!
//! - [`actor`]: actors, linking, monitoring, hot code upgrade
//! - [`supervisor`]: OneForOne / OneForAll / RestForOne restart strategies,
//!   [`ChildRegistry`] / [`ChildSlot`] for stable child refs after restart
//! - [`registry`]: global actor index
//! - [`distributed`]: TCP-framed remote messaging, [`Cluster`] roster, [`serve_actor`]
//! - [`mesh`]: TCP service mesh — registry, discovery, routing ([`ServiceMesh`], [`MeshRouter`])
//!
//! Ractor-based HTTP client: see the `gateway` example (`examples/gateway/`).

pub mod actor;
pub mod config;
pub mod distributed;
pub mod hash_ring;
pub mod mesh;
pub mod registry;
pub mod supervisor;

pub use actor::{
    spawn, spawn_on_current_runtime, spawn_on_runtime, spawn_with_config, Actor, ActorId,
    ActorProcessingErr, ActorRef, DynActor, Envelope, ExitReason,
};
pub use config::{
    build_multi_thread_runtime, spawn_on, ActorConfig, DedicatedRuntime, DistributedConfig,
    RuntimeOptions,
};
pub use distributed::{
    serve_actor, serve_actor_on_current_runtime, serve_actor_on_runtime,
    serve_actor_with_config, Cluster, ClusterMember, Frame, Node, NodeHandle, RemoteActorRef,
    RemoteMessage,
};
pub use hash_ring::{HashRing, RingNode};
pub use mesh::{
    join_mesh, serve_microservice, MeshControlMsg, MeshRegistry, MeshRegistryClient,
    MeshRegistryServer, MeshRouter, MicroserviceHandle, ServiceMesh, ServiceRecord,
};
pub use supervisor::{
    child_spec, spawn_child_spec, supervise_actor, supervise_actor_with_config, ChildRegistry, ChildSlot, ChildSpec,
    IntensityAction, RestartStrategy, Supervisor, SupervisorConfig, SupervisorHandle,
};
