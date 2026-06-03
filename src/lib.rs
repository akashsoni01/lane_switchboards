//! Lane Switchboards — OTP-style actor runtime.
//!
//! - [`actor`]: actors, linking, monitoring, hot code upgrade
//! - [`supervisor`]: OneForOne / OneForAll / RestForOne restart strategies,
//!   [`ChildRegistry`] / [`ChildSlot`] for stable child refs after restart
//! - [`registry`]: global actor index
//! - [`distributed`]: TCP-framed remote messaging, [`Cluster`] roster, [`serve_actor`]
//! - [`mesh`]: TCP service mesh — registry, discovery, routing ([`ServiceMesh`], [`MeshRouter`])
//! - [`stream`]: plain TCP framing; optional TLS with feature `tls`
//!
//! Ractor-based HTTP client: see the `gateway` example (`examples/gateway/`).

pub mod actor;
pub mod config;
pub mod consistency;
pub mod distributed;
pub mod hash_ring;
pub mod macros;
pub mod mesh;
pub mod monitor;
pub mod paxos;
pub mod registry;
pub mod stream;
pub mod supervisor;
#[cfg(feature = "tls")]
pub mod tls;

pub use actor::{
    spawn, spawn_on_current_runtime, spawn_on_runtime, spawn_with_config, Actor, ActorId,
    ActorProcessingErr, ActorRef, DynActor, Envelope, ExitReason, HandleStuckContext,
};
pub use config::{
    build_multi_thread_runtime, spawn_on, ActorConfig, DedicatedRuntime, DistributedConfig,
    RuntimeOptions,
};
pub use consistency::{
    each_quorum_acks_required, is_local_only, is_local_only_read, is_paxos_read,
    quorum_for, read_acks_required, write_acks_required, ConsistencyConfig, ConsistencyError,
    ReadConsistency, WriteConsistency,
};
#[cfg(feature = "metrics")]
pub use consistency::ConsistencyMetrics;
pub use monitor::{ActorMonitor, ActorStats};
pub use distributed::{
    serve_actor, serve_actor_on_current_runtime, serve_actor_on_runtime, AckFrame, Cluster,
    ClusterMember, Frame, Node, NodeHandle, RemoteActorRef, RemoteMessage, TlsAcceptor,
    TlsConnector,
};
#[cfg(feature = "tls")]
pub use distributed::serve_actor_tls_on_runtime;
pub use hash_ring::{HashRing, RingNode};
pub use mesh::{
    join_mesh, serve_microservice, MeshControlMsg, MeshRegistry, MeshRegistryClient,
    MeshRegistryServer, MeshRouter, MicroserviceHandle, ServiceMesh, ServiceRecord,
    DEFAULT_RECORD_TTL,
};
#[cfg(feature = "tls")]
pub use mesh::serve_microservice_tls;
pub use paxos::{
    paxos_target, serve_paxos_acceptor, serve_paxos_acceptor_on_runtime, Accept, Commit,
    PaxosAcceptor, PaxosHandle, PaxosMsg, PaxosNode, PaxosProposer, PaxosReplica, PaxosWireMsg,
    Prepare, Promise, Propose, Reject,
};
pub use stream::{
    accept as stream_accept, connect as stream_connect, host_from_addr, MaybeTlsStream,
};
#[cfg(feature = "tls")]
pub use tls::{
    accept as tls_accept, build_acceptor, build_connector, client_config_from_pem,
    connect as tls_connect, load_certs, load_ca_store, load_private_key, server_config_from_pem,
    TlsStream,
};
pub use supervisor::{
    child_spec, spawn_child_spec, supervise_actor, supervise_actor_with_config,
    supervise_named_child, supervise_named_child_settled, ChildRegistry, ChildSlot, ChildSpec,
    IntensityAction, RestartStrategy, Supervisor, SupervisorConfig, SupervisorHandle,
};
