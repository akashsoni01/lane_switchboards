//! Optional ServiceMesh-based peer discovery for messenger clusters.
//!
//! When enabled, gateways register as `lane.messenger` instances and poll the
//! mesh registry for peers, calling [`MessengerServer::add_peer`] /
//! [`MessengerServer::remove_peer`] as the set changes.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info, warn};

use crate::mesh::{MeshRegistryClient, ServiceRecord};
use crate::messenger::server::{MessengerServer, PeerAddr};
use crate::messenger::MessengerError;

/// Service name registered in the mesh for messenger gateways.
pub const MESSENGER_SERVICE: &str = "lane.messenger";

/// Configuration for mesh-driven peer discovery.
#[derive(Clone, Debug)]
pub struct MeshDiscoveryConfig {
    /// gRPC address of the [`crate::mesh::MeshRegistry`] (e.g. `http://127.0.0.1:50051`).
    pub registry_addr: String,
    /// This node's stable id (must match `ClusterConfig.node_id`).
    pub node_id: String,
    /// Public messenger listen address advertised to peers (`host:port`).
    pub listen_addr: String,
    /// How often to refresh the peer list from the registry.
    pub poll_interval: Duration,
}

impl Default for MeshDiscoveryConfig {
    fn default() -> Self {
        Self {
            registry_addr: "http://127.0.0.1:50051".into(),
            node_id: "node-0".into(),
            listen_addr: "127.0.0.1:9000".into(),
            poll_interval: Duration::from_secs(5),
        }
    }
}

/// Register this gateway and periodically sync peers from the mesh registry.
///
/// Returns a join handle; abort it to stop discovery. The server must already
/// be bound with [`MessengerServer::bind_cluster`] (possibly with an empty
/// initial peer list).
pub fn spawn_mesh_discovery(
    server: Arc<MessengerServer>,
    cfg: MeshDiscoveryConfig,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(e) = run_discovery(server, cfg).await {
            warn!(error = %e, "messenger mesh discovery stopped");
        }
    })
}

async fn run_discovery(
    server: Arc<MessengerServer>,
    cfg: MeshDiscoveryConfig,
) -> Result<(), MessengerError> {
    let mut client = MeshRegistryClient::connect(&cfg.registry_addr)
        .await
        .map_err(|e| MessengerError::Io(std::io::Error::other(e.to_string())))?;

    let record = ServiceRecord {
        service: MESSENGER_SERVICE.into(),
        instance_id: cfg.node_id.clone(),
        address: cfg.listen_addr.clone(),
        target: cfg.node_id.clone(),
        dc: None,
        registered_at: 0,
    };
    client
        .register(record)
        .await
        .map_err(|e| MessengerError::Protocol(format!("mesh register: {e}")))?;
    info!(node = %cfg.node_id, "registered messenger gateway with mesh");

    let mut known: HashSet<String> = HashSet::new();
    let mut tick = tokio::time::interval(cfg.poll_interval);
    loop {
        tick.tick().await;
        let records = match client.list().await {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "mesh list failed");
                continue;
            }
        };
        let peers: Vec<PeerAddr> = records
            .into_iter()
            .filter(|r| r.service == MESSENGER_SERVICE && r.instance_id != cfg.node_id)
            .map(|r| PeerAddr {
                node_id: r.instance_id,
                addr: r.address,
            })
            .collect();

        let next: HashSet<String> = peers.iter().map(|p| p.node_id.clone()).collect();
        for p in &peers {
            if !known.contains(&p.node_id) {
                debug!(peer = %p.node_id, "mesh discovery: adding peer");
                if let Err(e) = server.add_peer(p.clone()).await {
                    warn!(peer = %p.node_id, error = %e, "add_peer failed");
                }
            }
        }
        for id in known.difference(&next) {
            debug!(peer = %id, "mesh discovery: removing peer");
            if let Err(e) = server.remove_peer(id).await {
                warn!(peer = %id, error = %e, "remove_peer failed");
            }
        }
        known = next;

        // Heartbeat renew.
        let renew = ServiceRecord {
            service: MESSENGER_SERVICE.into(),
            instance_id: cfg.node_id.clone(),
            address: cfg.listen_addr.clone(),
            target: cfg.node_id.clone(),
            dc: None,
            registered_at: 0,
        };
        let _ = client.renew(renew).await;
    }
}
