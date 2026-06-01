//! TCP service mesh: register microservice instances, discover by name, route via hash ring.

use crate::distributed::{
    serve_actor, Cluster, ClusterMember, NodeHandle, RemoteActorRef, RemoteMessage,
};
use crate::hash_ring::HashRing;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

/// A running microservice instance (data-plane endpoint).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServiceRecord {
    pub service: String,
    pub instance_id: String,
    pub address: String,
    pub target: String,
}

impl ServiceRecord {
    pub fn member(&self) -> ClusterMember {
        ClusterMember::new(&self.instance_id, &self.address, &self.target)
    }
}

/// Per-service routing table (hash ring over instances).
struct ServiceRoute<M: RemoteMessage> {
    cluster: Cluster<M>,
}

/// In-process mesh control plane + data-plane router.
pub struct ServiceMesh<M: RemoteMessage> {
    routes: HashMap<String, ServiceRoute<M>>,
}

impl<M: RemoteMessage> Default for ServiceMesh<M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<M: RemoteMessage> ServiceMesh<M> {
    pub fn new() -> Self {
        Self {
            routes: HashMap::new(),
        }
    }

    pub fn services(&self) -> Vec<String> {
        let mut names: Vec<_> = self.routes.keys().cloned().collect();
        names.sort();
        names
    }

    pub fn instance_count(&self, service: &str) -> usize {
        self.routes
            .get(service)
            .map(|r| r.cluster.len())
            .unwrap_or(0)
    }

    pub fn records(&self, service: &str) -> Vec<ServiceRecord> {
        self.routes
            .get(service)
            .map(|route| {
                route
                    .cluster
                    .members()
                    .iter()
                    .map(|m| ServiceRecord {
                        service: service.to_string(),
                        instance_id: m.name.clone(),
                        address: m.node_addr.clone(),
                        target: m.target.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn ring(&self, service: &str) -> Option<&HashRing> {
        self.routes.get(service).map(|r| r.cluster.ring())
    }

    /// Register or upsert an instance under `record.service`.
    pub fn register(&mut self, record: ServiceRecord) {
        let service = record.service.clone();
        let route = self
            .routes
            .entry(service)
            .or_insert_with(|| ServiceRoute {
                cluster: Cluster::new(),
            });
        if route
            .cluster
            .members()
            .iter()
            .any(|m| m.name == record.instance_id)
        {
            route.cluster.leave(&record.instance_id);
        }
        route.cluster.join(record.member());
    }

    pub fn deregister(&mut self, service: &str, instance_id: &str) -> Option<ServiceRecord> {
        let route = self.routes.get_mut(service)?;
        let member = route.cluster.leave(instance_id)?;
        if route.cluster.is_empty() {
            self.routes.remove(service);
        }
        Some(ServiceRecord {
            service: service.to_string(),
            instance_id: member.name,
            address: member.node_addr,
            target: member.target,
        })
    }

    pub fn apply_snapshot(&mut self, records: impl IntoIterator<Item = ServiceRecord>) {
        self.routes.clear();
        for record in records {
            self.register(record);
        }
    }

    pub async fn invoke<T: Hash>(&self, service: &str, key: &T, msg: M) -> std::io::Result<()> {
        self.route(service)?
            .cluster
            .send_by_key(key, msg)
            .await
    }

    pub async fn invoke_all(&self, service: &str, msg: M) -> Vec<(String, std::io::Result<()>)>
    where
        M: Clone,
    {
        match self.routes.get(service) {
            Some(route) => route.cluster.send_all(msg).await,
            None => Vec::new(),
        }
    }

    pub async fn invoke_any(&self, service: &str, msg: M) -> std::io::Result<()> {
        self.route(service)?.cluster.send_round_robin(msg).await
    }

    pub fn ref_for_key<T: Hash>(
        &self,
        service: &str,
        key: &T,
    ) -> Option<&RemoteActorRef<M>> {
        self.routes.get(service)?.cluster.ref_for_key(key)
    }

    fn route(&self, service: &str) -> std::io::Result<&ServiceRoute<M>> {
        self.routes.get(service).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("unknown service: {service}"),
            )
        })
    }
}

/// Handle returned when a microservice instance is bound to TCP.
pub struct MicroserviceHandle<M: RemoteMessage> {
    pub record: ServiceRecord,
    _node: NodeHandle<M>,
}

impl<M: RemoteMessage> MicroserviceHandle<M> {
    pub fn service(&self) -> &str {
        &self.record.service
    }

    pub fn instance_id(&self) -> &str {
        &self.record.instance_id
    }

    pub fn address(&self) -> &str {
        &self.record.address
    }
}

/// Bind a microservice actor on TCP. Frame `target` defaults to the service name.
pub async fn serve_microservice<M, A>(
    service: impl Into<String>,
    instance_id: impl Into<String>,
    bind_addr: impl Into<String>,
    actor: A,
) -> std::io::Result<MicroserviceHandle<M>>
where
    M: RemoteMessage,
    A: crate::actor::Actor<M> + Send + Sync + 'static,
{
    let service = service.into();
    let instance_id = instance_id.into();
    let target = service.clone();
    let node = serve_actor(&instance_id, bind_addr, &target, actor).await?;
    Ok(MicroserviceHandle {
        record: ServiceRecord {
            service,
            instance_id,
            address: node.address().to_string(),
            target,
        },
        _node: node,
    })
}

// --- TCP control plane (discovery) ---

/// Control-plane message (length-prefixed JSON over TCP).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MeshControlMsg {
    Register(ServiceRecord),
    Deregister { service: String, instance_id: String },
    List,
    ListReply(Vec<ServiceRecord>),
    Ping,
    Pong,
}

async fn write_control(stream: &mut TcpStream, msg: &MeshControlMsg) -> std::io::Result<()> {
    let body = serde_json::to_vec(msg).map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, e)
    })?;
    stream.write_u32_le(body.len() as u32).await?;
    stream.write_all(&body).await?;
    stream.flush().await?;
    Ok(())
}

async fn read_control(stream: &mut TcpStream) -> std::io::Result<Option<MeshControlMsg>> {
    let len = match stream.read_u32_le().await {
        Ok(0) => return Ok(None),
        Ok(n) => n,
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut buf = vec![0u8; len as usize];
    stream.read_exact(&mut buf).await?;
    let msg = serde_json::from_slice(&buf).map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, e)
    })?;
    Ok(Some(msg))
}

/// Shared mesh registry backing the TCP control plane.
#[derive(Clone, Default)]
pub struct MeshRegistry {
    records: Arc<RwLock<Vec<ServiceRecord>>>,
}

impl MeshRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register(&self, record: ServiceRecord) {
        let mut records = self.records.write().await;
        records.retain(|r| {
            !(r.service == record.service && r.instance_id == record.instance_id)
        });
        records.push(record);
    }

    pub async fn deregister(&self, service: &str, instance_id: &str) -> Option<ServiceRecord> {
        let mut records = self.records.write().await;
        records
            .iter()
            .position(|r| r.service == service && r.instance_id == instance_id)
            .map(|pos| records.remove(pos))
    }

    pub async fn list(&self) -> Vec<ServiceRecord> {
        self.records.read().await.clone()
    }

    pub async fn apply_to_mesh<M: RemoteMessage>(&self, mesh: &mut ServiceMesh<M>) {
        mesh.apply_snapshot(self.list().await);
    }
}

/// TCP mesh registry (control plane). Microservices register here for discovery.
pub struct MeshRegistryServer {
    pub address: String,
    registry: MeshRegistry,
    _task: JoinHandle<()>,
}

impl MeshRegistryServer {
    pub async fn bind(addr: impl Into<String>) -> std::io::Result<Self> {
        let registry = MeshRegistry::new();
        let listener = TcpListener::bind(addr.into()).await?;
        let address = listener.local_addr()?.to_string();
        let reg = registry.clone();

        let task = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, peer)) => {
                        let reg = reg.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_registry_conn(stream, reg).await {
                                tracing::warn!(%peer, error = %e, "mesh registry connection error");
                            }
                        });
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "mesh registry accept failed");
                        break;
                    }
                }
            }
        });

        tracing::info!(%address, "mesh registry listening");
        Ok(Self {
            address,
            registry,
            _task: task,
        })
    }

    pub fn registry(&self) -> &MeshRegistry {
        &self.registry
    }
}

async fn handle_registry_conn(
    mut stream: TcpStream,
    registry: MeshRegistry,
) -> std::io::Result<()> {
    while let Some(msg) = read_control(&mut stream).await? {
        match msg {
            MeshControlMsg::Register(record) => {
                tracing::info!(
                    service = %record.service,
                    instance = %record.instance_id,
                    address = %record.address,
                    "mesh register"
                );
                registry.register(record).await;
                write_control(&mut stream, &MeshControlMsg::Pong).await?;
            }
            MeshControlMsg::Deregister { service, instance_id } => {
                registry.deregister(&service, &instance_id).await;
                write_control(&mut stream, &MeshControlMsg::Pong).await?;
            }
            MeshControlMsg::List => {
                let list = registry.list().await;
                write_control(&mut stream, &MeshControlMsg::ListReply(list)).await?;
            }
            MeshControlMsg::Ping => {
                write_control(&mut stream, &MeshControlMsg::Pong).await?;
            }
            MeshControlMsg::Pong | MeshControlMsg::ListReply(_) => {}
        }
    }
    Ok(())
}

/// Client for the TCP mesh registry (discovery).
pub struct MeshRegistryClient;

impl MeshRegistryClient {
    pub async fn register(registry_addr: &str, record: ServiceRecord) -> std::io::Result<()> {
        let mut stream = TcpStream::connect(registry_addr).await?;
        write_control(&mut stream, &MeshControlMsg::Register(record)).await?;
        match read_control(&mut stream).await? {
            Some(MeshControlMsg::Pong) => Ok(()),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unexpected registry reply",
            )),
        }
    }

    pub async fn list(registry_addr: &str) -> std::io::Result<Vec<ServiceRecord>> {
        let mut stream = TcpStream::connect(registry_addr).await?;
        write_control(&mut stream, &MeshControlMsg::List).await?;
        match read_control(&mut stream).await? {
            Some(MeshControlMsg::ListReply(list)) => Ok(list),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unexpected registry reply",
            )),
        }
    }

    pub async fn sync_mesh<M: RemoteMessage>(
        registry_addr: &str,
        mesh: &mut ServiceMesh<M>,
    ) -> std::io::Result<()> {
        mesh.apply_snapshot(Self::list(registry_addr).await?);
        Ok(())
    }
}

/// Sidecar router: local mesh view + optional sync from TCP registry.
pub struct MeshRouter<M: RemoteMessage> {
    pub mesh: ServiceMesh<M>,
    registry_addr: Option<String>,
}

impl<M: RemoteMessage> MeshRouter<M> {
    pub fn local() -> Self {
        Self {
            mesh: ServiceMesh::new(),
            registry_addr: None,
        }
    }

    pub fn with_registry(registry_addr: impl Into<String>) -> Self {
        Self {
            mesh: ServiceMesh::new(),
            registry_addr: Some(registry_addr.into()),
        }
    }

    pub async fn sync(&mut self) -> std::io::Result<()> {
        if let Some(addr) = &self.registry_addr {
            MeshRegistryClient::sync_mesh(addr, &mut self.mesh).await?;
        }
        Ok(())
    }

    pub async fn invoke<T: Hash>(&self, service: &str, key: &T, msg: M) -> std::io::Result<()> {
        self.mesh.invoke(service, key, msg).await
    }

    pub async fn invoke_all(&self, service: &str, msg: M) -> Vec<(String, std::io::Result<()>)>
    where
        M: Clone,
    {
        self.mesh.invoke_all(service, msg).await
    }
}

/// Register instance locally and with the remote TCP registry (if provided).
pub async fn join_mesh<M: RemoteMessage>(
    mesh: &mut ServiceMesh<M>,
    registry_addr: Option<&str>,
    handle: &MicroserviceHandle<M>,
) -> std::io::Result<()> {
    mesh.register(handle.record.clone());
    if let Some(addr) = registry_addr {
        MeshRegistryClient::register(addr, handle.record.clone()).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::{Actor, ActorProcessingErr};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Ping(String);

    struct Echo;

    #[async_trait::async_trait]
    impl Actor<Ping> for Echo {
        async fn handle(&mut self, msg: Ping) -> Result<(), ActorProcessingErr> {
            let _ = msg;
            Ok(())
        }
    }

    #[tokio::test]
    async fn mesh_routes_by_service_name() {
        let a = serve_microservice("orders", "orders-1", "127.0.0.1:0", Echo)
            .await
            .expect("a");
        let b = serve_microservice("orders", "orders-2", "127.0.0.1:0", Echo)
            .await
            .expect("b");

        let mut mesh = ServiceMesh::new();
        mesh.register(a.record.clone());
        mesh.register(b.record.clone());

        assert_eq!(mesh.instance_count("orders"), 2);
        mesh.invoke("orders", &42u64, Ping("x".into()))
            .await
            .expect("invoke");
    }

    #[tokio::test]
    async fn registry_control_plane() {
        let server = MeshRegistryServer::bind("127.0.0.1:0")
            .await
            .expect("registry");
        let record = ServiceRecord {
            service: "inventory".into(),
            instance_id: "inv-1".into(),
            address: "127.0.0.1:9999".into(),
            target: "inventory".into(),
        };
        MeshRegistryClient::register(&server.address, record.clone())
            .await
            .expect("register");
        let list = MeshRegistryClient::list(&server.address)
            .await
            .expect("list");
        assert_eq!(list, vec![record]);
    }
}
