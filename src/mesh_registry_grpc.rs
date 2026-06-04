//! gRPC control plane for the service mesh registry (tonic).

use crate::mesh::{MeshRegistry, ServiceRecord};
use crate::proto::control::mesh_registry_server::MeshRegistry as MeshRegistryGrpc;
use crate::proto::control::{
    Ack, DeregisterRequest, ListReply, ListRequest, PingRequest, RegisterRequest,
    ServiceEvent, WatchRequest,
};
use std::net::SocketAddr;
use std::pin::Pin;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};

/// gRPC [`MeshRegistry`] service implementation.
#[derive(Clone)]
pub struct MeshRegistryService {
    registry: MeshRegistry,
}

impl MeshRegistryService {
    pub fn new(registry: MeshRegistry) -> Self {
        Self { registry }
    }
}

fn ack_ok() -> Ack {
    Ack {
        ok: true,
        error: String::new(),
    }
}

#[tonic::async_trait]
impl MeshRegistryGrpc for MeshRegistryService {
    type WatchStream =
        Pin<Box<dyn Stream<Item = Result<ServiceEvent, Status>> + Send + 'static>>;

    async fn register(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<Ack>, Status> {
        let record = ServiceRecord::try_from(request.into_inner())
            .map_err(Status::invalid_argument)?;
        let span = tracing::info_span!(
            "grpc.register",
            service = %record.service,
            instance_id = %record.instance_id,
            address = %record.address,
        );
        let _guard = span.enter();
        self.registry.register(record).await;
        Ok(Response::new(ack_ok()))
    }

    async fn deregister(
        &self,
        request: Request<DeregisterRequest>,
    ) -> Result<Response<Ack>, Status> {
        let req = request.into_inner();
        let span = tracing::info_span!(
            "grpc.deregister",
            service = %req.service,
            instance_id = %req.instance_id,
        );
        let _guard = span.enter();
        self.registry
            .deregister(&req.service, &req.instance_id)
            .await;
        Ok(Response::new(ack_ok()))
    }

    async fn list(
        &self,
        _request: Request<ListRequest>,
    ) -> Result<Response<ListReply>, Status> {
        let span = tracing::info_span!("grpc.list");
        let _guard = span.enter();
        let records = self.registry.list().await;
        Ok(Response::new(ListReply {
            records: records.into_iter().map(Into::into).collect(),
        }))
    }

    async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<Ack>, Status> {
        let span = tracing::info_span!("grpc.ping");
        let _guard = span.enter();
        Ok(Response::new(ack_ok()))
    }

    async fn watch(
        &self,
        _request: Request<WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let span = tracing::info_span!("grpc.watch");
        let _guard = span.enter();
        let rx = self.registry.subscribe();
        let stream = BroadcastStream::new(rx).filter_map(|result| result.ok().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }
}

/// Running mesh registry (gRPC). Replaces the legacy TCP `MeshRegistryServer`.
pub struct MeshRegistryHandle {
    pub address: String,
    registry: MeshRegistry,
    _task: JoinHandle<()>,
    _eviction: JoinHandle<()>,
}

impl MeshRegistryHandle {
    pub async fn bind(addr: impl Into<String>) -> std::io::Result<Self> {
        Self::bind_with_tls(addr, None).await
    }

    pub async fn bind_with_tls(
        addr: impl Into<String>,
        tls: Option<crate::config::TlsConfig>,
    ) -> std::io::Result<Self> {
        #[cfg(not(feature = "tls"))]
        let _ = tls;
        let addr_str = addr.into();
        let socket_addr: SocketAddr = addr_str.parse().map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("invalid registry address {addr_str}: {e}"),
            )
        })?;

        let registry = MeshRegistry::new();
        let svc = MeshRegistryService::new(registry.clone());
        let grpc = crate::proto::control::mesh_registry_server::MeshRegistryServer::new(svc);

        let listener = tokio::net::TcpListener::bind(socket_addr).await?;
        let address = listener.local_addr()?.to_string();
        #[cfg(feature = "tls")]
        let server_tls = tls;

        let task = tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            #[cfg(feature = "tls")]
            let tls_ref = server_tls.as_ref();
            #[cfg(not(feature = "tls"))]
            let tls_ref: Option<&crate::config::TlsConfig> = None;
            if let Err(e) = crate::grpc_tls::apply_server_tls(
                tonic::transport::Server::builder(),
                tls_ref,
            )
            .add_service(grpc)
            .serve_with_incoming(incoming)
            .await
            {
                tracing::error!(error = %e, "mesh registry gRPC server exited");
            }
        });

        let reg_evict = registry.clone();
        let eviction = tokio::spawn(async move {
            let mut interval = tokio::time::interval(crate::mesh::EVICTION_INTERVAL);
            loop {
                interval.tick().await;
                reg_evict.evict_expired().await;
            }
        });

        tracing::info!(%address, "mesh registry listening (grpc)");
        Ok(Self {
            address,
            registry,
            _task: task,
            _eviction: eviction,
        })
    }

    pub fn registry(&self) -> &MeshRegistry {
        &self.registry
    }
}

/// Backward-compatible alias for examples and docs migrating from TCP registry.
pub type MeshRegistryServer = MeshRegistryHandle;

/// Persistent gRPC client for the mesh registry.
pub struct MeshRegistryClient {
    inner: crate::proto::control::mesh_registry_client::MeshRegistryClient<
        tonic::transport::Channel,
    >,
}

impl MeshRegistryClient {
    pub async fn connect(addr: &str) -> Result<Self, std::io::Error> {
        Self::connect_with_tls(addr, None).await
    }

    pub async fn connect_with_tls(
        addr: &str,
        tls: Option<&crate::config::TlsConfig>,
    ) -> Result<Self, std::io::Error> {
        let uri = grpc_endpoint(addr, tls.is_some());
        let endpoint = tonic::transport::Endpoint::from_shared(uri)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?;
        let domain = crate::grpc_tls::tls_domain_from_addr(addr);
        let endpoint = crate::grpc_tls::apply_client_tls(endpoint, tls, domain)?;
        let channel = endpoint
            .connect()
            .await
            .map_err(std::io::Error::other)?;
        let inner = crate::proto::control::mesh_registry_client::MeshRegistryClient::new(channel);
        Ok(Self { inner })
    }

    pub async fn register(&mut self, record: ServiceRecord) -> Result<(), tonic::Status> {
        let req: RegisterRequest = record.into();
        let ack = self.inner.register(req).await?.into_inner();
        ack_ok_or_status(ack)
    }

    pub async fn deregister(&mut self, service: &str, instance_id: &str) -> Result<(), tonic::Status> {
        let ack = self
            .inner
            .deregister(DeregisterRequest {
                service: service.to_string(),
                instance_id: instance_id.to_string(),
            })
            .await?
            .into_inner();
        ack_ok_or_status(ack)
    }

    pub async fn list(&mut self) -> Result<Vec<ServiceRecord>, tonic::Status> {
        let reply = self.inner.list(ListRequest {}).await?.into_inner();
        reply
            .records
            .into_iter()
            .map(ServiceRecord::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(Status::invalid_argument)
    }

    pub async fn ping(&mut self) -> Result<(), tonic::Status> {
        let ack = self.inner.ping(PingRequest {}).await?.into_inner();
        ack_ok_or_status(ack)
    }

    pub async fn watch(
        &mut self,
    ) -> Result<
        impl Stream<Item = Result<ServiceEvent, tonic::Status>> + Send + 'static,
        tonic::Status,
    > {
        let response = self.inner.watch(WatchRequest {}).await?;
        Ok(response.into_inner())
    }

    pub async fn sync_mesh<M: crate::distributed::RemoteMessage>(
        &mut self,
        mesh: &mut crate::mesh::ServiceMesh<M>,
    ) -> Result<(), tonic::Status> {
        mesh.apply_snapshot_diff(self.list().await?);
        Ok(())
    }

    pub async fn renew(&mut self, record: ServiceRecord) -> Result<(), tonic::Status> {
        self.register(record).await
    }
}

/// Deferred connection — call [`PendingMeshRegistryClient::connect`] before RPCs.
pub struct PendingMeshRegistryClient {
    addr: String,
    inner: Option<MeshRegistryClient>,
}

impl PendingMeshRegistryClient {
    pub fn new(registry_addr: impl Into<String>) -> Self {
        Self {
            addr: registry_addr.into(),
            inner: None,
        }
    }

    pub fn from_connected(client: MeshRegistryClient) -> Self {
        Self {
            addr: String::new(),
            inner: Some(client),
        }
    }

    pub async fn connect(&mut self) -> Result<&mut MeshRegistryClient, std::io::Error> {
        if self.inner.is_none() {
            self.inner = Some(MeshRegistryClient::connect(&self.addr).await?);
        }
        Ok(self.inner.as_mut().unwrap())
    }

    pub async fn register(&mut self, record: ServiceRecord) -> Result<(), tonic::Status> {
        self.connect().await.map_err(tonic_status_from_io)?;
        self.inner.as_mut().unwrap().register(record).await
    }

    pub async fn list(&mut self) -> Result<Vec<ServiceRecord>, tonic::Status> {
        self.connect().await.map_err(tonic_status_from_io)?;
        self.inner.as_mut().unwrap().list().await
    }

    pub async fn sync_mesh<M: crate::distributed::RemoteMessage>(
        &mut self,
        mesh: &mut crate::mesh::ServiceMesh<M>,
    ) -> Result<(), tonic::Status> {
        self.connect().await.map_err(tonic_status_from_io)?;
        self.inner.as_mut().unwrap().sync_mesh(mesh).await
    }
}

fn grpc_endpoint(addr: &str, tls: bool) -> String {
    crate::distributed::grpc_data_endpoint(addr, tls)
}

#[allow(clippy::result_large_err)]
fn ack_ok_or_status(ack: Ack) -> Result<(), tonic::Status> {
    if ack.ok {
        Ok(())
    } else {
        Err(Status::internal(if ack.error.is_empty() {
            "registry error".to_string()
        } else {
            ack.error
        }))
    }
}

fn tonic_status_from_io(e: std::io::Error) -> tonic::Status {
    Status::unavailable(e.to_string())
}

impl From<ServiceRecord> for RegisterRequest {
    fn from(r: ServiceRecord) -> Self {
        let proto: crate::proto::control::ServiceRecord = r.into();
        Self {
            service: proto.service,
            instance_id: proto.instance_id,
            address: proto.address,
            target: proto.target,
            dc: proto.dc,
        }
    }
}
