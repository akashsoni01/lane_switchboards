//! Minimal calculator on the gRPC service mesh (protobuf + one supervised child).
//!
//! Stripped-down sibling of [`calculator_mesh`](./calculator_mesh.rs):
//! - **1** mesh instance (not 3)
//! - **No** result timer or RestForOne tree
//! - **Add / Div / Reply** on the wire only
//!
//! Run: `cargo run --example calculator_mesh_simplified`
//! See: `examples/calculator_mesh_simplified.md`

use lane_switchboards::actor::{Actor, ActorProcessingErr};
use lane_switchboards::distributed::{serve_actor, RemoteActorRef};
use lane_switchboards::mesh::{
    join_mesh, serve_microservice, MeshRegistryClient, MeshRegistryHandle, MeshRouter,
};
use lane_switchboards::prost::Message;
use lane_switchboards::supervisor::{
    ChildRegistry, IntensityAction, RestartStrategy, Supervisor, SupervisorConfig,
    SupervisorHandle,
};
use lane_switchboards::{registry_ask, registry_child_spec};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, Mutex};

const CALC_SERVICE: &str = "calculator";
const COORD_TARGET: &str = "coordinator";

// --- In-process calculator (not on wire) ---

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum ChildName {
    Calculator,
}

enum CalcMsg {
    Add(f64, f64, oneshot::Sender<f64>),
    Div(f64, f64, oneshot::Sender<f64>),
}

#[derive(Clone)]
struct Calculator {
    last: f64,
}

#[async_trait::async_trait]
impl Actor<CalcMsg> for Calculator {
    async fn handle(&mut self, msg: CalcMsg) -> Result<(), ActorProcessingErr> {
        match msg {
            CalcMsg::Add(a, b, reply) => {
                let v = a + b;
                self.last = v;
                let _ = reply.send(v);
            }
            CalcMsg::Div(a, b, reply) => {
                if b == 0.0 {
                    panic!("division by zero");
                }
                let v = a / b;
                self.last = v;
                let _ = reply.send(v);
            }
        }
        Ok(())
    }
}

// --- Coordinator: collects protobuf replies from the gateway ---

struct PendingReplies {
    map: Mutex<HashMap<u64, oneshot::Sender<Result<f64, String>>>>,
}

impl PendingReplies {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            map: Mutex::new(HashMap::new()),
        })
    }

    async fn wait_for(&self, id: u64) -> oneshot::Receiver<Result<f64, String>> {
        let (tx, rx) = oneshot::channel();
        self.map.lock().await.insert(id, tx);
        rx
    }

    async fn finish(&self, id: u64, result: Result<f64, String>) {
        if let Some(tx) = self.map.lock().await.remove(&id) {
            let _ = tx.send(result);
        }
    }
}

struct Coordinator {
    pending: Arc<PendingReplies>,
}

#[async_trait::async_trait]
impl Actor<CalcWire> for Coordinator {
    async fn handle(&mut self, msg: CalcWire) -> Result<(), ActorProcessingErr> {
        if let Some(calc_wire::Kind::Reply(r)) = msg.kind {
            let result = if r.ok {
                Ok(r.value)
            } else {
                Err(r.error)
            };
            self.pending.finish(r.request_id, result).await;
        }
        Ok(())
    }
}

// --- gRPC wire ---

#[derive(Clone, PartialEq, Message)]
pub struct CalcWire {
    #[prost(oneof = "calc_wire::Kind", tags = "1, 2, 3")]
    pub kind: Option<calc_wire::Kind>,
}

pub mod calc_wire {
    use super::{AddReq, DivReq, Reply};
    use lane_switchboards::prost::Oneof;

    #[derive(Clone, PartialEq, Oneof)]
    pub enum Kind {
        #[prost(message, tag = "1")]
        Add(AddReq),
        #[prost(message, tag = "2")]
        Div(DivReq),
        #[prost(message, tag = "3")]
        Reply(Reply),
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct AddReq {
    #[prost(uint64, tag = "1")]
    pub request_id: u64,
    #[prost(double, tag = "2")]
    pub a: f64,
    #[prost(double, tag = "3")]
    pub b: f64,
}

#[derive(Clone, PartialEq, Message)]
pub struct DivReq {
    #[prost(uint64, tag = "1")]
    pub request_id: u64,
    #[prost(double, tag = "2")]
    pub a: f64,
    #[prost(double, tag = "3")]
    pub b: f64,
}

#[derive(Clone, PartialEq, Message)]
pub struct Reply {
    #[prost(uint64, tag = "1")]
    pub request_id: u64,
    #[prost(bool, tag = "2")]
    pub ok: bool,
    #[prost(double, tag = "3")]
    pub value: f64,
    #[prost(string, tag = "4")]
    pub error: String,
}

// --- Gateway: mesh entrypoint + OneForOne supervised calculator ---

struct CalcGateway {
    coordinator_addr: String,
    registry: Arc<ChildRegistry<CalcMsg, ChildName>>,
    _supervisor: Option<SupervisorHandle<CalcMsg>>,
}

impl CalcGateway {
    async fn reply(&self, request_id: u64, result: Result<f64, String>) {
        let remote = RemoteActorRef::<CalcWire>::new(&self.coordinator_addr, COORD_TARGET);
        let (ok, value, error) = match result {
            Ok(v) => (true, v, String::new()),
            Err(e) => (false, 0.0, e),
        };
        let _ = remote
            .send(CalcWire {
                kind: Some(calc_wire::Kind::Reply(Reply {
                    request_id,
                    ok,
                    value,
                    error,
                })),
            })
            .await;
    }
}

#[async_trait::async_trait]
impl Actor<CalcWire> for CalcGateway {
    async fn pre_start(&mut self) -> Result<(), ActorProcessingErr> {
        let registry = self.registry.clone();
        let handle = Supervisor::new(
            SupervisorConfig {
                strategy: RestartStrategy::OneForOne,
                max_restarts: 5,
                within_secs: 30,
                intensity_action: IntensityAction::ShutdownSupervisor,
                ..Default::default()
            },
            vec![registry_child_spec!(
                0,
                ChildName::Calculator,
                registry,
                Calculator { last: 0.0 }
            )],
        )
        .start_settled(Duration::from_millis(50))
        .await?;
        self._supervisor = Some(handle);
        println!("[calc] gateway up — OneForOne calculator child");
        Ok(())
    }

    async fn handle(&mut self, msg: CalcWire) -> Result<(), ActorProcessingErr> {
        match msg.kind {
            Some(calc_wire::Kind::Add(req)) => {
                let result = registry_ask!(
                    self.registry,
                    ChildName::Calculator,
                    "calculator not running",
                    |reply| CalcMsg::Add(req.a, req.b, reply)
                )
                .map_err(|e| e.to_string());
                self.reply(req.request_id, result).await;
            }
            Some(calc_wire::Kind::Div(req)) => {
                let result = match registry_ask!(
                    self.registry,
                    ChildName::Calculator,
                    "calculator not running",
                    |reply| CalcMsg::Div(req.a, req.b, reply)
                ) {
                    Ok(v) => Ok(v),
                    Err(_) => Err("calculator restarted after panic".into()),
                };
                self.reply(req.request_id, result).await;
            }
            _ => {}
        }
        Ok(())
    }
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

async fn mesh_add(
    router: &MeshRouter<CalcWire>,
    pending: &PendingReplies,
    a: f64,
    b: f64,
) -> anyhow::Result<f64> {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let rx = pending.wait_for(id).await;
    router
        .invoke(
            CALC_SERVICE,
            &1u64,
            CalcWire {
                kind: Some(calc_wire::Kind::Add(AddReq {
                    request_id: id,
                    a,
                    b,
                })),
            },
        )
        .await?;
    match tokio::time::timeout(Duration::from_secs(2), rx).await {
        Ok(Ok(Ok(v))) => Ok(v),
        Ok(Ok(Err(e))) => Err(anyhow::anyhow!("{e}")),
        Ok(Err(_)) => Err(anyhow::anyhow!("coordinator dropped reply")),
        Err(_) => Err(anyhow::anyhow!("timeout")),
    }
}

async fn mesh_div(
    router: &MeshRouter<CalcWire>,
    pending: &PendingReplies,
    a: f64,
    b: f64,
) -> anyhow::Result<f64> {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let rx = pending.wait_for(id).await;
    router
        .invoke(
            CALC_SERVICE,
            &1u64,
            CalcWire {
                kind: Some(calc_wire::Kind::Div(DivReq {
                    request_id: id,
                    a,
                    b,
                })),
            },
        )
        .await?;
    match tokio::time::timeout(Duration::from_secs(2), rx).await {
        Ok(Ok(Ok(v))) => Ok(v),
        Ok(Ok(Err(e))) => Err(anyhow::anyhow!("{e}")),
        Ok(Err(_)) => Err(anyhow::anyhow!("coordinator dropped reply")),
        Err(_) => Err(anyhow::anyhow!("timeout")),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    println!("=== Calculator mesh (simplified) ===\n");

    let pending = PendingReplies::new();
    let coord = serve_actor(
        "coordinator",
        "127.0.0.1:0",
        COORD_TARGET,
        Coordinator {
            pending: pending.clone(),
        },
    )
    .await?;
    let coord_addr = coord.address().to_string();
    println!("[coordinator] @ {coord_addr}");

    let registry = MeshRegistryHandle::bind("127.0.0.1:0").await?;
    println!("[registry] @ {}\n", registry.address);
    let mut registry_client = MeshRegistryClient::connect(&registry.address).await?;

    let handle = serve_microservice(
        CALC_SERVICE,
        "calc-0",
        "127.0.0.1:0",
        CalcGateway {
            coordinator_addr: coord_addr,
            registry: Arc::new(ChildRegistry::new()),
            _supervisor: None,
        },
    )
    .await?;
    let mut mesh = lane_switchboards::mesh::ServiceMesh::new();
    join_mesh(&mut mesh, Some(&mut registry_client), &handle).await?;
    println!("[mesh] calc-0 @ {}\n", handle.address());

    let mut router = MeshRouter::with_registry(&registry.address);
    router.sync().await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    println!("--- Add over mesh ---");
    println!("10 + 4 = {}", mesh_add(&router, &pending, 10.0, 4.0).await?);

    println!("\n--- Div by zero (supervisor restarts calculator) ---");
    let _ = mesh_div(&router, &pending, 10.0, 0.0).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    println!("\n--- Add after recovery ---");
    println!("1 + 1 = {}", mesh_add(&router, &pending, 1.0, 1.0).await?);

    println!("\nSee examples/calculator_mesh_simplified.md");
    println!("Full mesh demo: cargo run --example calculator_mesh\n");
    Ok(())
}
