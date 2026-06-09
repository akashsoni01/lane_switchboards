//! Calculator over gRPC service mesh — RestForOne supervision per replica + protobuf wire.
//!
//! Combines patterns from [`rest_for_one_calculator_timer_optimized`](./rest_for_one_calculator_timer_optimized.rs)
//! (supervisor macros, `registry_ask!`) with [`service_mesh`](./service_mesh.rs) (registry + router).
//!
//! Each **calculator** mesh instance runs a gateway actor with a **RestForOne** tree
//! (calculator + result timer). Remote `Add` / `Div` / `LastResult` use prost on the wire;
//! results return to a **coordinator** node via a second gRPC deliver.
//!
//! Run: `cargo run --example calculator_mesh`
//! See: `examples/calculator_mesh.md`

use lane_switchboards::actor::{Actor, ActorProcessingErr, ActorRef};
use lane_switchboards::distributed::{serve_actor, RemoteActorRef};
use lane_switchboards::mesh::{
    join_mesh, serve_microservice, MeshRegistryClient, MeshRegistryHandle, MeshRouter,
};
use lane_switchboards::prost::Message;
use lane_switchboards::supervisor::{
    ChildRegistry, IntensityAction, RestartStrategy, Supervisor, SupervisorConfig,
    SupervisorHandle,
};
use lane_switchboards::{actor_ask, registry_ask, registry_child_spec};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, Mutex};

const CALC_SERVICE: &str = "calculator";
const GATEWAY_TARGET: &str = "calc-gateway";
const COORD_TARGET: &str = "coordinator";

// --- Local supervision (not on wire) ---

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum ChildName {
    Calculator,
    Timer,
}

enum AppMsg {
    Add(f64, f64, oneshot::Sender<f64>),
    Div(f64, f64, oneshot::Sender<f64>),
    LastResult(oneshot::Sender<Option<f64>>),
    TimerStart(ActorRef<AppMsg>),
    TimerTick,
}

#[derive(Clone)]
struct Calculator {
    last_result: Option<f64>,
}

#[async_trait::async_trait]
impl Actor<AppMsg> for Calculator {
    async fn handle(&mut self, msg: AppMsg) -> Result<(), ActorProcessingErr> {
        match msg {
            AppMsg::Add(a, b, reply) => {
                let value = a + b;
                self.last_result = Some(value);
                let _ = reply.send(value);
            }
            AppMsg::Div(a, b, reply) => {
                if b == 0.0 {
                    panic!("division by zero");
                }
                let value = a / b;
                self.last_result = Some(value);
                let _ = reply.send(value);
            }
            AppMsg::LastResult(reply) => {
                let _ = reply.send(self.last_result);
            }
            AppMsg::TimerStart(_) | AppMsg::TimerTick => {}
        }
        Ok(())
    }
}

struct ResultTimer {
    registry: Arc<ChildRegistry<AppMsg, ChildName>>,
    self_ref: Option<ActorRef<AppMsg>>,
    interval: Duration,
    running: bool,
    instance: String,
}

#[async_trait::async_trait]
impl Actor<AppMsg> for ResultTimer {
    async fn handle(&mut self, msg: AppMsg) -> Result<(), ActorProcessingErr> {
        match msg {
            AppMsg::TimerStart(self_ref) => {
                self.self_ref = Some(self_ref);
                self.running = true;
                self.schedule_next();
            }
            AppMsg::TimerTick if self.running => {
                if let Some(calc) = self.registry.get(&ChildName::Calculator) {
                    match actor_ask!(calc, |reply| AppMsg::LastResult(reply)) {
                        Ok(Some(v)) => println!("[{}] timer last_result = {v}", self.instance),
                        Ok(None) => println!("[{}] timer last_result = (none)", self.instance),
                        Err(_) => println!("[{}] timer calculator unavailable", self.instance),
                    }
                }
                self.schedule_next();
            }
            _ => {}
        }
        Ok(())
    }
}

impl ResultTimer {
    fn schedule_next(&self) {
        let Some(self_ref) = self.self_ref.clone() else {
            return;
        };
        let delay = self.interval;
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = self_ref.send(AppMsg::TimerTick).await;
        });
    }
}

fn rest_for_one_specs(
    instance: &str,
    registry: Arc<ChildRegistry<AppMsg, ChildName>>,
    interval: Duration,
) -> Vec<Box<dyn lane_switchboards::supervisor::ChildSpec<AppMsg>>> {
    let inst = instance.to_string();
    vec![
        registry_child_spec!(
            0,
            ChildName::Calculator,
            registry.clone(),
            Calculator {
                last_result: None
            }
        ),
        registry_child_spec!(
            1,
            ChildName::Timer,
            registry.clone(),
            ResultTimer {
                registry: registry.clone(),
                self_ref: None,
                interval,
                running: false,
                instance: inst.clone(),
            }
        ),
    ]
}

// --- Coordinator reply bus (in-process, shared with gateways) ---

struct CoordState {
    pending: Mutex<HashMap<u64, oneshot::Sender<Result<f64, String>>>>,
}

impl CoordState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            pending: Mutex::new(HashMap::new()),
        })
    }

    async fn register(&self, id: u64) -> oneshot::Receiver<Result<f64, String>> {
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id, tx);
        rx
    }

    async fn complete(&self, id: u64, result: Result<f64, String>) {
        if let Some(tx) = self.pending.lock().await.remove(&id) {
            let _ = tx.send(result);
        }
    }
}

struct CoordinatorActor {
    state: Arc<CoordState>,
}

#[async_trait::async_trait]
impl Actor<CalcWire> for CoordinatorActor {
    async fn handle(&mut self, msg: CalcWire) -> Result<(), ActorProcessingErr> {
        if let Some(calc_wire::Kind::Reply(reply)) = msg.kind {
            let result = if reply.ok {
                Ok(reply.value)
            } else {
                Err(reply.error)
            };
            self.state.complete(reply.request_id, result).await;
        }
        Ok(())
    }
}

// --- gRPC wire (prost) ---

#[derive(Clone, PartialEq, Message)]
pub struct CalcWire {
    #[prost(oneof = "calc_wire::Kind", tags = "1, 2, 3, 4, 5")]
    pub kind: Option<calc_wire::Kind>,
}

pub mod calc_wire {
    use super::{AddReq, DivReq, HealthCheck, LastResultReq, Reply};
    use lane_switchboards::prost::Oneof;

    #[derive(Clone, PartialEq, Oneof)]
    pub enum Kind {
        #[prost(message, tag = "1")]
        Add(AddReq),
        #[prost(message, tag = "2")]
        Div(DivReq),
        #[prost(message, tag = "3")]
        LastResult(LastResultReq),
        #[prost(message, tag = "4")]
        Reply(Reply),
        #[prost(message, tag = "5")]
        Health(HealthCheck),
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
pub struct LastResultReq {
    #[prost(uint64, tag = "1")]
    pub request_id: u64,
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

#[derive(Clone, PartialEq, Message)]
pub struct HealthCheck {}

// --- Mesh gateway: supervised calculator + forwards replies ---

struct CalculatorGateway {
    instance: String,
    coordinator_addr: String,
    registry: Arc<ChildRegistry<AppMsg, ChildName>>,
    state: Arc<CoordState>,
    _supervisor: Option<SupervisorHandle<AppMsg>>,
    timer_started: bool,
}

impl CalculatorGateway {
    fn child_specs(registry: Arc<ChildRegistry<AppMsg, ChildName>>, instance: &str) -> Vec<Box<dyn lane_switchboards::supervisor::ChildSpec<AppMsg>>> {
        rest_for_one_specs(instance, registry, Duration::from_millis(400))
    }

    async fn start_timer(&mut self) -> Result<(), ActorProcessingErr> {
        if self.timer_started {
            return Ok(());
        }
        let timer = self
            .registry
            .get(&ChildName::Timer)
            .ok_or_else(|| "timer child not running")?;
        timer
            .send(AppMsg::TimerStart(timer.clone()))
            .await
            .map_err(|e| e.to_string())?;
        self.timer_started = true;
        Ok(())
    }

    async fn send_reply(&self, request_id: u64, result: Result<f64, String>) {
        let remote = RemoteActorRef::<CalcWire>::new(&self.coordinator_addr, COORD_TARGET);
        let (ok, value, error) = match result {
            Ok(v) => (true, v, String::new()),
            Err(e) => (false, 0.0, e),
        };
        let wire = CalcWire {
            kind: Some(calc_wire::Kind::Reply(Reply {
                request_id,
                ok,
                value,
                error,
            })),
        };
        if let Err(e) = remote.send(wire).await {
            eprintln!(
                "[{}] failed to send reply to coordinator: {e}",
                self.instance
            );
        }
    }
}

#[async_trait::async_trait]
impl Actor<CalcWire> for CalculatorGateway {
    async fn pre_start(&mut self) -> Result<(), ActorProcessingErr> {
        let registry = self.registry.clone();
        let instance = self.instance.clone();
        let handle = Supervisor::new(
            SupervisorConfig {
                strategy: RestartStrategy::RestForOne,
                max_restarts: 10,
                within_secs: 60,
                intensity_action: IntensityAction::ShutdownSupervisor,
                ..Default::default()
            },
            Self::child_specs(registry.clone(), &instance),
        )
        .start_settled(Duration::from_millis(50))
        .await?;
        self._supervisor = Some(handle);
        println!(
            "[{instance}] gateway up — RestForOne calculator + timer (order 0→1)"
        );
        self.start_timer().await.map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn handle(&mut self, msg: CalcWire) -> Result<(), ActorProcessingErr> {
        match msg.kind {
            Some(calc_wire::Kind::Add(req)) => {
                let result = registry_ask!(
                    self.registry,
                    ChildName::Calculator,
                    "calculator not running",
                    |reply| AppMsg::Add(req.a, req.b, reply)
                )
                .map_err(|e| e.to_string());
                self.send_reply(req.request_id, result).await;
            }
            Some(calc_wire::Kind::Div(req)) => {
                let result = match registry_ask!(
                    self.registry,
                    ChildName::Calculator,
                    "calculator not running",
                    |reply| AppMsg::Div(req.a, req.b, reply)
                ) {
                    Ok(v) => Ok(v),
                    Err(_) => Err("calculator crashed (RestForOne restarted calculator + timer)".into()),
                };
                self.send_reply(req.request_id, result).await;
            }
            Some(calc_wire::Kind::LastResult(req)) => {
                let result = match registry_ask!(
                    self.registry,
                    ChildName::Calculator,
                    "calculator not running",
                    |reply| AppMsg::LastResult(reply)
                ) {
                    Ok(Some(v)) => Ok(v),
                    Ok(None) => Err("no last result".to_string()),
                    Err(e) => Err(e.to_string()),
                };
                self.send_reply(req.request_id, result).await;
            }
            Some(calc_wire::Kind::Health(_)) => {
                println!("[{}] health ok", self.instance);
            }
            _ => {}
        }
        Ok(())
    }
}

static REQUEST_ID: AtomicU64 = AtomicU64::new(1);

async fn remote_calc(
    router: &MeshRouter<CalcWire>,
    state: &CoordState,
    key: u64,
    wire: CalcWire,
) -> anyhow::Result<f64> {
    let id = REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    let rx = state.register(id).await;

    let wire = match wire.kind {
        Some(calc_wire::Kind::Add(mut r)) => {
            r.request_id = id;
            CalcWire {
                kind: Some(calc_wire::Kind::Add(r)),
            }
        }
        Some(calc_wire::Kind::Div(mut r)) => {
            r.request_id = id;
            CalcWire {
                kind: Some(calc_wire::Kind::Div(r)),
            }
        }
        Some(calc_wire::Kind::LastResult(mut r)) => {
            r.request_id = id;
            CalcWire {
                kind: Some(calc_wire::Kind::LastResult(r)),
            }
        }
        _ => wire,
    };

    let start = Instant::now();
    router.invoke(CALC_SERVICE, &key, wire).await?;
    match tokio::time::timeout(Duration::from_secs(3), rx).await {
        Ok(Ok(Ok(v))) => {
            println!("  rpc {id} completed in {:?}", start.elapsed());
            Ok(v)
        }
        Ok(Ok(Err(e))) => Err(anyhow::anyhow!("{e}")),
        Ok(Err(_)) => Err(anyhow::anyhow!("coordinator dropped reply")),
        Err(_) => Err(anyhow::anyhow!("timeout waiting for calculator reply")),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    println!("=== Calculator service mesh (protobuf + RestForOne per replica) ===\n");

    let state = CoordState::new();

    let coord = serve_actor(
        "coordinator",
        "127.0.0.1:0",
        COORD_TARGET,
        CoordinatorActor {
            state: state.clone(),
        },
    )
    .await?;
    let coord_addr = coord.address().to_string();
    println!("[coordinator] reply bus @ {coord_addr}\n");

    let registry = MeshRegistryHandle::bind("127.0.0.1:0").await?;
    println!("[control] MeshRegistry @ {}\n", registry.address);
    let mut registry_client = MeshRegistryClient::connect(&registry.address).await?;

    let mut local_mesh = lane_switchboards::mesh::ServiceMesh::new();
    for i in 0..3 {
        let instance = format!("calc-{i}");
        let handle = serve_microservice(
            CALC_SERVICE,
            &instance,
            "127.0.0.1:0",
            CalculatorGateway {
                instance: instance.clone(),
                coordinator_addr: coord_addr.clone(),
                registry: Arc::new(ChildRegistry::new()),
                state: state.clone(),
                _supervisor: None,
                timer_started: false,
            },
        )
        .await?;
        join_mesh(&mut local_mesh, Some(&mut registry_client), &handle).await?;
        println!("[mesh] {instance} @ {}", handle.address());
    }

    let mut router = MeshRouter::with_registry(&registry.address);
    router.sync().await?;

    tokio::time::sleep(Duration::from_millis(200)).await;

    println!("--- Remote add (hash-ring sticky key) ---");
    let sum = remote_calc(
        &router,
        &state,
        42,
        CalcWire {
            kind: Some(calc_wire::Kind::Add(AddReq {
                request_id: 0,
                a: 10.0,
                b: 4.0,
            })),
        },
    )
    .await?;
    println!("[client] 10 + 4 = {sum}\n");

    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("--- Remote div by zero (RestForOne restarts calculator + timer on that replica) ---");
    let _ = remote_calc(
        &router,
        &state,
        42,
        CalcWire {
            kind: Some(calc_wire::Kind::Div(DivReq {
                request_id: 0,
                a: 10.0,
                b: 0.0,
            })),
        },
    )
    .await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    let sum2 = remote_calc(
        &router,
        &state,
        42,
        CalcWire {
            kind: Some(calc_wire::Kind::Add(AddReq {
                request_id: 0,
                a: 1.0,
                b: 1.0,
            })),
        },
    )
    .await?;
    println!("[client] after recovery: 1 + 1 = {sum2}\n");

    println!("--- Fan-out health ---");
    for (inst, res) in router
        .invoke_all(
            CALC_SERVICE,
            CalcWire {
                kind: Some(calc_wire::Kind::Health(HealthCheck {})),
            },
        )
        .await
    {
        println!("  {inst}: {res:?}");
    }

    println!("\n--- Why lane_switchboards helps ---");
    println!("  • Fault-tolerant: RestForOne restarts calculator + timer after div-by-zero panic");
    println!("  • Fast: warm gRPC bidi streams + protobuf (~µs–sub-ms per hop, see README benchmarks)");
    println!("  • Easy: registry_child_spec!, registry_ask!, mesh join/sync/invoke — no hand-rolled TCP/JSON");
    println!("\nSee examples/calculator_mesh.md for architecture.\n");

    Ok(())
}
