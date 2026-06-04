//! gRPC wire benchmarks (Criterion).
//!
//! Run: `cargo bench --bench wire`

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use lane_switchboards::distributed::{serve_actor, RemoteActorRef};
use lane_switchboards::mesh::{MeshRegistryClient, MeshRegistryHandle, ServiceRecord};
use lane_switchboards::prost::Message;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;

#[derive(Clone, PartialEq, Message)]
struct BenchPing {
    #[prost(uint64, tag = "1")]
    n: u64,
}

struct BenchActor;

#[async_trait::async_trait]
impl lane_switchboards::actor::Actor<BenchPing> for BenchActor {
    async fn handle(
        &mut self,
        _msg: BenchPing,
    ) -> Result<(), lane_switchboards::actor::ActorProcessingErr> {
        Ok(())
    }
}

fn rt() -> Runtime {
    Runtime::new().expect("tokio runtime")
}

fn bench_remote_send(c: &mut Criterion) {
    let runtime = rt();
    let remote = runtime.block_on(async {
        let node = serve_actor("bench", "127.0.0.1:0", "t", BenchActor)
            .await
            .expect("bind");
        let addr = node.address().to_string();
        RemoteActorRef::<BenchPing>::new(&addr, "t")
    });

    c.bench_function("remote_actor_ref_send", |b| {
        b.iter(|| {
            runtime.block_on(async {
                remote
                    .send(black_box(BenchPing { n: 1 }))
                    .await
                    .expect("send");
            });
        });
    });
}

fn bench_mesh_registry_list(c: &mut Criterion) {
    let runtime = rt();
    let client = runtime.block_on(async {
        let handle = MeshRegistryHandle::bind("127.0.0.1:0")
            .await
            .expect("registry bind");
        let mut client = MeshRegistryClient::connect(&handle.address)
            .await
            .expect("registry connect");
        for i in 0..32 {
            client
                .register(ServiceRecord {
                    service: "svc".into(),
                    instance_id: format!("i{i}"),
                    address: format!("127.0.0.1:{i}"),
                    target: format!("i{i}"),
                    dc: None,
                    registered_at: 0,
                })
                .await
                .expect("register");
        }
        Arc::new(Mutex::new(client))
    });

    c.bench_function("mesh_registry_list_32", |b| {
        b.iter(|| {
            let client = client.clone();
            runtime.block_on(async {
                let _ = black_box(
                    client
                        .lock()
                        .await
                        .list()
                        .await
                        .expect("list"),
                );
            });
        });
    });
}

fn bench_invoke_consistent_quorum(c: &mut Criterion) {
    use lane_switchboards::consistency::{ConsistencyConfig, WriteConsistency};
    use lane_switchboards::mesh::ServiceMesh;

    let runtime = rt();
    let mesh = runtime.block_on(async {
        let h1 = serve_actor("r1", "127.0.0.1:0", "inv", BenchActor)
            .await
            .expect("r1");
        let h2 = serve_actor("r2", "127.0.0.1:0", "inv", BenchActor)
            .await
            .expect("r2");
        let h3 = serve_actor("r3", "127.0.0.1:0", "inv", BenchActor)
            .await
            .expect("r3");
        let mut mesh = ServiceMesh::with_consistency(ConsistencyConfig {
            rf: 3,
            local_rf: 3,
            write_cl: WriteConsistency::Quorum,
            ack_timeout: Duration::from_secs(5),
            ..ConsistencyConfig::default()
        });
        for h in [&h1, &h2, &h3] {
            mesh.register(lane_switchboards::mesh::ServiceRecord {
                service: "inventory".into(),
                instance_id: h.name().to_string(),
                address: h.address().to_string(),
                target: "inv".into(),
                dc: None,
                registered_at: 0,
            });
        }
        mesh
    });

    c.bench_function("invoke_consistent_quorum_rf3", |b| {
        b.iter(|| {
            runtime.block_on(async {
                mesh.invoke_consistent(
                    "inventory",
                    &"sku",
                    black_box(BenchPing { n: 42 }),
                )
                .await
                .expect("quorum");
            });
        });
    });
}

criterion_group!(
    wire_benches,
    bench_remote_send,
    bench_mesh_registry_list,
    bench_invoke_consistent_quorum
);
criterion_main!(wire_benches);
