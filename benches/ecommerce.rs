//! End-to-end checkout pipeline benchmark (Criterion).
//!
//! Simulates one flash-sale checkout: orders round-robin + inventory QUORUM + billing invoke.
//!
//! Run: `cargo bench --bench ecommerce`

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use lane_switchboards::consistency::{ConsistencyConfig, WriteConsistency};
use lane_switchboards::distributed::serve_actor;
use lane_switchboards::mesh::{join_mesh, serve_microservice, MeshRegistryHandle, ServiceMesh};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;

#[path = "../examples/ecommerce_shared/mod.rs"]
mod ecommerce_shared;

use ecommerce_shared::{
    BillingActor, BillingMsg, InventoryActor, InventoryMsg, OrderCommand, OrderGatewayActor,
    BILLING_SERVICE, FLASH_SKU, INVENTORY_SERVICE, ORDER_TARGET, ORDERS_SERVICE,
};

struct BenchState {
    order_refs: Vec<lane_switchboards::distributed::RemoteActorRef<OrderCommand>>,
    order_rr: Mutex<usize>,
    quorum_mesh: ServiceMesh<InventoryMsg>,
    billing_mesh: ServiceMesh<BillingMsg>,
}

fn rt() -> Runtime {
    Runtime::new().expect("tokio runtime")
}

fn setup(runtime: &Runtime) -> BenchState {
    runtime.block_on(async {
        let registry = MeshRegistryHandle::bind("127.0.0.1:0")
            .await
            .expect("registry");
        let mut registry_client =
            lane_switchboards::mesh::MeshRegistryClient::connect(&registry.address)
                .await
                .expect("client");

        let mut quorum_mesh = ServiceMesh::with_consistency(ConsistencyConfig {
            rf: 3,
            local_rf: 3,
            write_cl: WriteConsistency::Quorum,
            ack_timeout: Duration::from_secs(5),
            ..ConsistencyConfig::default()
        });

        for i in 0..3 {
            let h = serve_microservice(
                INVENTORY_SERVICE,
                format!("inv-{i}"),
                "127.0.0.1:0",
                InventoryActor {
                    instance: format!("inv-{i}"),
                },
            )
            .await
            .expect("inv");
            join_mesh(&mut quorum_mesh, Some(&mut registry_client), &h)
                .await
                .expect("join");
        }

        let mut billing_mesh = ServiceMesh::new();
        for i in 0..2 {
            let h = serve_microservice(
                BILLING_SERVICE,
                format!("bill-{i}"),
                "127.0.0.1:0",
                BillingActor {
                    instance: format!("bill-{i}"),
                },
            )
            .await
            .expect("bill");
            join_mesh(&mut billing_mesh, Some(&mut registry_client), &h)
                .await
                .expect("join");
        }

        let mut order_refs = Vec::new();
        for i in 0..2 {
            let node = serve_actor(
                format!("{ORDERS_SERVICE}-{i}"),
                "127.0.0.1:0",
                ORDER_TARGET,
                OrderGatewayActor::new_replica(i),
            )
            .await
            .expect("order");
            order_refs.push(lane_switchboards::distributed::RemoteActorRef::new(
                node.address(),
                ORDER_TARGET,
            ));
        }

        BenchState {
            order_refs,
            order_rr: Mutex::new(0),
            quorum_mesh,
            billing_mesh,
        }
    })
}

fn bench_checkout_pipeline(c: &mut Criterion) {
    let runtime = rt();
    let state = Arc::new(setup(&runtime));
    let mut order_id = 1u64;

    c.bench_function("ecommerce_checkout_pipeline", |b| {
        b.iter(|| {
            let state = state.clone();
            order_id += 1;
            runtime.block_on(async move {
                let idx = {
                    let mut rr = state.order_rr.lock().await;
                    let i = *rr % state.order_refs.len();
                    *rr += 1;
                    i
                };
                state.order_refs[idx]
                    .send(black_box(OrderCommand::checkout(
                        order_id,
                        FLASH_SKU,
                        1,
                        4999,
                    )))
                    .await
                    .expect("order");

                state
                    .quorum_mesh
                    .invoke_consistent(
                        INVENTORY_SERVICE,
                        &FLASH_SKU,
                        black_box(InventoryMsg::reserve(order_id, FLASH_SKU, 1)),
                    )
                    .await
                    .expect("inventory");

                state
                    .billing_mesh
                    .invoke(
                        BILLING_SERVICE,
                        &order_id,
                        black_box(BillingMsg::charge(order_id, 4999)),
                    )
                    .await
                    .expect("billing");
            });
        });
    });
}

criterion_group!(ecommerce_benches, bench_checkout_pipeline);
criterion_main!(ecommerce_benches);
