//! TCP service mesh demo: orders, inventory, and billing microservices.
//!
//! - **Control plane**: `MeshRegistryServer` (TCP register / list)
//! - **Data plane**: length-prefixed JSON frames to each service instance
//! - **Router**: `MeshRouter` syncs from registry and invokes by service name + hash key
//!
//! Run: `cargo run --example service_mesh`
//! See: `examples/service_mesh.md`

use lane_switchboards::actor::{Actor, ActorProcessingErr};
use lane_switchboards::mesh::{
    join_mesh, serve_microservice, MeshRegistryClient, MeshRegistryServer, MeshRouter,
};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Service {
    Orders,
    Inventory,
    Billing,
}

impl Service {
    const ALL: [Self; 3] = [Self::Orders, Self::Inventory, Self::Billing];

    fn name(self) -> &'static str {
        match self {
            Self::Orders => "orders",
            Self::Inventory => "inventory",
            Self::Billing => "billing",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum MeshMsg {
    Orders(OrdersMsg),
    Inventory(InventoryMsg),
    Billing(BillingMsg),
    HealthCheck,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum OrdersMsg {
    Create { order_id: u64, sku: String, qty: u32 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum InventoryMsg {
    Reserve { order_id: u64, sku: String, qty: u32 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum BillingMsg {
    Charge { order_id: u64, amount_cents: u64 },
}

struct OrdersService {
    instance: String,
    count: Arc<AtomicU64>,
}

#[async_trait::async_trait]
impl Actor<MeshMsg> for OrdersService {
    async fn handle(&mut self, msg: MeshMsg) -> Result<(), ActorProcessingErr> {
        match msg {
            MeshMsg::Orders(OrdersMsg::Create { order_id, sku, qty }) => {
                let n = self.count.fetch_add(1, Ordering::Relaxed) + 1;
                println!(
                    "[orders:{}] create order {order_id} sku={sku} qty={qty} (total={n})",
                    self.instance
                );
            }
            MeshMsg::HealthCheck => {
                println!("[orders:{}] health ok", self.instance);
            }
            _ => {}
        }
        Ok(())
    }
}

struct InventoryService {
    instance: String,
}

#[async_trait::async_trait]
impl Actor<MeshMsg> for InventoryService {
    async fn handle(&mut self, msg: MeshMsg) -> Result<(), ActorProcessingErr> {
        match msg {
            MeshMsg::Inventory(InventoryMsg::Reserve { order_id, sku, qty }) => {
                println!(
                    "[inventory:{}] reserve order {order_id} sku={sku} qty={qty}",
                    self.instance
                );
            }
            MeshMsg::HealthCheck => {
                println!("[inventory:{}] health ok", self.instance);
            }
            _ => {}
        }
        Ok(())
    }
}

struct BillingService {
    instance: String,
}

#[async_trait::async_trait]
impl Actor<MeshMsg> for BillingService {
    async fn handle(&mut self, msg: MeshMsg) -> Result<(), ActorProcessingErr> {
        match msg {
            MeshMsg::Billing(BillingMsg::Charge { order_id, amount_cents }) => {
                println!(
                    "[billing:{}] charge order {order_id} amount={amount_cents}c",
                    self.instance
                );
            }
            MeshMsg::HealthCheck => {
                println!("[billing:{}] health ok", self.instance);
            }
            _ => {}
        }
        Ok(())
    }
}

async fn launch(
    service: Service,
    instance: &str,
    registry_addr: &str,
    mesh: &mut lane_switchboards::mesh::ServiceMesh<MeshMsg>,
) -> Result<lane_switchboards::mesh::MicroserviceHandle<MeshMsg>, Box<dyn std::error::Error>> {
    let handle = match service {
        Service::Orders => {
            serve_microservice(
                service.name(),
                instance,
                "127.0.0.1:0",
                OrdersService {
                    instance: instance.to_string(),
                    count: Arc::new(AtomicU64::new(0)),
                },
            )
            .await?
        }
        Service::Inventory => {
            serve_microservice(
                service.name(),
                instance,
                "127.0.0.1:0",
                InventoryService {
                    instance: instance.to_string(),
                },
            )
            .await?
        }
        Service::Billing => {
            serve_microservice(
                service.name(),
                instance,
                "127.0.0.1:0",
                BillingService {
                    instance: instance.to_string(),
                },
            )
            .await?
        }
    };
    join_mesh(mesh, Some(registry_addr), &handle).await?;
    println!(
        "[mesh] registered {} {} @ {}",
        service.name(),
        instance,
        handle.address()
    );
    Ok(handle)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("=== TCP service mesh ===\n");

    // Control plane
    let registry = MeshRegistryServer::bind("127.0.0.1:9050").await?;
    println!("[control] registry @ {}\n", registry.address);

    let mut local_mesh = lane_switchboards::mesh::ServiceMesh::new();

    // Data plane: microservice instances register over TCP
    let _orders1 = launch(Service::Orders, "orders-1", &registry.address, &mut local_mesh).await?;
    let _orders2 = launch(Service::Orders, "orders-2", &registry.address, &mut local_mesh).await?;
    let _inv1 = launch(Service::Inventory, "inv-1", &registry.address, &mut local_mesh).await?;
    let _inv2 = launch(Service::Inventory, "inv-2", &registry.address, &mut local_mesh).await?;
    let _bill1 = launch(Service::Billing, "bill-1", &registry.address, &mut local_mesh).await?;

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Router syncs routing table from control plane
    let mut router = MeshRouter::with_registry(&registry.address);
    router.sync().await?;

    println!("\n=== Discovered services ===");
    for name in router.mesh.services() {
        println!("  {name}: {} instance(s)", router.mesh.instance_count(&name));
    }

    let order_id = 9001u64;
    let sku = "widget".to_string();

    println!("\n=== Sticky invoke (hash ring per service) ===\n");

    router
        .invoke(
            Service::Orders.name(),
            &order_id,
            MeshMsg::Orders(OrdersMsg::Create {
                order_id,
                sku: sku.clone(),
                qty: 2,
            }),
        )
        .await?;

    router
        .invoke(
            Service::Inventory.name(),
            &order_id,
            MeshMsg::Inventory(InventoryMsg::Reserve {
                order_id,
                sku,
                qty: 2,
            }),
        )
        .await?;

    router
        .invoke(
            Service::Billing.name(),
            &order_id,
            MeshMsg::Billing(BillingMsg::Charge {
                order_id,
                amount_cents: 4999,
            }),
        )
        .await?;

    tokio::time::sleep(Duration::from_millis(80)).await;

    println!("\n=== Fan-out health check (all instances of each service) ===\n");

    for service in Service::ALL {
        let results = router
            .invoke_all(service.name(), MeshMsg::HealthCheck)
            .await;
        for (instance, result) in results {
            println!("[router] health {}/{}: {result:?}", service.name(), instance);
        }
    }

    println!("\n=== Scale out: add orders-3, re-sync router ===\n");

    let _orders3 = launch(
        Service::Orders,
        "orders-3",
        &registry.address,
        &mut local_mesh,
    )
    .await?;
    router.sync().await?;
    println!(
        "orders instances: {}",
        router.mesh.instance_count(Service::Orders.name())
    );

    let list = MeshRegistryClient::list(&registry.address).await?;
    println!("\n[control] registry records: {}", list.len());

    Ok(())
}
