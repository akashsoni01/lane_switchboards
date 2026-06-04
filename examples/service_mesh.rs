//! gRPC service mesh demo: orders, inventory, and billing microservices.
//!
//! - **Control plane**: `MeshRegistryHandle` (register / list / watch)
//! - **Data plane**: protobuf payloads over `ActorMessaging` bidi streams
//! - **Router**: `MeshRouter` syncs from registry and invokes by service name + hash key
//!
//! Run: `cargo run --example service_mesh`
//! See: `examples/service_mesh.md`

use lane_switchboards::actor::{Actor, ActorProcessingErr};
use lane_switchboards::mesh::{
    join_mesh, serve_microservice, MeshRegistryClient, MeshRegistryHandle, MeshRouter,
};
use lane_switchboards::prost::Message;
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

#[derive(Clone, PartialEq, Message)]
struct MeshMsg {
    #[prost(oneof = "mesh_msg::Kind", tags = "1, 2, 3, 4")]
    kind: Option<mesh_msg::Kind>,
}

mod mesh_msg {
    use super::{BillingWire, HealthCheck, InventoryWire, OrdersWire};
    use lane_switchboards::prost::Oneof;

    #[derive(Clone, PartialEq, Oneof)]
    pub enum Kind {
        #[prost(message, tag = "1")]
        Orders(OrdersWire),
        #[prost(message, tag = "2")]
        Inventory(InventoryWire),
        #[prost(message, tag = "3")]
        Billing(BillingWire),
        #[prost(message, tag = "4")]
        HealthCheck(HealthCheck),
    }
}

#[derive(Clone, PartialEq, Message)]
struct HealthCheck {}

#[derive(Clone, PartialEq, Message)]
struct OrdersWire {
    #[prost(uint64, tag = "1")]
    order_id: u64,
    #[prost(string, tag = "2")]
    sku: String,
    #[prost(uint32, tag = "3")]
    qty: u32,
}

#[derive(Clone, PartialEq, Message)]
struct InventoryWire {
    #[prost(uint64, tag = "1")]
    order_id: u64,
    #[prost(string, tag = "2")]
    sku: String,
    #[prost(uint32, tag = "3")]
    qty: u32,
}

#[derive(Clone, PartialEq, Message)]
struct BillingWire {
    #[prost(uint64, tag = "1")]
    order_id: u64,
    #[prost(uint64, tag = "2")]
    amount_cents: u64,
}

impl MeshMsg {
    fn orders(order_id: u64, sku: String, qty: u32) -> Self {
        Self {
            kind: Some(mesh_msg::Kind::Orders(OrdersWire {
                order_id,
                sku,
                qty,
            })),
        }
    }

    fn inventory(order_id: u64, sku: String, qty: u32) -> Self {
        Self {
            kind: Some(mesh_msg::Kind::Inventory(InventoryWire {
                order_id,
                sku,
                qty,
            })),
        }
    }

    fn billing(order_id: u64, amount_cents: u64) -> Self {
        Self {
            kind: Some(mesh_msg::Kind::Billing(BillingWire {
                order_id,
                amount_cents,
            })),
        }
    }

    fn health_check() -> Self {
        Self {
            kind: Some(mesh_msg::Kind::HealthCheck(HealthCheck {})),
        }
    }
}

struct OrdersService {
    instance: String,
    count: Arc<AtomicU64>,
}

#[async_trait::async_trait]
impl Actor<MeshMsg> for OrdersService {
    async fn handle(&mut self, msg: MeshMsg) -> Result<(), ActorProcessingErr> {
        match msg.kind {
            Some(mesh_msg::Kind::Orders(OrdersWire {
                order_id,
                sku,
                qty,
            })) => {
                let n = self.count.fetch_add(1, Ordering::Relaxed) + 1;
                println!(
                    "[orders:{}] create order {order_id} sku={sku} qty={qty} (total={n})",
                    self.instance
                );
            }
            Some(mesh_msg::Kind::HealthCheck(_)) => {
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
        match msg.kind {
            Some(mesh_msg::Kind::Inventory(InventoryWire {
                order_id,
                sku,
                qty,
            })) => {
                println!(
                    "[inventory:{}] reserve order {order_id} sku={sku} qty={qty}",
                    self.instance
                );
            }
            Some(mesh_msg::Kind::HealthCheck(_)) => {
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
        match msg.kind {
            Some(mesh_msg::Kind::Billing(BillingWire {
                order_id,
                amount_cents,
            })) => {
                println!(
                    "[billing:{}] charge order {order_id} amount={amount_cents}c",
                    self.instance
                );
            }
            Some(mesh_msg::Kind::HealthCheck(_)) => {
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
    registry_client: &mut MeshRegistryClient,
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
    join_mesh(mesh, Some(registry_client), &handle).await?;
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

    println!("=== gRPC service mesh ===\n");

    let registry = MeshRegistryHandle::bind("127.0.0.1:0").await?;
    println!("[control] registry @ {}\n", registry.address);

    let mut local_mesh = lane_switchboards::mesh::ServiceMesh::new();
    let mut registry_client = MeshRegistryClient::connect(&registry.address).await?;

    let _orders1 = launch(Service::Orders, "orders-1", &mut registry_client, &mut local_mesh).await?;
    let _orders2 = launch(Service::Orders, "orders-2", &mut registry_client, &mut local_mesh).await?;
    let _inv1 = launch(Service::Inventory, "inv-1", &mut registry_client, &mut local_mesh).await?;
    let _inv2 = launch(Service::Inventory, "inv-2", &mut registry_client, &mut local_mesh).await?;
    let _bill1 = launch(Service::Billing, "bill-1", &mut registry_client, &mut local_mesh).await?;

    tokio::time::sleep(Duration::from_millis(50)).await;

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
            MeshMsg::orders(order_id, sku.clone(), 2),
        )
        .await?;

    router
        .invoke(
            Service::Inventory.name(),
            &order_id,
            MeshMsg::inventory(order_id, sku, 2),
        )
        .await?;

    router
        .invoke(
            Service::Billing.name(),
            &order_id,
            MeshMsg::billing(order_id, 4999),
        )
        .await?;

    tokio::time::sleep(Duration::from_millis(80)).await;

    println!("\n=== Fan-out health check (all instances of each service) ===\n");

    for service in Service::ALL {
        let results = router
            .invoke_all(service.name(), MeshMsg::health_check())
            .await;
        for (instance, result) in results {
            println!("[router] health {}/{}: {result:?}", service.name(), instance);
        }
    }

    println!("\n=== Scale out: add orders-3, re-sync router ===\n");

    let _orders3 = launch(
        Service::Orders,
        "orders-3",
        &mut registry_client,
        &mut local_mesh,
    )
    .await?;
    router.sync().await?;
    println!(
        "orders instances: {}",
        router.mesh.instance_count(Service::Orders.name())
    );

    let list = registry_client.list().await?;
    println!("\n[control] registry records: {}", list.len());

    Ok(())
}
