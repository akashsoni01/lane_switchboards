//! E-commerce flash sale: gRPC mesh + supervised order gateways + autoscaling + QUORUM inventory.
//!
//! Realistic flow:
//! 1. **Orders** — autoscaling gRPC cluster; each replica runs a **OneForOne** supervisor tree
//!    (payment + fraud screening) before accepting checkout.
//! 2. **Inventory** — three mesh replicas; **QUORUM** reserve during the sale (`invoke_consistent`).
//! 3. **Billing** — mesh hash-ring charge per order.
//!
//! Run: `cargo run --example ecommerce_flash_sale`
//! See: `examples/ecommerce_flash_sale.md`

mod ecommerce_shared;

use ecommerce_shared::{
    AutoscaleConfig, AutoscalingCluster, BillingActor, BillingMsg, InventoryActor, InventoryMsg,
    OrderCommand, OrderGatewayActor, AUTOSCALE_REQ_PER_REPLICA, BILLING_SERVICE,
    CHECKOUTS_PER_WAVE, FLASH_SKU, INVENTORY_SERVICE, MAX_LOAD_WAVES, ORDER_TARGET,
    ORDERS_INITIAL, ORDERS_MAX, ORDERS_SERVICE,
};
use lane_switchboards::consistency::{ConsistencyConfig, WriteConsistency};
use lane_switchboards::mesh::{
    join_mesh, serve_microservice, MeshRegistryClient, MeshRegistryHandle, MeshRouter,
    ServiceMesh,
};
use std::time::{Duration, Instant};

async fn launch_order_replica(
    id: usize,
) -> std::io::Result<lane_switchboards::distributed::NodeHandle<OrderCommand>> {
    lane_switchboards::distributed::serve_actor(
        format!("{ORDERS_SERVICE}-replica-{id}"),
        "127.0.0.1:0",
        ORDER_TARGET,
        OrderGatewayActor::new_replica(id),
    )
    .await
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    println!("=== E-commerce flash sale (gRPC mesh + supervision + autoscale) ===\n");
    println!("  SKU: {FLASH_SKU}");
    println!(
        "  orders: {ORDERS_INITIAL}→{ORDERS_MAX} replicas, scale when ≥ {AUTOSCALE_REQ_PER_REPLICA} checkouts/replica/window"
    );
    println!("  inventory: 3 replicas, QUORUM reserve (rf=3, W=2)\n");

    // --- Control plane ---
    let registry = MeshRegistryHandle::bind("127.0.0.1:0").await?;
    println!("[control] MeshRegistry @ {}\n", registry.address);
    let mut registry_client = MeshRegistryClient::connect(&registry.address).await?;

    // --- Inventory (mesh, quorum writes) ---
    let mut quorum_mesh = ServiceMesh::with_consistency(ConsistencyConfig {
        rf: 3,
        local_rf: 3,
        write_cl: WriteConsistency::Quorum,
        ack_timeout: Duration::from_secs(3),
        ..ConsistencyConfig::default()
    });
    for i in 0..3 {
        let handle = serve_microservice(
            INVENTORY_SERVICE,
            &format!("inv-{i}"),
            "127.0.0.1:0",
            InventoryActor {
                instance: format!("inv-{i}"),
            },
        )
        .await?;
        join_mesh(&mut quorum_mesh, Some(&mut registry_client), &handle).await?;
        println!("[inventory] {} @ {}", handle.record.instance_id, handle.address());
    }

    // --- Billing (mesh, sticky invoke via router) ---
    let mut billing_mesh = ServiceMesh::new();
    for i in 0..2 {
        let handle = serve_microservice(
            BILLING_SERVICE,
            &format!("bill-{i}"),
            "127.0.0.1:0",
            BillingActor {
                instance: format!("bill-{i}"),
            },
        )
        .await?;
        join_mesh(&mut billing_mesh, Some(&mut registry_client), &handle).await?;
        println!("[billing] {} @ {}", handle.record.instance_id, handle.address());
    }
    let _ = billing_mesh;

    let mut router = MeshRouter::with_registry(&registry.address);
    router.sync().await?;

    // --- Orders (autoscaling cluster, supervised gateways) ---
    let mut orders = AutoscalingCluster::new(AutoscaleConfig::default());
    for replica in 0..ORDERS_INITIAL {
        let node = launch_order_replica(replica).await?;
        println!(
            "[orders] {} @ {} (supervised payment+fraud)",
            node.name(),
            node.address()
        );
        orders.join_node(node, replica);
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    println!("\n--- Flash sale: checkout waves (orders autoscale + mesh saga) ---\n");

    let sale_start = Instant::now();
    let mut total_checkouts = 0u64;

    for wave in 0..MAX_LOAD_WAVES {
        let wave_start = Instant::now();
        for checkout in 0..CHECKOUTS_PER_WAVE {
            let order_id = 10_000 + wave as u64 * CHECKOUTS_PER_WAVE as u64 + checkout as u64;
            let amount_cents = 4999 + checkout as u64;

            orders
                .send_round_robin(OrderCommand::checkout(
                    order_id,
                    FLASH_SKU,
                    1,
                    amount_cents,
                ))
                .await?;

            quorum_mesh
                .invoke_consistent(
                    INVENTORY_SERVICE,
                    &FLASH_SKU,
                    InventoryMsg::reserve(order_id, FLASH_SKU, 1),
                )
                .await?;

            router
                .invoke(
                    BILLING_SERVICE,
                    &order_id,
                    BillingMsg::charge(order_id, amount_cents),
                )
                .await?;

            total_checkouts += 1;
        }

        let added = orders
            .maybe_scale_up(ORDERS_SERVICE, |id| launch_order_replica(id))
            .await?;

        println!(
            "  wave {wave}: {CHECKOUTS_PER_WAVE} checkouts in {:?} — orders roster={} (+{added})",
            wave_start.elapsed(),
            orders.len(),
        );

        if orders.len() >= ORDERS_MAX {
            println!("  orders at max replicas ({ORDERS_MAX}); stopping waves");
            break;
        }

        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    let sale_elapsed = sale_start.elapsed();
    let per_checkout = sale_elapsed.as_secs_f64() / total_checkouts.max(1) as f64;

    println!("\n--- Sale summary ---");
    println!("  total checkouts: {total_checkouts}");
    println!("  wall time: {sale_elapsed:?}");
    println!("  ~{per_checkout:.4}s per checkout (in-process, localhost gRPC)");
    println!("  final orders replicas: {}", orders.len());
    println!(
        "  inventory instances: {}",
        router.mesh.instance_count(INVENTORY_SERVICE)
    );

    println!("\n--- Gateway health (hash-ring sample) ---");
    for replica in 0..orders.len().min(3) {
        orders
            .send_by_key(&replica, OrderCommand::health())
            .await?;
    }

    println!("\n--- Benchmarks (Criterion, release) ---");
    println!("  cargo bench --bench wire       # core gRPC primitives");
    println!("  cargo bench --bench ecommerce  # full checkout pipeline");
    println!("\nSee examples/ecommerce_flash_sale.md for architecture diagrams.\n");

    Ok(())
}
