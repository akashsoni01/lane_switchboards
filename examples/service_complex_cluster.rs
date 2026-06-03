//! Multi-node deployment with **autoscaling**: start with a small replica count, then add
//! TCP nodes when per-replica load rises ([`AutoscalingCluster`] in shared module).
//!
//! Same supervised service actors and inner DAO trees as [`service_complex`](./service_complex.rs).
//!
//! Run: `cargo run --example service_complex_cluster`
//! See: `examples/service_complex.md` (cluster + autoscale sections)

mod service_complex_shared;

use lane_switchboards::distributed::serve_actor;
use service_complex_shared::{
    AutoscaleConfig, AutoscalingCluster, ServiceACommand, ServiceBCommand,
    ServiceASupervisorActor, ServiceBSupervisorActor, AUTOSCALE_LOAD_WAVE_REQUESTS,
    AUTOSCALE_REQUESTS_PER_REPLICA, CLUSTER_REPLICAS_INITIAL, CLUSTER_REPLICAS_MAX,
    SERVICE_A, SERVICE_B, SERVICE_TARGET,
};
use std::time::Duration;

async fn launch_service_a(
    replica: usize,
) -> std::io::Result<lane_switchboards::distributed::NodeHandle<ServiceACommand>> {
    serve_actor(
        format!("{SERVICE_A}-replica-{replica}"),
        "127.0.0.1:0",
        SERVICE_TARGET,
        ServiceASupervisorActor::new_replica(replica),
    )
    .await
}

async fn launch_service_b(
    replica: usize,
) -> std::io::Result<lane_switchboards::distributed::NodeHandle<ServiceBCommand>> {
    serve_actor(
        format!("{SERVICE_B}-replica-{replica}"),
        "127.0.0.1:0",
        SERVICE_TARGET,
        ServiceBSupervisorActor::new_replica(replica),
    )
    .await
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let config = AutoscaleConfig::default();

    println!("=== service_complex_cluster: autoscale on load ===\n");
    println!(
        "  initial replicas: {CLUSTER_REPLICAS_INITIAL} per service (max {CLUSTER_REPLICAS_MAX})"
    );
    println!(
        "  scale up when ≥ {AUTOSCALE_REQUESTS_PER_REPLICA} dispatches/replica/window\n"
    );

    let mut cluster_a = AutoscalingCluster::new(config.clone());
    let mut cluster_b = AutoscalingCluster::new(config);

    println!("--- Boot Service A ({CLUSTER_REPLICAS_INITIAL} replicas) ---");
    for replica in 0..CLUSTER_REPLICAS_INITIAL {
        let node = launch_service_a(replica).await?;
        println!(
            "  [{replica}] {} @ {}",
            node.name(),
            node.address()
        );
        cluster_a.join_node(node, replica);
    }

    println!("\n--- Boot Service B ({CLUSTER_REPLICAS_INITIAL} replicas) ---");
    for replica in 0..CLUSTER_REPLICAS_INITIAL {
        let node = launch_service_b(replica).await?;
        println!(
            "  [{replica}] {} @ {}",
            node.name(),
            node.address()
        );
        cluster_b.join_node(node, replica);
    }

    tokio::time::sleep(Duration::from_millis(300)).await;

    println!("\n--- Autoscale phase: synthetic load waves (round-robin PingAll) ---");
    const MAX_WAVES: usize = 24;
    for wave in 0..MAX_WAVES {
        for _ in 0..AUTOSCALE_LOAD_WAVE_REQUESTS {
            cluster_a
                .send_round_robin(ServiceACommand::PingAll)
                .await?;
            cluster_b
                .send_round_robin(ServiceBCommand::PingAll)
                .await?;
        }

        let added_a = cluster_a
            .maybe_scale_up(SERVICE_A, |id| launch_service_a(id))
            .await?;
        let added_b = cluster_b
            .maybe_scale_up(SERVICE_B, |id| launch_service_b(id))
            .await?;

        let load_a = cluster_a.len();
        let load_b = cluster_b.len();
        println!(
            "  wave {wave}: dispatches/replica window — ServiceA roster={load_a} (+{added_a}), ServiceB roster={load_b} (+{added_b})"
        );

        if cluster_a.len() >= CLUSTER_REPLICAS_MAX && cluster_b.len() >= CLUSTER_REPLICAS_MAX {
            println!("  at max replicas ({CLUSTER_REPLICAS_MAX}); stopping load waves");
            break;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    println!(
        "\n--- Post-autoscale roster: ServiceA={} ServiceB={} (ring nodes={}) ---",
        cluster_a.len(),
        cluster_b.len(),
        cluster_a.cluster.ring().node_count()
    );

    tokio::time::sleep(Duration::from_millis(150)).await;

    println!("\n--- Broadcast PingAll to full rosters ---");
    cluster_a.broadcast(ServiceACommand::PingAll).await?;
    cluster_b.broadcast(ServiceBCommand::PingAll).await?;

    tokio::time::sleep(Duration::from_millis(200)).await;

    const FAIL_REPLICA: usize = 3;
    if cluster_a.len() > FAIL_REPLICA {
        println!("\n--- Fail DaoB on Service A replica {FAIL_REPLICA} (hash key) ---");
        cluster_a
            .send_by_key(&FAIL_REPLICA, ServiceACommand::FailDaoB)
            .await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
        cluster_a
            .send_by_key(&FAIL_REPLICA, ServiceACommand::PingAll)
            .await?;
    }

    if cluster_b.len() > 7 {
        println!("\n--- Fail DaoC on Service B replica 7 ---");
        cluster_b
            .send_by_key(&7, ServiceBCommand::FailDaoC)
            .await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    println!("\n--- Done ---");
    println!("  Autoscaling adds `serve_actor` nodes + `Cluster::join` when load/replica rises.");
    println!("  Service A vs B scale independently; DAO failures stay local to one replica.");
    Ok(())
}
