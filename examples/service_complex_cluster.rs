//! Multi-node deployment: **10 replicas** of Service A and **10 replicas** of Service B,
//! each replica on its own TCP node ([`serve_actor`] + [`Cluster`]).
//!
//! Same supervised service actors and inner DAO trees as [`service_complex`](./service_complex.rs).
//!
//! Run: `cargo run --example service_complex_cluster`
//! See: `examples/service_complex.md` (cluster section)

mod service_complex_shared;

use lane_switchboards::distributed::{serve_actor, Cluster};
use service_complex_shared::{
    ServiceACommand, ServiceBCommand, ServiceASupervisorActor, ServiceBSupervisorActor,
    CLUSTER_REPLICAS, SERVICE_A, SERVICE_B, SERVICE_TARGET,
};
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    println!(
        "=== service_complex_cluster: {CLUSTER_REPLICAS} replicas × ServiceA + ServiceB ===\n"
    );

    let mut cluster_a = Cluster::<ServiceACommand>::new();
    let mut cluster_b = Cluster::<ServiceBCommand>::new();

    println!("--- Launch Service A replicas ---");
    for replica in 0..CLUSTER_REPLICAS {
        let node = serve_actor(
            format!("{SERVICE_A}-replica-{replica}"),
            "127.0.0.1:0",
            SERVICE_TARGET,
            ServiceASupervisorActor::new_replica(replica),
        )
        .await?;
        println!(
            "  [{replica}] {} @ {}",
            node.name(),
            node.address()
        );
        cluster_a.join(node.member.clone());
    }

    println!("\n--- Launch Service B replicas ---");
    for replica in 0..CLUSTER_REPLICAS {
        let node = serve_actor(
            format!("{SERVICE_B}-replica-{replica}"),
            "127.0.0.1:0",
            SERVICE_TARGET,
            ServiceBSupervisorActor::new_replica(replica),
        )
        .await?;
        println!(
            "  [{replica}] {} @ {}",
            node.name(),
            node.address()
        );
        cluster_b.join(node.member.clone());
    }

    println!(
        "\n--- Cluster roster: ServiceA={} ServiceB={} (hash ring nodes={}) ---",
        cluster_a.len(),
        cluster_b.len(),
        cluster_a.ring().node_count()
    );

    tokio::time::sleep(Duration::from_millis(300)).await;

    println!("\n--- Broadcast PingAll to all Service A replicas ---");
    cluster_a
        .broadcast(ServiceACommand::PingAll)
        .await?;
    cluster_b
        .broadcast(ServiceBCommand::PingAll)
        .await?;

    tokio::time::sleep(Duration::from_millis(200)).await;

    const FAIL_REPLICA: usize = 3;
    println!("\n--- Fail DaoB on Service A replica {FAIL_REPLICA} only (send_by_key) ---");
    cluster_a
        .send_by_key(&FAIL_REPLICA, ServiceACommand::FailDaoB)
        .await?;

    tokio::time::sleep(Duration::from_millis(200)).await;

    println!("\n--- Ping Service A replica {FAIL_REPLICA} again (DAO restarted locally) ---");
    cluster_a
        .send_by_key(&FAIL_REPLICA, ServiceACommand::PingAll)
        .await?;

    println!("\n--- Round-robin 5 calls across Service B replicas ---");
    for i in 0..5 {
        cluster_b
            .send_round_robin(ServiceBCommand::PingAll)
            .await?;
        println!("  round-robin ping {i} sent");
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    println!("\n--- Fail DaoC on Service B replica 7 ---");
    cluster_b
        .send_by_key(&7, ServiceBCommand::FailDaoC)
        .await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    println!("\n--- Broadcast PingAll to all Service B replicas ---");
    cluster_b
        .broadcast(ServiceBCommand::PingAll)
        .await?;

    tokio::time::sleep(Duration::from_millis(150)).await;

    println!("\n--- Done ---");
    println!("  Each replica is an independent TCP node with its own DAO supervisors.");
    println!("  Service A vs B clusters are isolated (separate rosters and message types).");
    println!("  Crashing a DAO on one replica does not affect other replicas or Service B.");
    Ok(())
}
