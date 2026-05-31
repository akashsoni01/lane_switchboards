//! Horizontal scaling: add worker nodes to an existing cluster and spread load.
//!
//! Phase 1 — two worker nodes handle incoming jobs.
//! Phase 2 — two more nodes bind on new addresses (simulating extra hardware) and join
//! the same cluster roster; the coordinator round-robins across all four.
//!
//! Run: `cargo run --example horizontal_scaling`
//! See: `examples/horizontal_scaling.md`

use lane_switchboards::actor::{Actor, ActorProcessingErr};
use lane_switchboards::distributed::{serve_actor, Cluster};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
enum WorkMsg {
    Process { job_id: u64 },
}

struct Worker {
    node_name: String,
    processed: Arc<AtomicU64>,
}

#[async_trait::async_trait]
impl Actor<WorkMsg> for Worker {
    async fn handle(&mut self, msg: WorkMsg) -> Result<(), ActorProcessingErr> {
        let WorkMsg::Process { job_id } = msg;
        let count = self.processed.fetch_add(1, Ordering::Relaxed) + 1;
        println!(
            "[{}] processed job {job_id} (total on this node: {count})",
            self.node_name
        );
        Ok(())
    }
}

async fn launch_worker(name: &str) -> Result<lane_switchboards::distributed::NodeHandle<WorkMsg>, Box<dyn std::error::Error>> {
    let handle = serve_actor(
        name,
        "127.0.0.1:0",
        "worker",
        Worker {
            node_name: name.to_string(),
            processed: Arc::new(AtomicU64::new(0)),
        },
    )
    .await?;
    println!(
        "[cluster] node {} online at {}",
        handle.name(),
        handle.address()
    );
    Ok(handle)
}

fn join_cluster(cluster: &mut Cluster<WorkMsg>, handle: &lane_switchboards::distributed::NodeHandle<WorkMsg>) {
    let n = cluster.len() + 1;
    println!(
        "[cluster] hooking {} into roster ({n} workers total)",
        handle.name()
    );
    cluster.join(handle.member.clone());
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("=== Phase 1: initial cluster (2 worker nodes) ===\n");

    let node_a = launch_worker("worker-a").await?;
    let node_b = launch_worker("worker-b").await?;

    let mut cluster = Cluster::new();
    join_cluster(&mut cluster, &node_a);
    join_cluster(&mut cluster, &node_b);

    for job_id in 1..=6 {
        cluster
            .send_round_robin(WorkMsg::Process { job_id })
            .await?;
    }

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    println!("\n=== Phase 2: horizontal scale-out (+2 nodes on new hardware) ===\n");
    println!("Launch new nodes, bind TCP listeners, register addresses in the roster.\n");

    let before = cluster.len();
    let node_c = launch_worker("worker-c").await?;
    let node_d = launch_worker("worker-d").await?;
    join_cluster(&mut cluster, &node_c);
    join_cluster(&mut cluster, &node_d);

    println!(
        "\n[cluster] capacity: {before} → {} workers\n",
        cluster.len()
    );

    for job_id in 7..=14 {
        cluster
            .send_round_robin(WorkMsg::Process { job_id })
            .await?;
    }

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    println!("\nDone — jobs spread across all {} nodes.", cluster.len());
    Ok(())
}
