//! Three-node gRPC cluster: hash-ring routing and round-robin.
//!
//! Run: `cargo run --example grpc_cluster`

use lane_switchboards::actor::{Actor, ActorProcessingErr};
use lane_switchboards::distributed::{serve_actor, Cluster};
use lane_switchboards::prost::Message;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone, PartialEq, Message)]
struct WorkMsg {
    #[prost(uint64, tag = "1")]
    job_id: u64,
}

struct Worker {
    name: String,
    count: Arc<AtomicU64>,
}

#[async_trait::async_trait]
impl Actor<WorkMsg> for Worker {
    async fn handle(&mut self, msg: WorkMsg) -> Result<(), ActorProcessingErr> {
        let n = self.count.fetch_add(1, Ordering::Relaxed) + 1;
        println!("[{}] job {} (local total={n})", self.name, msg.job_id);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let mut cluster = Cluster::<WorkMsg>::new();
    for i in 0..3 {
        let handle = serve_actor(
            format!("node-{i}"),
            "127.0.0.1:0",
            "worker",
            Worker {
                name: format!("node-{i}"),
                count: Arc::new(AtomicU64::new(0)),
            },
        )
        .await?;
        cluster.join(handle.member.clone());
    }

    println!("cluster size: {}\n", cluster.len());

    for job_id in 1..=6 {
        cluster.send_by_key(&job_id, WorkMsg { job_id }).await?;
    }

    for _ in 0..3 {
        cluster.send_round_robin(WorkMsg { job_id: 99 }).await?;
    }

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    Ok(())
}
