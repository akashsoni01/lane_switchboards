//! Horizontal scaling: add worker nodes to an existing cluster and spread load.
//!
//! Phase 1 — two worker nodes handle incoming jobs.
//! Phase 2 — two more nodes bind on new addresses (simulating extra hardware) and join
//! the same cluster roster; the coordinator round-robins across all four.
//!
//! Run: `cargo run --example horizontal_scaling`
//! See: `examples/horizontal_scaling.md`

use lane_switchboards::actor::{spawn, Actor, ActorProcessingErr};
use lane_switchboards::distributed::{Node, RemoteActorRef};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

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

struct WorkerNode {
    name: String,
    address: String,
    _task: tokio::task::JoinHandle<()>,
}

impl WorkerNode {
    async fn launch(name: impl Into<String>) -> Result<Self, Box<dyn std::error::Error>> {
        let name = name.into();
        let node = Node::<WorkMsg>::bind(&name, "127.0.0.1:0").await?;
        let address = node.address().to_string();
        let processed = Arc::new(AtomicU64::new(0));

        let (tx, mut rx) = mpsc::channel(32);
        node.register("worker", tx).await;

        let worker_name = name.clone();
        let processed_for_actor = processed.clone();
        let task = tokio::spawn(async move {
            let (actor, _) = spawn(
                Worker {
                    node_name: worker_name.clone(),
                    processed: processed_for_actor,
                },
                None,
            )
            .await
            .expect("spawn worker");

            while let Some(msg) = rx.recv().await {
                let _ = actor.send(msg).await;
            }
        });

        println!("[cluster] node {name} online at {address}");
        Ok(Self {
            name,
            address,
            _task: task,
        })
    }
}

/// In-memory cluster roster — add a remote worker whenever new hardware comes online.
struct Cluster {
    workers: Vec<RemoteActorRef<WorkMsg>>,
}

impl Cluster {
    fn new() -> Self {
        Self {
            workers: Vec::new(),
        }
    }

    fn add_node(&mut self, node: &WorkerNode) {
        println!(
            "[cluster] hooking {} into roster ({} workers total)",
            node.name,
            self.workers.len() + 1
        );
        self.workers
            .push(RemoteActorRef::new(&node.address, "worker"));
    }

    async fn dispatch(&mut self, job_id: u64) -> Result<(), Box<dyn std::error::Error>> {
        let worker = &self.workers[job_id as usize % self.workers.len()];
        worker.send(WorkMsg::Process { job_id }).await?;
        Ok(())
    }

    fn len(&self) -> usize {
        self.workers.len()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("=== Phase 1: initial cluster (2 worker nodes) ===\n");

    let node_a = WorkerNode::launch("worker-a").await?;
    let node_b = WorkerNode::launch("worker-b").await?;

    let mut cluster = Cluster::new();
    cluster.add_node(&node_a);
    cluster.add_node(&node_b);

    for job_id in 1..=6 {
        cluster.dispatch(job_id).await?;
    }

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    println!("\n=== Phase 2: horizontal scale-out (+2 nodes on new hardware) ===\n");
    println!("Launch new nodes, bind TCP listeners, register addresses in the roster.\n");

    let node_c = WorkerNode::launch("worker-c").await?;
    let node_d = WorkerNode::launch("worker-d").await?;
    cluster.add_node(&node_c);
    cluster.add_node(&node_d);

    println!(
        "\n[cluster] capacity: {} → {} workers\n",
        2,
        cluster.len()
    );

    for job_id in 7..=14 {
        cluster.dispatch(job_id).await?;
    }

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    println!("\nDone — jobs spread across all {} nodes.", cluster.len());
    Ok(())
}
