//! Real-world consistency demo: replicated inventory over TLS + tunable W levels.
//!
//! Run: `cargo run --example consistency --features tls`
//! See: `examples/consistency.md` and `docs/consistency.md`

use lane_switchboards::actor::{Actor, ActorProcessingErr};
use lane_switchboards::config::TlsConfig;
use lane_switchboards::consistency::{
    ConsistencyConfig, ConsistencyError, WriteConsistency,
};
use lane_switchboards::mesh::{serve_microservice_tls, ServiceMesh};
use lane_switchboards::prost::Message;
use rcgen::generate_simple_self_signed;
use std::collections::HashMap;
use std::time::Duration;

#[derive(Clone, PartialEq, Message)]
struct StockMsg {
    #[prost(string, tag = "1")]
    sku: String,
    #[prost(uint32, tag = "2")]
    qty: u32,
}

/// Per-replica inventory ledger (application must replicate writes; mesh only routes + acks).
struct InventoryReplica {
    instance: String,
    ledger: HashMap<String, u32>,
}

#[async_trait::async_trait]
impl Actor<StockMsg> for InventoryReplica {
    async fn handle(&mut self, msg: StockMsg) -> Result<(), ActorProcessingErr> {
        *self.ledger.entry(msg.sku.clone()).or_insert(0) += msg.qty;
        let total = self.ledger.get(&msg.sku).copied().unwrap_or(0);
        println!(
            "[inventory:{}] reserved {} of {} (local total={total})",
            self.instance, msg.qty, msg.sku
        );
        Ok(())
    }
}

fn demo_tls() -> (TlsConfig, TlsConfig) {
    let cert = generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()])
        .expect("cert");
    let cert_pem = cert.cert.pem().into_bytes();
    let key_pem = cert.key_pair.serialize_pem().into_bytes();
    let server = TlsConfig {
        cert_pem: cert_pem.clone(),
        key_pem,
        ca_pem: None,
    };
    let client = TlsConfig {
        cert_pem: Vec::new(),
        key_pem: Vec::new(),
        ca_pem: Some(cert_pem),
    };
    (server, client)
}

async fn spawn_replica(
    instance: &str,
    server_tls: TlsConfig,
) -> lane_switchboards::mesh::MicroserviceHandle<StockMsg> {
    serve_microservice_tls(
        "inventory",
        instance,
        "127.0.0.1:0",
        InventoryReplica {
            instance: instance.into(),
            ledger: HashMap::new(),
        },
        server_tls,
    )
    .await
    .unwrap_or_else(|e| panic!("bind TLS replica {instance}: {e}"))
}

async fn settle() {
    tokio::time::sleep(Duration::from_millis(250)).await;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let (server_tls, client_tls) = demo_tls();

    println!("=== Consistency demo: 3 TLS inventory replicas (rf=3) ===\n");

    let r1 = spawn_replica("inv-1", server_tls.clone()).await;
    let r2 = spawn_replica("inv-2", server_tls.clone()).await;
    let r3 = spawn_replica("inv-3", server_tls).await;
    println!(
        "replicas listening: {}, {}, {}\n",
        r1.address(),
        r2.address(),
        r3.address()
    );

    let base_config = ConsistencyConfig {
        rf: 3,
        local_rf: 3,
        write_cl: WriteConsistency::Quorum,
        ack_timeout: Duration::from_secs(2),
        ..ConsistencyConfig::default()
    };

    let mut mesh = ServiceMesh::with_consistency(base_config);
    mesh.set_tls(Some(client_tls));
    mesh.register(r1.record.clone());
    mesh.register(r2.record.clone());
    mesh.register(r3.record.clone());

    let sku = "widget-42";
    let reserve = |qty| StockMsg {
        sku: sku.into(),
        qty,
    };

    println!("--- Problem: mesh.invoke() (ONE / hash-ring) ---");
    println!("Only the hash-selected replica should print a reservation.\n");
    mesh.invoke("inventory", &sku, reserve(10)).await?;
    settle().await;

    println!("--- Solution: invoke_consistent(QUORUM) — W=2 of rf=3 ---");
    println!("Expect three replicas to ack receipt (TLS fan-out).\n");
    mesh.invoke_consistent("inventory", &sku, reserve(5))
        .await?;
    settle().await;

    println!("--- Outage: stop inv-3 ---");
    println!("QUORUM write should still succeed on the two surviving replicas.\n");
    drop(r3);
    mesh.deregister("inventory", "inv-3");
    mesh.invoke_consistent("inventory", &sku, reserve(1))
        .await?;
    settle().await;

    println!("--- Stricter: write_cl=ALL with only 2 live replicas ---");
    mesh.set_consistency(ConsistencyConfig {
        write_cl: WriteConsistency::All,
        ..mesh.consistency().clone()
    });
    let err = mesh
        .invoke_consistent("inventory", &sku, reserve(99))
        .await
        .expect_err("ALL should fail with 2/3 replicas");
    match &err {
        ConsistencyError::NotEnoughReplicas { required, available } => {
            println!(
                "expected failure: NotEnoughReplicas required={required} available={available}\n"
            );
        }
        other => println!("expected failure: {other}\n"),
    }

    mesh.set_consistency(ConsistencyConfig {
        write_cl: WriteConsistency::Quorum,
        ..mesh.consistency().clone()
    });

    println!("=== Takeaways ===");
    println!("• invoke()       → fast, one replica; risky for replicated state.");
    println!("• QUORUM write   → W acks across replicas.");
    println!("• ALL write      → fails when any replica is missing.");
    println!("• See examples/consistency.md and docs/consistency.md.\n");

    Ok(())
}
