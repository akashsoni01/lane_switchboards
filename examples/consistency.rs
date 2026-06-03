//! Real-world consistency demo: replicated inventory over TLS + tunable W levels.
//!
//! **Production topology (not started by this binary):** every service-to-service hop is
//! `app → local Envoy sidecar → remote Envoy sidecar → peer app`. Outbound clusters use an
//! Envoy **circuit breaker** (max connections, pending requests, consecutive 5xx) so a failing
//! replica is ejected before it absorbs the whole flash-sale load. This example runs the
//! **lane_switchboards** data plane in-process (TLS + quorum acks); in Kubernetes you point
//! `ServiceRecord::address` at the sidecar listener (e.g. `127.0.0.1:15001`) rather than the
//! app port directly.
//!
//! Scenario (flash-sale SKU `widget-42`):
//! 1. **Problem** — `invoke()` routes to one replica; other replicas never see the reservation.
//! 2. **Solution** — `invoke_consistent` with `QUORUM` fans out and waits for W=2 acks (rf=3).
//! 3. **Outage** — one replica stops; sidecar CB may open; QUORUM still succeeds on survivors; `ALL` fails.
//!
//! Run: `cargo run --example consistency --features tls`
//! See: `examples/consistency.md` and `docs/consistency.md`

use lane_switchboards::actor::{Actor, ActorProcessingErr};
use lane_switchboards::consistency::{
    ConsistencyConfig, ConsistencyError, WriteConsistency,
};
use lane_switchboards::mesh::{serve_microservice_tls, ServiceMesh};
use lane_switchboards::tls::{
    build_acceptor, build_connector, client_config_from_pem, server_config_from_pem,
};
use rcgen::{CertificateParams, DnType, KeyPair, SanType};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
enum StockMsg {
    Reserve { sku: String, qty: u32 },
}

/// Per-replica inventory ledger (application must replicate writes; mesh only routes + acks).
struct InventoryReplica {
    instance: String,
    ledger: HashMap<String, u32>,
}

#[async_trait::async_trait]
impl Actor<StockMsg> for InventoryReplica {
    async fn handle(&mut self, msg: StockMsg) -> Result<(), ActorProcessingErr> {
        let StockMsg::Reserve { sku, qty } = msg;
        *self.ledger.entry(sku.clone()).or_insert(0) += qty;
        let total = self.ledger.get(&sku).copied().unwrap_or(0);
        println!(
            "[inventory:{}] reserved {qty} of {sku} (local total={total})",
            self.instance
        );
        Ok(())
    }
}

fn write_localhost_pem(dir: &PathBuf) -> (PathBuf, PathBuf) {
    let mut params = CertificateParams::new(vec!["localhost".into()]).expect("cert params");
    params
        .distinguished_name
        .push(DnType::CommonName, "localhost");
    params.subject_alt_names = vec![
        SanType::DnsName("localhost".try_into().expect("dns san")),
        SanType::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST)),
    ];
    let key_pair = KeyPair::generate().expect("key pair");
    let cert = params.self_signed(&key_pair).expect("self-signed cert");

    let cert_path = dir.join("localhost.crt");
    let key_path = dir.join("localhost.key");
    File::create(&cert_path)
        .and_then(|mut f| f.write_all(cert.pem().as_bytes()))
        .expect("write cert");
    File::create(&key_path)
        .and_then(|mut f| f.write_all(key_pair.serialize_pem().as_bytes()))
        .expect("write key");
    (cert_path, key_path)
}

fn secure_remove_file(path: &Path) -> std::io::Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let len = std::fs::metadata(path)?.len();
    if len > 0 {
        let mut file = OpenOptions::new().write(true).open(path)?;
        file.write_all(&vec![0u8; len as usize])?;
        file.sync_all()?;
    }
    std::fs::remove_file(path)
}

fn remove_ephemeral_pem(cert_path: &Path, key_path: &Path, temp_dir: &Path) -> std::io::Result<()> {
    secure_remove_file(key_path)?;
    if cert_path.exists() {
        std::fs::remove_file(cert_path)?;
    }
    if temp_dir.exists() {
        let _ = std::fs::remove_dir(temp_dir);
    }
    Ok(())
}

async fn spawn_replica(
    instance: &str,
    acceptor: Arc<lane_switchboards::tls::TlsAcceptor>,
) -> lane_switchboards::mesh::MicroserviceHandle<StockMsg> {
    serve_microservice_tls(
        "inventory",
        instance,
        "127.0.0.1:0",
        InventoryReplica {
            instance: instance.into(),
            ledger: HashMap::new(),
        },
        acceptor,
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

    let temp = std::env::temp_dir().join("lane_switchboards_consistency_demo");
    std::fs::create_dir_all(&temp)?;
    let (cert_path, key_path) = write_localhost_pem(&temp);

    let acceptor = Arc::new(build_acceptor(server_config_from_pem(
        &cert_path,
        &key_path,
        None::<&str>,
    )?));
    let connector = Arc::new(build_connector(client_config_from_pem(
        Some(&cert_path),
        None::<&str>,
        None::<&str>,
    )?));

    println!("=== Consistency demo: 3 TLS inventory replicas (rf=3) ===");
    println!("Production: each pod = app + Envoy sidecar; every s2s call goes through Envoy.");
    println!("Envoy cluster circuit breaker ejects unhealthy hosts (see consistency.md).\n");

    let r1 = spawn_replica("inv-1", acceptor.clone()).await;
    let r2 = spawn_replica("inv-2", acceptor.clone()).await;
    let r3 = spawn_replica("inv-3", acceptor.clone()).await;
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
    mesh.set_tls_connector(Some(connector));
    mesh.register(r1.record.clone());
    mesh.register(r2.record.clone());
    mesh.register(r3.record.clone());

    let sku = "widget-42";
    let reserve = |qty| StockMsg::Reserve {
        sku: sku.into(),
        qty,
    };

    // --- Problem: single-replica routing (fire-and-forget semantics) ---
    println!("--- Problem: mesh.invoke() (ONE / hash-ring) ---");
    println!("Only the hash-selected replica should print a reservation.\n");
    mesh.invoke("inventory", &sku, reserve(10)).await?;
    settle().await;

    // --- Solution: quorum write fans out with acks ---
    println!("--- Solution: invoke_consistent(QUORUM) — W=2 of rf=3 ---");
    println!("Expect three replicas to ack receipt (encrypted TLS fan-out).\n");
    mesh.invoke_consistent("inventory", &sku, reserve(5))
        .await?;
    settle().await;

    // --- Outage: lose one replica; Envoy CB would eject inv-3; QUORUM still OK ---
    println!("--- Outage: stop inv-3 (Envoy would open circuit breaker on that host) ---");
    println!("QUORUM write should still succeed on the two surviving replicas.\n");
    drop(r3);
    mesh.deregister("inventory", "inv-3");
    mesh.invoke_consistent("inventory", &sku, reserve(1))
        .await?;
    settle().await;

    // --- Stricter level fails when a replica is down ---
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

    // Restore QUORUM for closing message
    mesh.set_consistency(ConsistencyConfig {
        write_cl: WriteConsistency::Quorum,
        ..mesh.consistency().clone()
    });

    println!("=== Takeaways ===");
    println!("• Envoy sidecar  → all s2s traffic; circuit breaker sheds load to bad replicas.");
    println!("• invoke()       → fast, one replica; risky for replicated state.");
    println!("• QUORUM write   → W+R>N style durability of *receipt* across replicas.");
    println!("• ALL write      → fails fast when any replica is missing (or CB + quorum).");
    println!("• Each replica keeps its own ledger — replicate data in your actor logic.");
    println!("• See examples/consistency.md (Envoy) and docs/consistency.md (W/R levels).\n");

    if std::env::var("KEEP_DEMO_PEM").is_err() {
        match remove_ephemeral_pem(&cert_path, &key_path, &temp) {
            Ok(()) => println!("removed ephemeral demo cert/key from {}", temp.display()),
            Err(e) => eprintln!("warning: failed to remove demo PEM: {e}"),
        }
    }

    Ok(())
}
