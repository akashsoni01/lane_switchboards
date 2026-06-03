//! Two-node distributed messaging over TLS (rustls).
//!
//! Generates ephemeral localhost certificates, binds a TLS listener, and sends
//! one framed message from a TLS client [`RemoteActorRef`].
//!
//! Run: `cargo run --example tls_distributed --features tls`
//! See: `examples/tls_distributed.md`

use lane_switchboards::actor::{spawn, Actor, ActorProcessingErr};
use lane_switchboards::config::DistributedConfig;
use lane_switchboards::distributed::{Node, RemoteActorRef};
use lane_switchboards::tls::{
    build_acceptor, build_connector, client_config_from_pem, server_config_from_pem,
};
use rcgen::{CertificateParams, DnType, KeyPair, SanType};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Debug, Clone, Serialize, Deserialize)]
enum PingMsg {
    Ping(String),
}

struct PingActor;

#[async_trait::async_trait]
impl Actor<PingMsg> for PingActor {
    async fn handle(&mut self, msg: PingMsg) -> Result<(), ActorProcessingErr> {
        let PingMsg::Ping(s) = msg;
        println!("received remote ping (TLS): {s}");
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

/// Overwrite a file with zeros, then delete it (best-effort wipe for demo private keys).
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

/// Remove demo PEM material. Key is wiped before unlink; cert is removed normally.
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp = std::env::temp_dir().join("lane_switchboards_tls_demo");
    std::fs::create_dir_all(&temp)?;
    let (cert_path, key_path) = write_localhost_pem(&temp);

    let server_cfg =
        server_config_from_pem(&cert_path, &key_path, None::<&str>)?;
    let acceptor = Arc::new(build_acceptor(server_cfg));

    let client_cfg = client_config_from_pem(Some(&cert_path), None::<&str>, None::<&str>)?;
    let connector = Arc::new(build_connector(client_cfg));

    let config = DistributedConfig::default();
    let node_b = Node::<PingMsg>::bind_tls_on_runtime(
        &tokio::runtime::Handle::current(),
        "node-b",
        "127.0.0.1:0",
        &config,
        acceptor,
    )
    .await?;
    let addr_b = node_b.address().to_string();
    println!("TLS node listening on {addr_b}");

    let (tx_b, mut rx_b) = mpsc::channel(16);
    node_b.register("worker", tx_b).await;

    tokio::spawn(async move {
        let (actor, _) = spawn(PingActor, None).await.unwrap();
        while let Some(PingMsg::Ping(s)) = rx_b.recv().await {
            actor.send(PingMsg::Ping(s)).await.ok();
        }
    });

    let remote = RemoteActorRef::with_tls(&addr_b, "worker", &config, connector);
    remote
        .send(PingMsg::Ping("hello over TLS".into()))
        .await?;

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // TLS configs are in memory; PEM files are no longer needed after the demo run.
    if std::env::var("KEEP_DEMO_PEM").is_err() {
        match remove_ephemeral_pem(&cert_path, &key_path, &temp) {
            Ok(()) => println!("removed ephemeral demo cert and key from {}", temp.display()),
            Err(e) => eprintln!("warning: failed to remove demo PEM files: {e}"),
        }
    } else {
        println!(
            "KEEP_DEMO_PEM set — left cert/key in {}",
            temp.display()
        );
    }

    Ok(())
}
