//! Two-node distributed messaging over TLS (gRPC).
//!
//! Run: `cargo run --example tls_distributed --features tls`
//! See: `examples/tls_distributed.md`

use lane_switchboards::actor::{spawn, Actor, ActorProcessingErr};
use lane_switchboards::config::{DistributedConfig, TlsConfig};
use lane_switchboards::distributed::{Node, RemoteActorRef};
use lane_switchboards::prost::Message;
use rcgen::generate_simple_self_signed;
use tokio::sync::mpsc;

#[derive(Clone, PartialEq, Message)]
struct PingMsg {
    #[prost(string, tag = "1")]
    text: String,
}

struct PingActor;

#[async_trait::async_trait]
impl Actor<PingMsg> for PingActor {
    async fn handle(&mut self, msg: PingMsg) -> Result<(), ActorProcessingErr> {
        println!("received remote ping (TLS): {}", msg.text);
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (server_tls, client_tls) = demo_tls();
    let mut config = DistributedConfig::default();
    config.tls = Some(server_tls);

    let node_b = Node::<PingMsg>::bind_on_current_runtime("node-b", "127.0.0.1:0", &config).await?;
    let addr_b = node_b.address().to_string();
    println!("TLS node listening on {addr_b}");

    let (tx_b, mut rx_b) = mpsc::channel(16);
    node_b.register("worker", tx_b).await;

    tokio::spawn(async move {
        let (actor, _) = spawn(PingActor, None).await.unwrap();
        while let Some(msg) = rx_b.recv().await {
            actor.send(msg).await.ok();
        }
    });

    let mut client_config = DistributedConfig::default();
    client_config.tls = Some(client_tls);
    let remote = RemoteActorRef::connect(&addr_b, "worker", &client_config);
    remote
        .send(PingMsg {
            text: "hello over TLS".into(),
        })
        .await?;

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    Ok(())
}
