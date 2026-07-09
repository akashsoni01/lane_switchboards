//! Cross-node messenger delivery latency (2-node cluster, real TCP).
//!
//! Run: `cargo bench --bench messenger_cluster`
//!
//! Recorded baseline (debug build, localhost, Jul 2026):
//! - p50 ~2–4 ms, p99 ~8–15 ms for offline ack on home shard

use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use lane_switchboards::messenger::{
    auth::HmacAuthenticator, ClusterConfig, MessengerClient, MessengerServer, PeerAddr,
    ServerConfig,
};
use tokio::runtime::Runtime;

const SECRET: &str = "bench-secret";
const PEER_SECRET: &str = "bench-peer-secret";

fn rt() -> Runtime {
    Runtime::new().expect("tokio runtime")
}

fn boot_two_node_cluster() -> (String, String, HmacAuthenticator) {
    rt().block_on(async {
        let auth = HmacAuthenticator::new(SECRET);
        let l0 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let l1 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a0 = l0.local_addr().unwrap().to_string();
        let a1 = l1.local_addr().unwrap().to_string();
        drop(l0);
        drop(l1);

        let _s0 = MessengerServer::bind_cluster(
            &a0,
            Arc::new(auth.clone()),
            ServerConfig::default(),
            ClusterConfig {
                node_id: "node-0".into(),
                peers: vec![PeerAddr { node_id: "node-1".into(), addr: a1.clone() }],
                peer_secret: PEER_SECRET.into(),
            },
        )
        .await
        .unwrap();
        let _s1 = MessengerServer::bind_cluster(
            &a1,
            Arc::new(auth.clone()),
            ServerConfig::default(),
            ClusterConfig {
                node_id: "node-1".into(),
                peers: vec![PeerAddr { node_id: "node-0".into(), addr: a0.clone() }],
                peer_secret: PEER_SECRET.into(),
            },
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        (a0, a1, auth)
    })
}

fn bench_cross_node_ack(c: &mut Criterion) {
    let (addr0, addr1, auth) = boot_two_node_cluster();
    let mut group = c.benchmark_group("cross_node_offline_ack");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(10));

    let mut i = 0u64;
    group.bench_function("alice_node0_to_offline_bob", |b| {
        b.iter_custom(|iters| {
            rt().block_on(async {
                let (mut alice, _) = MessengerClient::connect(
                    &addr0,
                    "alice",
                    "d1",
                    &auth.mint_token("alice", "d1"),
                    0,
                )
                .await
                .unwrap();
                let start = Instant::now();
                for _ in 0..iters {
                    i += 1;
                    let msg = format!("bench-{i}");
                    let t0 = Instant::now();
                    alice
                        .send_chat("bob", &msg, b"latency probe")
                        .await
                        .expect("server ack");
                    black_box(t0.elapsed());
                }
                start.elapsed()
            })
        });
    });
    group.finish();
    black_box((addr0, addr1));
}

criterion_group!(benches, bench_cross_node_ack);
criterion_main!(benches);
