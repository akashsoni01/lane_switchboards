//! Single-node 1:1 chat routing latency (localhost TCP).
//!
//! Run: `cargo bench --bench messenger_routing`

use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use lane_switchboards::messenger::{
    auth::HmacAuthenticator, MessengerClient, MessengerServer, ServerConfig,
};
use tokio::runtime::Runtime;

const SECRET: &str = "bench-routing-secret";

fn rt() -> Runtime {
    Runtime::new().expect("tokio runtime")
}

fn boot() -> (String, HmacAuthenticator) {
    rt().block_on(async {
        let auth = HmacAuthenticator::new(SECRET);
        let server = MessengerServer::bind("127.0.0.1:0", Arc::new(auth.clone()), ServerConfig::default())
            .await
            .unwrap();
        (server.local_addr().to_string(), auth)
    })
}

fn routing_latency(c: &mut Criterion) {
    let (addr, auth) = boot();
    c.bench_function("messenger_routing/online_chat_server_ack", |b| {
        b.iter_custom(|iters| {
            rt().block_on(async {
                let mut total = Duration::ZERO;
                for n in 0..iters {
                    let (mut alice, _) = MessengerClient::connect(
                        &addr,
                        "alice",
                        "d1",
                        &auth.mint_token("alice", "d1"),
                        0,
                    )
                    .await
                    .unwrap();
                    let (mut bob, _) = MessengerClient::connect(
                        &addr,
                        "bob",
                        "d1",
                        &auth.mint_token("bob", "d1"),
                        0,
                    )
                    .await
                    .unwrap();

                    let mid = format!("bench-{n}");
                    let start = Instant::now();
                    let seq = alice
                        .send_chat(black_box("bob"), black_box(&mid), black_box(b"ping"))
                        .await
                        .expect("server ack");
                    black_box(seq);
                    total += start.elapsed();

                    bob.recv_until(|p| matches!(p, lane_switchboards::messenger::Packet::ChatMessage(_)))
                        .await
                        .ok();
                    alice.close().await.ok();
                    bob.close().await.ok();
                }
                total
            })
        });
    });
}

criterion_group!(benches, routing_latency);
criterion_main!(benches);
