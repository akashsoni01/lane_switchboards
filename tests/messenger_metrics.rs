//! Messenger metrics appear in the shared Prometheus export.

use std::sync::Arc;

use lane_switchboards::messenger::{
    auth::HmacAuthenticator, metrics, MessengerClient, MessengerServer, ServerConfig,
};
use lane_switchboards::metrics::render_prometheus_text;

const SECRET: &str = "metrics-secret";

#[tokio::test]
async fn messenger_metrics_exported() {
    metrics::init();
    let auth = HmacAuthenticator::new(SECRET);
    let server = MessengerServer::bind("127.0.0.1:0", Arc::new(auth.clone()), ServerConfig::default())
        .await
        .expect("bind");

    let addr = server.local_addr().to_string();
    let (mut alice, _) = MessengerClient::connect(
        &addr,
        "alice",
        "d1",
        &auth.mint_token("alice", "d1"),
        0,
    )
    .await
    .expect("login");

    alice.send_chat("bob", "m-1", b"hi").await.expect("send");
    assert_eq!(server.connected_sessions().await, 1);

    metrics::set_sessions(&server.node_id(), 1);
    metrics::set_peer_links(0);

    let body = render_prometheus_text().expect("render");
    assert!(body.contains("lane_messenger_sessions_connected"));
    assert!(body.contains("lane_messenger_peer_links_connected"));
}
