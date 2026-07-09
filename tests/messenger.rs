//! End-to-end tests for the FunXMPP-style messenger plane.
//!
//! Each test boots a real gateway on an ephemeral port and drives it with
//! the reference `MessengerClient` over real TCP sockets.

use std::sync::Arc;
use std::time::Duration;

use lane_switchboards::messenger::{
    wire, HmacAuthenticator, MessengerClient, MessengerServer, Packet, ServerConfig,
};

const SECRET: &str = "test-secret";

async fn boot() -> (MessengerServer, String, HmacAuthenticator) {
    boot_with(ServerConfig::default()).await
}

async fn boot_with(cfg: ServerConfig) -> (MessengerServer, String, HmacAuthenticator) {
    let auth = HmacAuthenticator::new(SECRET);
    let server = MessengerServer::bind("127.0.0.1:0", Arc::new(auth.clone()), cfg)
        .await
        .expect("bind gateway");
    let addr = server.local_addr().to_string();
    (server, addr, auth)
}

async fn login(
    addr: &str,
    auth: &HmacAuthenticator,
    user: &str,
    device: &str,
) -> (MessengerClient, lane_switchboards::messenger::client::LoginOutcome) {
    let token = auth.mint_token(user, device);
    MessengerClient::connect(addr, user, device, &token, 0)
        .await
        .expect("login")
}

// ---- Auth -------------------------------------------------------------------

#[tokio::test]
async fn login_with_valid_token_succeeds() {
    let (_server, addr, auth) = boot().await;
    let (client, outcome) = login(&addr, &auth, "akash", "phone-1").await;
    assert!(!outcome.session_id.is_empty());
    assert!(outcome.replayed.is_empty());
    client.close().await.ok();
}

#[tokio::test]
async fn login_with_bad_token_rejected() {
    let (_server, addr, _auth) = boot().await;
    match MessengerClient::connect(&addr, "akash", "phone-1", "deadbeef", 0).await {
        Err(err) => assert!(err.to_string().contains("invalid token"), "got: {err}"),
        Ok(_) => panic!("must reject bad token"),
    }
}

#[tokio::test]
async fn unauthenticated_chat_rejected() {
    use futures_util::{SinkExt, StreamExt};
    use lane_switchboards::messenger::FrameCodec;
    use tokio_util::codec::Framed;

    let (_server, addr, _auth) = boot().await;
    let socket = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let mut framed = Framed::new(socket, FrameCodec::default());
    framed
        .send(Packet::ChatMessage(wire::ChatMessage {
            message_id: "m-1".into(),
            from_user: "akash".into(),
            to_user: "john".into(),
            body: b"sneaky".to_vec(),
            sent_at: 0,
            seq: 0,
            media_id: String::new(),
        }))
        .await
        .unwrap();
    match framed.next().await {
        Some(Ok(Packet::Error(e))) => {
            assert_eq!(e.code, wire::ErrorCode::NotAuthenticated as i32);
        }
        other => panic!("expected NotAuthenticated error, got {other:?}"),
    }
}

#[tokio::test]
async fn same_device_reconnect_replaces_old_session() {
    let (_server, addr, auth) = boot().await;
    let (mut old, _) = login(&addr, &auth, "akash", "phone-1").await;
    let (mut new, _) = login(&addr, &auth, "akash", "phone-1").await;

    // Old session receives the typed Replaced error, then closes.
    let pkt = old
        .recv_until(|p| matches!(p, Packet::Error(_)))
        .await
        .expect("replaced notice");
    match pkt {
        Packet::Error(e) => assert_eq!(e.code, wire::ErrorCode::ReplacedByNewSession as i32),
        _ => unreachable!(),
    }
    new.ping().await.expect("new session alive");
}

// ---- 1:1 chat + ack ladder ----------------------------------------------------

#[tokio::test]
async fn online_delivery_with_full_tick_ladder() {
    let (_server, addr, auth) = boot().await;
    let (mut alice, _) = login(&addr, &auth, "alice", "d1").await;
    let (mut bob, _) = login(&addr, &auth, "bob", "d1").await;

    let seq = alice.send_chat("bob", "m-1", b"hello bob").await.expect("server ack");
    assert_eq!(seq, 1);

    // Bob receives it.
    let pkt = bob
        .recv_until(|p| matches!(p, Packet::ChatMessage(_)))
        .await
        .expect("delivery");
    let msg = match pkt {
        Packet::ChatMessage(m) => m,
        _ => unreachable!(),
    };
    assert_eq!(msg.from_user, "alice");
    assert_eq!(msg.body, b"hello bob");
    assert_eq!(msg.seq, 1);

    // Double tick then blue tick reach Alice.
    bob.ack_delivered("m-1").await.unwrap();
    let pkt = alice
        .recv_until(|p| matches!(p, Packet::DeliveredAck(a) if a.message_id == "m-1"))
        .await
        .expect("delivered ack");
    match pkt {
        Packet::DeliveredAck(a) => assert_eq!(a.from_user, "bob"),
        _ => unreachable!(),
    }

    bob.ack_read("m-1").await.unwrap();
    alice
        .recv_until(|p| matches!(p, Packet::ReadAck(a) if a.message_id == "m-1"))
        .await
        .expect("read ack");
}

#[tokio::test]
async fn duplicate_send_is_deduplicated() {
    let (server, addr, auth) = boot().await;
    let (mut alice, _) = login(&addr, &auth, "alice", "d1").await;

    let seq1 = alice.send_chat("bob", "m-dup", b"first").await.unwrap();
    let seq2 = alice.send_chat("bob", "m-dup", b"retry").await.unwrap();
    assert_eq!(seq1, 1);
    assert_eq!(seq2, 0, "duplicate acked with seq 0 sentinel");
    assert_eq!(server.pending_for("bob").await, 1, "stored exactly once");
}

// ---- Offline store + sync ------------------------------------------------------

#[tokio::test]
async fn offline_messages_replayed_in_order_on_login() {
    let (server, addr, auth) = boot().await;
    let (mut alice, _) = login(&addr, &auth, "alice", "d1").await;

    for i in 1..=5u32 {
        alice
            .send_chat("bob", &format!("m-{i}"), format!("msg {i}").as_bytes())
            .await
            .unwrap();
    }
    assert_eq!(server.pending_for("bob").await, 5);

    let (mut bob, outcome) = login(&addr, &auth, "bob", "d1").await;
    assert_eq!(outcome.replayed.len(), 5);
    let mut prev_seq = 0;
    for (i, pkt) in outcome.replayed.iter().enumerate() {
        let m = match pkt {
            Packet::ChatMessage(m) => m,
            other => panic!("expected chat, got {other:?}"),
        };
        assert_eq!(m.body, format!("msg {}", i + 1).as_bytes());
        assert!(m.seq > prev_seq, "gap-free ascending order");
        prev_seq = m.seq;
    }
    assert_eq!(outcome.latest_seq, prev_seq);

    // Acking clears the inbox.
    for i in 1..=5u32 {
        bob.ack_delivered(&format!("m-{i}")).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(server.pending_for("bob").await, 0);
}

#[tokio::test]
async fn resume_after_seq_skips_already_seen_messages() {
    let (_server, addr, auth) = boot().await;
    let (mut alice, _) = login(&addr, &auth, "alice", "d1").await;
    for i in 1..=4u32 {
        alice.send_chat("bob", &format!("m-{i}"), b"x").await.unwrap();
    }

    // Bob resumes claiming he has seen seq <= 2.
    let token = auth.mint_token("bob", "d1");
    let (_bob, outcome) = MessengerClient::connect(&addr, "bob", "d1", &token, 2)
        .await
        .expect("resume login");
    assert_eq!(outcome.replayed.len(), 2, "only the gap replays");
    let seqs: Vec<u64> = outcome
        .replayed
        .iter()
        .map(|p| match p {
            Packet::ChatMessage(m) => m.seq,
            _ => panic!(),
        })
        .collect();
    assert_eq!(seqs, vec![3, 4]);
}

// ---- Presence -------------------------------------------------------------------

#[tokio::test]
async fn presence_broadcast_on_login_and_disconnect() {
    let (server, addr, auth) = boot().await;
    let (mut alice, _) = login(&addr, &auth, "alice", "d1").await;

    let (bob, _) = login(&addr, &auth, "bob", "d1").await;
    let pkt = alice
        .recv_until(|p| matches!(p, Packet::Presence(pr) if pr.user_id == "bob"))
        .await
        .expect("available notice");
    match pkt {
        Packet::Presence(pr) => assert_eq!(pr.kind, wire::PresenceKind::Available as i32),
        _ => unreachable!(),
    }
    assert!(server.is_online("bob").await);

    bob.close().await.ok();
    let pkt = alice
        .recv_until(|p| {
            matches!(p, Packet::Presence(pr)
                if pr.user_id == "bob" && pr.kind == wire::PresenceKind::Unavailable as i32)
        })
        .await
        .expect("unavailable notice");
    match pkt {
        Packet::Presence(pr) => assert!(pr.last_seen > 0, "last_seen recorded"),
        _ => unreachable!(),
    }
    assert!(!server.is_online("bob").await);
}

// ---- Heartbeats -----------------------------------------------------------------

#[tokio::test]
async fn ping_pong_round_trip() {
    let (_server, addr, auth) = boot().await;
    let (mut c, _) = login(&addr, &auth, "akash", "d1").await;
    for _ in 0..3 {
        c.ping().await.expect("pong");
    }
}

#[tokio::test]
async fn idle_session_swept_after_timeout() {
    let cfg = ServerConfig {
        idle_timeout: Duration::from_millis(300),
        ..Default::default()
    };
    let (server, addr, auth) = boot_with(cfg).await;
    let (_c, _) = login(&addr, &auth, "akash", "d1").await;
    assert!(server.is_online("akash").await);
    // No pings: the 5s sweep interval dominates; poll until offline.
    for _ in 0..120 {
        if !server.is_online("akash").await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("idle session was not swept");
}

// ---- Media / bulk data (PDF) -------------------------------------------------------

#[tokio::test]
async fn upload_and_fetch_pdf_blob_round_trip() {
    let (_server, addr, auth) = boot().await;
    let (mut alice, _) = login(&addr, &auth, "alice", "d1").await;
    let (mut bob, _) = login(&addr, &auth, "bob", "d1").await;

    // 300 KiB pseudo-PDF spanning multiple 64 KiB chunks.
    let pdf: Vec<u8> = (0..300 * 1024).map(|i| (i % 251) as u8).collect();
    let stored = alice
        .upload_media("pdf-1", "report.pdf", "application/pdf", &pdf)
        .await
        .expect("upload");
    assert_eq!(stored, pdf.len() as u64);

    // Reference it from a chat message.
    alice
        .send_chat_with_media("bob", "m-pdf", b"see attached", "pdf-1")
        .await
        .unwrap();
    let pkt = bob
        .recv_until(|p| matches!(p, Packet::ChatMessage(_)))
        .await
        .unwrap();
    let media_id = match pkt {
        Packet::ChatMessage(m) => m.media_id,
        _ => unreachable!(),
    };
    assert_eq!(media_id, "pdf-1");

    let blob = bob.fetch_media(&media_id).await.expect("fetch");
    assert_eq!(blob.file_name, "report.pdf");
    assert_eq!(blob.mime_type, "application/pdf");
    assert_eq!(blob.data, pdf, "byte-identical after chunked round trip");
}

#[tokio::test]
async fn oversized_media_rejected() {
    let cfg = ServerConfig { max_media_bytes: 1024, ..Default::default() };
    let (_server, addr, auth) = boot_with(cfg).await;
    let (mut c, _) = login(&addr, &auth, "alice", "d1").await;
    let big = vec![0u8; 4096];
    let err = c
        .upload_media("big-1", "big.bin", "application/octet-stream", &big)
        .await
        .expect_err("must reject");
    assert!(err.to_string().contains("max"), "got: {err}");
}

#[tokio::test]
async fn corrupted_upload_fails_sha_check() {
    use futures_util::SinkExt;
    // Drive the raw protocol to send a wrong digest.
    let (_server, addr, auth) = boot().await;
    let (mut c, _) = login(&addr, &auth, "alice", "d1").await;

    // MediaStart with bogus sha256 via the raw framed API is not exposed on
    // the client, so declare digest mismatch by uploading data that differs.
    // Use the internal packet path through send/recv:
    let framed = &mut c;
    let _ = framed; // client keeps framing private; emulate with a raw socket.

    let socket = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let mut raw = tokio_util::codec::Framed::new(
        socket,
        lane_switchboards::messenger::FrameCodec::default(),
    );
    let token = auth.mint_token("eve", "d1");
    raw.send(Packet::Login(wire::Login {
        user_id: "eve".into(),
        device_id: "d1".into(),
        auth_token: token,
        client_version: "t".into(),
        resume_after_seq: 0,
    }))
    .await
    .unwrap();
    use futures_util::StreamExt;
    // Drain LoginAck + SyncComplete.
    loop {
        match raw.next().await.unwrap().unwrap() {
            Packet::SyncComplete(_) => break,
            _ => continue,
        }
    }
    raw.send(Packet::MediaStart(wire::MediaStart {
        media_id: "bad-1".into(),
        file_name: "x.bin".into(),
        mime_type: "application/octet-stream".into(),
        total_size: 3,
        sha256: "00".repeat(32),
    }))
    .await
    .unwrap();
    loop {
        if let Packet::MediaAck(a) = raw.next().await.unwrap().unwrap() {
            assert!(a.ok);
            break;
        }
    }
    raw.send(Packet::MediaChunk(wire::MediaChunk {
        media_id: "bad-1".into(),
        offset: 0,
        data: vec![1, 2, 3],
        last: true,
    }))
    .await
    .unwrap();
    loop {
        if let Packet::MediaAck(a) = raw.next().await.unwrap().unwrap() {
            assert!(!a.ok, "sha mismatch must fail the upload");
            assert!(a.error.contains("sha256"));
            break;
        }
    }
}

// ---- Groups --------------------------------------------------------------------

#[tokio::test]
async fn group_create_add_and_fanout() {
    let (_server, addr, auth) = boot().await;
    let (mut alice, _) = login(&addr, &auth, "alice", "d1").await;
    let (mut bob, _) = login(&addr, &auth, "bob", "d1").await;
    let (mut carol, _) = login(&addr, &auth, "carol", "d1").await;

    let v1 = alice.create_group("g-1").await.expect("create");
    assert_eq!(v1, 1);
    let v2 = alice.add_member("g-1", "bob").await.expect("add bob");
    let v3 = alice.add_member("g-1", "carol").await.expect("add carol");
    assert!(v3 > v2, "membership version is monotonic");

    alice.send_group("g-1", "gm-1", b"hi group").await.expect("group send");

    for member in [&mut bob, &mut carol] {
        let pkt = member
            .recv_until(|p| matches!(p, Packet::GroupMessage(_)))
            .await
            .expect("fan-out");
        match pkt {
            Packet::GroupMessage(m) => {
                assert_eq!(m.group_id, "g-1");
                assert_eq!(m.from_user, "alice");
                assert_eq!(m.body, b"hi group");
                assert!(m.seq > 0, "per-member inbox seq assigned");
            }
            _ => unreachable!(),
        }
    }
}

#[tokio::test]
async fn non_member_cannot_send_and_non_admin_cannot_add() {
    let (_server, addr, auth) = boot().await;
    let (mut alice, _) = login(&addr, &auth, "alice", "d1").await;
    let (mut mallory, _) = login(&addr, &auth, "mallory", "d1").await;

    alice.create_group("g-2").await.unwrap();

    let err = mallory
        .send_group("g-2", "gm-x", b"let me in")
        .await
        .expect_err("non-member rejected");
    // The server answers with a typed protocol error, surfaced via recv path;
    // the client sees the connection-level error frame.
    let _ = err;

    // Non-admin membership mutation rejected.
    alice.add_member("g-2", "bob").await.unwrap();
    let (mut bob, _) = login(&addr, &auth, "bob", "d1").await;
    let err = bob.add_member("g-2", "mallory").await.expect_err("non-admin add rejected");
    assert!(err.to_string().contains("admin"), "got: {err}");
}

#[tokio::test]
async fn offline_group_member_gets_message_on_login() {
    let (server, addr, auth) = boot().await;
    let (mut alice, _) = login(&addr, &auth, "alice", "d1").await;
    alice.create_group("g-3").await.unwrap();
    alice.add_member("g-3", "bob").await.unwrap();

    // Bob is offline for the send.
    alice.send_group("g-3", "gm-off", b"while away").await.unwrap();
    assert_eq!(server.pending_for("bob").await, 1);

    let (_bob, outcome) = login(&addr, &auth, "bob", "d1").await;
    assert_eq!(outcome.replayed.len(), 1);
    match &outcome.replayed[0] {
        Packet::GroupMessage(m) => assert_eq!(m.body, b"while away"),
        other => panic!("expected group message, got {other:?}"),
    }
}

// ---- Hardening: shutdown, rate limit, auth backoff -------------------------------

#[tokio::test]
async fn graceful_shutdown_closes_sessions_and_stops_accepting() {
    let (server, addr, auth) = boot().await;
    let (mut c, _) = login(&addr, &auth, "akash", "d1").await;
    c.ping().await.unwrap();

    server.shutdown().await;

    // Existing session observes EOF (recv errors with Closed).
    let err = c.recv().await.expect_err("session must close");
    assert!(matches!(err, lane_switchboards::messenger::MessengerError::Closed));

    // New connections are refused (listener is gone).
    let refused = tokio::time::timeout(
        Duration::from_secs(2),
        tokio::net::TcpStream::connect(&addr),
    )
    .await;
    match refused {
        Ok(Err(_)) | Err(_) => {} // connection refused or timed out — both fine
        Ok(Ok(_)) => panic!("gateway must stop accepting after shutdown"),
    }
}

#[tokio::test]
async fn per_ip_connection_rate_limit_drops_excess() {
    let cfg = ServerConfig { max_conns_per_ip_per_min: 3, ..Default::default() };
    let (_server, addr, auth) = boot_with(cfg).await;

    // First 3 logins succeed.
    let mut kept = Vec::new();
    for i in 0..3 {
        let user = format!("u{i}");
        let (c, _) = login(&addr, &auth, &user, "d1").await;
        kept.push(c);
    }
    // Fourth connection is dropped before login.
    let token = auth.mint_token("u3", "d1");
    let res = MessengerClient::connect(&addr, "u3", "d1", &token, 0).await;
    assert!(res.is_err(), "over-limit connection must fail");
}

#[tokio::test]
async fn repeated_auth_failures_backoff_grows() {
    let cfg = ServerConfig {
        auth_backoff_base: Duration::from_millis(200),
        auth_backoff_max: Duration::from_secs(2),
        ..Default::default()
    };
    let (_server, addr, _auth) = boot_with(cfg).await;

    // 1st failure: ~200ms, 2nd: ~400ms — measure the second is slower.
    let t1 = std::time::Instant::now();
    let _ = MessengerClient::connect(&addr, "victim", "d1", "bad", 0).await;
    let d1 = t1.elapsed();
    let t2 = std::time::Instant::now();
    let _ = MessengerClient::connect(&addr, "victim", "d1", "bad", 0).await;
    let d2 = t2.elapsed();
    assert!(d1 >= Duration::from_millis(180), "first failure delayed: {d1:?}");
    assert!(d2 > d1, "backoff must grow: {d1:?} -> {d2:?}");
}

// ---- Durable inboxes (crash recovery) ------------------------------------------------

#[tokio::test]
async fn acked_message_survives_gateway_restart() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = || ServerConfig {
        durable_dir: Some(dir.path().to_path_buf()),
        ..Default::default()
    };

    // Boot, send to an offline user, "crash" (drop the server).
    {
        let auth = HmacAuthenticator::new(SECRET);
        let server = MessengerServer::bind("127.0.0.1:0", Arc::new(auth.clone()), cfg())
            .await
            .unwrap();
        let addr = server.local_addr().to_string();
        let (mut alice, _) = login(&addr, &auth, "alice", "d1").await;
        let seq = alice.send_chat("bob", "durable-1", b"survives crashes").await.expect("ack");
        assert_eq!(seq, 1);
        assert_eq!(server.pending_for("bob").await, 1);
        // Server dropped here without graceful shutdown.
    }

    // Reboot from the same directory: the message must still be there.
    let auth = HmacAuthenticator::new(SECRET);
    let server = MessengerServer::bind("127.0.0.1:0", Arc::new(auth.clone()), cfg())
        .await
        .unwrap();
    let addr = server.local_addr().to_string();
    assert_eq!(server.pending_for("bob").await, 1, "journal replay restores inbox");

    let (mut bob, outcome) = login(&addr, &auth, "bob", "d1").await;
    assert_eq!(outcome.replayed.len(), 1);
    match &outcome.replayed[0] {
        Packet::ChatMessage(m) => {
            assert_eq!(m.message_id, "durable-1");
            assert_eq!(m.body, b"survives crashes");
            assert_eq!(m.seq, 1);
        }
        other => panic!("expected chat, got {other:?}"),
    }

    // Ack, restart again: inbox is empty but the seq high-water mark holds.
    bob.ack_delivered("durable-1").await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(bob);
    drop(server);

    let server = MessengerServer::bind("127.0.0.1:0", Arc::new(auth.clone()), cfg())
        .await
        .unwrap();
    let addr = server.local_addr().to_string();
    assert_eq!(server.pending_for("bob").await, 0, "tombstone survived restart");

    // New message must continue the sequence, not restart at 1.
    let (mut alice, _) = login(&addr, &auth, "alice", "d1").await;
    let seq = alice.send_chat("bob", "durable-2", b"after restart").await.unwrap();
    assert_eq!(seq, 2, "seq high-water mark survives compaction");
}

#[tokio::test]
async fn dedup_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = || ServerConfig {
        durable_dir: Some(dir.path().to_path_buf()),
        ..Default::default()
    };
    let auth = HmacAuthenticator::new(SECRET);
    {
        let server = MessengerServer::bind("127.0.0.1:0", Arc::new(auth.clone()), cfg())
            .await
            .unwrap();
        let addr = server.local_addr().to_string();
        let (mut alice, _) = login(&addr, &auth, "alice", "d1").await;
        alice.send_chat("bob", "dup-1", b"first").await.unwrap();
    }
    let server = MessengerServer::bind("127.0.0.1:0", Arc::new(auth.clone()), cfg())
        .await
        .unwrap();
    let addr = server.local_addr().to_string();
    let (mut alice, _) = login(&addr, &auth, "alice", "d1").await;
    // Retry of the same message_id after restart must be deduplicated.
    let seq = alice.send_chat("bob", "dup-1", b"retry").await.unwrap();
    assert_eq!(seq, 0, "duplicate sentinel");
    assert_eq!(server.pending_for("bob").await, 1, "still exactly one copy");
}

// ---- Multi-node cluster -------------------------------------------------------------

mod cluster_tests {
    use super::*;
    use lane_switchboards::messenger::{ClusterConfig, PeerAddr};

    const PEER_SECRET: &str = "peer-secret";

    /// Boot a full-mesh cluster of `n` gateways on ephemeral ports and wait
    /// for all peer links to establish.
    async fn boot_cluster(n: usize) -> (Vec<MessengerServer>, Vec<String>, HmacAuthenticator) {
        let auth = HmacAuthenticator::new(SECRET);
        // Reserve addresses first so every node knows all peers up front.
        let mut listeners = Vec::new();
        for _ in 0..n {
            // Bind to grab a free port, record it, release the socket; the
            // gateway rebinds the same port right after (no TIME_WAIT on
            // listening sockets, so this is race-free enough for tests).
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            listeners.push(l.local_addr().unwrap().to_string());
        }

        let mut servers = Vec::new();
        for (i, addr) in listeners.iter().enumerate() {
            let peers: Vec<PeerAddr> = listeners
                .iter()
                .enumerate()
                .filter(|(j, _)| *j != i)
                .map(|(j, a)| PeerAddr { node_id: format!("node-{j}"), addr: a.clone() })
                .collect();
            let server = MessengerServer::bind_cluster(
                addr,
                Arc::new(auth.clone()),
                ServerConfig::default(),
                ClusterConfig {
                    node_id: format!("node-{i}"),
                    peers,
                    peer_secret: PEER_SECRET.into(),
                },
            )
            .await
            .expect("bind cluster node");
            servers.push(server);
        }
        let addrs: Vec<String> = servers.iter().map(|s| s.local_addr().to_string()).collect();
        // Give the reconnecting peer links a moment to establish.
        tokio::time::sleep(Duration::from_millis(300)).await;
        (servers, addrs, auth)
    }

    /// Find which user is homed on which node by probing: send to the user
    /// and check where the inbox landed.
    async fn home_index(servers: &[MessengerServer], user: &str) -> Option<usize> {
        for (i, s) in servers.iter().enumerate() {
            if s.pending_for(user).await > 0 {
                return Some(i);
            }
        }
        None
    }

    #[tokio::test]
    async fn cross_node_online_delivery_with_acks() {
        let (_servers, addrs, auth) = boot_cluster(3).await;

        // Alice on node 0, Bob on node 2 — different gateways.
        let (mut alice, _) = MessengerClient::connect(
            &addrs[0], "alice", "d1", &auth.mint_token("alice", "d1"), 0,
        )
        .await
        .expect("alice login");
        let (mut bob, _) = MessengerClient::connect(
            &addrs[2], "bob", "d1", &auth.mint_token("bob", "d1"), 0,
        )
        .await
        .expect("bob login");
        // Let presence broadcasts propagate.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let seq = alice.send_chat("bob", "xm-1", b"across the cluster").await.expect("ack");
        assert!(seq >= 1);

        let pkt = bob
            .recv_until(|p| matches!(p, Packet::ChatMessage(_)))
            .await
            .expect("cross-node delivery");
        match pkt {
            Packet::ChatMessage(m) => {
                assert_eq!(m.from_user, "alice");
                assert_eq!(m.body, b"across the cluster");
            }
            _ => unreachable!(),
        }

        // Delivered ack crosses back to Alice's gateway.
        bob.ack_delivered("xm-1").await.unwrap();
        alice
            .recv_until(|p| matches!(p, Packet::DeliveredAck(a) if a.message_id == "xm-1"))
            .await
            .expect("cross-node delivered ack");
    }

    #[tokio::test]
    async fn offline_message_replays_across_nodes() {
        let (servers, addrs, auth) = boot_cluster(3).await;

        // Alice on node 1 messages Bob who is offline everywhere.
        let (mut alice, _) = MessengerClient::connect(
            &addrs[1], "alice", "d1", &auth.mint_token("alice", "d1"), 0,
        )
        .await
        .unwrap();
        let seq = alice.send_chat("bob", "xm-off", b"catch up later").await.expect("ack");
        assert_eq!(seq, 1);

        // The message is persisted on exactly one node (Bob's home shard).
        let home = home_index(&servers, "bob").await.expect("stored somewhere");
        let total: usize = {
            let mut t = 0;
            for s in &servers {
                t += s.pending_for("bob").await;
            }
            t
        };
        assert_eq!(total, 1, "exactly one copy cluster-wide");

        // Bob logs in on a *different* node than his home shard.
        let login_node = (home + 1) % servers.len();
        let (mut bob, outcome) = MessengerClient::connect(
            &addrs[login_node], "bob", "d1", &auth.mint_token("bob", "d1"), 0,
        )
        .await
        .expect("bob login on non-home node");

        // Replay arrives via the peer mesh: either in the login outcome or
        // shortly after (remote sync completes asynchronously).
        let got = if outcome
            .replayed
            .iter()
            .any(|p| matches!(p, Packet::ChatMessage(m) if m.message_id == "xm-off"))
        {
            true
        } else {
            matches!(
                bob.recv_until(|p| matches!(p, Packet::ChatMessage(m) if m.message_id == "xm-off"))
                    .await,
                Ok(_)
            )
        };
        assert!(got, "offline message must replay across nodes");
    }

    #[tokio::test]
    async fn group_chat_spans_nodes() {
        let (_servers, addrs, auth) = boot_cluster(3).await;

        let (mut alice, _) = MessengerClient::connect(
            &addrs[0], "alice", "d1", &auth.mint_token("alice", "d1"), 0,
        )
        .await
        .unwrap();
        let (mut bob, _) = MessengerClient::connect(
            &addrs[1], "bob", "d1", &auth.mint_token("bob", "d1"), 0,
        )
        .await
        .unwrap();
        let (mut carol, _) = MessengerClient::connect(
            &addrs[2], "carol", "d1", &auth.mint_token("carol", "d1"), 0,
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;

        alice.create_group("xg-1").await.expect("create");
        alice.add_member("xg-1", "bob").await.expect("add bob");
        alice.add_member("xg-1", "carol").await.expect("add carol");
        alice.send_group("xg-1", "xgm-1", b"cluster group hello").await.expect("group send");

        for (name, c) in [("bob", &mut bob), ("carol", &mut carol)] {
            let pkt = c
                .recv_until(|p| matches!(p, Packet::GroupMessage(gm) if gm.message_id == "xgm-1"))
                .await
                .unwrap_or_else(|e| panic!("{name} missed group message: {e}"));
            match pkt {
                Packet::GroupMessage(gm) => assert_eq!(gm.body, b"cluster group hello"),
                _ => unreachable!(),
            }
        }
    }

    #[tokio::test]
    async fn presence_propagates_across_nodes() {
        let (_servers, addrs, auth) = boot_cluster(2).await;

        let (mut alice, _) = MessengerClient::connect(
            &addrs[0], "alice", "d1", &auth.mint_token("alice", "d1"), 0,
        )
        .await
        .unwrap();

        // Bob logs in on the other node; Alice sees him come online.
        let (bob, _) = MessengerClient::connect(
            &addrs[1], "bob", "d1", &auth.mint_token("bob", "d1"), 0,
        )
        .await
        .unwrap();
        alice
            .recv_until(|p| {
                matches!(p, Packet::Presence(pr)
                    if pr.user_id == "bob" && pr.kind == wire::PresenceKind::Available as i32)
            })
            .await
            .expect("cross-node available");

        bob.close().await.ok();
        alice
            .recv_until(|p| {
                matches!(p, Packet::Presence(pr)
                    if pr.user_id == "bob" && pr.kind == wire::PresenceKind::Unavailable as i32)
            })
            .await
            .expect("cross-node unavailable");
    }

    #[tokio::test]
    async fn dynamic_peer_join_rebalances_and_replays() {
        let (servers, addrs, auth) = boot_cluster(2).await;

        let (mut alice, _) = MessengerClient::connect(
            &addrs[0], "alice", "d1", &auth.mint_token("alice", "d1"), 0,
        )
        .await
        .unwrap();
        alice.send_chat("bob", "join-1", b"before third node").await.expect("ack");

        let home_before = home_index(&servers, "bob").await.expect("stored on a home shard");
        assert_eq!(
            servers[home_before].pending_for("bob").await,
            1,
            "exactly one copy before join"
        );

        let node2_addr = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap()
            .local_addr()
            .unwrap()
            .to_string();
        let node2 = MessengerServer::bind_cluster(
            &node2_addr,
            Arc::new(auth.clone()),
            ServerConfig::default(),
            ClusterConfig {
                node_id: "node-2".into(),
                peers: vec![
                    PeerAddr { node_id: "node-0".into(), addr: addrs[0].clone() },
                    PeerAddr { node_id: "node-1".into(), addr: addrs[1].clone() },
                ],
                peer_secret: PEER_SECRET.into(),
            },
        )
        .await
        .expect("bind third node");

        servers[0]
            .add_peer(PeerAddr { node_id: "node-2".into(), addr: node2.local_addr().to_string() })
            .await
            .expect("node-0 adds node-2");
        servers[1]
            .add_peer(PeerAddr { node_id: "node-2".into(), addr: node2.local_addr().to_string() })
            .await
            .expect("node-1 adds node-2");

        assert_eq!(node2.cluster_size(), 3);
        tokio::time::sleep(Duration::from_millis(400)).await;

        let mut total = 0;
        for s in servers.iter().chain(std::iter::once(&node2)) {
            total += s.pending_for("bob").await;
        }
        assert_eq!(total, 1, "exactly one copy after rebalance");

        let (mut bob, outcome) = MessengerClient::connect(
            &node2.local_addr().to_string(),
            "bob",
            "d1",
            &auth.mint_token("bob", "d1"),
            0,
        )
        .await
        .expect("bob login on new node");

        let got = if outcome
            .replayed
            .iter()
            .any(|p| matches!(p, Packet::ChatMessage(m) if m.message_id == "join-1"))
        {
            true
        } else {
            matches!(
                bob.recv_until(|p| matches!(p, Packet::ChatMessage(m) if m.message_id == "join-1"))
                    .await,
                Ok(_)
            )
        };
        assert!(got, "offline message survives join rebalance");
    }

    #[tokio::test]
    async fn cross_node_media_fetch() {
        let (_servers, addrs, auth) = boot_cluster(2).await;
        let pdf = b"%PDF-1.4 cluster media";

        let (mut alice, _) = MessengerClient::connect(
            &addrs[0], "alice", "d1", &auth.mint_token("alice", "d1"), 0,
        )
        .await
        .unwrap();
        alice
            .upload_media("pdf-x", "doc.pdf", "application/pdf", pdf)
            .await
            .expect("upload on node 0");

        tokio::time::sleep(Duration::from_millis(300)).await;

        let (mut bob, _) = MessengerClient::connect(
            &addrs[1], "bob", "d1", &auth.mint_token("bob", "d1"), 0,
        )
        .await
        .unwrap();
        let dl = bob.fetch_media("pdf-x").await.expect("fetch from node 1");
        assert_eq!(dl.data, pdf);
        assert_eq!(dl.mime_type, "application/pdf");
    }

    #[tokio::test]
    async fn node_failure_during_traffic() {
        let (mut servers, addrs, auth) = boot_cluster(3).await;

        let (mut alice, _) = MessengerClient::connect(
            &addrs[0], "alice", "d1", &auth.mint_token("alice", "d1"), 0,
        )
        .await
        .unwrap();

        alice.send_chat("bob", "chaos-0", b"x").await.expect("seed");
        let home_idx = home_index(&servers, "bob").await.expect("bob homed");
        let victim = (home_idx + 1) % servers.len();

        for i in 1..10 {
            let mid = format!("chaos-{i}");
            alice.send_chat("bob", &mid, b"x").await.expect("send");
            if i == 5 {
                let doomed = servers.remove(victim);
                let victim_id = if victim == 0 {
                    "node-0"
                } else if victim == 1 {
                    "node-1"
                } else {
                    "node-2"
                };
                tokio::spawn(async move { doomed.shutdown().await });
                tokio::time::sleep(Duration::from_millis(300)).await;
                for s in &servers {
                    s.remove_peer(victim_id).await.expect("drop failed node");
                }
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
        }

        let login_addr = &addrs[(home_idx + 2) % addrs.len()];
        let (mut bob, outcome) = MessengerClient::connect(
            login_addr, "bob", "d1", &auth.mint_token("bob", "d1"), 0,
        )
        .await
        .expect("bob login after node loss");

        for i in 0..10 {
            let mid = format!("chaos-{i}");
            let got = outcome.replayed.iter().any(|p| {
                matches!(p, Packet::ChatMessage(m) if m.message_id == mid)
            }) || matches!(
                bob.recv_until(|p| matches!(p, Packet::ChatMessage(m) if m.message_id == mid))
                    .await,
                Ok(_)
            );
            assert!(got, "missing {mid} after non-home node failure");
        }
    }

    #[tokio::test]
    async fn soak_short_burst() {
        let (_servers, addrs, auth) = boot_cluster(2).await;
        let (mut alice, _) = MessengerClient::connect(
            &addrs[0], "alice", "d1", &auth.mint_token("alice", "d1"), 0,
        )
        .await
        .unwrap();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut n = 0u64;
        while tokio::time::Instant::now() < deadline {
            alice
                .send_chat("bob", &format!("soak-{n}"), b"z")
                .await
                .expect("send under load");
            n += 1;
        }
        assert!(n >= 20, "expected sustained throughput, got {n} msgs");
    }

    #[tokio::test]
    #[ignore = "manual soak: cargo test soak_sustained -- --ignored"]
    async fn soak_sustained() {
        let (_servers, addrs, auth) = boot_cluster(2).await;
        let (mut alice, _) = MessengerClient::connect(
            &addrs[0], "alice", "d1", &auth.mint_token("alice", "d1"), 0,
        )
        .await
        .unwrap();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let mut n = 0u64;
        while tokio::time::Instant::now() < deadline {
            alice
                .send_chat("bob", &format!("long-{n}"), b"z")
                .await
                .expect("send");
            n += 1;
            if n % 100 == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        assert!(n >= 500);
    }
}

// ---- E2EE (Phase 10) -------------------------------------------------------------

mod e2ee_tests {
    use super::*;
    use lane_switchboards::messenger::E2eeDevice;

    #[tokio::test]
    async fn e2ee_chat_single_node_server_never_sees_plaintext() {
        let (_server, addr, auth) = boot().await;
        let (mut alice, _) = login(&addr, &auth, "alice", "d1").await;
        let (mut bob, _) = login(&addr, &auth, "bob", "d1").await;

        let mut alice_e2ee = E2eeDevice::generate();
        let mut bob_e2ee = E2eeDevice::generate();

        alice.publish_e2ee_device("d1", &mut alice_e2ee, 5).await.unwrap();
        bob.publish_e2ee_device("d1", &mut bob_e2ee, 5).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        alice.establish_e2ee_session("bob", &mut alice_e2ee).await.unwrap();

        let secret = b"plausible deniability text";
        alice
            .send_encrypted_chat(&mut alice_e2ee, "bob", "e2ee-1", secret)
            .await
            .unwrap();

        let pkt = bob
            .recv_until(|p| matches!(p, Packet::ChatMessage(_)))
            .await
            .unwrap();
        let body = match pkt {
            Packet::ChatMessage(m) => {
                assert!(E2eeDevice::is_encrypted_body(&m.body));
                assert!(!E2eeDevice::body_contains_substring(&m.body, secret));
                m.body
            }
            _ => unreachable!(),
        };

        let plain = MessengerClient::decrypt_chat(&mut bob_e2ee, "alice", &body).unwrap();
        assert_eq!(plain, secret);
    }

    #[tokio::test]
    async fn e2ee_chat_cross_node_cluster() {
        use lane_switchboards::messenger::{ClusterConfig, E2eeDevice, PeerAddr};

        const PEER_SECRET: &str = "e2ee-peer-secret";
        let auth = HmacAuthenticator::new(SECRET);
        let mut listeners = Vec::new();
        for _ in 0..2 {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            listeners.push(l.local_addr().unwrap().to_string());
        }
        let mut servers = Vec::new();
        for (i, addr) in listeners.iter().enumerate() {
            let peers: Vec<PeerAddr> = listeners
                .iter()
                .enumerate()
                .filter(|(j, _)| *j != i)
                .map(|(j, a)| PeerAddr { node_id: format!("node-{j}"), addr: a.clone() })
                .collect();
            servers.push(
                MessengerServer::bind_cluster(
                    addr,
                    Arc::new(auth.clone()),
                    ServerConfig::default(),
                    ClusterConfig {
                        node_id: format!("node-{i}"),
                        peers,
                        peer_secret: PEER_SECRET.into(),
                    },
                )
                .await
                .unwrap(),
            );
        }
        let addrs: Vec<String> = servers.iter().map(|s| s.local_addr().to_string()).collect();
        tokio::time::sleep(Duration::from_millis(300)).await;

        let (mut alice, _) = MessengerClient::connect(
            &addrs[0], "alice", "d1", &auth.mint_token("alice", "d1"), 0,
        )
        .await
        .unwrap();
        let (mut bob, _) = MessengerClient::connect(
            &addrs[1], "bob", "d1", &auth.mint_token("bob", "d1"), 0,
        )
        .await
        .unwrap();

        let mut alice_e2ee = E2eeDevice::generate();
        let mut bob_e2ee = E2eeDevice::generate();
        alice.publish_e2ee_device("d1", &mut alice_e2ee, 5).await.unwrap();
        bob.publish_e2ee_device("d1", &mut bob_e2ee, 5).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        alice.establish_e2ee_session("bob", &mut alice_e2ee).await.unwrap();

        let secret = b"cluster ciphertext only";
        alice
            .send_encrypted_chat(&mut alice_e2ee, "bob", "e2ee-x", secret)
            .await
            .unwrap();

        let pkt = bob
            .recv_until(|p| matches!(p, Packet::ChatMessage(_)))
            .await
            .unwrap();
        match pkt {
            Packet::ChatMessage(m) => {
                assert!(!E2eeDevice::body_contains_substring(&m.body, secret));
                let plain = MessengerClient::decrypt_chat(&mut bob_e2ee, "alice", &m.body).unwrap();
                assert_eq!(plain, secret);
            }
            _ => unreachable!(),
        }
    }
}

// ---- TLS (feature = "tls") ---------------------------------------------------------

#[cfg(feature = "tls")]
mod tls_tests {
    use super::*;
    use lane_switchboards::messenger::MessengerServer;
    use std::io::Write as _;

    /// Self-signed cert for 127.0.0.1 written to temp PEM files.
    fn make_cert() -> (tempfile::NamedTempFile, tempfile::NamedTempFile) {
        let cert = rcgen::generate_simple_self_signed(vec![
            "127.0.0.1".to_string(),
            "localhost".to_string(),
        ])
        .expect("generate cert");
        let mut cert_file = tempfile::NamedTempFile::new().unwrap();
        cert_file.write_all(cert.cert.pem().as_bytes()).unwrap();
        let mut key_file = tempfile::NamedTempFile::new().unwrap();
        key_file
            .write_all(cert.key_pair.serialize_pem().as_bytes())
            .unwrap();
        (cert_file, key_file)
    }

    #[tokio::test]
    async fn tls_login_and_chat_round_trip() {
        let (cert, key) = make_cert();
        let server_cfg = lane_switchboards::tls::server_config_from_pem(
            cert.path(),
            key.path(),
            None::<&std::path::Path>,
        )
        .expect("server tls config");
        let acceptor = lane_switchboards::tls::build_acceptor(server_cfg);

        let auth = HmacAuthenticator::new(SECRET);
        let server = MessengerServer::bind_tls(
            "127.0.0.1:0",
            Arc::new(auth.clone()),
            ServerConfig::default(),
            Some(acceptor),
        )
        .await
        .expect("bind tls gateway");
        let addr = server.local_addr().to_string();

        // Client trusts the self-signed cert as its CA root.
        let client_cfg = lane_switchboards::tls::client_config_from_pem(
            Some(cert.path()),
            None::<&std::path::Path>,
            None::<&std::path::Path>,
        )
        .expect("client tls config");
        let connector = lane_switchboards::tls::build_connector(client_cfg);

        let (mut alice, _) = MessengerClient::connect_tls(
            &addr,
            Some(&connector),
            "alice",
            "d1",
            &auth.mint_token("alice", "d1"),
            0,
        )
        .await
        .expect("tls login alice");
        let (mut bob, _) = MessengerClient::connect_tls(
            &addr,
            Some(&connector),
            "bob",
            "d1",
            &auth.mint_token("bob", "d1"),
            0,
        )
        .await
        .expect("tls login bob");

        alice.send_chat("bob", "m-tls", b"encrypted transport").await.expect("send over tls");
        let pkt = bob
            .recv_until(|p| matches!(p, Packet::ChatMessage(_)))
            .await
            .expect("recv over tls");
        match pkt {
            Packet::ChatMessage(m) => assert_eq!(m.body, b"encrypted transport"),
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn plaintext_client_rejected_by_tls_gateway() {
        let (cert, key) = make_cert();
        let server_cfg = lane_switchboards::tls::server_config_from_pem(
            cert.path(),
            key.path(),
            None::<&std::path::Path>,
        )
        .unwrap();
        let acceptor = lane_switchboards::tls::build_acceptor(server_cfg);

        let auth = HmacAuthenticator::new(SECRET);
        let server = MessengerServer::bind_tls(
            "127.0.0.1:0",
            Arc::new(auth.clone()),
            ServerConfig::default(),
            Some(acceptor),
        )
        .await
        .unwrap();
        let addr = server.local_addr().to_string();

        // Plain connect: the TLS handshake fails, login never succeeds.
        let token = auth.mint_token("alice", "d1");
        let res = MessengerClient::connect(&addr, "alice", "d1", &token, 0).await;
        assert!(res.is_err(), "plaintext client must be rejected by TLS gateway");
    }

    #[tokio::test]
    async fn cluster_peer_links_over_tls() {
        use lane_switchboards::messenger::{ClusterConfig, PeerAddr};

        let (cert, key) = make_cert();
        let server_cfg = lane_switchboards::tls::server_config_from_pem(
            cert.path(),
            key.path(),
            None::<&std::path::Path>,
        )
        .unwrap();
        let acceptor = lane_switchboards::tls::build_acceptor(server_cfg);
        let client_cfg = lane_switchboards::tls::client_config_from_pem(
            Some(cert.path()),
            None::<&std::path::Path>,
            None::<&std::path::Path>,
        )
        .unwrap();
        let connector = lane_switchboards::tls::build_connector(client_cfg);

        let auth = HmacAuthenticator::new(SECRET);
        let l0 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let l1 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a0 = l0.local_addr().unwrap().to_string();
        let a1 = l1.local_addr().unwrap().to_string();
        drop(l0);
        drop(l1);

        let _s0 = MessengerServer::bind_cluster_tls(
            &a0,
            Arc::new(auth.clone()),
            ServerConfig::default(),
            ClusterConfig {
                node_id: "node-0".into(),
                peers: vec![PeerAddr { node_id: "node-1".into(), addr: a1.clone() }],
                peer_secret: "peer-tls-secret".into(),
            },
            Some(acceptor.clone()),
            Some(connector.clone()),
        )
        .await
        .unwrap();
        let s1 = MessengerServer::bind_cluster_tls(
            &a1,
            Arc::new(auth.clone()),
            ServerConfig::default(),
            ClusterConfig {
                node_id: "node-1".into(),
                peers: vec![PeerAddr { node_id: "node-0".into(), addr: a0.clone() }],
                peer_secret: "peer-tls-secret".into(),
            },
            Some(acceptor),
            Some(connector),
        )
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(s1.is_ready().await);

        let (mut alice, _) = MessengerClient::connect_tls(
            &a0,
            Some(&lane_switchboards::tls::build_connector(
                lane_switchboards::tls::client_config_from_pem(
                    Some(cert.path()),
                    None::<&std::path::Path>,
                    None::<&std::path::Path>,
                )
                .unwrap(),
            )),
            "alice",
            "d1",
            &auth.mint_token("alice", "d1"),
            0,
        )
        .await
        .unwrap();
        let (mut bob, _) = MessengerClient::connect_tls(
            &a1,
            Some(&lane_switchboards::tls::build_connector(
                lane_switchboards::tls::client_config_from_pem(
                    Some(cert.path()),
                    None::<&std::path::Path>,
                    None::<&std::path::Path>,
                )
                .unwrap(),
            )),
            "bob",
            "d1",
            &auth.mint_token("bob", "d1"),
            0,
        )
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_millis(200)).await;
        alice.send_chat("bob", "tls-peer", b"over tls mesh").await.unwrap();
        let pkt = bob
            .recv_until(|p| matches!(p, Packet::ChatMessage(_)))
            .await
            .unwrap();
        match pkt {
            Packet::ChatMessage(m) => assert_eq!(m.body, b"over tls mesh"),
            _ => unreachable!(),
        }
    }
}

// ---- Load smoke ------------------------------------------------------------------

#[tokio::test]
async fn hundred_concurrent_clients_chat_pairwise() {
    let (_server, addr, auth) = boot().await;
    const N: usize = 100;

    let mut tasks = Vec::new();
    for i in 0..N / 2 {
        let addr = addr.clone();
        let auth = auth.clone();
        tasks.push(tokio::spawn(async move {
            let a = format!("user-{}", 2 * i);
            let b = format!("user-{}", 2 * i + 1);
            let (mut ca, _) = MessengerClient::connect(
                &addr, &a, "d1", &auth.mint_token(&a, "d1"), 0,
            )
            .await
            .expect("login a");
            let (mut cb, _) = MessengerClient::connect(
                &addr, &b, "d1", &auth.mint_token(&b, "d1"), 0,
            )
            .await
            .expect("login b");
            let mid = format!("m-{i}");
            ca.send_chat(&b, &mid, b"ping").await.expect("send");
            let pkt = cb
                .recv_until(|p| matches!(p, Packet::ChatMessage(_)))
                .await
                .expect("recv");
            match pkt {
                Packet::ChatMessage(m) => assert_eq!(m.body, b"ping"),
                _ => unreachable!(),
            }
        }));
    }
    for t in tasks {
        t.await.expect("pair task");
    }
}
