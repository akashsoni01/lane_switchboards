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
