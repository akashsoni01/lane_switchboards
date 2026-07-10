//! FFI integration tests against a real `MessengerServer`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use lane_messenger_ffi::{
    version, ConnectOptions, E2eeHandle, LaneEvent, SessionHandle, PROTOCOL_VERSION,
};
use lane_switchboards::messenger::{HmacAuthenticator, MessengerServer, ServerConfig};

const SECRET: &str = "ffi-test-secret";

async fn boot() -> (MessengerServer, String, HmacAuthenticator) {
    let auth = HmacAuthenticator::new(SECRET);
    let server = MessengerServer::bind("127.0.0.1:0", Arc::new(auth.clone()), ServerConfig::default())
        .await
        .expect("bind");
    let addr = server.local_addr().to_string();
    (server, addr, auth)
}

fn wait_event(session: &SessionHandle, timeout_ms: u64, pred: impl Fn(&LaneEvent) -> bool) -> LaneEvent {
    let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        if let Some(ev) = session.poll_event(50) {
            if pred(&ev) {
                return ev;
            }
        }
        if std::time::Instant::now() >= deadline {
            panic!("timeout waiting for event");
        }
    }
}

fn boot_server() -> (MessengerServer, String, HmacAuthenticator) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    let out = rt.block_on(boot());
    std::mem::forget(rt);
    out
}

fn opts(host: &str, port: u16, user: &str, device: &str, token: String) -> ConnectOptions {
    ConnectOptions {
        host: host.into(),
        port,
        user_id: user.into(),
        device_id: device.into(),
        auth_token: token,
        client_version: "ffi-test".into(),
        ping_interval_secs: 0,
        ..Default::default()
    }
}

fn drain_sync(session: &SessionHandle) {
    let _ = wait_event(session, 2000, |e| matches!(e, LaneEvent::LoginAck(_)));
    let _ = wait_event(session, 2000, |e| matches!(e, LaneEvent::SyncComplete(_)));
}

fn split_addr(addr: &str) -> (String, u16) {
    let (h, p) = addr.rsplit_once(':').expect("host:port");
    (h.to_string(), p.parse().unwrap())
}

#[test]
fn version_and_constants() {
    assert!(!version().is_empty());
    assert_eq!(PROTOCOL_VERSION, 1);
}

#[test]
fn connect_ping_close() {
    let (_server, addr, auth) = boot_server();
    let (host, port) = split_addr(&addr);
    let session = SessionHandle::connect(opts(
        &host,
        port,
        "alice",
        "phone-1",
        auth.mint_token("alice", "phone-1"),
    ))
    .expect("connect");
    drain_sync(&session);
    session.ping().expect("ping");
    session.close().expect("close");
}

#[test]
fn event_handler_push() {
    let (_server, addr, auth) = boot_server();
    let (host, port) = split_addr(&addr);
    let alice = SessionHandle::connect(opts(
        &host,
        port,
        "alice",
        "h1",
        auth.mint_token("alice", "h1"),
    ))
    .unwrap();
    drain_sync(&alice);

    let bob = SessionHandle::connect(opts(&host, port, "bob", "h2", auth.mint_token("bob", "h2")))
        .unwrap();
    drain_sync(&bob);

    let seen = Arc::new(Mutex::new(0u32));
    let seen2 = Arc::clone(&seen);
    bob.set_event_handler(Some(Arc::new(move |ev| {
        if matches!(ev, LaneEvent::ChatMessage(_)) {
            *seen2.lock().unwrap() += 1;
        }
    })));

    alice.send_chat("bob", "push-1", b"via handler").unwrap();
    let _ = wait_event(&bob, 3000, |e| {
        matches!(e, LaneEvent::ChatMessage(m) if m.message_id == "push-1")
    });
    assert!(*seen.lock().unwrap() >= 1, "push handler should fire");

    alice.close().ok();
    bob.close().ok();
}

#[test]
fn chat_round_trip() {
    let (_server, addr, auth) = boot_server();
    let (host, port) = split_addr(&addr);

    let alice = SessionHandle::connect(opts(
        &host,
        port,
        "alice",
        "a1",
        auth.mint_token("alice", "a1"),
    ))
    .unwrap();
    drain_sync(&alice);

    let bob = SessionHandle::connect(opts(&host, port, "bob", "b1", auth.mint_token("bob", "b1")))
        .unwrap();
    drain_sync(&bob);

    let seq = alice.send_chat("bob", "m-1", b"hello from ffi").expect("send");
    assert!(seq > 0);

    let chat = wait_event(&bob, 3000, |e| {
        matches!(e, LaneEvent::ChatMessage(m) if m.message_id == "m-1")
    });
    match chat {
        LaneEvent::ChatMessage(m) => {
            assert_eq!(m.body, b"hello from ffi");
            bob.ack_delivered(&m.message_id).unwrap();
            bob.ack_read(&m.message_id).unwrap();
        }
        _ => unreachable!(),
    }

    let _ = wait_event(&alice, 3000, |e| {
        matches!(e, LaneEvent::DeliveredAck(a) if a.message_id == "m-1")
    });
    let _ = wait_event(&alice, 3000, |e| {
        matches!(e, LaneEvent::ReadAck(a) if a.message_id == "m-1")
    });

    alice.close().ok();
    bob.close().ok();
}

#[test]
fn duplicate_message_id_sentinel_seq() {
    let (_server, addr, auth) = boot_server();
    let (host, port) = split_addr(&addr);
    let alice = SessionHandle::connect(opts(
        &host,
        port,
        "alice",
        "a1",
        auth.mint_token("alice", "a1"),
    ))
    .unwrap();
    drain_sync(&alice);
    let bob = SessionHandle::connect(opts(&host, port, "bob", "b1", auth.mint_token("bob", "b1")))
        .unwrap();
    drain_sync(&bob);

    let seq1 = alice
        .send_chat_with_retry("bob", "dedup-1", b"once", 3)
        .unwrap();
    assert!(seq1 > 0);
    let seq2 = alice
        .send_chat_with_retry("bob", "dedup-1", b"again", 3)
        .unwrap();
    assert_eq!(seq2, 0);

    alice.close().ok();
    bob.close().ok();
}

#[test]
fn same_device_kick_emits_replaced() {
    let (_server, addr, auth) = boot_server();
    let (host, port) = split_addr(&addr);
    let old = SessionHandle::connect(opts(
        &host,
        port,
        "akash",
        "phone-1",
        auth.mint_token("akash", "phone-1"),
    ))
    .unwrap();
    drain_sync(&old);

    let new = SessionHandle::connect(opts(
        &host,
        port,
        "akash",
        "phone-1",
        auth.mint_token("akash", "phone-1"),
    ))
    .unwrap();
    drain_sync(&new);

    let _ = wait_event(&old, 3000, |e| matches!(e, LaneEvent::ReplacedByNewSession));
    new.ping().expect("new alive");
    new.close().ok();
}

#[test]
fn group_ack_summary_via_ffi() {
    let (_server, addr, auth) = boot_server();
    let (host, port) = split_addr(&addr);
    let alice = SessionHandle::connect(opts(
        &host,
        port,
        "alice",
        "a1",
        auth.mint_token("alice", "a1"),
    ))
    .unwrap();
    drain_sync(&alice);
    let bob = SessionHandle::connect(opts(&host, port, "bob", "b1", auth.mint_token("bob", "b1")))
        .unwrap();
    drain_sync(&bob);
    let carol = SessionHandle::connect(opts(
        &host,
        port,
        "carol",
        "c1",
        auth.mint_token("carol", "c1"),
    ))
    .unwrap();
    drain_sync(&carol);

    alice.create_group("g-ffi").unwrap();
    alice.add_member("g-ffi", "bob").unwrap();
    alice.add_member("g-ffi", "carol").unwrap();
    alice.send_group("g-ffi", "gm-1", b"hello group").unwrap();

    for member in [&bob, &carol] {
        let msg = wait_event(member, 3000, |e| {
            matches!(e, LaneEvent::GroupMessage(m) if m.message_id == "gm-1")
        });
        if let LaneEvent::GroupMessage(m) = msg {
            member.ack_delivered(&m.message_id).unwrap();
            member.ack_read(&m.message_id).unwrap();
        }
    }

    let mut saw_full = false;
    for _ in 0..12 {
        let ev = wait_event(&alice, 2000, |e| {
            matches!(e, LaneEvent::GroupAckSummary(s) if s.message_id == "gm-1")
        });
        if let LaneEvent::GroupAckSummary(s) = ev {
            if s.read_by.len() >= 2 {
                assert_eq!(s.member_count, 2);
                saw_full = true;
                break;
            }
        }
    }
    assert!(saw_full, "expected full GroupAckSummary");

    alice.close().ok();
    bob.close().ok();
    carol.close().ok();
}

#[test]
fn media_round_trip() {
    let (_server, addr, auth) = boot_server();
    let (host, port) = split_addr(&addr);
    let alice = SessionHandle::connect(opts(
        &host,
        port,
        "alice",
        "a1",
        auth.mint_token("alice", "a1"),
    ))
    .unwrap();
    drain_sync(&alice);

    let pdf = b"%PDF-1.4 ffi media test";
    let stored = alice
        .upload_media("pdf-ffi", "report.pdf", "application/pdf", pdf)
        .expect("upload");
    assert_eq!(stored, pdf.len() as u64);

    let bob = SessionHandle::connect(opts(&host, port, "bob", "b1", auth.mint_token("bob", "b1")))
        .unwrap();
    drain_sync(&bob);
    let down = bob.fetch_media("pdf-ffi").expect("fetch");
    assert_eq!(down.data, pdf);
    assert_eq!(down.file_name, "report.pdf");

    alice.close().ok();
    bob.close().ok();
}

#[test]
fn e2ee_chat_opacity() {
    let (_server, addr, auth) = boot_server();
    let (host, port) = split_addr(&addr);
    let alice = SessionHandle::connect(opts(
        &host,
        port,
        "alice",
        "a1",
        auth.mint_token("alice", "a1"),
    ))
    .unwrap();
    drain_sync(&alice);
    let bob = SessionHandle::connect(opts(&host, port, "bob", "b1", auth.mint_token("bob", "b1")))
        .unwrap();
    drain_sync(&bob);

    let alice_e2ee = E2eeHandle::generate();
    let bob_e2ee = E2eeHandle::generate();
    alice_e2ee.publish(&alice, "a1", 5).unwrap();
    bob_e2ee.publish(&bob, "b1", 5).unwrap();

    let plain = b"secret-ffi-plaintext";
    let seq = alice_e2ee
        .send_encrypted_chat(&alice, "bob", "e2ee-1", plain)
        .unwrap();
    assert!(seq > 0);

    let chat = wait_event(&bob, 3000, |e| {
        matches!(e, LaneEvent::ChatMessage(m) if m.message_id == "e2ee-1")
    });
    match chat {
        LaneEvent::ChatMessage(m) => {
            assert!(
                !m.body.windows(plain.len()).any(|w| w == plain),
                "ciphertext must not contain plaintext"
            );
            let dec = bob_e2ee.decrypt_chat("alice", &m.body).unwrap();
            assert_eq!(dec, plain);
        }
        _ => unreachable!(),
    }

    alice.close().ok();
    bob.close().ok();
}

#[test]
fn e2ee_pickle_round_trip() {
    let a = E2eeHandle::generate();
    let key = a.identity_key();
    let pickle = a.export_pickle("test-passphrase");
    let b = E2eeHandle::import_pickle(&pickle, "test-passphrase").unwrap();
    assert_eq!(b.identity_key(), key);
}

#[test]
fn e2ee_generate_and_safety_number() {
    let a = E2eeHandle::generate();
    let b = E2eeHandle::generate();
    let n1 = E2eeHandle::safety_number(&a.identity_key(), &b.identity_key());
    let n2 = E2eeHandle::safety_number(&b.identity_key(), &a.identity_key());
    assert_eq!(n1, n2);
    assert!(!n1.is_empty());
}

#[test]
fn presence_roster_via_ffi() {
    let (_server, addr, auth) = boot_server();
    let (host, port) = split_addr(&addr);
    let alice = SessionHandle::connect(opts(
        &host,
        port,
        "alice",
        "a1",
        auth.mint_token("alice", "a1"),
    ))
    .unwrap();
    drain_sync(&alice);
    let bob = SessionHandle::connect(opts(&host, port, "bob", "b1", auth.mint_token("bob", "b1")))
        .unwrap();
    drain_sync(&bob);
    let carol = SessionHandle::connect(opts(
        &host,
        port,
        "carol",
        "c1",
        auth.mint_token("carol", "c1"),
    ))
    .unwrap();
    drain_sync(&carol);

    alice.subscribe_presence(&["bob".into()]).unwrap();
    bob.subscribe_presence(&["alice".into()]).unwrap();
    // carol: legacy (no subscribe)

    alice.send_presence(1).unwrap(); // Available
    std::thread::sleep(Duration::from_millis(100));

    let bob_got = poll_presence_event(&bob, "alice", 400);
    let carol_got = poll_presence_event(&carol, "alice", 200);
    assert!(bob_got, "bob on roster");
    assert!(!carol_got, "carol not on roster");

    alice.close().ok();
    bob.close().ok();
    carol.close().ok();
}

fn poll_presence_event(session: &SessionHandle, user: &str, timeout_ms: u64) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms);
    while std::time::Instant::now() < deadline {
        if let Some(LaneEvent::Presence(p)) = session.poll_event(40) {
            if p.user_id == user {
                return true;
            }
        }
    }
    false
}

#[test]
fn e2ee_megolm_three_members() {
    let (_server, addr, auth) = boot_server();
    let (host, port) = split_addr(&addr);
    let alice = SessionHandle::connect(opts(
        &host,
        port,
        "alice",
        "a1",
        auth.mint_token("alice", "a1"),
    ))
    .unwrap();
    drain_sync(&alice);
    let bob = SessionHandle::connect(opts(&host, port, "bob", "b1", auth.mint_token("bob", "b1")))
        .unwrap();
    drain_sync(&bob);
    let carol = SessionHandle::connect(opts(
        &host,
        port,
        "carol",
        "c1",
        auth.mint_token("carol", "c1"),
    ))
    .unwrap();
    drain_sync(&carol);

    let alice_e2ee = E2eeHandle::generate();
    let bob_e2ee = E2eeHandle::generate();
    let carol_e2ee = E2eeHandle::generate();
    alice_e2ee.publish(&alice, "a1", 5).unwrap();
    bob_e2ee.publish(&bob, "b1", 5).unwrap();
    carol_e2ee.publish(&carol, "c1", 5).unwrap();
    std::thread::sleep(Duration::from_millis(50));

    alice.create_group("g-megolm").unwrap();
    alice.add_member("g-megolm", "bob").unwrap();
    alice.add_member("g-megolm", "carol").unwrap();

    alice_e2ee.create_group_session("g-megolm");
    alice_e2ee
        .distribute_group_key(
            &alice,
            "g-megolm",
            &["bob".into(), "carol".into()],
        )
        .unwrap();

    for (member, e2ee) in [(&bob, &bob_e2ee), (&carol, &carol_e2ee)] {
        let chat = wait_event(member, 3000, |e| matches!(e, LaneEvent::ChatMessage(_)));
        if let LaneEvent::ChatMessage(m) = chat {
            assert!(e2ee.try_import_group_key("alice", &m.body).unwrap());
        }
    }

    let secret = b"encrypted group payload ffi";
    alice_e2ee
        .send_encrypted_group(&alice, "g-megolm", "gm-megolm-1", secret)
        .unwrap();

    for (name, member, e2ee) in [
        ("bob", &bob, &bob_e2ee),
        ("carol", &carol, &carol_e2ee),
    ] {
        let msg = wait_event(member, 3000, |e| {
            matches!(e, LaneEvent::GroupMessage(m) if m.message_id == "gm-megolm-1")
        });
        if let LaneEvent::GroupMessage(m) = msg {
            assert!(
                !m.body.windows(secret.len()).any(|w| w == secret),
                "{name}: ciphertext opacity"
            );
            let plain = e2ee.decrypt_group("g-megolm", &m.body).unwrap();
            assert_eq!(plain, secret, "{name}");
        }
    }

    alice.close().ok();
    bob.close().ok();
    carol.close().ok();
}

#[test]
fn websocket_connect_ping() {
    use lane_switchboards::messenger::bind_ws;
    use lane_messenger_ffi::Transport;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    let auth = HmacAuthenticator::new(SECRET);
    let (ws_addr, _handle) = rt
        .block_on(bind_ws(
            "127.0.0.1:0",
            Arc::new(auth.clone()),
            ServerConfig::default(),
        ))
        .expect("bind_ws");
    std::mem::forget(rt);

    let (host, port) = split_addr(&ws_addr.to_string());
    let session = SessionHandle::connect(ConnectOptions {
        host: host.clone(),
        port,
        transport: Transport::WebSocket,
        ws_url: format!("ws://{host}:{port}/"),
        user_id: "alice".into(),
        device_id: "ws-1".into(),
        auth_token: auth.mint_token("alice", "ws-1"),
        client_version: "ffi-ws".into(),
        ping_interval_secs: 0,
        ..Default::default()
    })
    .expect("ws connect");
    drain_sync(&session);
    session.ping().expect("ws ping");
    session.close().ok();
}
