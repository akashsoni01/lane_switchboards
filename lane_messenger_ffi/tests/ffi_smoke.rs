//! FFI integration tests against a real `MessengerServer`.

use std::sync::Arc;
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

#[test]
fn version_and_constants() {
    assert!(!version().is_empty());
    assert_eq!(PROTOCOL_VERSION, 1);
}

fn boot_server() -> (MessengerServer, String, HmacAuthenticator) {
    // Multi-thread so accept/session tasks keep running after block_on returns.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap();
    let out = rt.block_on(boot());
    std::mem::forget(rt); // keep workers alive for the test duration
    out
}

#[test]
fn connect_ping_close() {
    let (_server, addr, auth) = boot_server();
    let (host, port) = split_addr(&addr);
    let token = auth.mint_token("alice", "phone-1");

    let session = SessionHandle::connect(ConnectOptions {
        host,
        port,
        use_tls: false,
        user_id: "alice".into(),
        device_id: "phone-1".into(),
        auth_token: token,
        client_version: "ffi-test".into(),
        resume_after_seq: 0,
        ping_interval_secs: 0,
    })
    .expect("connect");

    let login = wait_event(&session, 2000, |e| matches!(e, LaneEvent::LoginAck(_)));
    assert!(matches!(login, LaneEvent::LoginAck(a) if a.ok));
    let _ = wait_event(&session, 2000, |e| matches!(e, LaneEvent::SyncComplete(_)));

    session.ping().expect("ping");
    session.close().expect("close");
}

#[test]
fn chat_round_trip() {
    let (_server, addr, auth) = boot_server();
    let (host, port) = split_addr(&addr);

    let alice = SessionHandle::connect(ConnectOptions {
        host: host.clone(),
        port,
        use_tls: false,
        user_id: "alice".into(),
        device_id: "a1".into(),
        auth_token: auth.mint_token("alice", "a1"),
        client_version: "ffi-test".into(),
        resume_after_seq: 0,
        ping_interval_secs: 0,
    })
    .unwrap();
    drain_sync(&alice);

    let bob = SessionHandle::connect(ConnectOptions {
        host,
        port,
        use_tls: false,
        user_id: "bob".into(),
        device_id: "b1".into(),
        auth_token: auth.mint_token("bob", "b1"),
        client_version: "ffi-test".into(),
        resume_after_seq: 0,
        ping_interval_secs: 0,
    })
    .unwrap();
    drain_sync(&bob);

    let seq = alice
        .send_chat("bob", "m-1", b"hello from ffi")
        .expect("send");
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
fn e2ee_generate_and_safety_number() {
    let a = E2eeHandle::generate();
    let b = E2eeHandle::generate();
    let n1 = E2eeHandle::safety_number(&a.identity_key(), &b.identity_key());
    let n2 = E2eeHandle::safety_number(&b.identity_key(), &a.identity_key());
    assert_eq!(n1, n2);
    assert!(!n1.is_empty());
}

fn drain_sync(session: &SessionHandle) {
    let _ = wait_event(session, 2000, |e| matches!(e, LaneEvent::LoginAck(_)));
    let _ = wait_event(session, 2000, |e| matches!(e, LaneEvent::SyncComplete(_)));
}

fn split_addr(addr: &str) -> (String, u16) {
    let (h, p) = addr.rsplit_once(':').expect("host:port");
    (h.to_string(), p.parse().unwrap())
}
