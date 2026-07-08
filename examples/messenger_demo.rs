//! FunXMPP-style messenger demo: login, presence, 1:1 chat with the full
//! tick ladder, offline delivery + sync, group chat, and a chunked PDF
//! transfer — all over the compact binary protocol.
//!
//! Run: `cargo run --example messenger_demo`

use std::sync::Arc;

use lane_switchboards::messenger::{
    HmacAuthenticator, MessengerClient, MessengerServer, Packet, ServerConfig,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    // ---- Boot a gateway ---------------------------------------------------
    let auth = HmacAuthenticator::new("demo-secret");
    let server =
        MessengerServer::bind("127.0.0.1:0", Arc::new(auth.clone()), ServerConfig::default())
            .await?;
    let addr = server.local_addr().to_string();
    println!("gateway listening on {addr}\n");

    // ---- Alice logs in ------------------------------------------------------
    let (mut alice, _) = MessengerClient::connect(
        &addr,
        "alice",
        "phone-1",
        &auth.mint_token("alice", "phone-1"),
        0,
    )
    .await?;
    println!("[alice] logged in");
    alice.ping().await?;
    println!("[alice] heartbeat ok");

    // ---- Offline send: bob is not connected yet ------------------------------
    alice.send_chat("bob", "m-1", b"hey bob, are you there?").await?;
    println!("[alice] sent m-1 (server ack = single tick); bob is offline");
    println!("        pending for bob: {}", server.pending_for("bob").await);

    // ---- Bob logs in and syncs ------------------------------------------------
    let (mut bob, outcome) = MessengerClient::connect(
        &addr,
        "bob",
        "phone-1",
        &auth.mint_token("bob", "phone-1"),
        0,
    )
    .await?;
    println!("[bob] logged in, replayed {} offline message(s):", outcome.replayed.len());
    for pkt in &outcome.replayed {
        if let Packet::ChatMessage(m) = pkt {
            println!("      from {}: {:?} (seq {})", m.from_user, String::from_utf8_lossy(&m.body), m.seq);
            bob.ack_delivered(&m.message_id).await?;
            bob.ack_read(&m.message_id).await?;
        }
    }

    // Alice sees the double tick and blue tick.
    alice
        .recv_until(|p| matches!(p, Packet::DeliveredAck(a) if a.message_id == "m-1"))
        .await?;
    println!("[alice] m-1 delivered (double tick)");
    alice
        .recv_until(|p| matches!(p, Packet::ReadAck(a) if a.message_id == "m-1"))
        .await?;
    println!("[alice] m-1 read (blue tick)");

    // ---- Online 1:1 reply --------------------------------------------------------
    bob.send_chat("alice", "m-2", b"yes! got it instantly").await?;
    let pkt = alice.recv_until(|p| matches!(p, Packet::ChatMessage(_))).await?;
    if let Packet::ChatMessage(m) = pkt {
        println!("[alice] received: {:?}", String::from_utf8_lossy(&m.body));
    }

    // ---- Bulk data: send a "PDF" -------------------------------------------------
    let pdf: Vec<u8> = (0..200 * 1024).map(|i| (i % 251) as u8).collect(); // fake 200 KiB PDF
    alice.upload_media("pdf-1", "design.pdf", "application/pdf", &pdf).await?;
    println!("\n[alice] uploaded design.pdf ({} bytes, chunked + sha256 verified)", pdf.len());
    alice.send_chat_with_media("bob", "m-3", b"the design doc", "pdf-1").await?;
    let pkt = bob.recv_until(|p| matches!(p, Packet::ChatMessage(m) if !m.media_id.is_empty())).await?;
    if let Packet::ChatMessage(m) = pkt {
        let blob = bob.fetch_media(&m.media_id).await?;
        println!(
            "[bob] downloaded {} ({} bytes, {}) — integrity ok: {}",
            blob.file_name,
            blob.data.len(),
            blob.mime_type,
            blob.data == pdf
        );
    }

    // ---- Group chat -----------------------------------------------------------------
    let (mut carol, _) = MessengerClient::connect(
        &addr,
        "carol",
        "phone-1",
        &auth.mint_token("carol", "phone-1"),
        0,
    )
    .await?;
    alice.create_group("team").await?;
    alice.add_member("team", "bob").await?;
    alice.add_member("team", "carol").await?;
    alice.send_group("team", "gm-1", b"welcome to the team group!").await?;
    println!("\n[alice] created group 'team' and messaged it");
    for (name, c) in [("bob", &mut bob), ("carol", &mut carol)] {
        let pkt = c.recv_until(|p| matches!(p, Packet::GroupMessage(_))).await?;
        if let Packet::GroupMessage(m) = pkt {
            println!("[{name}] group message: {:?}", String::from_utf8_lossy(&m.body));
        }
    }

    println!("\ndemo complete");
    Ok(())
}
