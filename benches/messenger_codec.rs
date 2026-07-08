//! Codec throughput: compact binary frames vs an XML-stanza baseline.
//!
//! Run: `cargo bench --bench messenger_codec`

use bytes::BytesMut;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use tokio_util::codec::{Decoder, Encoder};

use lane_switchboards::messenger::{wire, FrameCodec, Packet};

fn chat_packet() -> Packet {
    Packet::ChatMessage(wire::ChatMessage {
        message_id: "018f4c2e-1234-7abc-9def-0123456789ab".into(),
        from_user: "alice".into(),
        to_user: "bob".into(),
        body: b"hello bob, how are you doing today?".to_vec(),
        sent_at: 1_752_000_000_000,
        seq: 42,
        media_id: String::new(),
    })
}

/// The verbose XML equivalent WhatsApp moved away from — built and parsed
/// naively as a size/CPU reference point, not as a real XMPP implementation.
fn xml_encode(m: &wire::ChatMessage) -> String {
    format!(
        "<message id=\"{}\" from=\"{}\" to=\"{}\" ts=\"{}\" seq=\"{}\"><body>{}</body></message>",
        m.message_id,
        m.from_user,
        m.to_user,
        m.sent_at,
        m.seq,
        String::from_utf8_lossy(&m.body),
    )
}

fn xml_decode(s: &str) -> (String, String) {
    // Toy extraction of two attributes; real XML parsing would be slower.
    let id = s.split("id=\"").nth(1).and_then(|r| r.split('"').next()).unwrap_or("");
    let body = s.split("<body>").nth(1).and_then(|r| r.split('<').next()).unwrap_or("");
    (id.to_string(), body.to_string())
}

fn bench_codec(c: &mut Criterion) {
    let pkt = chat_packet();
    let m = match &pkt {
        Packet::ChatMessage(m) => m.clone(),
        _ => unreachable!(),
    };

    // Report encoded sizes once for context.
    let mut codec = FrameCodec::default();
    let mut buf = BytesMut::new();
    codec.encode(pkt.clone(), &mut buf).unwrap();
    println!(
        "encoded sizes — binary frame: {} bytes, xml stanza: {} bytes",
        buf.len(),
        xml_encode(&m).len()
    );

    let mut group = c.benchmark_group("chat_message");
    group.throughput(Throughput::Elements(1));

    group.bench_function("binary_encode", |b| {
        let mut codec = FrameCodec::default();
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(256);
            codec.encode(black_box(pkt.clone()), &mut buf).unwrap();
            black_box(buf);
        })
    });

    group.bench_function("binary_encode_decode", |b| {
        let mut codec = FrameCodec::default();
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(256);
            codec.encode(black_box(pkt.clone()), &mut buf).unwrap();
            let out = codec.decode(&mut buf).unwrap().unwrap();
            black_box(out);
        })
    });

    group.bench_function("xml_encode", |b| {
        b.iter(|| black_box(xml_encode(black_box(&m))))
    });

    group.bench_function("xml_encode_decode", |b| {
        b.iter(|| {
            let s = xml_encode(black_box(&m));
            black_box(xml_decode(&s));
        })
    });

    group.finish();
}

criterion_group!(benches, bench_codec);
criterion_main!(benches);
