//! Fuzz target for the messenger frame decoder.
//!
//! Build / run (nightly + cargo-fuzz):
//! ```text
//! cargo install cargo-fuzz
//! cargo +nightly fuzz run messenger_codec -- -max_total_time=60
//! ```

#![no_main]

use libfuzzer_sys::fuzz_target;
use lane_switchboards::messenger::{FrameCodec, Packet};
use bytes::BytesMut;
use tokio_util::codec::Decoder;

fuzz_target!(|data: &[u8]| {
    let mut codec = FrameCodec::with_max_frame(64 * 1024);
    let mut buf = BytesMut::from(data);
    // Must never panic — Err / None are fine.
    let _ = codec.decode(&mut buf);
    // Second pass after partial consume.
    let _ = codec.decode(&mut buf);
    let _: Option<Result<Option<Packet>, _>> = None;
});
