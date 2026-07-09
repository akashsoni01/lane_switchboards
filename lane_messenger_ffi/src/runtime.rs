//! Process-wide Tokio runtime for FFI calls from foreign threads.

use once_cell::sync::Lazy;
use std::env;
use tokio::runtime::Runtime;

static RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    let threads = env::var("LANE_MESSENGER_WORKER_THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
                .clamp(2, 8)
        });
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .thread_name("lane-messenger-ffi")
        .build()
        .expect("lane_messenger_ffi: failed to build Tokio runtime")
});

/// Shared multi-thread runtime (one per process).
pub fn runtime() -> &'static Runtime {
    &RUNTIME
}

/// Block the calling thread on a future using the FFI runtime.
pub fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    RUNTIME.block_on(fut)
}
