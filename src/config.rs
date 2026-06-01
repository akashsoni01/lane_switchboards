//! Runtime tuning — mailbox capacities, load limits, and dedicated runtimes.

use std::future::Future;
use std::io;
use std::time::Duration;
use tokio::runtime::{Builder, Runtime};
use tokio::task::JoinHandle;

/// Actor mailbox sizing and deadlock / slow-handle limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActorConfig {
    pub mailbox_capacity: usize,
    /// Max wall time for one `handle()` call. Exceeded → `on_handle_stuck` then actor exit.
    /// `None` disables handle timeouts.
    pub handle_timeout: Option<Duration>,
    /// Log + count handles that finish successfully but exceed this duration.
    /// Defaults to `handle_timeout` when set; `None` disables slow-handle warnings.
    pub slow_handle_threshold: Option<Duration>,
}

impl Default for ActorConfig {
    fn default() -> Self {
        Self {
            mailbox_capacity: 64,
            handle_timeout: None,
            slow_handle_threshold: None,
        }
    }
}

impl ActorConfig {
    /// Threshold used when recording slow successful handles.
    pub fn effective_slow_threshold(&self) -> Option<Duration> {
        self.slow_handle_threshold.or(self.handle_timeout)
    }
}

/// Distributed TCP bridge sizing and per-node load limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DistributedConfig {
    pub bridge_capacity: usize,
    /// Max in-flight frame dispatches per TCP node (semaphore backpressure).
    pub max_in_flight: usize,
    /// Reject inbound frames larger than this (bytes).
    pub max_frame_bytes: u32,
    /// Per-read timeout on inbound TCP connections.
    pub read_timeout: Duration,
    /// Outbound remote send queue depth per [`crate::distributed::RemoteActorRef`].
    pub remote_send_capacity: usize,
}

impl Default for DistributedConfig {
    fn default() -> Self {
        Self {
            bridge_capacity: 32,
            max_in_flight: 32,
            max_frame_bytes: 4 * 1024 * 1024,
            read_timeout: Duration::from_secs(30),
            remote_send_capacity: 32,
        }
    }
}

/// Options for building a dedicated Tokio runtime for actors / nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RuntimeOptions {
    pub worker_threads: Option<usize>,
}

/// Dedicated multi-thread Tokio runtime (keeps the OS runtime alive).
pub struct DedicatedRuntime {
    inner: Runtime,
}

impl DedicatedRuntime {
    pub fn new(options: RuntimeOptions) -> io::Result<Self> {
        build_multi_thread_runtime(options.worker_threads).map(|inner| Self { inner })
    }

    pub fn handle(&self) -> tokio::runtime::Handle {
        self.inner.handle().clone()
    }

    /// Run a future to completion on this runtime (blocking the calling thread).
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.inner.block_on(future)
    }
}

/// Build a multi-thread Tokio runtime for isolating actors or distributed nodes.
pub fn build_multi_thread_runtime(worker_threads: Option<usize>) -> io::Result<Runtime> {
    let mut builder = Builder::new_multi_thread();
    builder.enable_all();
    if let Some(n) = worker_threads {
        builder.worker_threads(n);
    }
    builder.build()
}

/// Spawn a task on `runtime`, or the current runtime when `runtime` is `None`.
pub fn spawn_on<F>(runtime: Option<&tokio::runtime::Handle>, future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    match runtime {
        Some(handle) => handle.spawn(future),
        None => tokio::spawn(future),
    }
}
