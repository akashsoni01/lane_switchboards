//! Distributed key-value storage layer.
//!
//! Combines a Cassandra-style consistent-hash ring for partition ownership with
//! Paxos for per-key linearisable writes and tunable consistency levels.
//!
//! # Quick start
//!
//! ```no_run
//! # use lane_switchboards::storage::{StorageNode, Key, Value};
//! # use lane_switchboards::{HashRing, RingNode, ConsistencyConfig, WriteConsistency, ReadConsistency};
//! # #[tokio::main] async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mut ring = HashRing::new(150);
//! ring.add_node(RingNode::new("n1", "127.0.0.1", 7001));
//!
//! let node = StorageNode::new(
//!     "n1".into(), ring, ConsistencyConfig { rf: 1, local_rf: 1, ..Default::default() },
//!     "127.0.0.1:0", None, None,
//! ).await?;
//!
//! let k = Key::from("hello".as_bytes().to_vec());
//! let v = Value::from("world".as_bytes().to_vec());
//! node.put(k.clone(), v, WriteConsistency::One).await?;
//! let got = node.get(&k, ReadConsistency::One).await?;
//! assert_eq!(got.as_deref(), Some("world".as_bytes()));
//! # Ok(()) }
//! ```

pub mod replication;
pub mod router;
pub mod table;
pub mod wal;

pub use table::{Key, MemTable, Record, TableKind, Value};
pub use wal::WalHandle;

use crate::consistency::{
    is_local_only, is_paxos_read, read_acks_required, write_acks_required, ConsistencyConfig,
    ReadConsistency, WriteConsistency,
};
use crate::paxos_grpc::{PaxosAcceptorHandle, PaxosProposerClient};
use crate::proto::storage::{
    storage_gateway_client::StorageGatewayClient,
    storage_gateway_server::{StorageGateway, StorageGatewayServer},
    storage_service_server::{StorageService, StorageServiceServer},
    Ack, DeleteRequest, GetReply, GetRequest, PutRequest, ReadReplicaReply, ReadReplicaRequest,
    ReplicateRequest, ScanChunk, ScanRequest, SnapshotChunk, SnapshotRequest,
};
use replication::ReplicationClient;
use router::{ReplicaSet, StorageRouter};
use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, RwLock};
use std::time::SystemTime;
use tokio::task::JoinHandle;
use tonic::{Request, Response, Status};

// ── StorageError ────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct StorageError(pub String);

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "storage error: {}", self.0)
    }
}

impl std::error::Error for StorageError {}

impl From<StorageError> for std::io::Error {
    fn from(e: StorageError) -> Self {
        std::io::Error::other(e)
    }
}

impl From<crate::consistency::ConsistencyError> for StorageError {
    fn from(e: crate::consistency::ConsistencyError) -> Self {
        StorageError(e.to_string())
    }
}

impl From<StorageError> for Status {
    fn from(e: StorageError) -> Self {
        Status::internal(e.0)
    }
}

// ── StorageStats / StorageHealth ─────────────────────────────────────────────

/// Atomic counters kept inside `StorageNode` — same pattern as `monitor::StatsCell`.
struct StorageStatsCell {
    puts_total: AtomicU64,
    gets_total: AtomicU64,
    deletes_total: AtomicU64,
    read_repairs: AtomicU64,
    paxos_writes: AtomicU64,
    quorum_failures: AtomicU64,
    wal_bytes_written: AtomicU64,
}

impl StorageStatsCell {
    fn new() -> Self {
        Self {
            puts_total: AtomicU64::new(0),
            gets_total: AtomicU64::new(0),
            deletes_total: AtomicU64::new(0),
            read_repairs: AtomicU64::new(0),
            paxos_writes: AtomicU64::new(0),
            quorum_failures: AtomicU64::new(0),
            wal_bytes_written: AtomicU64::new(0),
        }
    }

    fn snapshot(&self, tombstone_count: usize) -> StorageStats {
        StorageStats {
            puts_total: self.puts_total.load(Ordering::Relaxed),
            gets_total: self.gets_total.load(Ordering::Relaxed),
            deletes_total: self.deletes_total.load(Ordering::Relaxed),
            read_repairs: self.read_repairs.load(Ordering::Relaxed),
            paxos_writes: self.paxos_writes.load(Ordering::Relaxed),
            quorum_failures: self.quorum_failures.load(Ordering::Relaxed),
            wal_bytes_written: self.wal_bytes_written.load(Ordering::Relaxed),
            tombstone_count: tombstone_count as u64,
        }
    }
}

/// Snapshot of `StorageNode` runtime counters.
#[derive(Debug, Clone)]
pub struct StorageStats {
    pub puts_total: u64,
    pub gets_total: u64,
    pub deletes_total: u64,
    pub read_repairs: u64,
    pub paxos_writes: u64,
    pub quorum_failures: u64,
    pub wal_bytes_written: u64,
    pub tombstone_count: u64,
}

/// Lightweight health summary for a `StorageNode`.
#[derive(Debug, Clone)]
pub struct StorageHealth {
    pub node_id: String,
    pub record_count: usize,
    pub tombstone_count: usize,
    /// `None` when the node is RAM-only (no WAL configured).
    pub wal_bytes_written: Option<u64>,
    pub replica_peers: usize,
    pub ring_rf: usize,
}

// ── StorageClient ─────────────────────────────────────────────────────────────

/// External client for a `StorageNode`'s `StorageGateway` gRPC service.
///
/// ```no_run
/// # use lane_switchboards::storage::{StorageClient, Key, Value};
/// # use lane_switchboards::{WriteConsistency, ReadConsistency};
/// # #[tokio::main] async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let mut client = StorageClient::connect("http://127.0.0.1:7001").await?;
/// let k = Key::from("hello".as_bytes().to_vec());
/// let v = Value::from("world".as_bytes().to_vec());
/// client.put(k.clone(), v).await?;
/// let got = client.get(&k).await?;
/// assert_eq!(got.as_deref(), Some("world".as_bytes()));
/// # Ok(()) }
/// ```
pub struct StorageClient {
    inner: StorageGatewayClient<tonic::transport::Channel>,
    pub default_write_cl: WriteConsistency,
    pub default_read_cl: ReadConsistency,
}

impl StorageClient {
    pub async fn connect(addr: &str) -> Result<Self, StorageError> {
        let uri: tonic::transport::Uri = addr
            .parse()
            .map_err(|e| StorageError(format!("StorageClient bad addr {addr}: {e}")))?;
        let inner = StorageGatewayClient::connect(uri)
            .await
            .map_err(|e| StorageError(format!("StorageClient connect {addr}: {e}")))?;
        Ok(Self {
            inner,
            default_write_cl: WriteConsistency::Quorum,
            default_read_cl: ReadConsistency::Quorum,
        })
    }

    pub async fn put(&mut self, key: Key, value: Value) -> Result<(), StorageError> {
        self.put_cl(key, value, self.default_write_cl).await
    }

    pub async fn put_cl(
        &mut self,
        key: Key,
        value: Value,
        cl: WriteConsistency,
    ) -> Result<(), StorageError> {
        let resp = self
            .inner
            .put(PutRequest {
                key: key.to_vec(),
                value: value.to_vec(),
                write_consistency: format!("{cl:?}").to_uppercase(),
            })
            .await
            .map_err(|s| StorageError(format!("put rpc: {s}")))?
            .into_inner();
        if !resp.ok {
            return Err(StorageError(resp.error));
        }
        Ok(())
    }

    pub async fn get(&mut self, key: &Key) -> Result<Option<Value>, StorageError> {
        self.get_cl(key, self.default_read_cl).await
    }

    pub async fn get_cl(
        &mut self,
        key: &Key,
        cl: ReadConsistency,
    ) -> Result<Option<Value>, StorageError> {
        let resp = self
            .inner
            .get(GetRequest {
                key: key.to_vec(),
                read_consistency: format!("{cl:?}").to_uppercase(),
            })
            .await
            .map_err(|s| StorageError(format!("get rpc: {s}")))?
            .into_inner();
        Ok(if resp.found {
            Some(Value::from(resp.value))
        } else {
            None
        })
    }

    pub async fn delete(&mut self, key: &Key) -> Result<(), StorageError> {
        let resp = self
            .inner
            .delete(DeleteRequest {
                key: key.to_vec(),
                write_consistency: format!("{:?}", self.default_write_cl).to_uppercase(),
            })
            .await
            .map_err(|s| StorageError(format!("delete rpc: {s}")))?
            .into_inner();
        if !resp.ok {
            return Err(StorageError(resp.error));
        }
        Ok(())
    }
}

// ── StorageNode ─────────────────────────────────────────────────────────────

/// A single node in the distributed storage cluster.
///
/// Owns a [`MemTable`], acts as a Paxos acceptor for keys it owns, and can
/// forward replication fan-out to peers. Create with [`StorageNode::new`].
pub struct StorageNode {
    pub id: String,
    /// Actual bound address of the storage gRPC server.
    pub address: String,
    table: Arc<MemTable>,
    router: StorageRouter,
    /// Replication clients keyed by peer node-id.
    replica_clients: RwLock<HashMap<String, ReplicationClient>>,
    /// Paxos gRPC addresses of peers: node_id → paxos acceptor address.
    paxos_addrs: RwLock<HashMap<String, String>>,
    consistency: ConsistencyConfig,
    pub paxos: Option<PaxosAcceptorHandle>,
    ballot: AtomicU64,
    /// Optional WAL actor handle (`None` for RAM-only nodes).
    wal: Option<WalHandle>,
    stats: Arc<StorageStatsCell>,
    _server_task: StdMutex<Option<JoinHandle<()>>>,
}

impl StorageNode {
    /// Create a `StorageNode` and bind its gRPC servers.
    ///
    /// - `bind_addr`  — address for the storage replication + gateway gRPC server (`:0` in tests).
    /// - `paxos_addr` — address to bind the Paxos acceptor on; `None` disables Paxos.
    /// - `wal_path`   — directory for WAL + snapshot files; `None` for RAM-only mode.
    pub async fn new(
        id: String,
        ring: crate::hash_ring::HashRing,
        consistency: ConsistencyConfig,
        bind_addr: &str,
        paxos_addr: Option<&str>,
        wal_path: Option<PathBuf>,
    ) -> Result<Arc<Self>, StorageError> {
        let rf = consistency.rf;

        let paxos = if let Some(addr) = paxos_addr {
            let handle = crate::paxos_grpc::bind_paxos_acceptor_on_runtime(
                &tokio::runtime::Handle::current(),
                &id,
                addr,
            )
            .await
            .map_err(|e| StorageError(format!("bind paxos acceptor: {e}")))?;
            Some(handle)
        } else {
            None
        };

        // WAL startup sequence (Phase 6):
        // 1. If snapshot exists → load it into MemTable.
        // 2. Replay WAL entries with lsn > checkpoint_lsn.
        // 3. Open WAL actor for new writes.
        let (initial_table, wal_handle) = if let Some(ref dir) = wal_path {
            tokio::fs::create_dir_all(dir)
                .await
                .map_err(|e| StorageError(format!("create wal dir: {e}")))?;
            let snap = dir.join("snapshot.dat");
            let lsn_file = dir.join("snapshot.lsn");
            let wal_file = dir.join("wal.dat");

            let (table, checkpoint_lsn) = wal::load_snapshot(&snap, &lsn_file).await?;
            let replay_records = wal::replay(&wal_file, checkpoint_lsn).await?;
            for rec in replay_records {
                table.insert(rec);
            }

            let handle = WalHandle::open(&wal_file).await?;
            (Arc::new(table), Some(handle))
        } else {
            (Arc::new(MemTable::new(TableKind::Set)), None)
        };

        // Bind the TCP listener before building the node so we know the real address.
        let listener = tokio::net::TcpListener::bind(bind_addr)
            .await
            .map_err(|e| StorageError(format!("bind storage grpc {bind_addr}: {e}")))?;
        let address = listener
            .local_addr()
            .map_err(|e| StorageError(format!("local addr: {e}")))?
            .to_string();

        let node = Arc::new(StorageNode {
            id: id.clone(),
            address: address.clone(),
            table: initial_table,
            router: StorageRouter::new(ring, id, rf),
            replica_clients: RwLock::new(HashMap::new()),
            paxos_addrs: RwLock::new(HashMap::new()),
            consistency,
            paxos,
            ballot: AtomicU64::new(1),
            wal: wal_handle,
            stats: Arc::new(StorageStatsCell::new()),
            _server_task: StdMutex::new(None),
        });

        // Spawn the gRPC server and store the handle inside the already-Arc'd node.
        let svc_node = node.clone();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let task = tokio::spawn(async move {
            let storage_svc = StorageServiceServer::new(svc_node.clone());
            let gateway_svc = StorageGatewayServer::new(svc_node);
            if let Err(e) = tonic::transport::Server::builder()
                .add_service(storage_svc)
                .add_service(gateway_svc)
                .serve_with_incoming(incoming)
                .await
            {
                tracing::error!(error = %e, "storage gRPC server exited");
            }
        });
        *node._server_task.lock().unwrap() = Some(task);

        Ok(node)
    }

    /// Connect replication clients to peer nodes.
    ///
    /// `peers` is a list of `(node_id, storage_grpc_addr)` pairs.
    /// Optionally pass `paxos_addr` alongside each peer via [`connect_peers_with_paxos`].
    pub async fn connect_peers(
        &self,
        peers: &[(String, String)],
    ) -> Result<(), StorageError> {
        // Connect all peers before acquiring the write lock to avoid holding it across awaits.
        let mut connected = Vec::with_capacity(peers.len());
        for (node_id, addr) in peers {
            if node_id == &self.id {
                continue;
            }
            let client = ReplicationClient::connect(node_id.clone(), addr)
                .await
                .map_err(|e| StorageError(format!("connect peer {node_id} at {addr}: {e}")))?;
            connected.push((node_id.clone(), client));
        }
        let mut clients = self.replica_clients.write().unwrap();
        for (node_id, client) in connected {
            clients.insert(node_id, client);
        }
        Ok(())
    }

    /// Connect peers with both storage and Paxos addresses.
    pub async fn connect_peers_with_paxos(
        &self,
        peers: &[(String, String, String)], // (node_id, storage_addr, paxos_addr)
    ) -> Result<(), StorageError> {
        let mut connected = Vec::with_capacity(peers.len());
        for (node_id, addr, paxos_addr) in peers {
            if node_id == &self.id {
                continue;
            }
            let client = ReplicationClient::connect(node_id.clone(), addr)
                .await
                .map_err(|e| StorageError(format!("connect peer {node_id}: {e}")))?;
            connected.push((node_id.clone(), client, paxos_addr.clone()));
        }
        let mut clients = self.replica_clients.write().unwrap();
        let mut paxos_map = self.paxos_addrs.write().unwrap();
        for (node_id, client, paxos_addr) in connected {
            clients.insert(node_id.clone(), client);
            paxos_map.insert(node_id, paxos_addr);
        }
        Ok(())
    }

    // ── write path ──────────────────────────────────────────────────────────

    /// Store `(key, value)` with the given write consistency level.
    pub async fn put(
        self: &Arc<Self>,
        key: Key,
        value: Value,
        cl: WriteConsistency,
    ) -> Result<(), StorageError> {
        tracing::info!(key_len = key.len(), cl = ?cl, "storage.put");
        self.stats.puts_total.fetch_add(1, Ordering::Relaxed);

        if cl == WriteConsistency::Serial {
            self.stats.paxos_writes.fetch_add(1, Ordering::Relaxed);
            let result = self.paxos_put(key, value, false).await;
            if result.is_err() {
                self.stats.quorum_failures.fetch_add(1, Ordering::Relaxed);
            }
            return result;
        }

        let result = self.replicate_to_quorum(key, value, false, cl).await;
        if result.is_err() {
            self.stats.quorum_failures.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    /// Delete `key` by writing a tombstone.
    pub async fn delete(
        self: &Arc<Self>,
        key: &Key,
        cl: WriteConsistency,
    ) -> Result<(), StorageError> {
        tracing::info!(key_len = key.len(), cl = ?cl, "storage.delete");
        self.stats.deletes_total.fetch_add(1, Ordering::Relaxed);

        if cl == WriteConsistency::Serial {
            self.stats.paxos_writes.fetch_add(1, Ordering::Relaxed);
            let empty = Value::new();
            self.paxos_put(key.clone(), empty, false).await?;
            let ballot = self.ballot.fetch_add(1, Ordering::Relaxed);
            self.table.delete(key, ballot);
            return Ok(());
        }

        let result = self
            .replicate_to_quorum(key.clone(), Value::new(), true, cl)
            .await;
        if result.is_err() {
            self.stats.quorum_failures.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    /// Shared fan-out logic for both `put` and `delete`.
    async fn replicate_to_quorum(
        self: &Arc<Self>,
        key: Key,
        value: Value,
        tombstone: bool,
        cl: WriteConsistency,
    ) -> Result<(), StorageError> {
        let rf = self.consistency.rf;
        let local_rf = self.consistency.local_rf;
        let required = write_acks_required(cl, rf, local_rf, None)?;

        let replicas = self.router.replica_set_for(&key)?;

        let ballot = self.ballot.fetch_add(1, Ordering::Relaxed);
        let record = Record {
            key: key.clone(),
            value: value.clone(),
            ballot,
            tombstone,
            written_at: SystemTime::now(),
        };

        // WAL: append before any replication so the write survives a crash.
        if let Some(ref wal) = self.wal {
            match wal.append(&record).await {
                Ok(_) => {
                    let approx_bytes = record.key.len() + record.value.len() + 32;
                    self.stats
                        .wal_bytes_written
                        .fetch_add(approx_bytes as u64, Ordering::Relaxed);
                }
                Err(e) => {
                    tracing::error!(error = %e, "WAL append failure");
                    return Err(e);
                }
            }
        }

        // Write locally if this node is in the replica set for this key.
        let wrote_local = if self.router.is_local_replica(&key) {
            if tombstone {
                self.table.delete(&key, ballot);
            } else {
                self.table.insert(record.clone());
            }
            true
        } else {
            false
        };

        let peers = self.peers_for(&replicas, is_local_only(cl));

        match cl {
            WriteConsistency::Any => {
                // Fire-and-forget to all peers, return immediately.
                for (_, mut client) in peers {
                    let k = key.clone();
                    let v = value.clone();
                    tokio::spawn(async move {
                        let _ = client.replicate(k, v, ballot, tombstone, false).await;
                    });
                }
                Ok(())
            }

            WriteConsistency::One | WriteConsistency::LocalOne => {
                // Local write is sufficient; replicate asynchronously.
                for (_, mut client) in peers {
                    let k = key.clone();
                    let v = value.clone();
                    tokio::spawn(async move {
                        let _ = client.replicate(k, v, ballot, tombstone, false).await;
                    });
                }
                Ok(())
            }

            _ => {
                // Quorum / All: Cassandra fan-out — spawn ALL peer writes as detached
                // tasks so every replica eventually receives the write regardless of
                // whether we waited for its ack. Collect acks via an mpsc channel.
                let initial_acks = if wrote_local { 1usize } else { 0 };

                if initial_acks >= required {
                    // Local write alone satisfies the requirement; background-replicate.
                    for (_, mut client) in peers {
                        let k = key.clone();
                        let v = value.clone();
                        tokio::spawn(async move {
                            let _ = client.replicate(k, v, ballot, tombstone, false).await;
                        });
                    }
                    return Ok(());
                }

                let needed = required - initial_acks;
                let (ack_tx, mut ack_rx) = tokio::sync::mpsc::channel::<()>(peers.len() + 1);
                for (_, mut client) in peers {
                    let k = key.clone();
                    let v = value.clone();
                    let tx = ack_tx.clone();
                    // Detached spawn: runs to completion even after we've collected quorum.
                    tokio::spawn(async move {
                        if client.replicate(k, v, ballot, tombstone, true).await.is_ok() {
                            let _ = tx.send(()).await;
                        }
                    });
                }
                drop(ack_tx); // channel closes when all spawns finish

                let timeout = self.consistency.ack_timeout;
                let mut peer_acks = 0usize;
                let result = tokio::time::timeout(timeout, async {
                    while let Some(()) = ack_rx.recv().await {
                        peer_acks += 1;
                        if peer_acks >= needed {
                            return Ok(());
                        }
                    }
                    Err(peer_acks)
                })
                .await;

                let total_acks = initial_acks + peer_acks;
                match result {
                    Ok(Ok(())) => {
                        tracing::debug!(required, acks = total_acks, "write quorum satisfied");
                        Ok(())
                    }
                    Ok(Err(_)) => {
                        tracing::warn!(required, received = total_acks, "write quorum shortfall");
                        Err(StorageError(format!(
                            "not enough write acks: required {required}, got {total_acks}"
                        )))
                    }
                    Err(_) => {
                        // Timed out — check if we accumulated enough anyway.
                        if total_acks >= required {
                            Ok(())
                        } else {
                            tracing::warn!(required, received = total_acks, timeout_ms = timeout.as_millis(), "write timed out");
                            Err(StorageError(format!(
                                "write timed out: got {total_acks} of {required} acks"
                            )))
                        }
                    }
                }
            }
        }
    }

    // ── read path ───────────────────────────────────────────────────────────

    /// Retrieve `key` with the given read consistency level.
    /// Returns `None` if the key does not exist or has been deleted.
    pub async fn get(
        self: &Arc<Self>,
        key: &Key,
        cl: ReadConsistency,
    ) -> Result<Option<Value>, StorageError> {
        tracing::info!(key_len = key.len(), cl = ?cl, "storage.get");
        self.stats.gets_total.fetch_add(1, Ordering::Relaxed);

        if is_paxos_read(cl) {
            let local_only = matches!(cl, ReadConsistency::LocalSerial);
            return self.paxos_get(key, local_only).await;
        }

        let rf = self.consistency.rf;
        let local_rf = self.consistency.local_rf;
        let required = read_acks_required(cl, rf, local_rf)?;
        let replicas = self.router.replica_set_for(key)?;
        let peers = self.peers_for(&replicas, is_paxos_read(cl));

        // Gather: local read + peer reads concurrently.
        let mut join_set: tokio::task::JoinSet<(String, Option<Record>)> =
            tokio::task::JoinSet::new();

        // Local read as one "replica" response.
        let local_record = self.table.get_raw(key);
        let local_node_id = self.id.clone();
        join_set.spawn(async move { (local_node_id, local_record) });

        for (node_id, mut client) in peers {
            let k = key.clone();
            join_set.spawn(async move {
                let rec = client.read_replica(&k).await.unwrap_or(None);
                (node_id, rec)
            });
        }

        let timeout = self.consistency.ack_timeout;
        let responses: Vec<(String, Option<Record>)> = tokio::time::timeout(timeout, async {
            let mut out = Vec::new();
            while let Some(res) = join_set.join_next().await {
                if let Ok(pair) = res {
                    out.push(pair);
                    if out.len() >= required {
                        break;
                    }
                }
            }
            out
        })
        .await
        .unwrap_or_default();

        join_set.abort_all();

        if responses.len() < required {
            return Err(StorageError(format!(
                "not enough read acks: required {required}, got {}",
                responses.len()
            )));
        }

        // Pick winner: highest ballot.
        let winner: Option<&Record> = responses
            .iter()
            .filter_map(|(_, r)| r.as_ref())
            .max_by_key(|r| r.ballot);

        if let Some(winning) = winner {
            // Read repair: push winning value to stale replicas asynchronously.
            let stale_nodes: Vec<String> = responses
                .iter()
                .filter_map(|(nid, r)| {
                    let r_ballot = r.as_ref().map(|rec| rec.ballot).unwrap_or(0);
                    if r_ballot < winning.ballot {
                        Some(nid.clone())
                    } else {
                        None
                    }
                })
                .collect();

            if !stale_nodes.is_empty() {
                // Collect clients while holding the lock, then release before any await.
                let stale_clients: Vec<(String, ReplicationClient)> = {
                    let clients = self.replica_clients.read().unwrap();
                    stale_nodes
                        .iter()
                        .filter_map(|nid| {
                            clients.get(nid).map(|c| (nid.clone(), c.clone()))
                        })
                        .collect()
                }; // RwLockReadGuard dropped here
                let winning_clone = winning.clone();
                let repairs = stale_clients.len() as u64;
                self.stats.read_repairs.fetch_add(repairs, Ordering::Relaxed);
                tokio::spawn(async move {
                    for (nid, mut client) in stale_clients {
                        tracing::debug!(stale_node = %nid, "read repair triggered");
                        let _ = client
                            .replicate(
                                winning_clone.key.clone(),
                                winning_clone.value.clone(),
                                winning_clone.ballot,
                                winning_clone.tombstone,
                                false,
                            )
                            .await;
                    }
                });
            }

            if winning.tombstone {
                return Ok(None);
            }
            return Ok(Some(winning.value.clone()));
        }

        Ok(None)
    }

    // ── Paxos paths ─────────────────────────────────────────────────────────

    async fn paxos_put(
        self: &Arc<Self>,
        key: Key,
        value: Value,
        local_only: bool,
    ) -> Result<(), StorageError> {
        let replicas = self.router.replica_set_for(&key)?;
        let rf = self.consistency.rf;
        let quorum = rf / 2 + 1;
        let local_dc = self.consistency.local_dc.clone();
        let own_paxos_addr = self.paxos.as_ref().map(|h| h.address.clone());
        let own_id = self.id.clone();

        // Collect owned String addresses while holding the read lock, then release.
        let addrs: Vec<String> = {
            let paxos_addrs = self.paxos_addrs.read().unwrap();
            replicas
                .nodes
                .iter()
                .filter(|n| {
                    !local_only || n.dc.as_deref() == Some(&local_dc)
                })
                .filter_map(|n| {
                    if n.id == own_id {
                        own_paxos_addr.clone()
                    } else {
                        paxos_addrs.get(&n.id).cloned()
                    }
                })
                .collect()
        }; // paxos_addrs guard dropped here

        if addrs.is_empty() {
            return Err(StorageError("no paxos acceptors available".into()));
        }

        let addrs_ref: Vec<&str> = addrs.iter().map(|s| s.as_str()).collect();
        let mut client = PaxosProposerClient::connect(&addrs_ref)
            .await
            .map_err(|e| StorageError(format!("paxos connect: {e}")))?;

        let paxos_key = String::from_utf8_lossy(&key).into_owned();
        let timeout = self.consistency.ack_timeout;
        client
            .write(&paxos_key, value.to_vec(), quorum, timeout)
            .await?;

        // Reflect the committed value in the local table immediately.
        let ballot = self.ballot.fetch_add(1, Ordering::Relaxed);
        self.table.insert(Record {
            key,
            value,
            ballot,
            tombstone: false,
            written_at: SystemTime::now(),
        });

        Ok(())
    }

    async fn paxos_get(
        self: &Arc<Self>,
        key: &Key,
        local_only: bool,
    ) -> Result<Option<Value>, StorageError> {
        let replicas = self.router.replica_set_for(key)?;
        let rf = self.consistency.rf;
        let quorum = rf / 2 + 1;
        let local_dc = self.consistency.local_dc.clone();
        let own_paxos_addr = self.paxos.as_ref().map(|h| h.address.clone());
        let own_id = self.id.clone();

        let addrs: Vec<String> = {
            let paxos_addrs = self.paxos_addrs.read().unwrap();
            replicas
                .nodes
                .iter()
                .filter(|n| {
                    !local_only || n.dc.as_deref() == Some(&local_dc)
                })
                .filter_map(|n| {
                    if n.id == own_id {
                        own_paxos_addr.clone()
                    } else {
                        paxos_addrs.get(&n.id).cloned()
                    }
                })
                .collect()
        }; // paxos_addrs guard dropped here

        if addrs.is_empty() {
            return Ok(self.table.get(key).map(|r| r.value));
        }

        let addrs_ref: Vec<&str> = addrs.iter().map(|s| s.as_str()).collect();
        let mut client = PaxosProposerClient::connect(&addrs_ref)
            .await
            .map_err(|e| StorageError(format!("paxos connect: {e}")))?;

        let paxos_key = String::from_utf8_lossy(key).into_owned();
        let timeout = self.consistency.ack_timeout;
        let bytes = client.read(&paxos_key, quorum, timeout).await?;
        Ok(bytes.map(Value::from))
    }

    // ── Snapshot / repair ───────────────────────────────────────────────────

    /// Pull a full snapshot from `peer_node_id` and merge it into the local table.
    /// Only records with a higher ballot than what we have locally are applied.
    pub async fn sync_from_peer(&self, peer_node_id: &str) -> Result<(), StorageError> {
        let mut client = self
            .replica_clients
            .read()
            .unwrap()
            .get(peer_node_id)
            .cloned()
            .ok_or_else(|| {
                StorageError(format!("no replication client for peer {peer_node_id}"))
            })?;

        let mut stream = client.snapshot(0).await?;
        use tokio_stream::StreamExt as _;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|s| StorageError(format!("snapshot stream: {s}")))?;
            for rec in chunk.records {
                let key = Key::from(rec.key);
                let record = Record {
                    key: key.clone(),
                    value: Value::from(rec.value),
                    ballot: rec.ballot,
                    tombstone: rec.tombstone,
                    written_at: SystemTime::now(),
                };
                // Higher ballot wins.
                let should_apply = self
                    .table
                    .get_raw(&key)
                    .map(|existing| rec.ballot > existing.ballot)
                    .unwrap_or(true);
                if should_apply {
                    self.table.insert(record);
                }
            }
        }
        Ok(())
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    fn peers_for(&self, replicas: &ReplicaSet, local_only: bool) -> Vec<(String, ReplicationClient)> {
        let clients = self.replica_clients.read().unwrap();
        let local_dc = self.consistency.local_dc.as_str();
        replicas
            .nodes
            .iter()
            .filter(|n| n.id != self.id)
            .filter(|n| {
                !local_only || n.dc.as_deref() == Some(local_dc)
            })
            .filter_map(|n| clients.get(&n.id).map(|c| (n.id.clone(), c.clone())))
            .collect()
    }

    pub fn table(&self) -> &MemTable {
        &self.table
    }

    /// Snapshot of runtime counters (cheap — all atomic loads).
    pub fn stats(&self) -> StorageStats {
        let tombstones = self.table.tombstone_count();
        self.stats.snapshot(tombstones)
    }

    /// Lightweight health summary.
    pub fn health(&self) -> StorageHealth {
        let record_count = self.table.len();
        let tombstone_count = self.table.tombstone_count();
        let wal_bytes = self
            .wal
            .as_ref()
            .map(|w| w.bytes_appended.load(Ordering::Relaxed));
        let replica_peers = self.replica_clients.read().unwrap().len();
        StorageHealth {
            node_id: self.id.clone(),
            record_count,
            tombstone_count,
            wal_bytes_written: wal_bytes,
            replica_peers,
            ring_rf: self.consistency.rf,
        }
    }

    /// Atomically write a snapshot of the current MemTable and truncate the WAL.
    /// The node continues accepting writes during and after the checkpoint.
    /// Returns `Ok(())` for RAM-only nodes (no-op).
    pub async fn checkpoint(&self, dir: &std::path::Path) -> Result<(), StorageError> {
        let wal = match &self.wal {
            Some(w) => w,
            None => return Ok(()),
        };
        let records = self.table.all_with_tombstones();
        let snap = dir.join("snapshot.dat");
        let lsn_file = dir.join("snapshot.lsn");
        wal.checkpoint(snap, records, lsn_file).await
    }
}

// ── StorageService tonic impl ────────────────────────────────────────────────

#[tonic::async_trait]
impl StorageService for Arc<StorageNode> {
    async fn replicate(
        &self,
        request: Request<ReplicateRequest>,
    ) -> Result<Response<Ack>, Status> {
        let req = request.into_inner();
        let key = Key::from(req.key);
        let record = Record {
            key: key.clone(),
            value: Value::from(req.value),
            ballot: req.ballot,
            tombstone: req.tombstone,
            written_at: SystemTime::now(),
        };
        if req.tombstone {
            self.table.delete(&key, req.ballot);
        } else {
            self.table.insert(record);
        }
        Ok(Response::new(Ack {
            ok: true,
            error: String::new(),
        }))
    }

    async fn read_replica(
        &self,
        request: Request<ReadReplicaRequest>,
    ) -> Result<Response<ReadReplicaReply>, Status> {
        let key = Key::from(request.into_inner().key);
        match self.table.get_raw(&key) {
            Some(r) => Ok(Response::new(ReadReplicaReply {
                found: true,
                value: r.value.to_vec(),
                ballot: r.ballot,
                tombstone: r.tombstone,
            })),
            None => Ok(Response::new(ReadReplicaReply {
                found: false,
                value: Vec::new(),
                ballot: 0,
                tombstone: false,
            })),
        }
    }

    type SnapshotStream = tokio_stream::wrappers::ReceiverStream<Result<SnapshotChunk, Status>>;

    async fn snapshot(
        &self,
        request: Request<SnapshotRequest>,
    ) -> Result<Response<Self::SnapshotStream>, Status> {
        let from_ballot = request.into_inner().from_ballot;
        let records: Vec<_> = self
            .table
            .all_with_tombstones()
            .into_iter()
            .filter(|r| r.ballot >= from_ballot)
            .collect();

        let (tx, rx) = tokio::sync::mpsc::channel(32);
        tokio::spawn(async move {
            const CHUNK_SIZE: usize = 500;
            let mut chunks = records.chunks(CHUNK_SIZE).peekable();
            loop {
                match chunks.next() {
                    None => {
                        let _ = tx
                            .send(Ok(SnapshotChunk {
                                records: Vec::new(),
                                done: true,
                            }))
                            .await;
                        break;
                    }
                    Some(batch) => {
                        let proto_recs = batch
                            .iter()
                            .map(|r| crate::proto::storage::ReplicateRequest {
                                key: r.key.to_vec(),
                                value: r.value.to_vec(),
                                ballot: r.ballot,
                                tombstone: r.tombstone,
                                expect_ack: false,
                            })
                            .collect();
                        let done = chunks.peek().is_none();
                        let _ = tx
                            .send(Ok(SnapshotChunk {
                                records: proto_recs,
                                done,
                            }))
                            .await;
                        if done {
                            break;
                        }
                    }
                }
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }
}

// ── StorageGateway tonic impl ────────────────────────────────────────────────

#[tonic::async_trait]
impl StorageGateway for Arc<StorageNode> {
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<Ack>, Status> {
        let req = request.into_inner();
        let cl: WriteConsistency = req.write_consistency.parse().unwrap_or(WriteConsistency::Quorum);
        let key = Key::from(req.key);
        let value = Value::from(req.value);
        self.put(key, value, cl).await.map_err(Status::from)?;
        Ok(Response::new(Ack {
            ok: true,
            error: String::new(),
        }))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetReply>, Status> {
        let req = request.into_inner();
        let cl: ReadConsistency = req.read_consistency.parse().unwrap_or(ReadConsistency::Quorum);
        let key = Key::from(req.key);
        match self.get(&key, cl).await.map_err(Status::from)? {
            Some(v) => Ok(Response::new(GetReply {
                found: true,
                value: v.to_vec(),
            })),
            None => Ok(Response::new(GetReply {
                found: false,
                value: Vec::new(),
            })),
        }
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<Ack>, Status> {
        let req = request.into_inner();
        let cl: WriteConsistency = req.write_consistency.parse().unwrap_or(WriteConsistency::Quorum);
        let key = Key::from(req.key);
        self.delete(&key, cl).await.map_err(Status::from)?;
        Ok(Response::new(Ack {
            ok: true,
            error: String::new(),
        }))
    }

    type ScanStream = tokio_stream::wrappers::ReceiverStream<Result<ScanChunk, Status>>;

    async fn scan(&self, request: Request<ScanRequest>) -> Result<Response<Self::ScanStream>, Status> {
        let req = request.into_inner();
        // TODO: fan-out ReadReplica for each key to satisfy read quorum on scans (expensive; O(keys) RPCs)
        let start = Key::from(req.start);
        let end = Key::from(req.end);
        let records = self.table.scan(start..=end);

        let (tx, rx) = tokio::sync::mpsc::channel(32);
        tokio::spawn(async move {
            const CHUNK: usize = 500;
            for batch in records.chunks(CHUNK) {
                let items: Vec<GetReply> = batch
                    .iter()
                    .map(|r| GetReply {
                        found: true,
                        value: r.value.to_vec(),
                    })
                    .collect();
                let done = false;
                let _ = tx.send(Ok(ScanChunk { records: items, done })).await;
            }
            let _ = tx.send(Ok(ScanChunk { records: Vec::new(), done: true })).await;
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }
}

// ── FromStr for consistency levels ──────────────────────────────────────────

impl std::str::FromStr for WriteConsistency {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "ANY" => Ok(Self::Any),
            "ONE" => Ok(Self::One),
            "TWO" => Ok(Self::Two),
            "THREE" => Ok(Self::Three),
            "LOCAL_ONE" => Ok(Self::LocalOne),
            "LOCAL_QUORUM" => Ok(Self::LocalQuorum),
            "QUORUM" => Ok(Self::Quorum),
            "EACH_QUORUM" => Ok(Self::EachQuorum),
            "ALL" => Ok(Self::All),
            "SERIAL" => Ok(Self::Serial),
            other => Err(format!("unknown WriteConsistency: {other}")),
        }
    }
}

impl std::str::FromStr for ReadConsistency {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "ONE" => Ok(Self::One),
            "TWO" => Ok(Self::Two),
            "THREE" => Ok(Self::Three),
            "LOCAL_ONE" => Ok(Self::LocalOne),
            "LOCAL_QUORUM" => Ok(Self::LocalQuorum),
            "QUORUM" => Ok(Self::Quorum),
            "SERIAL" => Ok(Self::Serial),
            "LOCAL_SERIAL" => Ok(Self::LocalSerial),
            "ALL" => Ok(Self::All),
            other => Err(format!("unknown ReadConsistency: {other}")),
        }
    }
}

// ── helper fns ──────────────────────────────────────────────────────────────

// ── Phase 3.8: integration tests ────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash_ring::{HashRing, RingNode};

    fn single_node_ring(addr: &str) -> HashRing {
        let mut ring = HashRing::new(150);
        ring.add_node(RingNode::from_socket_addr("n1", addr));
        ring
    }

    async fn single_node(addr: &str) -> Arc<StorageNode> {
        let ring = single_node_ring(addr);
        let consistency = ConsistencyConfig {
            rf: 1,
            local_rf: 1,
            ..Default::default()
        };
        StorageNode::new("n1".into(), ring, consistency, "127.0.0.1:0", None, None)
            .await
            .expect("storage node")
    }

    #[tokio::test]
    async fn single_node_put_get_delete() {
        let node = single_node("127.0.0.1:0").await;

        let k = Key::from(b"hello".as_ref());
        let v = Value::from(b"world".as_ref());

        node.put(k.clone(), v.clone(), WriteConsistency::One)
            .await
            .unwrap();

        let got = node.get(&k, ReadConsistency::One).await.unwrap();
        assert_eq!(got.as_deref(), Some(b"world".as_ref()));

        node.delete(&k, WriteConsistency::One).await.unwrap();

        let after_delete = node.get(&k, ReadConsistency::One).await.unwrap();
        assert!(after_delete.is_none());
    }

    #[tokio::test]
    async fn missing_key_returns_none() {
        let node = single_node("127.0.0.1:0").await;
        let k = Key::from(b"no-such-key".as_ref());
        let got = node.get(&k, ReadConsistency::One).await.unwrap();
        assert!(got.is_none());
    }

    // ── Phase 4 helpers ───────────────────────────────────────────────────────

    async fn three_node_cluster() -> (Arc<StorageNode>, Arc<StorageNode>, Arc<StorageNode>) {
        // Build a 3-node ring with placeholder ports — the hash ring uses node-id
        // strings for virtual-node keys, so the addresses don't affect routing.
        let mut ring = HashRing::new(150);
        ring.add_node(RingNode::new("n1", "127.0.0.1", 9901));
        ring.add_node(RingNode::new("n2", "127.0.0.1", 9902));
        ring.add_node(RingNode::new("n3", "127.0.0.1", 9903));

        let cons = |rf: usize| ConsistencyConfig {
            rf,
            local_rf: rf,
            ..Default::default()
        };

        let n1 = StorageNode::new("n1".into(), ring.clone(), cons(3), "127.0.0.1:0", None, None)
            .await
            .expect("n1");
        let n2 = StorageNode::new("n2".into(), ring.clone(), cons(3), "127.0.0.1:0", None, None)
            .await
            .expect("n2");
        let n3 = StorageNode::new("n3".into(), ring.clone(), cons(3), "127.0.0.1:0", None, None)
            .await
            .expect("n3");

        // Connect each node's replication clients to its two peers.
        let addr1 = format!("http://{}", n1.address);
        let addr2 = format!("http://{}", n2.address);
        let addr3 = format!("http://{}", n3.address);

        n1.connect_peers(&[
            ("n2".into(), addr2.clone()),
            ("n3".into(), addr3.clone()),
        ])
        .await
        .expect("n1 peers");
        n2.connect_peers(&[
            ("n1".into(), addr1.clone()),
            ("n3".into(), addr3.clone()),
        ])
        .await
        .expect("n2 peers");
        n3.connect_peers(&[
            ("n1".into(), addr1.clone()),
            ("n2".into(), addr2.clone()),
        ])
        .await
        .expect("n3 peers");

        (n1, n2, n3)
    }

    // ── 4.1 Quorum write propagates to all 3 replicas ────────────────────────
    #[tokio::test]
    async fn quorum_write_all_three_have_record() {
        let (n1, n2, n3) = three_node_cluster().await;

        let k = Key::from(b"k41".as_ref());
        let v = Value::from(b"v41".as_ref());

        n1.put(k.clone(), v.clone(), WriteConsistency::Quorum)
            .await
            .expect("quorum write");

        // Give background replication time to propagate to all 3 nodes.
        tokio::time::sleep(tokio::time::Duration::from_millis(80)).await;

        assert!(n1.table().get(&k).is_some(), "n1 should have record");
        assert!(n2.table().get(&k).is_some(), "n2 should have record");
        assert!(n3.table().get(&k).is_some(), "n3 should have record");
    }

    // ── 4.2 Quorum read returns correct value from 2-of-3 replicas ───────────
    #[tokio::test]
    async fn quorum_read_two_of_three() {
        let (n1, n2, n3) = three_node_cluster().await;
        let _ = n3; // unused node; 2-of-3 quorum is satisfied by n1+n2.

        let k = Key::from(b"k42".as_ref());
        let v = Value::from(b"v42".as_ref());
        let ballot = 100u64;

        // Write directly to n1 and n2's MemTables (bypass put).
        n1.table().insert(Record {
            key: k.clone(),
            value: v.clone(),
            ballot,
            tombstone: false,
            written_at: SystemTime::now(),
        });
        n2.table().insert(Record {
            key: k.clone(),
            value: v.clone(),
            ballot,
            tombstone: false,
            written_at: SystemTime::now(),
        });

        let got = n1
            .get(&k, ReadConsistency::Quorum)
            .await
            .expect("quorum read");
        assert_eq!(got.as_deref(), Some(b"v42".as_ref()));
    }

    // ── 4.3 Read repair propagates winning value to stale replica ─────────────
    #[tokio::test]
    async fn read_repair_propagates() {
        let (n1, n2, n3) = three_node_cluster().await;

        let k = Key::from(b"k43".as_ref());
        let v = Value::from(b"v43".as_ref());
        let ballot = 200u64;

        // Only n1 has the record.
        n1.table().insert(Record {
            key: k.clone(),
            value: v.clone(),
            ballot,
            tombstone: false,
            written_at: SystemTime::now(),
        });

        // ReadConsistency::All ensures all 3 responses are collected so both n2
        // and n3 appear as stale and both get repaired.
        let got = n1
            .get(&k, ReadConsistency::All)
            .await
            .expect("read-all");
        assert_eq!(got.as_deref(), Some(b"v43".as_ref()));

        // Wait for async read repair to complete.
        tokio::time::sleep(tokio::time::Duration::from_millis(120)).await;

        let n2_has = n2.table().get(&k).is_some();
        let n3_has = n3.table().get(&k).is_some();
        assert!(
            n2_has || n3_has,
            "at least one stale replica should be repaired"
        );
    }

    // ── 4.4 Quorum write fails without peers ─────────────────────────────────
    #[tokio::test]
    async fn quorum_write_fails_without_peers() {
        // Single node with rf=3 ring but no peers connected → cannot reach quorum.
        let mut ring = HashRing::new(150);
        ring.add_node(RingNode::new("n1", "127.0.0.1", 9904));
        ring.add_node(RingNode::new("n2", "127.0.0.1", 9905));
        ring.add_node(RingNode::new("n3", "127.0.0.1", 9906));
        let cons = ConsistencyConfig {
            rf: 3,
            local_rf: 3,
            ..Default::default()
        };
        let n1 =
            StorageNode::new("n1".into(), ring, cons, "127.0.0.1:0", None, None)
                .await
                .expect("n1");

        let k = Key::from(b"k44".as_ref());
        let v = Value::from(b"v44".as_ref());
        let result = n1.put(k, v, WriteConsistency::Quorum).await;
        assert!(result.is_err(), "quorum write should fail without peers");
        let msg = result.unwrap_err().0;
        assert!(
            msg.contains("not enough") || msg.contains("timed out"),
            "unexpected error: {msg}"
        );
    }

    // ── 4.5 Node join via sync_from_peer ─────────────────────────────────────
    #[tokio::test]
    async fn node_join_sync_from_peer() {
        let mut ring = HashRing::new(150);
        ring.add_node(RingNode::new("n1", "127.0.0.1", 9907));
        ring.add_node(RingNode::new("n2", "127.0.0.1", 9908));
        ring.add_node(RingNode::new("n3", "127.0.0.1", 9909));

        let cons = ConsistencyConfig {
            rf: 1, // rf=1 so n1 is primary for all keys
            local_rf: 1,
            ..Default::default()
        };
        let n1 =
            StorageNode::new("n1".into(), ring.clone(), cons.clone(), "127.0.0.1:0", None, None)
                .await
                .expect("n1");
        let n3 =
            StorageNode::new("n3".into(), ring.clone(), cons, "127.0.0.1:0", None, None)
                .await
                .expect("n3");

        // Write 100 records directly to n1's table.
        for i in 0u64..100 {
            n1.table().insert(Record {
                key: Key::from(format!("sync-key-{i}").into_bytes()),
                value: Value::from(format!("v{i}").into_bytes()),
                ballot: i + 1,
                tombstone: false,
                written_at: SystemTime::now(),
            });
        }

        // n3 connects to n1 for snapshot.
        let n1_addr = format!("http://{}", n1.address);
        n3.connect_peers(&[("n1".into(), n1_addr)])
            .await
            .expect("connect n3→n1");

        n3.sync_from_peer("n1").await.expect("sync_from_peer");

        assert_eq!(
            n3.table().all().len(),
            100,
            "n3 should have all 100 records after sync"
        );
    }

    // ── Phase 6 WAL tests ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn wal_recovery_after_crash() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal_path = dir.path().to_path_buf();

        let mut ring = HashRing::new(150);
        ring.add_node(RingNode::from_socket_addr("n1", "127.0.0.1:0"));
        let cons = ConsistencyConfig {
            rf: 1,
            local_rf: 1,
            ..Default::default()
        };

        // 1. Write 50 records.
        {
            let node = StorageNode::new(
                "n1".into(),
                ring.clone(),
                cons.clone(),
                "127.0.0.1:0",
                None,
                Some(wal_path.clone()),
            )
            .await
            .expect("node");

            for i in 0u64..50 {
                let k = Key::from(format!("wal-k{i}").into_bytes());
                let v = Value::from(format!("wal-v{i}").into_bytes());
                node.put(k, v, WriteConsistency::One).await.expect("put");
            }
            // Flush so the WAL is fully on disk (actor flushes on post_stop,
            // but we explicitly flush here to be safe).
            node.wal.as_ref().unwrap().flush().await.expect("flush");
            // Drop the node — simulate crash (WAL actor's post_stop flushes).
        }

        // 2. Reconstruct node from the same WAL path.
        let node2 = StorageNode::new(
            "n1".into(),
            ring.clone(),
            cons.clone(),
            "127.0.0.1:0",
            None,
            Some(wal_path.clone()),
        )
        .await
        .expect("node2");

        for i in 0u64..50 {
            let k = Key::from(format!("wal-k{i}").into_bytes());
            let got = node2
                .get(&k, ReadConsistency::One)
                .await
                .expect("get");
            assert!(
                got.is_some(),
                "key wal-k{i} should survive recovery"
            );
        }
    }

    #[tokio::test]
    async fn checkpoint_and_wal_recovery() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal_path = dir.path().to_path_buf();

        let mut ring = HashRing::new(150);
        ring.add_node(RingNode::from_socket_addr("n1", "127.0.0.1:0"));
        let cons = ConsistencyConfig {
            rf: 1,
            local_rf: 1,
            ..Default::default()
        };

        {
            let node = StorageNode::new(
                "n1".into(),
                ring.clone(),
                cons.clone(),
                "127.0.0.1:0",
                None,
                Some(wal_path.clone()),
            )
            .await
            .expect("node");

            // Write 50 records, checkpoint, then 50 more.
            for i in 0u64..50 {
                let k = Key::from(format!("cp-k{i}").into_bytes());
                let v = Value::from(format!("cp-v{i}").into_bytes());
                node.put(k, v, WriteConsistency::One).await.expect("put");
            }

            node.checkpoint(&wal_path).await.expect("checkpoint");

            for i in 50u64..100 {
                let k = Key::from(format!("cp-k{i}").into_bytes());
                let v = Value::from(format!("cp-v{i}").into_bytes());
                node.put(k, v, WriteConsistency::One).await.expect("put");
            }

            node.wal.as_ref().unwrap().flush().await.expect("flush");
        }

        // Recover — all 100 keys should be present.
        let node2 = StorageNode::new(
            "n1".into(),
            ring.clone(),
            cons.clone(),
            "127.0.0.1:0",
            None,
            Some(wal_path.clone()),
        )
        .await
        .expect("node2");

        assert_eq!(node2.table().all().len(), 100, "all 100 keys after recovery");

        // WAL should only contain the 50 post-checkpoint entries.
        let wal_entries =
            wal::replay(&wal_path.join("wal.dat"), 0).await.expect("replay");
        assert_eq!(
            wal_entries.len(),
            50,
            "WAL should contain only post-checkpoint entries"
        );
    }

    // ── Phase 7.4 StorageClient round-trip ───────────────────────────────────
    #[tokio::test]
    async fn storage_client_round_trip() {
        let node = single_node("127.0.0.1:0").await;

        let mut client = StorageClient::connect(&format!("http://{}", node.address))
            .await
            .expect("client connect");

        let k = Key::from(b"hello".as_ref());
        let v = Value::from(b"world".as_ref());

        client.put(k.clone(), v.clone()).await.expect("put");

        let got = client.get(&k).await.expect("get");
        assert_eq!(got.as_deref(), Some(b"world".as_ref()));

        client.delete(&k).await.expect("delete");

        let after_del = client.get(&k).await.expect("get after delete");
        assert!(after_del.is_none());
    }
}
