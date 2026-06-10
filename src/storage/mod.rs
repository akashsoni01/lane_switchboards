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
//!     "127.0.0.1:0", None,
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

pub use table::{Key, MemTable, Record, TableKind, Value};

use crate::consistency::{
    is_local_only, is_paxos_read, read_acks_required, write_acks_required, ConsistencyConfig,
    ReadConsistency, WriteConsistency,
};
use crate::paxos_grpc::{PaxosAcceptorHandle, PaxosProposerClient};
use crate::proto::storage::{
    storage_gateway_server::{StorageGateway, StorageGatewayServer},
    storage_service_server::{StorageService, StorageServiceServer},
    Ack, DeleteRequest, GetReply, GetRequest, PutRequest, ReadReplicaReply, ReadReplicaRequest,
    ReplicateRequest, ScanChunk, ScanRequest, SnapshotChunk, SnapshotRequest,
};
use replication::ReplicationClient;
use router::{ReplicaSet, StorageRouter};
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::SystemTime;
use std::sync::Mutex as StdMutex;
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
    _server_task: StdMutex<Option<JoinHandle<()>>>,
}

impl StorageNode {
    /// Create a `StorageNode` and bind its gRPC servers.
    ///
    /// - `bind_addr`  — address for the storage replication + gateway gRPC server (`:0` in tests).
    /// - `paxos_addr` — address to bind the Paxos acceptor on; `None` disables Paxos.
    pub async fn new(
        id: String,
        ring: crate::hash_ring::HashRing,
        consistency: ConsistencyConfig,
        bind_addr: &str,
        paxos_addr: Option<&str>,
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

        // Bind the TCP listener before building the node so we know the real address.
        let listener = tokio::net::TcpListener::bind(bind_addr)
            .await
            .map_err(|e| StorageError(format!("bind storage grpc {bind_addr}: {e}")))?;
        let address = listener.local_addr()
            .map_err(|e| StorageError(format!("local addr: {e}")))?
            .to_string();

        let node = Arc::new(StorageNode {
            id: id.clone(),
            address: address.clone(),
            table: Arc::new(MemTable::new(TableKind::Set)),
            router: StorageRouter::new(ring, id, rf),
            replica_clients: RwLock::new(HashMap::new()),
            paxos_addrs: RwLock::new(HashMap::new()),
            consistency,
            paxos,
            ballot: AtomicU64::new(1),
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

        if cl == WriteConsistency::Serial {
            return self.paxos_put(key, value, false).await;
        }

        self.replicate_to_quorum(key, value, false, cl).await
    }

    /// Delete `key` by writing a tombstone.
    pub async fn delete(
        self: &Arc<Self>,
        key: &Key,
        cl: WriteConsistency,
    ) -> Result<(), StorageError> {
        tracing::info!(key_len = key.len(), cl = ?cl, "storage.delete");

        if cl == WriteConsistency::Serial {
            // Tombstone via Paxos: value = empty bytes, tombstone flag handled locally after commit
            let empty = Value::new();
            self.paxos_put(key.clone(), empty, false).await?;
            let ballot = self.ballot.fetch_add(1, Ordering::Relaxed);
            self.table.delete(key, ballot);
            return Ok(());
        }

        self.replicate_to_quorum(key.clone(), Value::new(), true, cl)
            .await
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

        // Write locally if this node owns the key.
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
                // Quorum / All: collect acks from peers.
                let mut acks = if wrote_local { 1usize } else { 0 };

                if acks >= required {
                    // Already satisfied; fire-and-forget remainder.
                    for (_, mut client) in peers {
                        let k = key.clone();
                        let v = value.clone();
                        tokio::spawn(async move {
                            let _ = client.replicate(k, v, ballot, tombstone, false).await;
                        });
                    }
                    return Ok(());
                }

                let needed = required - acks;
                let mut join_set = tokio::task::JoinSet::new();
                for (_, mut client) in peers {
                    let k = key.clone();
                    let v = value.clone();
                    join_set.spawn(async move {
                        client.replicate(k, v, ballot, tombstone, true).await
                    });
                }

                let timeout = self.consistency.ack_timeout;
                let peer_result = tokio::time::timeout(timeout, async {
                    let mut peer_acks = 0usize;
                    while let Some(res) = join_set.join_next().await {
                        if let Ok(Ok(())) = res {
                            peer_acks += 1;
                            if peer_acks >= needed {
                                return Ok(peer_acks);
                            }
                        }
                    }
                    Err(peer_acks)
                })
                .await;
                join_set.abort_all();

                match peer_result {
                    Ok(Ok(_)) => {
                        tracing::debug!(required, acks = required, "write quorum satisfied");
                        Ok(())
                    }
                    Ok(Err(got)) => {
                        acks += got;
                        tracing::warn!(required, received = acks, "write quorum shortfall");
                        Err(StorageError(format!(
                            "not enough write acks: required {required}, got {acks}"
                        )))
                    }
                    Err(_) => {
                        tracing::warn!(
                            required,
                            timeout_ms = timeout.as_millis(),
                            "write timed out"
                        );
                        Err(StorageError(format!(
                            "write timed out after {:?}",
                            timeout
                        )))
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
        StorageNode::new("n1".into(), ring, consistency, "127.0.0.1:0", None)
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
}
