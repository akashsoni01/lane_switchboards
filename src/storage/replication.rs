//! gRPC replication client — coordinator → replica fan-out.

use super::table::{Key, Record, Value};
use super::StorageError;
use crate::proto::storage::{
    storage_service_client::StorageServiceClient, ReadReplicaRequest, ReplicateRequest,
    SnapshotRequest,
};
use std::time::SystemTime;
use tonic::transport::Channel;

/// Client for the internal storage replication gRPC service on a peer node.
#[derive(Clone)]
pub struct ReplicationClient {
    pub node_id: String,
    inner: StorageServiceClient<Channel>,
}

impl ReplicationClient {
    pub async fn connect(
        node_id: String,
        addr: &str,
    ) -> Result<Self, tonic::transport::Error> {
        let uri = crate::distributed::grpc_data_endpoint(addr, false);
        let inner = StorageServiceClient::connect(uri).await?;
        Ok(Self { node_id, inner })
    }

    /// Send a write to this replica.
    ///
    /// `expect_ack = false` → fire-and-forget (returns immediately after sending).
    /// `expect_ack = true`  → waits for the replica's `Ack` before returning.
    pub async fn replicate(
        &mut self,
        key: Key,
        value: Value,
        ballot: u64,
        tombstone: bool,
        expect_ack: bool,
    ) -> Result<(), StorageError> {
        let req = ReplicateRequest {
            key: key.to_vec(),
            value: value.to_vec(),
            ballot,
            tombstone,
            expect_ack,
        };
        let resp = self
            .inner
            .replicate(req)
            .await
            .map_err(|s| StorageError(format!("replicate rpc to {}: {s}", self.node_id)))?
            .into_inner();
        if !resp.ok {
            return Err(StorageError(format!(
                "replicate rejected by {}: {}",
                self.node_id, resp.error
            )));
        }
        Ok(())
    }

    /// Request this replica's local view of `key`.
    pub async fn read_replica(&mut self, key: &Key) -> Result<Option<Record>, StorageError> {
        let req = ReadReplicaRequest {
            key: key.to_vec(),
        };
        let resp = self
            .inner
            .read_replica(req)
            .await
            .map_err(|s| StorageError(format!("read_replica rpc to {}: {s}", self.node_id)))?
            .into_inner();
        if !resp.found {
            return Ok(None);
        }
        Ok(Some(Record {
            key: key.clone(),
            value: Value::from(resp.value),
            ballot: resp.ballot,
            tombstone: resp.tombstone,
            written_at: SystemTime::now(),
        }))
    }

    /// Open a streaming snapshot from this replica.
    pub async fn snapshot(
        &mut self,
        from_ballot: u64,
    ) -> Result<
        tonic::Streaming<crate::proto::storage::SnapshotChunk>,
        StorageError,
    > {
        let stream = self
            .inner
            .snapshot(SnapshotRequest { from_ballot })
            .await
            .map_err(|s| StorageError(format!("snapshot rpc to {}: {s}", self.node_id)))?
            .into_inner();
        Ok(stream)
    }
}
