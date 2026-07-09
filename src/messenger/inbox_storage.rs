//! Optional StorageNode-backed inbox replication.
//!
//! When configured, each inbox append is also written to a [`StorageNode`]
//! under key `messenger:inbox:{user}:{seq}`. On startup the gateway can warm
//! empty inboxes from storage (best-effort). The local WAL journal remains
//! the primary durability path; StorageNode provides cross-node replication
//! when RF > 1.

use std::sync::Arc;

use crate::consistency::{ReadConsistency, WriteConsistency};
use crate::messenger::codec::{FrameCodec, Packet};
use crate::storage::{Key, StorageNode, Value};
use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};
use tracing::warn;

/// Prefix for inbox entries in StorageNode.
pub const INBOX_KEY_PREFIX: &str = "messenger:inbox:";

/// Helper wrapping a [`StorageNode`] for messenger inbox replication.
#[derive(Clone)]
pub struct InboxStorage {
    storage: Arc<StorageNode>,
}

impl InboxStorage {
    pub fn new(storage: Arc<StorageNode>) -> Self {
        Self { storage }
    }

    fn key(user_id: &str, seq: u64) -> Key {
        Key::from(format!("{INBOX_KEY_PREFIX}{user_id}:{seq:020}").into_bytes())
    }

    /// Persist one inbox packet (best-effort; errors are logged).
    pub async fn put_packet(&self, user_id: &str, seq: u64, pkt: &Packet) {
        let mut codec = FrameCodec::default();
        let mut buf = BytesMut::new();
        if codec.encode(pkt.clone(), &mut buf).is_err() {
            return;
        }
        let key = Self::key(user_id, seq);
        if let Err(e) = self
            .storage
            .put(key, Value::from(buf.to_vec()), WriteConsistency::One)
            .await
        {
            warn!(user = %user_id, seq, error = %e, "inbox storage put failed");
        }
    }

    /// Load a single packet if present.
    pub async fn get_packet(&self, user_id: &str, seq: u64) -> Option<Packet> {
        let key = Self::key(user_id, seq);
        let got = self
            .storage
            .get(&key, ReadConsistency::One)
            .await
            .ok()??;
        let mut codec = FrameCodec::default();
        let mut buf = BytesMut::from(got.as_ref());
        codec.decode(&mut buf).ok()?
    }
}
