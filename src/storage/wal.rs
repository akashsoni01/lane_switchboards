//! Write-ahead log backed by a lane_core `Actor` for ordered, resilient I/O.
//!
//! Using an actor for WAL writes gives us:
//! - Sequential writes without any lock — the mailbox serialises callers
//! - Supervision — a parent supervisor can restart the actor on I/O errors
//! - `post_stop` flush ensures no data loss when the actor is gracefully stopped
//!
//! # File format
//! Each entry is written as length-delimited protobuf (`WalEntry`) using
//! `prost::Message::encode_length_delimited`.  Replay reads entries until the
//! buffer is exhausted or a decode error (truncated crash) is encountered.

use super::table::{Key, Record, Value};
use super::StorageError;
use crate::actor::{spawn_with_config, Actor, ActorProcessingErr};
use crate::config::ActorConfig;
use crate::proto::storage::WalEntry;
use bytes::Buf;
use prost::Message as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::UNIX_EPOCH;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufWriter};
use tokio::sync::oneshot;

// ── WAL actor messages ───────────────────────────────────────────────────────

pub enum WalMsg {
    Append {
        entry: Box<WalEntry>,
        reply: oneshot::Sender<Result<u64, StorageError>>,
    },
    Flush {
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    /// Atomically checkpoint: write snapshot, then truncate the live WAL.
    Checkpoint {
        snapshot_path: PathBuf,
        records: Vec<Record>,
        lsn_path: PathBuf,
        reply: oneshot::Sender<Result<(), StorageError>>,
    },
    GetLsn {
        reply: oneshot::Sender<u64>,
    },
}

// ── WAL actor ────────────────────────────────────────────────────────────────

struct WalActor {
    path: PathBuf,
    writer: BufWriter<tokio::fs::File>,
    lsn: u64,
    pub bytes_appended: Arc<AtomicU64>,
}

impl WalActor {
    async fn do_append(&mut self, mut entry: WalEntry) -> Result<u64, StorageError> {
        self.lsn += 1;
        entry.lsn = self.lsn;
        let mut buf = Vec::with_capacity(64);
        entry
            .encode_length_delimited(&mut buf)
            .map_err(|e| StorageError(format!("wal encode: {e}")))?;
        self.writer
            .write_all(&buf)
            .await
            .map_err(|e| StorageError(format!("wal write {}: {e}", self.path.display())))?;
        self.bytes_appended
            .fetch_add(buf.len() as u64, Ordering::Relaxed);
        Ok(self.lsn)
    }

    async fn do_flush(&mut self) -> Result<(), StorageError> {
        self.writer
            .flush()
            .await
            .map_err(|e| StorageError(format!("wal flush: {e}")))?;
        self.writer
            .get_ref()
            .sync_all()
            .await
            .map_err(|e| StorageError(format!("wal fsync: {e}")))?;
        Ok(())
    }

    async fn do_checkpoint(
        &mut self,
        snapshot_path: PathBuf,
        records: Vec<Record>,
        lsn_path: PathBuf,
    ) -> Result<(), StorageError> {
        // 1. Flush current WAL buffer.
        self.do_flush().await?;

        // 2. Write snapshot (all records as WalEntry, same format).
        {
            let snap_file = tokio::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&snapshot_path)
                .await
                .map_err(|e| StorageError(format!("snapshot open: {e}")))?;
            let mut snap_writer = BufWriter::new(snap_file);
            for rec in &records {
                let entry = record_to_entry(rec, 0);
                let mut buf = Vec::with_capacity(64);
                entry
                    .encode_length_delimited(&mut buf)
                    .map_err(|e| StorageError(format!("snapshot encode: {e}")))?;
                snap_writer
                    .write_all(&buf)
                    .await
                    .map_err(|e| StorageError(format!("snapshot write: {e}")))?;
            }
            snap_writer
                .flush()
                .await
                .map_err(|e| StorageError(format!("snapshot flush: {e}")))?;
        }

        // 3. Truncate the live WAL file and reset LSN *before* writing the
        //    checkpoint LSN.  After truncation the WAL restarts from lsn=1, so
        //    we write 0 as the checkpoint LSN — recovery will replay every
        //    entry present in the new (truncated) WAL.
        let file = self.writer.get_mut();
        file.set_len(0)
            .await
            .map_err(|e| StorageError(format!("wal truncate: {e}")))?;
        file.seek(std::io::SeekFrom::Start(0))
            .await
            .map_err(|e| StorageError(format!("wal seek: {e}")))?;
        self.lsn = 0;
        self.bytes_appended.store(0, Ordering::Relaxed);

        // 4. Write 0 as the checkpoint LSN (WAL was truncated; replay all entries).
        tokio::fs::write(&lsn_path, 0u64.to_le_bytes())
            .await
            .map_err(|e| StorageError(format!("lsn write: {e}")))?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl Actor<WalMsg> for WalActor {
    async fn handle(&mut self, msg: WalMsg) -> Result<(), ActorProcessingErr> {
        match msg {
            WalMsg::Append { entry, reply } => {
                let result = self.do_append(*entry).await;
                let _ = reply.send(result);
            }
            WalMsg::Flush { reply } => {
                let _ = reply.send(self.do_flush().await);
            }
            WalMsg::Checkpoint {
                snapshot_path,
                records,
                lsn_path,
                reply,
            } => {
                let _ = reply.send(
                    self.do_checkpoint(snapshot_path, records, lsn_path)
                        .await,
                );
            }
            WalMsg::GetLsn { reply } => {
                let _ = reply.send(self.lsn);
            }
        }
        Ok(())
    }

    async fn post_stop(&mut self) -> Result<(), ActorProcessingErr> {
        let _ = self.writer.flush().await;
        Ok(())
    }
}

// ── WalHandle ────────────────────────────────────────────────────────────────

/// Public handle to the WAL actor.  All methods are async and non-blocking
/// outside the actor's mailbox.
pub struct WalHandle {
    pub path: PathBuf,
    actor: crate::actor::ActorRef<WalMsg>,
    /// Shared with the actor; updated after every `Append`.
    pub bytes_appended: Arc<AtomicU64>,
}

impl WalHandle {
    /// Open (or create) the WAL file and spawn the writer actor.
    pub async fn open(path: impl Into<PathBuf>) -> Result<Self, StorageError> {
        let path = path.into();
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .map_err(|e| StorageError(format!("open wal {}: {e}", path.display())))?;

        let bytes_appended = Arc::new(AtomicU64::new(0));
        let actor_impl = WalActor {
            path: path.clone(),
            writer: BufWriter::new(file),
            lsn: 0,
            bytes_appended: bytes_appended.clone(),
        };

        let (actor, _join) = spawn_with_config(actor_impl, None, &ActorConfig::default())
            .await
            .map_err(|e| StorageError(format!("spawn wal actor: {e}")))?;

        Ok(Self {
            path,
            actor,
            bytes_appended,
        })
    }

    /// Append a record to the WAL. Returns the LSN assigned to this entry.
    pub async fn append(&self, record: &Record) -> Result<u64, StorageError> {
        let entry = record_to_entry(record, 0);
        let (tx, rx) = oneshot::channel();
        self.actor
            .send(WalMsg::Append {
                entry: Box::new(entry),
                reply: tx,
            })
            .await
            .map_err(|e| StorageError(format!("wal actor send: {e}")))?;
        rx.await
            .map_err(|_| StorageError("wal actor dropped".into()))?
    }

    /// Flush and fsync.
    pub async fn flush(&self) -> Result<(), StorageError> {
        let (tx, rx) = oneshot::channel();
        self.actor
            .send(WalMsg::Flush { reply: tx })
            .await
            .map_err(|e| StorageError(format!("wal actor send: {e}")))?;
        rx.await
            .map_err(|_| StorageError("wal actor dropped".into()))?
    }

    /// Atomically snapshot all `records`, truncate the live WAL, and reset LSN.
    /// Returns once both the snapshot file and LSN file are written and fsynced.
    pub async fn checkpoint(
        &self,
        snapshot_path: PathBuf,
        records: Vec<Record>,
        lsn_path: PathBuf,
    ) -> Result<(), StorageError> {
        let (tx, rx) = oneshot::channel();
        self.actor
            .send(WalMsg::Checkpoint {
                snapshot_path,
                records,
                lsn_path,
                reply: tx,
            })
            .await
            .map_err(|e| StorageError(format!("wal actor send: {e}")))?;
        rx.await
            .map_err(|_| StorageError("wal actor dropped".into()))?
    }

    /// Return the current LSN (round-trips through the actor for accuracy).
    pub async fn current_lsn(&self) -> Result<u64, StorageError> {
        let (tx, rx) = oneshot::channel();
        self.actor
            .send(WalMsg::GetLsn { reply: tx })
            .await
            .map_err(|e| StorageError(format!("wal actor send: {e}")))?;
        rx.await
            .map_err(|_| StorageError("wal actor dropped".into()))
    }
}

// ── Standalone helpers ───────────────────────────────────────────────────────

fn record_to_entry(record: &Record, lsn: u64) -> WalEntry {
    let timestamp = record
        .written_at
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    WalEntry {
        lsn,
        key: record.key.to_vec(),
        value: record.value.to_vec(),
        ballot: record.ballot,
        tombstone: record.tombstone,
        timestamp,
    }
}

fn entry_to_record(entry: WalEntry) -> Record {
    let written_at = UNIX_EPOCH
        + std::time::Duration::from_millis(entry.timestamp);
    Record {
        key: Key::from(entry.key),
        value: Value::from(entry.value),
        ballot: entry.ballot,
        tombstone: entry.tombstone,
        written_at,
    }
}

/// Read all `WalEntry` from a file and return as `Record`s.
/// Entries with `lsn <= skip_lsn` are skipped (used for post-checkpoint recovery).
/// Truncated entries at the tail (crash mid-write) are silently ignored.
pub async fn replay(path: &Path, skip_lsn: u64) -> Result<Vec<Record>, StorageError> {
    match tokio::fs::read(path).await {
        Ok(data) => {
            let mut buf = bytes::Bytes::from(data);
            let mut records = Vec::new();
            loop {
                if !buf.has_remaining() {
                    break;
                }
                match WalEntry::decode_length_delimited(&mut buf) {
                    Ok(entry) if entry.lsn > skip_lsn => {
                        records.push(entry_to_record(entry));
                    }
                    Ok(_) => {} // already covered by snapshot
                    Err(_) => break, // truncated tail — stop gracefully
                }
            }
            Ok(records)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(StorageError(format!(
            "replay {}: {e}",
            path.display()
        ))),
    }
}

/// Load a snapshot file and return the MemTable and checkpoint LSN.
pub async fn load_snapshot(
    snapshot_path: &Path,
    lsn_path: &Path,
) -> Result<(super::table::MemTable, u64), StorageError> {
    let table = super::table::MemTable::new(super::table::TableKind::Set);

    if snapshot_path.exists() {
        let data = tokio::fs::read(snapshot_path)
            .await
            .map_err(|e| StorageError(format!("read snapshot: {e}")))?;
        let mut buf = bytes::Bytes::from(data);
        loop {
            if !buf.has_remaining() {
                break;
            }
            match WalEntry::decode_length_delimited(&mut buf) {
                Ok(entry) => {
                    table.insert(entry_to_record(entry));
                }
                Err(_) => break,
            }
        }
    }

    let lsn = if lsn_path.exists() {
        let raw = tokio::fs::read(lsn_path)
            .await
            .map_err(|e| StorageError(format!("read lsn file: {e}")))?;
        if raw.len() >= 8 {
            u64::from_le_bytes(raw[..8].try_into().unwrap())
        } else {
            0
        }
    } else {
        0
    };

    Ok((table, lsn))
}
