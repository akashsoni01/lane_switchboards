//! Durable inbox journal.
//!
//! Append-only file of binary protocol frames (same codec as the wire):
//!
//! - `ChatMessage` / `GroupMessage` — a message persisted to a recipient's
//!   inbox (seq already assigned).
//! - `DeliveredAck` — tombstone: `from_user`'s copy of `message_id` was
//!   delivered.
//! - `SyncComplete { user_id, latest_seq }` — seq high-water mark for a user,
//!   written during compaction so sequence numbers never regress even after
//!   delivered entries are dropped.
//!
//! On startup the journal is replayed to rebuild the in-memory inboxes, then
//! compacted (rewritten with only the surviving state). Every append is
//! fsynced before `ServerAck` is sent, so an acked message survives a crash.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use bytes::BytesMut;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::codec::{Decoder, Encoder};
use tracing::{info, warn};

use super::codec::{FrameCodec, Packet};
use super::wire;
use super::MessengerError;

/// File name inside the durable directory.
const JOURNAL_FILE: &str = "inbox.wal";

/// Rebuilt inbox state for one user after replay.
#[derive(Default)]
pub(super) struct ReplayedInbox {
    /// Highest seq ever assigned (never regresses).
    pub next_seq: u64,
    /// Pending (undelivered) stored packets, ascending seq.
    pub pending: Vec<Packet>,
    /// Dedup keys of pending messages (`message_id` or `message_id:member`).
    pub seen: Vec<String>,
}

/// Open journal handle; appends are serialized by the caller (see
/// `State.journal: Option<Mutex<Journal>>`).
pub(super) struct Journal {
    file: File,
    path: PathBuf,
}

/// Dedup key for a stored packet (mirrors the router's store keys).
fn dedup_key(pkt: &Packet) -> Option<String> {
    match pkt {
        Packet::ChatMessage(m) => Some(m.message_id.clone()),
        Packet::GroupMessage(m) => Some(format!("{}:{}", m.message_id, m.to_user)),
        _ => None,
    }
}

fn recipient_of(pkt: &Packet) -> Option<&str> {
    match pkt {
        Packet::ChatMessage(m) => Some(&m.to_user),
        Packet::GroupMessage(m) => Some(&m.to_user),
        _ => None,
    }
}

fn seq_of(pkt: &Packet) -> u64 {
    match pkt {
        Packet::ChatMessage(m) => m.seq,
        Packet::GroupMessage(m) => m.seq,
        _ => 0,
    }
}

fn message_id_of(pkt: &Packet) -> Option<&str> {
    match pkt {
        Packet::ChatMessage(m) => Some(&m.message_id),
        Packet::GroupMessage(m) => Some(&m.message_id),
        _ => None,
    }
}

impl Journal {
    /// Open (creating if missing) the journal in `dir`, replay it into
    /// per-user inbox state, and compact the file to the surviving entries.
    pub(super) async fn open_and_replay(
        dir: &Path,
    ) -> Result<(Self, HashMap<String, ReplayedInbox>), MessengerError> {
        tokio::fs::create_dir_all(dir).await?;
        let path = dir.join(JOURNAL_FILE);

        let inboxes = match tokio::fs::read(&path).await {
            Ok(bytes) => replay_bytes(&bytes),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => HashMap::new(),
            Err(e) => return Err(e.into()),
        };

        // Compact: rewrite with high-water marks + pending messages only.
        let tmp = dir.join(format!("{JOURNAL_FILE}.tmp"));
        {
            let mut codec = FrameCodec::default();
            let mut buf = BytesMut::new();
            for (user, inbox) in &inboxes {
                codec
                    .encode(
                        Packet::SyncComplete(wire::SyncComplete {
                            delivered: 0,
                            latest_seq: inbox.next_seq,
                            user_id: user.clone(),
                        }),
                        &mut buf,
                    )
                    .map_err(|e| MessengerError::Protocol(format!("compact encode: {e}")))?;
                for pkt in &inbox.pending {
                    codec
                        .encode(pkt.clone(), &mut buf)
                        .map_err(|e| MessengerError::Protocol(format!("compact encode: {e}")))?;
                }
            }
            let mut f = File::create(&tmp).await?;
            f.write_all(&buf).await?;
            f.sync_data().await?;
        }
        tokio::fs::rename(&tmp, &path).await?;

        let file = OpenOptions::new().append(true).open(&path).await?;
        let total: usize = inboxes.values().map(|i| i.pending.len()).sum();
        info!(path = %path.display(), users = inboxes.len(), pending = total, "inbox journal replayed");
        Ok((Self { file, path }, inboxes))
    }

    /// Append one frame and fsync. Called before the corresponding
    /// `ServerAck` leaves the node.
    pub(super) async fn append(&mut self, pkt: &Packet) -> Result<(), MessengerError> {
        let mut codec = FrameCodec::default();
        let mut buf = BytesMut::new();
        codec.encode(pkt.clone(), &mut buf)?;
        self.file.write_all(&buf).await?;
        self.file.sync_data().await?;
        Ok(())
    }

    #[allow(dead_code)]
    pub(super) fn path(&self) -> &Path {
        &self.path
    }
}

/// Decode the journal byte stream and fold it into per-user inbox state.
/// Torn tails (partial final frame after a crash) are tolerated and dropped.
fn replay_bytes(bytes: &[u8]) -> HashMap<String, ReplayedInbox> {
    let mut inboxes: HashMap<String, ReplayedInbox> = HashMap::new();
    let mut codec = FrameCodec::default();
    let mut buf = BytesMut::from(bytes);
    loop {
        match codec.decode(&mut buf) {
            Ok(Some(pkt)) => match &pkt {
                Packet::SyncComplete(s) => {
                    let inbox = inboxes.entry(s.user_id.clone()).or_default();
                    inbox.next_seq = inbox.next_seq.max(s.latest_seq);
                }
                Packet::ChatMessage(_) | Packet::GroupMessage(_) => {
                    let (Some(user), Some(key)) = (recipient_of(&pkt), dedup_key(&pkt)) else {
                        continue;
                    };
                    let inbox = inboxes.entry(user.to_string()).or_default();
                    if inbox.seen.contains(&key) {
                        continue; // duplicate append (crash between insert+ack retry)
                    }
                    inbox.next_seq = inbox.next_seq.max(seq_of(&pkt));
                    inbox.seen.push(key);
                    inbox.pending.push(pkt);
                }
                Packet::DeliveredAck(a) => {
                    if let Some(inbox) = inboxes.get_mut(&a.from_user) {
                        inbox.pending.retain(|p| message_id_of(p) != Some(a.message_id.as_str()));
                    }
                }
                other => {
                    warn!(ty = ?other.packet_type(), "unexpected journal entry ignored");
                }
            },
            Ok(None) => break, // clean end (or torn tail waiting for more bytes)
            Err(e) => {
                warn!(error = %e, "journal replay stopped at corrupt entry (torn tail dropped)");
                break;
            }
        }
    }
    for inbox in inboxes.values_mut() {
        inbox.pending.sort_by_key(seq_of);
    }
    inboxes
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chat(id: &str, to: &str, seq: u64) -> Packet {
        Packet::ChatMessage(wire::ChatMessage {
            message_id: id.into(),
            from_user: "a".into(),
            to_user: to.into(),
            body: b"x".to_vec(),
            sent_at: 1,
            seq,
            media_id: String::new(),
        })
    }

    #[tokio::test]
    async fn replay_restores_pending_and_drops_delivered() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (mut j, inboxes) = Journal::open_and_replay(dir.path()).await.unwrap();
            assert!(inboxes.is_empty());
            j.append(&chat("m1", "bob", 1)).await.unwrap();
            j.append(&chat("m2", "bob", 2)).await.unwrap();
            j.append(&Packet::DeliveredAck(wire::DeliveredAck {
                message_id: "m1".into(),
                from_user: "bob".into(),
            }))
            .await
            .unwrap();
        }
        let (_j, inboxes) = Journal::open_and_replay(dir.path()).await.unwrap();
        let bob = inboxes.get("bob").expect("bob inbox");
        assert_eq!(bob.pending.len(), 1);
        assert_eq!(message_id_of(&bob.pending[0]), Some("m2"));
        assert_eq!(bob.next_seq, 2, "seq high-water mark survives");
    }

    #[tokio::test]
    async fn seq_never_regresses_after_full_delivery_and_compaction() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (mut j, _) = Journal::open_and_replay(dir.path()).await.unwrap();
            j.append(&chat("m1", "bob", 7)).await.unwrap();
            j.append(&Packet::DeliveredAck(wire::DeliveredAck {
                message_id: "m1".into(),
                from_user: "bob".into(),
            }))
            .await
            .unwrap();
        }
        // First reopen compacts to just the high-water mark…
        {
            let (_j, inboxes) = Journal::open_and_replay(dir.path()).await.unwrap();
            assert_eq!(inboxes.get("bob").map(|i| i.next_seq), Some(7));
        }
        // …which survives a second reopen.
        let (_j, inboxes) = Journal::open_and_replay(dir.path()).await.unwrap();
        assert_eq!(inboxes.get("bob").map(|i| i.next_seq), Some(7));
        assert!(inboxes.get("bob").is_some_and(|i| i.pending.is_empty()));
    }

    #[tokio::test]
    async fn torn_tail_is_tolerated() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (mut j, _) = Journal::open_and_replay(dir.path()).await.unwrap();
            j.append(&chat("m1", "bob", 1)).await.unwrap();
        }
        // Simulate a crash mid-write: append half a frame.
        let path = dir.path().join(super::JOURNAL_FILE);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.extend_from_slice(&[1u8, 0x20, 0, 0]); // truncated header
        std::fs::write(&path, bytes).unwrap();

        let (_j, inboxes) = Journal::open_and_replay(dir.path()).await.unwrap();
        assert_eq!(inboxes.get("bob").map(|i| i.pending.len()), Some(1));
    }
}
