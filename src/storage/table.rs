//! In-memory BTreeMap table for the storage layer.

use std::collections::BTreeMap;
use std::ops::RangeBounds;
use std::sync::RwLock;
use std::time::SystemTime;

pub type Key = bytes::Bytes;
pub type Value = bytes::Bytes;

/// A single versioned record in the table.
#[derive(Debug, Clone)]
pub struct Record {
    pub key: Key,
    pub value: Value,
    /// Last Paxos ballot that wrote this record. Higher ballot wins (LWW).
    pub ballot: u64,
    /// `true` means the key was deleted. Tombstones are never physically removed here.
    /// TODO: tombstone GC after gc_grace_seconds
    pub tombstone: bool,
    /// Wall-clock time of the write (diagnostic only — LWW uses ballot, not SystemTime).
    pub written_at: SystemTime,
}

/// Storage table shape (reserved for future scan / index optimisations).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableKind {
    Set,
    OrderedSet,
    Bag,
}

/// Concurrent, ordered, in-memory table backed by a `BTreeMap`.
pub struct MemTable {
    #[allow(dead_code)]
    kind: TableKind,
    data: RwLock<BTreeMap<Key, Record>>,
}

impl MemTable {
    pub fn new(kind: TableKind) -> Self {
        Self {
            kind,
            data: RwLock::new(BTreeMap::new()),
        }
    }

    /// Insert a record. Returns the displaced record if the key already existed.
    /// Higher ballot wins; a write with a lower ballot than the stored one is ignored.
    pub fn insert(&self, record: Record) -> Option<Record> {
        let mut data = self.data.write().unwrap();
        let prev = data.get(&record.key).cloned();
        if let Some(ref existing) = prev {
            if record.ballot < existing.ballot {
                return prev;
            }
        }
        data.insert(record.key.clone(), record);
        prev
    }

    /// Return a live (non-tombstone) record, or `None` if absent or deleted.
    pub fn get(&self, key: &Key) -> Option<Record> {
        let data = self.data.read().unwrap();
        data.get(key)
            .filter(|r| !r.tombstone)
            .cloned()
    }

    /// Return the raw record regardless of tombstone status.
    pub fn get_raw(&self, key: &Key) -> Option<Record> {
        self.data.read().unwrap().get(key).cloned()
    }

    /// Write a tombstone for `key`. The record stays in the map for read-repair
    /// and replication; GC happens externally.
    /// TODO: tombstone GC after gc_grace_seconds
    pub fn delete(&self, key: &Key, ballot: u64) -> Option<Record> {
        let prev = self.get_raw(key);
        if let Some(ref existing) = prev {
            if ballot < existing.ballot {
                return prev;
            }
        }
        let tombstone = Record {
            key: key.clone(),
            value: Value::new(),
            ballot,
            tombstone: true,
            written_at: SystemTime::now(),
        };
        self.data.write().unwrap().insert(key.clone(), tombstone);
        prev
    }

    /// Return all live records whose keys fall in `range`.
    pub fn scan(&self, range: impl RangeBounds<Key>) -> Vec<Record> {
        let data = self.data.read().unwrap();
        data.range(range)
            .filter(|(_, r)| !r.tombstone)
            .map(|(_, r)| r.clone())
            .collect()
    }

    /// Return all live records (tombstones excluded).
    pub fn all(&self) -> Vec<Record> {
        let data = self.data.read().unwrap();
        data.values()
            .filter(|r| !r.tombstone)
            .cloned()
            .collect()
    }

    /// Return all records including tombstones — used by replication snapshots.
    pub fn all_with_tombstones(&self) -> Vec<Record> {
        let data = self.data.read().unwrap();
        data.values().cloned().collect()
    }

    /// Number of entries including tombstones.
    pub fn len(&self) -> usize {
        self.data.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.read().unwrap().is_empty()
    }

    /// Number of tombstone entries.
    pub fn tombstone_count(&self) -> usize {
        self.data
            .read()
            .unwrap()
            .values()
            .filter(|r| r.tombstone)
            .count()
    }
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new(TableKind::Set)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(s: &str) -> Key {
        Key::from(s.as_bytes().to_vec())
    }

    fn val(s: &str) -> Value {
        Value::from(s.as_bytes().to_vec())
    }

    fn record(k: &str, v: &str, ballot: u64) -> Record {
        Record {
            key: key(k),
            value: val(v),
            ballot,
            tombstone: false,
            written_at: SystemTime::now(),
        }
    }

    #[test]
    fn insert_get_roundtrip() {
        let t = MemTable::default();
        t.insert(record("k", "v", 1));
        let r = t.get(&key("k")).unwrap();
        assert_eq!(r.value, val("v"));
        assert!(!r.tombstone);
    }

    #[test]
    fn delete_produces_tombstone_get_returns_none() {
        let t = MemTable::default();
        t.insert(record("k", "v", 1));
        t.delete(&key("k"), 2);
        assert!(t.get(&key("k")).is_none(), "should be hidden by tombstone");
        let raw = t.get_raw(&key("k")).unwrap();
        assert!(raw.tombstone);
    }

    #[test]
    fn scan_range_filters_correctly() {
        let t = MemTable::default();
        for k in ["a", "b", "c", "d"] {
            t.insert(record(k, k, 1));
        }
        t.delete(&key("c"), 2);
        let results = t.scan(key("a")..=key("c"));
        let keys: Vec<_> = results.iter().map(|r| r.key.clone()).collect();
        assert!(keys.contains(&key("a")));
        assert!(keys.contains(&key("b")));
        assert!(!keys.contains(&key("c")), "tombstone must be excluded");
    }

    #[test]
    fn all_excludes_tombstones() {
        let t = MemTable::default();
        t.insert(record("x", "1", 1));
        t.insert(record("y", "2", 1));
        t.delete(&key("y"), 2);
        let live = t.all();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].key, key("x"));
    }

    #[test]
    fn higher_ballot_wins_on_insert() {
        let t = MemTable::default();
        t.insert(record("k", "old", 5));
        t.insert(record("k", "new", 10));
        assert_eq!(t.get(&key("k")).unwrap().value, val("new"));
        // lower ballot ignored
        t.insert(record("k", "stale", 3));
        assert_eq!(t.get(&key("k")).unwrap().value, val("new"));
    }
}
