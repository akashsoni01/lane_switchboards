//! Multi-device E2EE key directory with optional persistence.
//!
//! Keys are homed per user on the messenger gateway (cluster hash shard).
//! Each `(user_id, device_id)` pair stores identity + one-time prekeys.
//! Backends: in-memory (default), files under `durable_dir/keys/`, or
//! [`StorageNode`](crate::storage::StorageNode).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::consistency::{ReadConsistency, WriteConsistency};
use crate::storage::{Key, StorageNode, Value};

use super::wire;

/// Published public key material for one device.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredDeviceKeys {
    pub device_id: String,
    pub identity_key: String,
    pub one_time_keys: Vec<String>,
    pub published_at: u64,
}

impl StoredDeviceKeys {
    fn from_publish(k: &wire::PublishKeys, published_at: u64) -> Self {
        Self {
            device_id: k.device_id.clone(),
            identity_key: k.identity_key.clone(),
            one_time_keys: k.one_time_keys.clone(),
            published_at,
        }
    }
}

fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn storage_key(user_id: &str, device_id: &str) -> Key {
    Key::from(format!("messenger:keys:{user_id}:{device_id}").into_bytes())
}

fn device_path(base: &Path, user_id: &str, device_id: &str) -> PathBuf {
    base.join("keys").join(user_id).join(format!("{device_id}.json"))
}

/// Multi-device key directory.
pub struct KeyDirectory {
    devices: Mutex<HashMap<String, HashMap<String, StoredDeviceKeys>>>,
    storage: Option<Arc<StorageNode>>,
    persist_dir: Option<PathBuf>,
    publish_seq: AtomicU64,
}

impl KeyDirectory {
    /// In-memory only (lost on restart).
    pub fn memory() -> Self {
        Self {
            devices: Mutex::new(HashMap::new()),
            storage: None,
            persist_dir: None,
            publish_seq: AtomicU64::new(0),
        }
    }

    /// Persist keys as JSON under `{dir}/keys/{user}/{device}.json`.
    pub async fn with_persist_dir(dir: &Path) -> std::io::Result<Self> {
        let keys_root = dir.join("keys");
        tokio::fs::create_dir_all(&keys_root).await?;
        let mut devices: HashMap<String, HashMap<String, StoredDeviceKeys>> = HashMap::new();
        let mut users = tokio::fs::read_dir(&keys_root).await?;
        while let Some(user_entry) = users.next_entry().await? {
            if !user_entry.file_type().await?.is_dir() {
                continue;
            }
            let user_id = user_entry.file_name().to_string_lossy().into_owned();
            let mut per_user = HashMap::new();
            let mut devs = tokio::fs::read_dir(user_entry.path()).await?;
            while let Some(dev_entry) = devs.next_entry().await? {
                if !dev_entry.file_type().await?.is_file() {
                    continue;
                }
                let raw = tokio::fs::read_to_string(dev_entry.path()).await?;
                if let Ok(keys) = serde_json::from_str::<StoredDeviceKeys>(&raw) {
                    per_user.insert(keys.device_id.clone(), keys);
                }
            }
            if !per_user.is_empty() {
                devices.insert(user_id, per_user);
            }
        }
        Ok(Self {
            devices: Mutex::new(devices),
            storage: None,
            persist_dir: Some(dir.to_path_buf()),
            publish_seq: AtomicU64::new(unix_secs()),
        })
    }

    /// Back the directory with a [`StorageNode`] (keys survive node restart when
    /// the storage layer is durable).
    pub fn with_storage(storage: Arc<StorageNode>) -> Self {
        Self {
            devices: Mutex::new(HashMap::new()),
            storage: Some(storage),
            persist_dir: None,
            publish_seq: AtomicU64::new(0),
        }
    }

    async fn persist_one(&self, user_id: &str, keys: &StoredDeviceKeys) {
        if let Some(dir) = &self.persist_dir {
            let path = device_path(dir, user_id, &keys.device_id);
            if let Some(parent) = path.parent() {
                let _ = tokio::fs::create_dir_all(parent).await;
            }
            if let Ok(json) = serde_json::to_string(keys) {
                let _ = tokio::fs::write(path, json).await;
            }
        }
        if let Some(storage) = &self.storage {
            let key = storage_key(user_id, &keys.device_id);
            let bytes = serde_json::to_vec(keys).unwrap_or_default();
            let _ = storage
                .put(key, Value::from(bytes), WriteConsistency::One)
                .await;
        }
    }

    async fn remove_persisted(&self, user_id: &str, device_id: &str) {
        if let Some(dir) = &self.persist_dir {
            let path = device_path(dir, user_id, device_id);
            let _ = tokio::fs::remove_file(path).await;
        }
        if let Some(storage) = &self.storage {
            let key = storage_key(user_id, device_id);
            let _ = storage.delete(&key, WriteConsistency::One).await;
        }
    }

    async fn load_from_storage(&self, user_id: &str, device_id: &str) -> Option<StoredDeviceKeys> {
        let storage = self.storage.as_ref()?;
        let key = storage_key(user_id, device_id);
        let got = storage.get(&key, ReadConsistency::One).await.ok()??;
        serde_json::from_slice(got.as_ref()).ok()
    }

    /// Publish or rotate keys for one device (replaces prior keys for that device).
    pub async fn publish(&self, k: &wire::PublishKeys) {
        let user_id = k.user_id.clone();
        let seq = self.publish_seq.fetch_add(1, Ordering::Relaxed);
        let stored = StoredDeviceKeys::from_publish(k, seq);
        {
            let mut guard = self.devices.lock().await;
            guard
                .entry(user_id.clone())
                .or_default()
                .insert(stored.device_id.clone(), stored.clone());
        }
        self.persist_one(&user_id, &stored).await;
    }

    /// Remove a device's published keys (revocation / device logout).
    pub async fn remove_device(&self, user_id: &str, device_id: &str) {
        {
            let mut guard = self.devices.lock().await;
            if let Some(devs) = guard.get_mut(user_id) {
                devs.remove(device_id);
                if devs.is_empty() {
                    guard.remove(user_id);
                }
            }
        }
        self.remove_persisted(user_id, device_id).await;
    }

    fn pick_device<'a>(
        devs: &'a HashMap<String, StoredDeviceKeys>,
        device_id: &str,
    ) -> Option<&'a StoredDeviceKeys> {
        if device_id.is_empty() {
            devs.values().max_by_key(|d| d.published_at)
        } else {
            devs.get(device_id)
        }
    }

    /// Build a prekey bundle, consuming one one-time key from the chosen device.
    pub async fn fetch_bundle(
        &self,
        user_id: &str,
        device_id: &str,
        for_user: &str,
    ) -> wire::KeyBundle {
        let mut guard = self.devices.lock().await;
        let device_ids: Vec<String> = guard
            .get(user_id)
            .map(|d| d.keys().cloned().collect())
            .unwrap_or_default();

        if let Some(devs) = guard.get_mut(user_id) {
            if let Some(keys) = Self::pick_device(devs, device_id).cloned() {
                let mut keys = keys;
                let one_time = keys.one_time_keys.first().cloned().unwrap_or_default();
                if !one_time.is_empty() {
                    keys.one_time_keys.remove(0);
                }
                let has_keys = !keys.identity_key.is_empty() && !one_time.is_empty();
                devs.insert(keys.device_id.clone(), keys.clone());
                self.persist_one(user_id, &keys).await;
                return wire::KeyBundle {
                    user_id: user_id.to_string(),
                    device_id: keys.device_id,
                    identity_key: keys.identity_key,
                    one_time_key: one_time,
                    for_user: for_user.to_string(),
                    found: has_keys,
                    device_ids,
                };
            }
        }

        // Storage fallback: load device if not in memory cache.
        if !device_id.is_empty() {
            if let Some(mut keys) = self.load_from_storage(user_id, device_id).await {
                let one_time = keys.one_time_keys.first().cloned().unwrap_or_default();
                if !one_time.is_empty() {
                    keys.one_time_keys.remove(0);
                }
                let has_keys = !keys.identity_key.is_empty() && !one_time.is_empty();
                guard
                    .entry(user_id.to_string())
                    .or_default()
                    .insert(keys.device_id.clone(), keys.clone());
                self.persist_one(user_id, &keys).await;
                return wire::KeyBundle {
                    user_id: user_id.to_string(),
                    device_id: keys.device_id,
                    identity_key: keys.identity_key,
                    one_time_key: one_time,
                    for_user: for_user.to_string(),
                    found: has_keys,
                    device_ids,
                };
            }
        }

        wire::KeyBundle {
            user_id: user_id.to_string(),
            device_id: String::new(),
            identity_key: String::new(),
            one_time_key: String::new(),
            for_user: for_user.to_string(),
            found: false,
            device_ids,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn multi_device_publish_and_fetch() {
        let dir = KeyDirectory::memory();
        dir.publish(&wire::PublishKeys {
            user_id: "alice".into(),
            device_id: "phone".into(),
            identity_key: "id-phone".into(),
            one_time_keys: vec!["otk-p".into()],
        })
        .await;
        dir.publish(&wire::PublishKeys {
            user_id: "alice".into(),
            device_id: "laptop".into(),
            identity_key: "id-laptop".into(),
            one_time_keys: vec!["otk-l1".into(), "otk-l2".into()],
        })
        .await;

        let laptop = dir.fetch_bundle("alice", "laptop", "bob").await;
        assert!(laptop.found);
        assert_eq!(laptop.device_id, "laptop");
        assert_eq!(laptop.one_time_key, "otk-l1");

        let primary = dir.fetch_bundle("alice", "", "bob").await;
        assert!(primary.found);
        assert_eq!(primary.device_id, "laptop");
        assert_eq!(primary.one_time_key, "otk-l2");
    }

    #[tokio::test]
    async fn persist_dir_survives_reload() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = KeyDirectory::with_persist_dir(tmp.path()).await.unwrap();
        dir.publish(&wire::PublishKeys {
            user_id: "bob".into(),
            device_id: "d1".into(),
            identity_key: "id-bob".into(),
            one_time_keys: vec!["otk1".into(), "otk2".into()],
        })
        .await;

        let reloaded = KeyDirectory::with_persist_dir(tmp.path()).await.unwrap();
        let bundle = reloaded.fetch_bundle("bob", "d1", "alice").await;
        assert!(bundle.found);
        assert_eq!(bundle.one_time_key, "otk1");
        let bundle2 = reloaded.fetch_bundle("bob", "d1", "alice").await;
        assert_eq!(bundle2.one_time_key, "otk2");
    }
}
