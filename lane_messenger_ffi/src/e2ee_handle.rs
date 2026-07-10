//! Opaque E2EE device handle (Olm + Megolm via vodozemac).

use std::sync::atomic::{AtomicU64, Ordering};

use lane_switchboards::messenger::E2eeDevice;
use parking_lot::Mutex;

use crate::error::FfiError;
use crate::events::KeyBundleInfo;
use crate::session::SessionHandle;

static NEXT_DEVICE_ID: AtomicU64 = AtomicU64::new(1);

/// Host-owned E2EE device (private keys never leave Rust).
pub struct E2eeHandle {
    id: u64,
    inner: Mutex<E2eeDevice>,
}

impl E2eeHandle {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn generate() -> Self {
        Self {
            id: NEXT_DEVICE_ID.fetch_add(1, Ordering::Relaxed),
            inner: Mutex::new(E2eeDevice::generate()),
        }
    }

    pub fn identity_key(&self) -> String {
        self.inner.lock().identity_key_base64()
    }

    pub fn safety_number(local_b64: &str, remote_b64: &str) -> String {
        E2eeDevice::safety_number(local_b64, remote_b64)
    }

    /// Export Olm account pickle (UTF-8 ciphertext). Derive a 32-byte key from
    /// `passphrase` via SHA-256 (hosts should prefer a KDF in production).
    pub fn export_pickle(&self, passphrase: &str) -> Vec<u8> {
        let key = passphrase_to_key(passphrase);
        self.inner.lock().export_account_pickle(&key).into_bytes()
    }

    pub fn import_pickle(bytes: &[u8], passphrase: &str) -> Result<Self, FfiError> {
        let encrypted = std::str::from_utf8(bytes)
            .map_err(|_| FfiError::InvalidArgument("pickle must be UTF-8".into()))?;
        let key = passphrase_to_key(passphrase);
        let device = E2eeDevice::import_account_pickle(encrypted, &key)
            .map_err(|e| FfiError::E2ee(e.to_string()))?;
        Ok(Self {
            id: NEXT_DEVICE_ID.fetch_add(1, Ordering::Relaxed),
            inner: Mutex::new(device),
        })
    }

    /// Publish identity + `otk_count` one-time keys on the connected session.
    pub fn publish(&self, session: &SessionHandle, device_id: &str, otk_count: u32) -> Result<(), FfiError> {
        let mut dev = self.inner.lock();
        let identity = dev.identity_key_base64();
        let keys = dev.generate_one_time_keys(otk_count as usize);
        drop(dev);
        session.publish_e2ee_keys(device_id, &identity, &keys)?;
        self.inner.lock().mark_keys_published();
        Ok(())
    }

    pub fn establish_session(
        &self,
        session: &SessionHandle,
        peer_user: &str,
    ) -> Result<KeyBundleInfo, FfiError> {
        let bundle = session.fetch_key_bundle(peer_user, "")?;
        if !bundle.found {
            return Err(FfiError::E2ee("peer key bundle not found".into()));
        }
        let mut dev = self.inner.lock();
        if !dev.has_olm_session(peer_user) {
            let peer = lane_switchboards::messenger::PeerKeyBundle {
                identity_key: bundle.identity_key.clone(),
                one_time_key: bundle.one_time_key.clone(),
            };
            dev.establish_outbound(peer_user, &peer)
                .map_err(|e| FfiError::E2ee(e.to_string()))?;
        }
        Ok(bundle)
    }

    pub fn encrypt_chat(&self, peer_user: &str, plaintext: &[u8]) -> Result<Vec<u8>, FfiError> {
        self.inner
            .lock()
            .encrypt_for_peer(peer_user, plaintext)
            .map_err(|e| FfiError::E2ee(e.to_string()))
    }

    pub fn decrypt_chat(&self, from_user: &str, body: &[u8]) -> Result<Vec<u8>, FfiError> {
        self.inner
            .lock()
            .decrypt_from_sender(from_user, body)
            .map_err(|e| FfiError::E2ee(e.to_string()))
    }

    pub fn send_encrypted_chat(
        &self,
        session: &SessionHandle,
        to_user: &str,
        message_id: &str,
        plaintext: &[u8],
    ) -> Result<u64, FfiError> {
        if !self.inner.lock().has_olm_session(to_user) {
            self.establish_session(session, to_user)?;
        }
        let ct = self.encrypt_chat(to_user, plaintext)?;
        session.send_encrypted_chat_body(to_user, message_id, &ct)
    }

    pub fn create_group_session(&self, group_id: &str) -> String {
        self.inner.lock().create_group_sender_session(group_id)
    }

    /// Olm-wrap the Megolm session key and send as 1:1 chat to each member.
    pub fn distribute_group_key(
        &self,
        session: &SessionHandle,
        group_id: &str,
        members: &[String],
    ) -> Result<(), FfiError> {
        let share = self
            .inner
            .lock()
            .group_session_key_share(group_id)
            .map_err(|e| FfiError::E2ee(e.to_string()))?;
        for member in members {
            if !self.inner.lock().has_olm_session(member) {
                self.establish_session(session, member)?;
            }
            let ct = self.encrypt_chat(member, &share)?;
            let mid = format!("gsk-{}-{}", group_id, member);
            session.send_encrypted_chat_body(member, &mid, &ct)?;
        }
        Ok(())
    }

    pub fn encrypt_group(&self, group_id: &str, plaintext: &[u8]) -> Result<Vec<u8>, FfiError> {
        self.inner
            .lock()
            .encrypt_group_message(group_id, plaintext)
            .map_err(|e| FfiError::E2ee(e.to_string()))
    }

    pub fn decrypt_group(&self, group_id: &str, body: &[u8]) -> Result<Vec<u8>, FfiError> {
        self.inner
            .lock()
            .decrypt_group_message(group_id, body)
            .map_err(|e| FfiError::E2ee(e.to_string()))
    }

    pub fn send_encrypted_group(
        &self,
        session: &SessionHandle,
        group_id: &str,
        message_id: &str,
        plaintext: &[u8],
    ) -> Result<(), FfiError> {
        let ct = self.encrypt_group(group_id, plaintext)?;
        session.send_encrypted_group_body(group_id, message_id, &ct)
    }

    pub fn try_import_group_key(&self, from_user: &str, body: &[u8]) -> Result<bool, FfiError> {
        self.inner
            .lock()
            .try_import_group_key_share(from_user, body)
            .map_err(|e| FfiError::E2ee(e.to_string()))
    }
}

fn passphrase_to_key(passphrase: &str) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(passphrase.as_bytes());
    let mut key = [0u8; 32];
    key.copy_from_slice(&digest);
    key
}
