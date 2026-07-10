//! End-to-end encryption: 1:1 Olm (Double Ratchet) and group Megolm sender-keys.
//!
//! The server only stores public keys and routes opaque message bodies;
//! encryption/decryption happens entirely on clients.

use std::collections::HashMap;

use prost::Message;
use sha2::{Digest, Sha256};
use vodozemac::Curve25519PublicKey;
use vodozemac::megolm::{
    GroupSession, InboundGroupSession, MegolmMessage, SessionConfig, SessionKey,
};
use vodozemac::olm::{Account, OlmMessage, Session, SessionConfig as OlmSessionConfig};

use super::wire;

/// Errors from the E2EE layer.
#[derive(Debug, thiserror::Error)]
pub enum E2eeError {
    #[error("vodozemac decode: {0}")]
    Decode(#[from] vodozemac::DecodeError),
    #[error("vodozemac key: {0}")]
    Key(#[from] vodozemac::KeyError),
    #[error("megolm session key: {0}")]
    SessionKey(#[from] vodozemac::megolm::SessionKeyDecodeError),
    #[error("megolm decryption: {0}")]
    MegolmDecryption(#[from] vodozemac::megolm::DecryptionError),
    #[error("session creation: {0}")]
    SessionCreation(#[from] vodozemac::olm::SessionCreationError),
    #[error("encryption: {0}")]
    Encryption(#[from] vodozemac::olm::EncryptionError),
    #[error("decryption: {0}")]
    Decryption(#[from] vodozemac::olm::DecryptionError),
    #[error("prost decode: {0}")]
    Prost(#[from] prost::DecodeError),
    #[error("no Olm session with {0}; call establish_outbound first")]
    NoSession(String),
    #[error("no Megolm outbound session for group {0}")]
    NoGroupSession(String),
    #[error("no Megolm inbound session {session_id} for group {group_id}")]
    NoInboundGroupSession { group_id: String, session_id: String },
    #[error("peer key bundle incomplete")]
    IncompleteBundle,
    #[error("expected a pre-key message to create a new session")]
    NotPreKeyMessage,
    #[error("account pickle: {0}")]
    Pickle(String),
}

/// Public key material fetched from the server's key directory.
#[derive(Debug, Clone)]
pub struct PeerKeyBundle {
    pub identity_key: String,
    pub one_time_key: String,
}

impl PeerKeyBundle {
    pub fn from_wire(b: &wire::KeyBundle) -> Result<Self, E2eeError> {
        if !b.found || b.identity_key.is_empty() || b.one_time_key.is_empty() {
            return Err(E2eeError::IncompleteBundle);
        }
        Ok(Self {
            identity_key: b.identity_key.clone(),
            one_time_key: b.one_time_key.clone(),
        })
    }
}

/// One device's Olm account, per-peer sessions, and Megolm group state.
pub struct E2eeDevice {
    account: Account,
    sessions: HashMap<String, Session>,
    group_outbound: HashMap<String, GroupSession>,
    group_inbound: HashMap<String, HashMap<String, InboundGroupSession>>,
}

impl E2eeDevice {
    /// Generate a fresh device with a new identity keypair.
    pub fn generate() -> Self {
        Self {
            account: Account::new(),
            sessions: HashMap::new(),
            group_outbound: HashMap::new(),
            group_inbound: HashMap::new(),
        }
    }

    /// Curve25519 identity key (base64), published via `PublishKeys`.
    pub fn identity_key_base64(&self) -> String {
        self.account.curve25519_key().to_base64()
    }

    /// Generate one-time prekeys and return their base64 public forms.
    pub fn generate_one_time_keys(&mut self, count: usize) -> Vec<String> {
        self.account.generate_one_time_keys(count);
        self.account
            .one_time_keys()
            .values()
            .map(|k| k.to_base64())
            .collect()
    }

    /// Mark the current one-time keys as published (call after `PublishKeys`).
    pub fn mark_keys_published(&mut self) {
        self.account.mark_keys_as_published();
    }

    /// Signal-style numeric safety number from two identity keys (hex groups).
    pub fn safety_number(local_identity_b64: &str, remote_identity_b64: &str) -> String {
        let mut keys = [local_identity_b64, remote_identity_b64];
        keys.sort_unstable();
        let digest = Sha256::digest(format!("{}{}", keys[0], keys[1]).as_bytes());
        let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
        hex.chars()
            .collect::<Vec<_>>()
            .chunks(5)
            .map(|c| c.iter().collect::<String>())
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// True if an outbound/inbound Olm session exists for `peer`.
    pub fn has_olm_session(&self, peer: &str) -> bool {
        self.sessions.contains_key(peer)
    }

    /// Start an outbound Olm session with `peer` using their published bundle.
    pub fn establish_outbound(&mut self, peer: &str, bundle: &PeerKeyBundle) -> Result<(), E2eeError> {
        let identity = Curve25519PublicKey::from_base64(&bundle.identity_key)?;
        let otk = Curve25519PublicKey::from_base64(&bundle.one_time_key)?;
        let session = self
            .account
            .create_outbound_session(OlmSessionConfig::version_1(), identity, otk)?;
        self.sessions.insert(peer.to_string(), session);
        Ok(())
    }

    /// Encrypt `plaintext` for `peer` and return a prost-encoded
    /// [`wire::EncryptedPayload`] suitable for `ChatMessage.body`.
    pub fn encrypt_for_peer(&mut self, peer: &str, plaintext: &[u8]) -> Result<Vec<u8>, E2eeError> {
        let session = self
            .sessions
            .get_mut(peer)
            .ok_or_else(|| E2eeError::NoSession(peer.to_string()))?;
        let olm = session.encrypt(plaintext)?;
        Ok(encode_olm_payload(&self.account.curve25519_key().to_base64(), &olm))
    }

    /// Decrypt a `ChatMessage.body` from `sender_user`.
    pub fn decrypt_from_sender(&mut self, sender_user: &str, body: &[u8]) -> Result<Vec<u8>, E2eeError> {
        let (sender_key, _msg_type, olm) = decode_olm_payload(body)?;
        if let Some(session) = self.sessions.get_mut(sender_user) {
            return session.decrypt(&olm).map_err(E2eeError::from);
        }
        let identity = Curve25519PublicKey::from_base64(&sender_key)?;
        let OlmMessage::PreKey(ref prekey) = olm else {
            return Err(E2eeError::NotPreKeyMessage);
        };
        let result = self.account.create_inbound_session(
            OlmSessionConfig::version_1(),
            identity,
            prekey,
        )?;
        let plaintext = result.plaintext;
        self.sessions.insert(sender_user.to_string(), result.session);
        if !plaintext.is_empty() {
            return Ok(plaintext);
        }
        self.sessions
            .get_mut(sender_user)
            .expect("session just inserted")
            .decrypt(&olm)
            .map_err(E2eeError::from)
    }

    /// If `body` decrypts to a [`wire::GroupSessionKeyShare`], import the Megolm
    /// session key and return `true`.
    pub fn try_import_group_key_share(
        &mut self,
        sender_user: &str,
        body: &[u8],
    ) -> Result<bool, E2eeError> {
        let plain = self.decrypt_from_sender(sender_user, body)?;
        let Ok(share) = wire::GroupSessionKeyShare::decode(plain.as_slice()) else {
            return Ok(false);
        };
        self.import_group_session_key(&share.group_id, &share.session_id, &share.session_key)?;
        Ok(true)
    }

    // ---- Megolm group sender-keys -------------------------------------------

    /// Create an outbound Megolm session for `group_id`; returns session id.
    pub fn create_group_sender_session(&mut self, group_id: &str) -> String {
        let session = GroupSession::new(SessionConfig::version_1());
        let session_id = session.session_id();
        self.group_outbound.insert(group_id.to_string(), session);
        session_id
    }

    /// Portable session key (base64) for distributing to group members.
    pub fn export_group_session_key(&self, group_id: &str) -> Result<String, E2eeError> {
        let session = self
            .group_outbound
            .get(group_id)
            .ok_or_else(|| E2eeError::NoGroupSession(group_id.to_string()))?;
        Ok(session.session_key().to_base64())
    }

    /// Build a prost [`wire::GroupSessionKeyShare`] for Olm distribution.
    pub fn group_session_key_share(&self, group_id: &str) -> Result<Vec<u8>, E2eeError> {
        let session = self
            .group_outbound
            .get(group_id)
            .ok_or_else(|| E2eeError::NoGroupSession(group_id.to_string()))?;
        let share = wire::GroupSessionKeyShare {
            group_id: group_id.to_string(),
            session_id: session.session_id(),
            session_key: session.session_key().to_base64(),
        };
        Ok(share.encode_to_vec())
    }

    /// Import a Megolm session key received from a group member.
    pub fn import_group_session_key(
        &mut self,
        group_id: &str,
        session_id: &str,
        session_key_b64: &str,
    ) -> Result<(), E2eeError> {
        let key = SessionKey::from_base64(session_key_b64)?;
        let inbound = InboundGroupSession::new(&key, SessionConfig::version_1());
        self.group_inbound
            .entry(group_id.to_string())
            .or_default()
            .insert(session_id.to_string(), inbound);
        Ok(())
    }

    /// Encrypt a group message body (Megolm); suitable for `GroupMessage.body`.
    pub fn encrypt_group_message(&mut self, group_id: &str, plaintext: &[u8]) -> Result<Vec<u8>, E2eeError> {
        let session = self
            .group_outbound
            .get_mut(group_id)
            .ok_or_else(|| E2eeError::NoGroupSession(group_id.to_string()))?;
        let msg = session.encrypt(plaintext);
        let payload = wire::EncryptedGroupPayload {
            session_id: session.session_id(),
            ciphertext: msg.to_bytes(),
        };
        Ok(payload.encode_to_vec())
    }

    /// Decrypt a `GroupMessage.body` encrypted with Megolm.
    pub fn decrypt_group_message(&mut self, group_id: &str, body: &[u8]) -> Result<Vec<u8>, E2eeError> {
        let payload = wire::EncryptedGroupPayload::decode(body)?;
        let megolm = MegolmMessage::from_bytes(&payload.ciphertext)?;
        let inbound = self
            .group_inbound
            .get_mut(group_id)
            .and_then(|m| m.get_mut(&payload.session_id))
            .ok_or_else(|| E2eeError::NoInboundGroupSession {
                group_id: group_id.to_string(),
                session_id: payload.session_id,
            })?;
        let decrypted = inbound.decrypt(&megolm)?;
        Ok(decrypted.plaintext)
    }

    /// True when `body` is a valid prost `EncryptedPayload`.
    pub fn is_encrypted_body(body: &[u8]) -> bool {
        wire::EncryptedPayload::decode(body).is_ok()
    }

    /// True when `body` is a valid prost `EncryptedGroupPayload`.
    pub fn is_encrypted_group_body(body: &[u8]) -> bool {
        wire::EncryptedGroupPayload::decode(body).is_ok()
    }

    /// True if raw `needle` appears in `body` (used to assert server opacity).
    pub fn body_contains_substring(body: &[u8], needle: &[u8]) -> bool {
        body.windows(needle.len()).any(|w| w == needle)
    }

    /// Encrypt the Olm account pickle with a 32-byte key (sessions not included;
    /// re-establish Olm/Megolm after import).
    pub fn export_account_pickle(&self, pickle_key: &[u8; 32]) -> String {
        self.account.pickle().encrypt(pickle_key)
    }

    /// Restore a device from [`Self::export_account_pickle`].
    pub fn import_account_pickle(
        encrypted: &str,
        pickle_key: &[u8; 32],
    ) -> Result<Self, E2eeError> {
        let pickle = vodozemac::olm::AccountPickle::from_encrypted(encrypted, pickle_key)
            .map_err(|e| E2eeError::Pickle(e.to_string()))?;
        Ok(Self {
            account: Account::from_pickle(pickle),
            sessions: HashMap::new(),
            group_outbound: HashMap::new(),
            group_inbound: HashMap::new(),
        })
    }
}

fn encode_olm_payload(sender_key: &str, olm: &OlmMessage) -> Vec<u8> {
    let (message_type, ciphertext) = olm.to_parts();
    let payload = wire::EncryptedPayload {
        sender_key: sender_key.to_string(),
        message_type: message_type as u32,
        ciphertext,
    };
    payload.encode_to_vec()
}

fn decode_olm_payload(body: &[u8]) -> Result<(String, u32, OlmMessage), E2eeError> {
    let payload = wire::EncryptedPayload::decode(body)?;
    let olm = OlmMessage::from_parts(payload.message_type as usize, &payload.ciphertext)?;
    Ok((payload.sender_key, payload.message_type, olm))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_pair() -> (E2eeDevice, E2eeDevice, PeerKeyBundle) {
        let mut bob = E2eeDevice::generate();
        let otk = bob.generate_one_time_keys(1);
        bob.mark_keys_published();
        let bundle = PeerKeyBundle {
            identity_key: bob.identity_key_base64(),
            one_time_key: otk[0].clone(),
        };
        (E2eeDevice::generate(), bob, bundle)
    }

    #[test]
    fn session_establishment_and_round_trip() {
        let (mut alice, mut bob, bundle) = setup_pair();
        alice.establish_outbound("bob", &bundle).unwrap();

        let secret = b"forward-secret payload";
        let body = alice.encrypt_for_peer("bob", secret).unwrap();
        assert!(E2eeDevice::is_encrypted_body(&body));
        assert!(!E2eeDevice::body_contains_substring(&body, secret));

        let plain = bob.decrypt_from_sender("alice", &body).unwrap();
        assert_eq!(plain, secret);

        let reply = bob.encrypt_for_peer("alice", b"acknowledged").unwrap();
        let got = alice.decrypt_from_sender("bob", &reply).unwrap();
        assert_eq!(got, b"acknowledged");
    }

    #[test]
    fn out_of_order_messages_decrypt() {
        let (mut alice, mut bob, bundle) = setup_pair();
        alice.establish_outbound("bob", &bundle).unwrap();

        let m1 = alice.encrypt_for_peer("bob", b"one").unwrap();
        let m2 = alice.encrypt_for_peer("bob", b"two").unwrap();
        let m3 = alice.encrypt_for_peer("bob", b"three").unwrap();

        assert_eq!(bob.decrypt_from_sender("alice", &m2).unwrap(), b"two");
        assert_eq!(bob.decrypt_from_sender("alice", &m1).unwrap(), b"one");
        assert_eq!(bob.decrypt_from_sender("alice", &m3).unwrap(), b"three");
    }

    #[test]
    fn safety_number_is_stable() {
        let a = E2eeDevice::generate();
        let b = E2eeDevice::generate();
        let ka = a.identity_key_base64();
        let kb = b.identity_key_base64();
        let s1 = E2eeDevice::safety_number(&ka, &kb);
        let s2 = E2eeDevice::safety_number(&kb, &ka);
        assert_eq!(s1, s2);
        assert!(s1.contains(' '));
    }

    #[test]
    fn megolm_group_round_trip() {
        let mut alice = E2eeDevice::generate();
        let mut bob = E2eeDevice::generate();

        let group_id = "g-e2ee";
        alice.create_group_sender_session(group_id);
        let key_b64 = alice.export_group_session_key(group_id).unwrap();
        let session_id = alice.group_outbound[group_id].session_id();
        bob.import_group_session_key(group_id, &session_id, &key_b64)
            .unwrap();

        let secret = b"group secret payload";
        let body = alice.encrypt_group_message(group_id, secret).unwrap();
        assert!(E2eeDevice::is_encrypted_group_body(&body));
        assert!(!E2eeDevice::body_contains_substring(&body, secret));

        let plain = bob.decrypt_group_message(group_id, &body).unwrap();
        assert_eq!(plain, secret);

        let body2 = alice.encrypt_group_message(group_id, b"second").unwrap();
        assert_eq!(bob.decrypt_group_message(group_id, &body2).unwrap(), b"second");
    }

    #[test]
    fn megolm_key_share_via_olm() {
        let (mut alice, mut bob, bundle) = setup_pair();
        alice.establish_outbound("bob", &bundle).unwrap();

        let group_id = "g-share";
        alice.create_group_sender_session(group_id);
        let share = alice.group_session_key_share(group_id).unwrap();
        let olm_body = alice.encrypt_for_peer("bob", &share).unwrap();

        assert!(bob.try_import_group_key_share("alice", &olm_body).unwrap());

        let body = alice.encrypt_group_message(group_id, b"via share").unwrap();
        assert_eq!(bob.decrypt_group_message(group_id, &body).unwrap(), b"via share");
    }

}
