//! End-to-end encryption for 1:1 chat using vodozemac Olm (Double Ratchet).
//!
//! The server only stores public keys and routes opaque `ChatMessage.body`
//! bytes; encryption/decryption happens entirely on clients.

use std::collections::HashMap;

use prost::Message;
use sha2::{Digest, Sha256};
use vodozemac::Curve25519PublicKey;
use vodozemac::olm::{Account, OlmMessage, Session, SessionConfig};

use super::wire;

/// Errors from the E2EE layer.
#[derive(Debug, thiserror::Error)]
pub enum E2eeError {
    #[error("vodozemac decode: {0}")]
    Decode(#[from] vodozemac::DecodeError),
    #[error("vodozemac key: {0}")]
    Key(#[from] vodozemac::KeyError),
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
    #[error("peer key bundle incomplete")]
    IncompleteBundle,
    #[error("expected a pre-key message to create a new session")]
    NotPreKeyMessage,
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

/// One device's Olm account and per-peer sessions.
pub struct E2eeDevice {
    account: Account,
    sessions: HashMap<String, Session>,
}

impl E2eeDevice {
    /// Generate a fresh device with a new identity keypair.
    pub fn generate() -> Self {
        Self {
            account: Account::new(),
            sessions: HashMap::new(),
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

    /// Start an outbound Olm session with `peer` using their published bundle.
    pub fn establish_outbound(&mut self, peer: &str, bundle: &PeerKeyBundle) -> Result<(), E2eeError> {
        let identity = Curve25519PublicKey::from_base64(&bundle.identity_key)?;
        let otk = Curve25519PublicKey::from_base64(&bundle.one_time_key)?;
        let session = self
            .account
            .create_outbound_session(SessionConfig::version_1(), identity, otk)?;
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
        Ok(encode_payload(&self.account.curve25519_key().to_base64(), &olm))
    }

    /// Decrypt a `ChatMessage.body` from `sender_user`.
    pub fn decrypt_from_sender(&mut self, sender_user: &str, body: &[u8]) -> Result<Vec<u8>, E2eeError> {
        let (sender_key, _msg_type, olm) = decode_payload(body)?;
        if let Some(session) = self.sessions.get_mut(sender_user) {
            return session.decrypt(&olm).map_err(E2eeError::from);
        }
        let identity = Curve25519PublicKey::from_base64(&sender_key)?;
        let OlmMessage::PreKey(ref prekey) = olm else {
            return Err(E2eeError::NotPreKeyMessage);
        };
        let result = self.account.create_inbound_session(
            SessionConfig::version_1(),
            identity,
            prekey,
        )?;
        let plaintext = result.plaintext;
        self.sessions.insert(sender_user.to_string(), result.session);
        // Pre-key messages embed the first plaintext; later messages use decrypt.
        if !plaintext.is_empty() {
            return Ok(plaintext);
        }
        // Fallback: decrypt the normal message inside the pre-key wrapper.
        self.sessions
            .get_mut(sender_user)
            .expect("session just inserted")
            .decrypt(&olm)
            .map_err(E2eeError::from)
    }

    /// True when `body` is a valid prost `EncryptedPayload`.
    pub fn is_encrypted_body(body: &[u8]) -> bool {
        wire::EncryptedPayload::decode(body).is_ok()
    }

    /// True if raw `needle` appears in `body` (used to assert server opacity).
    pub fn body_contains_substring(body: &[u8], needle: &[u8]) -> bool {
        body.windows(needle.len()).any(|w| w == needle)
    }
}

fn encode_payload(sender_key: &str, olm: &OlmMessage) -> Vec<u8> {
    let (message_type, ciphertext) = olm.to_parts();
    let payload = wire::EncryptedPayload {
        sender_key: sender_key.to_string(),
        message_type: message_type as u32,
        ciphertext,
    };
    payload.encode_to_vec()
}

fn decode_payload(body: &[u8]) -> Result<(String, u32, OlmMessage), E2eeError> {
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
}
