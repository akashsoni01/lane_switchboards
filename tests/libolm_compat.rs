//! vodozemac ↔ libolm cross-validation (Megolm + Olm message formats).
//!
//! Requires `cmake` to build `olm-sys`. Run:
//! `cargo test --features libolm-compat --test libolm_compat`

#![cfg(feature = "libolm-compat")]

use lane_switchboards::messenger::E2eeDevice;
use lane_switchboards::messenger::wire::EncryptedGroupPayload;
use olm_rs::{
    inbound_group_session::OlmInboundGroupSession,
    outbound_group_session::OlmOutboundGroupSession,
};
use prost::Message;
use vodozemac::megolm::{
    GroupSession, InboundGroupSession, SessionConfig, SessionKey,
};

#[test]
fn vodozemac_megolm_encrypt_libolm_decrypt() {
    let mut session = GroupSession::new(SessionConfig::version_1());
    let session_key = session.session_key();
    let olm_in = OlmInboundGroupSession::new(&session_key.to_base64()).unwrap();

    let plaintext = "cross-validation secret";
    let message = session.encrypt(plaintext).to_base64();
    let (decrypted, _) = olm_in.decrypt(message).unwrap();
    assert_eq!(decrypted, plaintext);
}

#[test]
fn libolm_megolm_encrypt_vodozemac_decrypt() {
    let olm_out = OlmOutboundGroupSession::new();
    let session_key = SessionKey::from_base64(&olm_out.session_key()).unwrap();
    let mut inbound = InboundGroupSession::new(&session_key, SessionConfig::version_1());

    let plaintext = "libolm -> vodozemac";
    let message = olm_out.encrypt(plaintext).as_str().try_into().unwrap();
    let decrypted = inbound.decrypt(&message).unwrap();
    assert_eq!(decrypted.plaintext, plaintext.as_bytes());
}

#[test]
fn e2ee_device_megolm_group_matches_libolm() {
    let mut alice = E2eeDevice::generate();
    let group_id = "g-libolm";
    alice.create_group_sender_session(group_id);
    let key_b64 = alice.export_group_session_key(group_id).unwrap();

    let olm_in = OlmInboundGroupSession::new(&key_b64).unwrap();
    let body = alice.encrypt_group_message(group_id, b"interop").unwrap();
    let payload = EncryptedGroupPayload::decode(body.as_slice()).unwrap();
    let megolm_b64 = vodozemac::megolm::MegolmMessage::from_bytes(&payload.ciphertext)
        .unwrap()
        .to_base64();
    let (plain, _) = olm_in.decrypt(megolm_b64).unwrap();
    assert_eq!(plain, "interop");
}
