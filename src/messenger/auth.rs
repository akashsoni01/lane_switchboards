//! Pluggable authentication for the messenger gateway.

use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Verifies login credentials. Implementations must be constant-time with
/// respect to the token contents.
pub trait Authenticator: Send + Sync + 'static {
    /// Returns `true` when `token` is valid for `(user_id, device_id)`.
    fn verify(&self, user_id: &str, device_id: &str, token: &str) -> bool;
}

/// HMAC-SHA256 token authenticator.
///
/// Token format: `hex(HMAC-SHA256(secret, "user_id:device_id"))`.
/// Verification uses [`Mac::verify_slice`], which is constant-time.
#[derive(Clone)]
pub struct HmacAuthenticator {
    secret: Vec<u8>,
}

impl HmacAuthenticator {
    /// Build an authenticator from a shared secret.
    pub fn new(secret: impl AsRef<[u8]>) -> Self {
        Self { secret: secret.as_ref().to_vec() }
    }

    /// Mint a token for a user/device pair. Intended for provisioning,
    /// tests, and demos — production systems should mint tokens in a
    /// separate identity service.
    pub fn mint_token(&self, user_id: &str, device_id: &str) -> String {
        let mut mac = HmacSha256::new_from_slice(&self.secret).expect("hmac accepts any key len");
        mac.update(format!("{user_id}:{device_id}").as_bytes());
        hex_encode(&mac.finalize().into_bytes())
    }
}

impl Authenticator for HmacAuthenticator {
    fn verify(&self, user_id: &str, device_id: &str, token: &str) -> bool {
        let Some(token_bytes) = hex_decode(token) else {
            return false;
        };
        let mut mac = HmacSha256::new_from_slice(&self.secret).expect("hmac accepts any key len");
        mac.update(format!("{user_id}:{device_id}").as_bytes());
        mac.verify_slice(&token_bytes).is_ok()
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_token_verifies() {
        let auth = HmacAuthenticator::new("secret");
        let token = auth.mint_token("akash", "phone-1");
        assert!(auth.verify("akash", "phone-1", &token));
    }

    #[test]
    fn wrong_user_or_device_fails() {
        let auth = HmacAuthenticator::new("secret");
        let token = auth.mint_token("akash", "phone-1");
        assert!(!auth.verify("john", "phone-1", &token));
        assert!(!auth.verify("akash", "phone-2", &token));
    }

    #[test]
    fn tampered_or_garbage_token_fails() {
        let auth = HmacAuthenticator::new("secret");
        let mut token = auth.mint_token("akash", "phone-1");
        token.replace_range(0..2, "00");
        assert!(!auth.verify("akash", "phone-1", &token) || token == auth.mint_token("akash", "phone-1"));
        assert!(!auth.verify("akash", "phone-1", "not-hex"));
        assert!(!auth.verify("akash", "phone-1", ""));
    }

    #[test]
    fn different_secret_fails() {
        let a = HmacAuthenticator::new("secret-a");
        let b = HmacAuthenticator::new("secret-b");
        let token = a.mint_token("akash", "phone-1");
        assert!(!b.verify("akash", "phone-1", &token));
    }
}
