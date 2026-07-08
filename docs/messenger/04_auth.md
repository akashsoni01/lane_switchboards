# Authentication

## Model

Verification is pluggable via the `Authenticator` trait
(`src/messenger/auth.rs`):

```rust
pub trait Authenticator: Send + Sync + 'static {
    fn verify(&self, user_id: &str, device_id: &str, token: &str) -> bool;
}
```

The shipped implementation is `HmacAuthenticator`:

- Token format: `hex(HMAC-SHA256(secret, "user_id:device_id"))`.
- Verification uses `Mac::verify_slice`, which is constant-time — no
  early-exit on a byte mismatch.
- `mint_token` exists for provisioning, tests, and demos; production should
  mint tokens in a separate identity service (JWT with expiry is the planned
  upgrade — the trait boundary means the gateway does not change).

## Login flow

The first frame on a connection must be `Login { user_id, device_id,
auth_token, client_version, resume_after_seq }` within `login_deadline`
(default 10 s). Anything else, or a timeout, gets `NOT_AUTHENTICATED` and the
socket closes. No other packet type is processed pre-auth.

The server never trusts client-supplied identity fields after login:
`from_user` on every chat/ack packet is overwritten with the authenticated
user before routing.

## Multi-device

Session key is `(user_id, device_id)`. A relogin from the same device kicks
the previous session, which receives a typed `REPLACED_BY_NEW_SESSION` error
before closing. Different devices of the same user coexist and each receive
message deliveries.

## Brute-force protection

- Failed logins per user drive exponential backoff before the server even
  responds: `auth_backoff_base × 2^(failures−1)`, capped at
  `auth_backoff_max` (defaults 250 ms → 30 s). The counter resets on success.
- Per-IP connection admission (`max_conns_per_ip_per_min`) bounds how fast an
  attacker can open fresh sockets to dodge the per-user delay.

## Logging rules

Tokens are never logged. Login success and failure are logged with user id
and failure count for audit; production deployments should hash user ids in
log output (Phase 9 observability work).

## Threat-model notes / current limits

- Transport TLS is available (`feature = "tls"`): `MessengerServer::bind_tls`
  wraps every client socket via rustls before the first frame, and
  `MessengerClient::connect_tls` validates the server certificate against the
  host portion of the address. Plaintext clients are rejected at the
  handshake. Production must always run with an acceptor.
- Tokens do not expire; JWT with `exp` + rotation is the planned follow-up.
- Message bodies are visible to the server until E2EE (Phase 10) lands.
