# E2EE Security Review Checklist

External review checklist for the messenger E2EE milestone (Phase 10).
Use before production deployment with real user data.

## Cryptographic primitives

- [ ] All crypto via **vodozemac** (Olm + Megolm); no custom ciphers or MACs
- [ ] Dependency versions pinned; `cargo deny` / advisory DB clean
- [ ] Optional libolm cross-validation passes in CI (`--features libolm-compat`)

## Key directory (server)

- [ ] Server stores **public keys only**; private keys never leave clients
- [ ] One-time prekeys consumed on fetch (FIFO); cannot be replayed for X3DH
- [ ] `RemoveDeviceKeys` tested for device revocation
- [ ] Multi-device: per `(user_id, device_id)` isolation verified
- [ ] Key directory homed on user shard; cross-node fetch routing reviewed
- [ ] Persistence backend (files / StorageNode) access controls documented

## 1:1 Olm sessions

- [ ] Pre-key message required for first inbound session (no rogue identity)
- [ ] Out-of-order decrypt tested (`e2ee.rs` unit tests)
- [ ] Safety numbers documented for user verification
- [ ] Session state not logged or persisted server-side

## Group Megolm

- [ ] Session keys distributed only via **Olm-encrypted** 1:1 channel
- [ ] `GroupSessionKeyShare` authenticity bound to pairwise channel
- [ ] Megolm message index monotonic per session (replay resistance)
- [ ] Plan for periodic Megolm session rotation documented
- [ ] Group opacity: server never sees plaintext in `GroupMessage.body`

## Server opacity

- [ ] Integration tests assert plaintext substrings absent from bodies
- [ ] Server does not parse `EncryptedPayload` / `EncryptedGroupPayload`
- [ ] Plaintext fallback path documented (non-E2EE clients during migration)

## Operational

- [ ] TLS on client and peer links in production (`--features tls`)
- [ ] Auth tokens separate from `peer_secret`
- [ ] Rate limits on `PublishKeys` / `FetchKeys` (if needed at scale)
- [ ] Key directory backup / restore runbook when using durable storage

## Threat model (document assumptions)

| Threat | Mitigation | Residual risk |
|--------|------------|---------------|
| Server reads messages | Client-side E2EE | Server sees metadata (who, when, size) |
| MITM on first message | Safety numbers / OOB verify | User must verify fingerprints |
| Compromised device | Device revocation API | Historical group keys may leak |
| Stolen one-time prekey | Single-use consumption | Identity key still required |
| Group member expelled | No automatic key rotation | Former member may decrypt until rotation |

## Review sign-off

| Role | Name | Date | Notes |
|------|------|------|-------|
| Engineering | | | |
| Security | | | |
| Product | | | |

## References

- [10_e2ee.md](./10_e2ee.md) — protocol and API
- [vodozemac](https://github.com/matrix-org/vodozemac)
- [Matrix Megolm spec](https://gitlab.matrix.org/matrix-org/olm/blob/master/docs/megolm.md)
