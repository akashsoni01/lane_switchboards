# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- WebSocket transport (`feature = "ws"`) bridging browser clients to the binary protocol
- Contact-list presence filtering (`SubscribePresence` 0x11)
- Group per-member read/delivery aggregation (`GroupAckSummary` 0x42)
- ServiceMesh peer discovery helper (`spawn_mesh_discovery`)
- Optional StorageNode inbox replication helper (`InboxStorage`)
- `deny.toml` + CI `cargo-deny` job
- cargo-fuzz target `fuzz/fuzz_targets/messenger_codec.rs`
- Deployment examples under `deploy/` (systemd + Kubernetes)
- Criterion baseline notes under `benches/baselines/`

## [0.9.2] — 2026-07-10

### Added
- FunXMPP-style messenger plane: wire codec, gateway, auth, presence, routing,
  offline WAL, groups, media, multi-node cluster, E2EE (Olm + Megolm)
- `messenger` feature flag (default on); TLS / metrics / libolm-compat optional
- 38+ end-to-end messenger integration tests
- Docs under `docs/messenger/`; release notes `READMEv0.9.2.md`

### Changed
- CI: fmt, clippy, messenger with/without default features, TLS tests

## [0.9.0] — see READMEv0.9.0.md

Distributed key-value storage (`StorageNode`), Paxos SERIAL writes, WAL recovery.

[Unreleased]: https://github.com/example/lane_switchboards/compare/v0.9.2...HEAD
[0.9.2]: https://github.com/example/lane_switchboards/compare/v0.9.0...v0.9.2
[0.9.0]: https://github.com/example/lane_switchboards/releases/tag/v0.9.0
