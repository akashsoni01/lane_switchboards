# FFI release hardening (F9)

## Build flags

Workspace `[profile.release]` for mobile artifacts:

- `lto = true`
- `codegen-units = 1`
- `panic = "abort"`
- `strip = "symbols"`

```bash
cargo build -p lane_messenger_ffi --release
# optional: --features ws
```

## Panic boundary

C entry points (`lane_session_connect`, `lane_session_ping`, …) use
`catch_unwind` → `FfiErrorCode::Internal` (14). With `panic=abort` in release,
unwinding is disabled; keep catch_unwind for debug/dev builds.

## TLS / CA pinning

`ConnectOptions.ca_pem_path` — PEM file for a custom CA (staging / enterprise).
Production default uses webpki roots when `use_tls = true`.

## Compatibility

| FFI | Server |
|-----|--------|
| 0.9.x | 0.9.x messenger plane |

## Size

Document stripped `.so` / XCFramework size after first CI release build.
Enable LTO as above; avoid debug symbols in App Store / Play uploads.

## License / NOTICE

See [`NOTICE`](../../NOTICE) for vodozemac, rustls, and related attributions.

## Soak / chaos (manual)

- 1k connect/disconnect cycles (Instruments / Android Profiler)
- Kill gateway mid-send → retry same `message_id` succeeds
