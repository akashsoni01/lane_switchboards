#!/usr/bin/env bash
# Regenerate UniFFI Swift + Kotlin bindings from a built library.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

FEATURES="${FEATURES:-c-api,tls,ws,uniffi}"
cargo build -p lane_messenger_ffi --release --features "$FEATURES"

LIB=""
if [[ -f target/release/liblane_messenger_ffi.dylib ]]; then
  LIB=target/release/liblane_messenger_ffi.dylib
elif [[ -f target/release/liblane_messenger_ffi.so ]]; then
  LIB=target/release/liblane_messenger_ffi.so
else
  echo "No release cdylib found under target/release/" >&2
  exit 1
fi

SWIFT_OUT=bindings/swift/LaneMessengerFFI/Sources/LaneMessengerFFI/generated
KT_OUT=bindings/android/lane-messenger-ffi/src/main/java
mkdir -p "$SWIFT_OUT" "$KT_OUT"

cargo run -p lane_messenger_ffi --features uniffi --bin uniffi-bindgen -- \
  generate --library "$LIB" --language swift --out-dir "$SWIFT_OUT"

cargo run -p lane_messenger_ffi --features uniffi --bin uniffi-bindgen -- \
  generate --library "$LIB" --language kotlin --out-dir "$KT_OUT"

echo "Swift → $SWIFT_OUT"
echo "Kotlin → $KT_OUT/uniffi/lane_messenger/"
