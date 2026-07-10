#!/usr/bin/env bash
# Cross-compile liblane_messenger_ffi.so for Android ABIs (requires NDK + cargo-ndk).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

FEATURES="${FEATURES:-c-api,tls,ws,jni}"
OUT_JNI="${OUT_JNI:-$ROOT/bindings/android/lane-messenger-ffi/src/main/jniLibs}"
ABIS="${ABIS:-arm64-v8a armeabi-v7a x86_64}"

if ! command -v cargo-ndk >/dev/null 2>&1; then
  echo "Installing cargo-ndk…"
  cargo install cargo-ndk --locked
fi

if [[ -z "${ANDROID_NDK_HOME:-}${NDK_HOME:-}" ]]; then
  echo "Set ANDROID_NDK_HOME (or NDK_HOME) to your Android NDK root." >&2
  exit 1
fi

mkdir -p "$OUT_JNI"
# shellcheck disable=SC2086
cargo ndk $(for a in $ABIS; do printf -- '-t %s ' "$a"; done) -o "$OUT_JNI" \
  build -p lane_messenger_ffi --release --features "$FEATURES"

echo "Wrote .so files under $OUT_JNI"
find "$OUT_JNI" -name 'liblane_messenger_ffi.so' -print
