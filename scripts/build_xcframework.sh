#!/usr/bin/env bash
# Build LaneMessengerFFI.xcframework from lane_messenger_ffi (macOS host).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

FEATURES="${FEATURES:-c-api,tls,ws,uniffi}"
OUT="${OUT:-$ROOT/dist/LaneMessengerFFI.xcframework}"
BUILD_DIR="${BUILD_DIR:-$ROOT/target/xcframework-build}"

TARGETS=(
  aarch64-apple-ios
  aarch64-apple-ios-sim
  x86_64-apple-ios
)

for t in "${TARGETS[@]}"; do
  rustup target add "$t" >/dev/null
done

mkdir -p "$BUILD_DIR"
rm -rf "$OUT"

echo "==> Building staticlibs (features=$FEATURES)"
for t in "${TARGETS[@]}"; do
  cargo build -p lane_messenger_ffi --release --features "$FEATURES" --target "$t"
done

IOS_LIB="$ROOT/target/aarch64-apple-ios/release/liblane_messenger_ffi.a"
SIM_A64="$ROOT/target/aarch64-apple-ios-sim/release/liblane_messenger_ffi.a"
SIM_X64="$ROOT/target/x86_64-apple-ios/release/liblane_messenger_ffi.a"
SIM_FAT="$BUILD_DIR/liblane_messenger_ffi-ios-sim.a"

lipo -create -output "$SIM_FAT" "$SIM_A64" "$SIM_X64"

HEADER_DIR="$BUILD_DIR/Headers"
mkdir -p "$HEADER_DIR"
if [[ -f "$ROOT/bindings/swift/LaneMessengerFFI/Sources/LaneMessengerFFI/generated/lane_messengerFFI.h" ]]; then
  cp "$ROOT/bindings/swift/LaneMessengerFFI/Sources/LaneMessengerFFI/generated/lane_messengerFFI.h" "$HEADER_DIR/"
fi
cp "$ROOT/lane_messenger_ffi/include/lane_messenger_ffi.h" "$HEADER_DIR/"

xcodebuild -create-xcframework \
  -library "$IOS_LIB" -headers "$HEADER_DIR" \
  -library "$SIM_FAT" -headers "$HEADER_DIR" \
  -output "$OUT"

echo "Wrote $OUT"
