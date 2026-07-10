# Flutter FFI demo

Uses the **same** `liblane_messenger_ffi` as iOS/Android — do not reimplement
frames in Dart.

## Setup

```bash
# From repo root — build host .so / dylib for desktop smoke, or NDK/XCFramework
# for device:
cargo build -p lane_messenger_ffi --release
./scripts/build_android_ndk.sh   # Android
./scripts/build_xcframework.sh   # iOS (macOS)

cd examples/flutter_ffi_demo
flutter create --platforms=ios,android .   # once
# pubspec already depends on path: ../../bindings/flutter/lane_messenger
flutter pub get
flutter run
```

## Dart API

```dart
import 'package:lane_messenger/lane_messenger.dart';

final session = LaneSession.connect(
  host: '127.0.0.1',
  port: 9000,
  userId: 'alice',
  deviceId: 'flutter-1',
  authToken: token,
);
session.ping();
final ev = session.pollEvent(timeoutMs: 500);
session.sendChat(to: 'bob', messageId: 'm1', body: utf8.encode('hi'));
session.close();
session.dispose();
```

See `lib/main.dart` for a runnable loop that prints LoginAck / SyncComplete.
