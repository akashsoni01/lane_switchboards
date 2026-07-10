# Flutter FFI demo

```bash
# Place liblane_messenger_ffi.so / XCFramework next to the plugin
flutter create --platforms=ios,android .
# depend on path: ../../bindings/flutter/lane_messenger
```

```dart
final session = LaneSession.connect(
  host: '127.0.0.1',
  port: 9000,
  userId: 'alice',
  deviceId: 'flutter-1',
  authToken: token,
);
session.ping();
```

**Do not** reimplement frames in Dart — use the same native library as iOS/Android.
