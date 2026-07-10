import 'dart:ffi';
import 'dart:typed_data';
import 'package:ffi/ffi.dart';

/// Dart FFI bindings to `liblane_messenger_ffi`.
/// Ship the same `.so` / XCFramework as native Android/iOS apps — do not
/// reimplement the wire protocol in Dart.
class LaneSession {
  LaneSession._(this._handle);

  final Pointer<Void> _handle;
  static final DynamicLibrary _lib = DynamicLibrary.open(_libName);

  static String get _libName {
    // Platform-specific: adjust for iOS (process) vs Android (.so).
    return 'liblane_messenger_ffi.so';
  }

  static final _connect = _lib.lookupFunction<
      Pointer<Void> Function(
          Pointer<Utf8>, Uint16, Int32, Pointer<Utf8>, Pointer<Utf8>,
          Pointer<Utf8>, Pointer<Utf8>, Uint64, Uint64, Pointer<Pointer<Utf8>>),
      Pointer<Void> Function(
          Pointer<Utf8>, int, int, Pointer<Utf8>, Pointer<Utf8>, Pointer<Utf8>,
          Pointer<Utf8>, int, int, Pointer<Pointer<Utf8>>)>('lane_session_connect');

  static final _free =
      _lib.lookupFunction<Void Function(Pointer<Void>), void Function(Pointer<Void>)>(
          'lane_session_free');
  static final _ping =
      _lib.lookupFunction<Int32 Function(Pointer<Void>), int Function(Pointer<Void>)>(
          'lane_session_ping');
  static final _close =
      _lib.lookupFunction<Int32 Function(Pointer<Void>), int Function(Pointer<Void>)>(
          'lane_session_close');

  factory LaneSession.connect({
    required String host,
    required int port,
    required String userId,
    required String deviceId,
    required String authToken,
    bool useTls = false,
    String clientVersion = 'flutter-ffi',
    int resumeAfterSeq = 0,
    int pingIntervalSecs = 30,
  }) {
    final err = calloc<Pointer<Utf8>>();
    final h = _connect(
      host.toNativeUtf8(),
      port,
      useTls ? 1 : 0,
      userId.toNativeUtf8(),
      deviceId.toNativeUtf8(),
      authToken.toNativeUtf8(),
      clientVersion.toNativeUtf8(),
      resumeAfterSeq,
      pingIntervalSecs,
      err,
    );
    if (h == nullptr) {
      final msg = err.value == nullptr ? 'connect failed' : err.value.toDartString();
      throw StateError(msg);
    }
    return LaneSession._(h);
  }

  void ping() {
    final c = _ping(_handle);
    if (c != 0) throw StateError('ping failed: $c');
  }

  void close() {
    _close(_handle);
  }

  void dispose() {
    _free(_handle);
  }
}

/// Sample entry — see `examples/flutter_ffi_demo`.
void main() {}
