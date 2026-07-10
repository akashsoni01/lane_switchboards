import 'dart:ffi';
import 'dart:io';

import 'package:ffi/ffi.dart';

/// Dart FFI bindings to `liblane_messenger_ffi`.
/// Ship the same `.so` / XCFramework as native Android/iOS apps — do not
/// reimplement the wire protocol in Dart.
class LaneSession {
  LaneSession._(this._handle);

  Pointer<Void> _handle;
  static final DynamicLibrary _lib = DynamicLibrary.open(_libName);

  static String get _libName {
    if (Platform.isIOS || Platform.isMacOS) {
      return 'lane_messenger_ffi';
    }
    return 'liblane_messenger_ffi.so';
  }

  static final _connect = _lib.lookupFunction<
      Pointer<Void> Function(
          Pointer<Utf8>,
          Uint16,
          Int32,
          Pointer<Utf8>,
          Pointer<Utf8>,
          Pointer<Utf8>,
          Pointer<Utf8>,
          Uint64,
          Uint64,
          Pointer<Pointer<Utf8>>),
      Pointer<Void> Function(
          Pointer<Utf8>,
          int,
          int,
          Pointer<Utf8>,
          Pointer<Utf8>,
          Pointer<Utf8>,
          Pointer<Utf8>,
          int,
          int,
          Pointer<Pointer<Utf8>>)>('lane_session_connect');

  static final _free =
      _lib.lookupFunction<Void Function(Pointer<Void>), void Function(Pointer<Void>)>(
          'lane_session_free');
  static final _ping =
      _lib.lookupFunction<Int32 Function(Pointer<Void>), int Function(Pointer<Void>)>(
          'lane_session_ping');
  static final _close =
      _lib.lookupFunction<Int32 Function(Pointer<Void>), int Function(Pointer<Void>)>(
          'lane_session_close');
  static final _poll = _lib.lookupFunction<
      Int32 Function(Pointer<Void>, Uint64, Pointer<Pointer<Utf8>>),
      int Function(Pointer<Void>, int, Pointer<Pointer<Utf8>>)>('lane_session_poll_event');
  static final _sendChat = _lib.lookupFunction<
      Int32 Function(Pointer<Void>, Pointer<Utf8>, Pointer<Utf8>, Pointer<Uint8>,
          IntPtr, Pointer<Uint64>),
      int Function(Pointer<Void>, Pointer<Utf8>, Pointer<Utf8>, Pointer<Uint8>, int,
          Pointer<Uint64>)>('lane_send_chat');
  static final _stringFree =
      _lib.lookupFunction<Void Function(Pointer<Utf8>), void Function(Pointer<Utf8>)>(
          'lane_string_free');

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
    final hostN = host.toNativeUtf8();
    final userN = userId.toNativeUtf8();
    final deviceN = deviceId.toNativeUtf8();
    final tokenN = authToken.toNativeUtf8();
    final verN = clientVersion.toNativeUtf8();
    try {
      final h = _connect(
        hostN,
        port,
        useTls ? 1 : 0,
        userN,
        deviceN,
        tokenN,
        verN,
        resumeAfterSeq,
        pingIntervalSecs,
        err,
      );
      if (h == nullptr) {
        final msg = err.value == nullptr ? 'connect failed' : err.value.toDartString();
        if (err.value != nullptr) {
          _stringFree(err.value);
        }
        throw StateError(msg);
      }
      return LaneSession._(h);
    } finally {
      calloc.free(hostN);
      calloc.free(userN);
      calloc.free(deviceN);
      calloc.free(tokenN);
      calloc.free(verN);
      calloc.free(err);
    }
  }

  void ping() {
    final c = _ping(_handle);
    if (c != 0) throw StateError('ping failed: $c');
  }

  /// Poll one JSON event; `null` on timeout / empty.
  String? pollEvent({int timeoutMs = 100}) {
    final out = calloc<Pointer<Utf8>>();
    try {
      final n = _poll(_handle, timeoutMs, out);
      if (n != 1 || out.value == nullptr) return null;
      final s = out.value.toDartString();
      _stringFree(out.value);
      return s;
    } finally {
      calloc.free(out);
    }
  }

  int sendChat({
    required String to,
    required String messageId,
    required List<int> body,
  }) {
    final toN = to.toNativeUtf8();
    final midN = messageId.toNativeUtf8();
    final seq = calloc<Uint64>();
    final buf = calloc<Uint8>(body.length);
    try {
      final bytes = buf.asTypedList(body.length);
      bytes.setAll(0, body);
      final code = _sendChat(_handle, toN, midN, buf, body.length, seq);
      if (code != 0) throw StateError('send_chat failed: $code');
      return seq.value;
    } finally {
      calloc.free(toN);
      calloc.free(midN);
      calloc.free(seq);
      calloc.free(buf);
    }
  }

  void close() {
    _close(_handle);
  }

  void dispose() {
    if (_handle != nullptr) {
      _free(_handle);
      _handle = nullptr;
    }
  }
}

/// Sample entry — prefer `examples/flutter_ffi_demo/lib/main.dart`.
void main() {
  // ignore: avoid_print
  print('use examples/flutter_ffi_demo');
}
