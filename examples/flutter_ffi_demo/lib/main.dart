import 'dart:convert';
import 'dart:io';

import 'package:flutter/material.dart';
import 'package:lane_messenger/lane_messenger.dart';

void main() {
  runApp(const LaneFfiDemoApp());
}

class LaneFfiDemoApp extends StatelessWidget {
  const LaneFfiDemoApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      home: Scaffold(
        appBar: AppBar(title: const Text('Lane FFI demo')),
        body: const _DemoBody(),
      ),
    );
  }
}

class _DemoBody extends StatefulWidget {
  const _DemoBody();

  @override
  State<_DemoBody> createState() => _DemoBodyState();
}

class _DemoBodyState extends State<_DemoBody> {
  String _log = 'idle';

  Future<void> _run() async {
    final token = Platform.environment['LANE_AUTH_TOKEN'] ?? 'demo-token';
    final host = Platform.isAndroid ? '10.0.2.2' : '127.0.0.1';
    final session = LaneSession.connect(
      host: host,
      port: 9000,
      userId: Platform.environment['LANE_USER'] ?? 'alice',
      deviceId: Platform.environment['LANE_DEVICE'] ?? 'flutter-demo-1',
      authToken: token,
    );
    final buf = StringBuffer();
    try {
      session.ping();
      buf.writeln('ping ok');
      for (var i = 0; i < 25; i++) {
        final ev = session.pollEvent(timeoutMs: 200);
        if (ev != null) buf.writeln(ev);
      }
      final mid = 'flutter-demo-${DateTime.now().millisecondsSinceEpoch}';
      final seq = session.sendChat(
        to: Platform.environment['LANE_PEER'] ?? 'bob',
        messageId: mid,
        body: utf8.encode('hello from flutter'),
      );
      buf.writeln('sent seq=$seq id=$mid');
      for (var i = 0; i < 20; i++) {
        final ev = session.pollEvent(timeoutMs: 200);
        if (ev != null) buf.writeln(ev);
      }
      session.close();
    } catch (e) {
      buf.writeln('error: $e');
    } finally {
      session.dispose();
    }
    setState(() => _log = buf.toString());
  }

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.all(16),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        children: [
          ElevatedButton(onPressed: _run, child: const Text('Connect + chat')),
          const SizedBox(height: 12),
          Expanded(child: SingleChildScrollView(child: Text(_log))),
        ],
      ),
    );
  }
}
