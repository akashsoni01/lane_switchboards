package com.lane.messenger.demo

import android.os.Bundle
import android.util.Log
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.Button
import androidx.compose.material3.Text
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier.Modifier
import androidx.compose.ui.unit.dp
import com.lane.messenger.LaneSession
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.util.UUID

/**
 * Minimal Compose host for the JNI [LaneSession] wrapper.
 * Point [HOST] at `10.0.2.2` from an emulator, or your LAN IP from a device.
 */
class DemoActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContent {
            val scope = rememberCoroutineScope()
            var log by remember { mutableStateOf("idle") }
            Column(Modifier.padding(16.dp)) {
                Text(log)
                Button(onClick = {
                    scope.launch {
                        log = runDemo()
                    }
                }) {
                    Text("Connect + chat")
                }
            }
        }
    }

    private suspend fun runDemo(): String = withContext(Dispatchers.IO) {
        val token = System.getenv("LANE_AUTH_TOKEN") ?: "demo-token"
        val session = LaneSession.connect(
            host = HOST,
            port = 9000,
            useTls = false,
            userId = System.getenv("LANE_USER") ?: "alice",
            deviceId = System.getenv("LANE_DEVICE") ?: "android-demo-1",
            authToken = token,
            clientVersion = "android-ffi-demo"
        )
        try {
            session.ping()
            val buf = StringBuilder("ping ok\n")
            repeat(25) {
                session.pollEvent(200)?.let { buf.append(it).append('\n') }
            }
            val mid = "android-demo-${UUID.randomUUID()}"
            val seq = session.sendChat(
                System.getenv("LANE_PEER") ?: "bob",
                mid,
                "hello from android".toByteArray()
            )
            buf.append("sent seq=$seq id=$mid\n")
            repeat(20) {
                session.pollEvent(200)?.let { buf.append(it).append('\n') }
            }
            session.close()
            Log.i(TAG, buf.toString())
            buf.toString()
        } finally {
            session.free()
        }
    }

    companion object {
        private const val TAG = "LaneFfiDemo"
        private const val HOST = "10.0.2.2"
    }
}
