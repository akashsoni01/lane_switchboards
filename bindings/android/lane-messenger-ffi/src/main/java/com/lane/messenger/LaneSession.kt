package com.lane.messenger

/**
 * Thin Kotlin/JNI wrapper over `liblane_messenger_ffi.so`
 * (`cargo build -p lane_messenger_ffi --features jni`).
 *
 * UniFFI/JNA bindings also ship under `uniffi.lane_messenger` for hosts that
 * prefer that stack; this class stays as the lightweight C-ABI path.
 */
class LaneSession private constructor(private var handle: Long) {
    companion object {
        init {
            System.loadLibrary("lane_messenger_ffi")
        }

        @JvmStatic
        external fun nativeConnect(
            host: String,
            port: Int,
            useTls: Boolean,
            userId: String,
            deviceId: String,
            authToken: String,
            clientVersion: String,
            resumeAfterSeq: Long,
            pingIntervalSecs: Long
        ): Long

        fun connect(
            host: String,
            port: Int,
            useTls: Boolean,
            userId: String,
            deviceId: String,
            authToken: String,
            clientVersion: String = "android-ffi",
            resumeAfterSeq: Long = 0,
            pingIntervalSecs: Long = 30
        ): LaneSession {
            val h = nativeConnect(
                host, port, useTls, userId, deviceId, authToken,
                clientVersion, resumeAfterSeq, pingIntervalSecs
            )
            require(h != 0L) { "lane_session_connect failed" }
            return LaneSession(h)
        }
    }

    external fun nativePing(handle: Long): Int
    external fun nativeClose(handle: Long): Int
    external fun nativeFree(handle: Long)
    external fun nativePollEvent(handle: Long, timeoutMs: Long): String?
    external fun nativeSendChat(handle: Long, to: String, messageId: String, body: ByteArray): Long

    fun ping() {
        check(nativePing(handle) == 0)
    }

    fun sendChat(to: String, messageId: String, body: ByteArray): Long =
        nativeSendChat(handle, to, messageId, body)

    fun pollEvent(timeoutMs: Long): String? = nativePollEvent(handle, timeoutMs)

    fun close() {
        nativeClose(handle)
    }

    fun free() {
        if (handle != 0L) {
            nativeFree(handle)
            handle = 0
        }
    }

    protected fun finalize() {
        free()
    }
}
