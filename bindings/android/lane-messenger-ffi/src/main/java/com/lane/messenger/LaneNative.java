package com.lane.messenger;

/**
 * Library bootstrap: load {@code liblane_messenger_ffi.so} / {@code .dylib} / {@code .dll}
 * and expose crate / protocol versions.
 *
 * <p>Build the native library with:
 * <pre>{@code cargo build -p lane_messenger_ffi --release --features jni,tls}</pre>
 * For Android ABIs use {@code ./scripts/build_android_ndk.sh}.
 */
public final class LaneNative {
    private static volatile boolean loaded;

    static {
        load();
    }

    private LaneNative() {}

    /** Idempotent load of the native library (also invoked from static initializers). */
    public static synchronized void load() {
        if (loaded) {
            return;
        }
        System.loadLibrary("lane_messenger_ffi");
        loaded = true;
    }

    /** Allow tests to load from an absolute path (JVM desktop). */
    public static synchronized void loadFromPath(String absolutePath) {
        if (loaded) {
            return;
        }
        System.load(absolutePath);
        loaded = true;
    }

    public static native String nativeVersion();

    public static native int nativeProtocolVersion();

    /** Last native error message for this thread (cleared on read). */
    public static native String nativeLastError();

    public static String version() {
        load();
        return nativeVersion();
    }

    public static int protocolVersion() {
        load();
        return nativeProtocolVersion();
    }

    public static String lastError() {
        load();
        return nativeLastError();
    }
}
