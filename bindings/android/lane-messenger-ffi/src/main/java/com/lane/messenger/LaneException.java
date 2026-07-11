package com.lane.messenger;

/**
 * Checked failure from the Lane JNI / C ABI layer.
 * {@link #getCode()} mirrors {@code FfiErrorCode} when known; {@code -1} for generic failures.
 */
public final class LaneException extends Exception {
    private final int code;

    public LaneException(String message) {
        this(message, -1);
    }

    public LaneException(String message, int code) {
        super(message == null || message.isEmpty() ? "lane ffi error code=" + code : message);
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    static LaneException fromLastError(String fallback) {
        String err = LaneNative.lastError();
        if (err == null || err.isEmpty()) {
            return new LaneException(fallback);
        }
        return new LaneException(err);
    }

    static LaneException fromCode(int code) {
        String err = LaneNative.lastError();
        if (err != null && !err.isEmpty()) {
            return new LaneException(err, code);
        }
        return new LaneException("lane ffi error", code);
    }
}
