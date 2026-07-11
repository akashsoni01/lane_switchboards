package com.lane.messenger;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Production Java / Android wrapper over {@code LaneSession} (C ABI / JNI).
 *
 * <p><b>Thread safety:</b> one session must not be used concurrently from multiple
 * threads without external synchronization. Prefer a single IO dispatcher.
 * Never call connect/send/poll on the Android main thread.
 *
 * <p><b>Lifecycle:</b> always {@link #close()} (try-with-resources). {@link #close()}
 * sends a graceful close; the native handle is freed in {@link #close()} as well.
 */
public final class LaneSession implements AutoCloseable {
    private long handle;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    static {
        LaneNative.load();
    }

    private LaneSession(long handle) {
        if (handle == 0L) {
            throw new IllegalArgumentException("null session handle");
        }
        this.handle = handle;
    }

    public static LaneSession connect(ConnectOptions opts) throws LaneException {
        Objects.requireNonNull(opts, "opts");
        long h = nativeConnect(
                opts.host,
                opts.port,
                opts.useTls,
                opts.userId,
                opts.deviceId,
                opts.authToken,
                opts.clientVersion,
                opts.resumeAfterSeq,
                opts.pingIntervalSecs
        );
        if (h == 0L) {
            throw LaneException.fromLastError("lane_session_connect failed");
        }
        return new LaneSession(h);
    }

    /** Convenience connect (TLS on by default for production). */
    public static LaneSession connect(
            String host,
            int port,
            boolean useTls,
            String userId,
            String deviceId,
            String authToken
    ) throws LaneException {
        return connect(ConnectOptions.builder()
                .host(host)
                .port(port)
                .useTls(useTls)
                .userId(userId)
                .deviceId(deviceId)
                .authToken(authToken)
                .build());
    }

    /** Raw handle for {@link LaneE2eeDevice} APIs that need the session pointer. */
    public long rawHandle() {
        ensureOpen();
        return handle;
    }

    public void ping() throws LaneException {
        ensureOpen();
        int code = nativePing(handle);
        if (code != 0) {
            throw LaneException.fromCode(code);
        }
    }

    public void setResumeSeq(long seq) throws LaneException {
        ensureOpen();
        int code = nativeSetResumeSeq(handle, seq);
        if (code != 0) {
            throw LaneException.fromCode(code);
        }
    }

    /**
     * Poll one inbound event as JSON (same schema as Swift / C {@code poll_event}).
     * Returns {@code null} on timeout / empty.
     */
    public String pollEvent(long timeoutMs) throws LaneException {
        ensureOpen();
        return nativePollEvent(handle, timeoutMs);
    }

    public long sendChat(String to, String messageId, byte[] body) throws LaneException {
        return sendChat(to, messageId, body, "");
    }

    public long sendChat(String to, String messageId, String utf8Body) throws LaneException {
        return sendChat(to, messageId, utf8Body.getBytes(StandardCharsets.UTF_8), "");
    }

    public long sendChat(String to, String messageId, byte[] body, String mediaId)
            throws LaneException {
        ensureOpen();
        Objects.requireNonNull(to, "to");
        Objects.requireNonNull(messageId, "messageId");
        byte[] bytes = body != null ? body : new byte[0];
        long seq;
        if (mediaId == null || mediaId.isEmpty()) {
            seq = nativeSendChat(handle, to, messageId, bytes);
        } else {
            seq = nativeSendChatWithMedia(handle, to, messageId, bytes, mediaId);
        }
        if (seq < 0) {
            throw LaneException.fromLastError("sendChat failed");
        }
        return seq;
    }

    public long sendChatRetry(String to, String messageId, byte[] body, int maxAttempts)
            throws LaneException {
        ensureOpen();
        long seq = nativeSendChatRetry(handle, to, messageId, body != null ? body : new byte[0], maxAttempts);
        if (seq < 0) {
            throw LaneException.fromLastError("sendChatRetry failed");
        }
        return seq;
    }

    public void ackDelivered(String messageId) throws LaneException {
        ensureOpen();
        int code = nativeAckDelivered(handle, messageId);
        if (code != 0) {
            throw LaneException.fromCode(code);
        }
    }

    public void ackRead(String messageId) throws LaneException {
        ensureOpen();
        int code = nativeAckRead(handle, messageId);
        if (code != 0) {
            throw LaneException.fromCode(code);
        }
    }

    /** CSV or collection of contact user ids for presence filtering. */
    public void subscribePresence(Collection<String> contactIds) throws LaneException {
        Objects.requireNonNull(contactIds, "contactIds");
        subscribePresence(String.join(",", contactIds));
    }

    public void subscribePresence(String contactIdsCsv) throws LaneException {
        ensureOpen();
        int code = nativeSubscribePresence(handle, contactIdsCsv != null ? contactIdsCsv : "");
        if (code != 0) {
            throw LaneException.fromCode(code);
        }
    }

    /**
     * Presence kind: {@code 0} available, {@code 1} unavailable (see wire Presence).
     */
    public void sendPresence(int kind) throws LaneException {
        ensureOpen();
        int code = nativeSendPresence(handle, kind);
        if (code != 0) {
            throw LaneException.fromCode(code);
        }
    }

    public long createGroup(String groupId) throws LaneException {
        ensureOpen();
        long version = nativeCreateGroup(handle, groupId);
        if (version < 0) {
            throw LaneException.fromLastError("createGroup failed");
        }
        return version;
    }

    public long addMember(String groupId, String user) throws LaneException {
        ensureOpen();
        long version = nativeAddMember(handle, groupId, user);
        if (version < 0) {
            throw LaneException.fromLastError("addMember failed");
        }
        return version;
    }

    public long removeMember(String groupId, String user) throws LaneException {
        ensureOpen();
        long version = nativeRemoveMember(handle, groupId, user);
        if (version < 0) {
            throw LaneException.fromLastError("removeMember failed");
        }
        return version;
    }

    public long leaveGroup(String groupId) throws LaneException {
        ensureOpen();
        long version = nativeLeaveGroup(handle, groupId);
        if (version < 0) {
            throw LaneException.fromLastError("leaveGroup failed");
        }
        return version;
    }

    public void sendGroup(String groupId, String messageId, byte[] body) throws LaneException {
        sendGroup(groupId, messageId, body, "");
    }

    public void sendGroup(String groupId, String messageId, byte[] body, String mediaId)
            throws LaneException {
        ensureOpen();
        byte[] bytes = body != null ? body : new byte[0];
        int code;
        if (mediaId == null || mediaId.isEmpty()) {
            code = nativeSendGroup(handle, groupId, messageId, bytes);
        } else {
            code = nativeSendGroupWithMedia(handle, groupId, messageId, bytes, mediaId);
        }
        if (code != 0) {
            throw LaneException.fromCode(code);
        }
    }

    public long uploadMedia(String mediaId, String fileName, String mimeType, byte[] data)
            throws LaneException {
        ensureOpen();
        long n = nativeUploadMedia(
                handle,
                mediaId,
                fileName != null ? fileName : "file",
                mimeType != null ? mimeType : "application/octet-stream",
                data != null ? data : new byte[0]
        );
        if (n < 0) {
            throw LaneException.fromLastError("uploadMedia failed");
        }
        return n;
    }

    public MediaBlob fetchMedia(String mediaId) throws LaneException {
        ensureOpen();
        Object[] parts = nativeFetchMedia(handle, mediaId);
        if (parts == null || parts.length < 4) {
            throw LaneException.fromLastError("fetchMedia failed");
        }
        byte[] data = parts[0] instanceof byte[] ? (byte[]) parts[0] : new byte[0];
        String name = parts[1] != null ? parts[1].toString() : "";
        String mime = parts[2] != null ? parts[2].toString() : "";
        String sha = parts[3] != null ? parts[3].toString() : "";
        return new MediaBlob(data, name, mime, sha);
    }

    private void ensureOpen() {
        if (closed.get() || handle == 0L) {
            throw new IllegalStateException("LaneSession is closed");
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        long h = handle;
        handle = 0L;
        if (h != 0L) {
            try {
                nativeClose(h);
            } finally {
                nativeFree(h);
            }
        }
    }

    // ---- JNI ----------------------------------------------------------------

    private static native long nativeConnect(
            String host,
            int port,
            boolean useTls,
            String userId,
            String deviceId,
            String authToken,
            String clientVersion,
            long resumeAfterSeq,
            long pingIntervalSecs
    );

    private native int nativePing(long handle);

    private native int nativeClose(long handle);

    private native void nativeFree(long handle);

    private native int nativeSetResumeSeq(long handle, long seq);

    private native String nativePollEvent(long handle, long timeoutMs);

    private native long nativeSendChat(long handle, String to, String messageId, byte[] body);

    private native long nativeSendChatWithMedia(
            long handle, String to, String messageId, byte[] body, String mediaId);

    private native long nativeSendChatRetry(
            long handle, String to, String messageId, byte[] body, int maxAttempts);

    private native int nativeAckDelivered(long handle, String messageId);

    private native int nativeAckRead(long handle, String messageId);

    private native int nativeSubscribePresence(long handle, String contactIdsCsv);

    private native int nativeSendPresence(long handle, int kind);

    private native long nativeCreateGroup(long handle, String groupId);

    private native long nativeAddMember(long handle, String groupId, String user);

    private native long nativeRemoveMember(long handle, String groupId, String user);

    private native long nativeLeaveGroup(long handle, String groupId);

    private native int nativeSendGroup(long handle, String groupId, String messageId, byte[] body);

    private native int nativeSendGroupWithMedia(
            long handle, String groupId, String messageId, byte[] body, String mediaId);

    private native long nativeUploadMedia(
            long handle, String mediaId, String fileName, String mimeType, byte[] data);

    private native Object[] nativeFetchMedia(long handle, String mediaId);
}
