package com.lane.messenger;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Host-owned Olm / Megolm device. Private keys never leave Rust.
 *
 * <p>Use {@link #generate()} for a new account, or {@link #importPickle(byte[], String)}
 * to restore from encrypted pickle (store the blob in Android Keystore / EncryptedSharedPreferences).
 */
public final class LaneE2eeDevice implements AutoCloseable {
    private long handle;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    static {
        LaneNative.load();
    }

    private LaneE2eeDevice(long handle) {
        if (handle == 0L) {
            throw new IllegalArgumentException("null e2ee handle");
        }
        this.handle = handle;
    }

    public static LaneE2eeDevice generate() {
        long h = nativeGenerate();
        if (h == 0L) {
            throw new IllegalStateException("lane_e2ee_generate failed");
        }
        return new LaneE2eeDevice(h);
    }

    public static LaneE2eeDevice importPickle(byte[] pickle, String passphrase) throws LaneException {
        Objects.requireNonNull(pickle, "pickle");
        Objects.requireNonNull(passphrase, "passphrase");
        long h = nativeImportPickle(pickle, passphrase);
        if (h == 0L) {
            throw LaneException.fromLastError("importPickle failed");
        }
        return new LaneE2eeDevice(h);
    }

    public long rawHandle() {
        ensureOpen();
        return handle;
    }

    public String identityKey() throws LaneException {
        ensureOpen();
        String key = nativeIdentityKey(handle);
        if (key == null) {
            throw LaneException.fromLastError("identityKey failed");
        }
        return key;
    }

    public static String safetyNumber(String localIdentityB64, String remoteIdentityB64)
            throws LaneException {
        String n = nativeSafetyNumber(localIdentityB64, remoteIdentityB64);
        if (n == null) {
            throw LaneException.fromLastError("safetyNumber failed");
        }
        return n;
    }

    public void publish(LaneSession session, String deviceId, int otkCount) throws LaneException {
        ensureOpen();
        Objects.requireNonNull(session, "session");
        int code = nativePublish(handle, session.rawHandle(), deviceId, otkCount);
        if (code != 0) {
            throw LaneException.fromCode(code);
        }
    }

    public void publish(LaneSession session, String deviceId) throws LaneException {
        publish(session, deviceId, 20);
    }

    public long sendEncryptedChat(
            LaneSession session, String to, String messageId, byte[] plaintext
    ) throws LaneException {
        ensureOpen();
        Objects.requireNonNull(session, "session");
        long seq = nativeSendEncryptedChat(
                handle,
                session.rawHandle(),
                to,
                messageId,
                plaintext != null ? plaintext : new byte[0]
        );
        if (seq < 0) {
            throw LaneException.fromLastError("sendEncryptedChat failed");
        }
        return seq;
    }

    public long sendEncryptedChat(
            LaneSession session, String to, String messageId, String utf8Plaintext
    ) throws LaneException {
        return sendEncryptedChat(
                session, to, messageId, utf8Plaintext.getBytes(StandardCharsets.UTF_8));
    }

    public byte[] decryptChat(String fromUser, byte[] body) throws LaneException {
        ensureOpen();
        byte[] plain = nativeDecryptChat(handle, fromUser, body != null ? body : new byte[0]);
        if (plain == null) {
            throw LaneException.fromLastError("decryptChat failed");
        }
        return plain;
    }

    public byte[] exportPickle(String passphrase) throws LaneException {
        ensureOpen();
        byte[] blob = nativeExportPickle(handle, passphrase);
        if (blob == null) {
            throw LaneException.fromLastError("exportPickle failed");
        }
        return blob;
    }

    public String createGroupSession(String groupId) throws LaneException {
        ensureOpen();
        String sid = nativeCreateGroupSession(handle, groupId);
        if (sid == null) {
            throw LaneException.fromLastError("createGroupSession failed");
        }
        return sid;
    }

    public void distributeGroupKey(LaneSession session, String groupId, Collection<String> members)
            throws LaneException {
        Objects.requireNonNull(members, "members");
        distributeGroupKey(session, groupId, String.join(",", members));
    }

    public void distributeGroupKey(LaneSession session, String groupId, String membersCsv)
            throws LaneException {
        ensureOpen();
        Objects.requireNonNull(session, "session");
        int code = nativeDistributeGroupKey(
                handle, session.rawHandle(), groupId, membersCsv != null ? membersCsv : "");
        if (code != 0) {
            throw LaneException.fromCode(code);
        }
    }

    public void sendEncryptedGroup(
            LaneSession session, String groupId, String messageId, byte[] plaintext
    ) throws LaneException {
        ensureOpen();
        Objects.requireNonNull(session, "session");
        int code = nativeSendEncryptedGroup(
                handle,
                session.rawHandle(),
                groupId,
                messageId,
                plaintext != null ? plaintext : new byte[0]
        );
        if (code != 0) {
            throw LaneException.fromCode(code);
        }
    }

    public byte[] decryptGroup(String groupId, byte[] body) throws LaneException {
        ensureOpen();
        byte[] plain = nativeDecryptGroup(handle, groupId, body != null ? body : new byte[0]);
        if (plain == null) {
            throw LaneException.fromLastError("decryptGroup failed");
        }
        return plain;
    }

    /** @return true if {@code body} was a group key share and was imported */
    public boolean tryImportGroupKey(String fromUser, byte[] body) throws LaneException {
        ensureOpen();
        int imported = nativeTryImportGroupKey(handle, fromUser, body != null ? body : new byte[0]);
        if (imported < 0) {
            throw LaneException.fromLastError("tryImportGroupKey failed");
        }
        return imported != 0;
    }

    private void ensureOpen() {
        if (closed.get() || handle == 0L) {
            throw new IllegalStateException("LaneE2eeDevice is closed");
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
            nativeFree(h);
        }
    }

    private static native long nativeGenerate();

    private native void nativeFree(long handle);

    private native String nativeIdentityKey(long handle);

    private static native String nativeSafetyNumber(String localB64, String remoteB64);

    private native int nativePublish(long e2ee, long session, String deviceId, int otkCount);

    private native long nativeSendEncryptedChat(
            long e2ee, long session, String to, String messageId, byte[] plaintext);

    private native byte[] nativeDecryptChat(long e2ee, String fromUser, byte[] body);

    private native byte[] nativeExportPickle(long e2ee, String passphrase);

    private static native long nativeImportPickle(byte[] pickle, String passphrase);

    private native String nativeCreateGroupSession(long e2ee, String groupId);

    private native int nativeDistributeGroupKey(
            long e2ee, long session, String groupId, String membersCsv);

    private native int nativeSendEncryptedGroup(
            long e2ee, long session, String groupId, String messageId, byte[] plaintext);

    private native byte[] nativeDecryptGroup(long e2ee, String groupId, byte[] body);

    private native int nativeTryImportGroupKey(long e2ee, String fromUser, byte[] body);
}
