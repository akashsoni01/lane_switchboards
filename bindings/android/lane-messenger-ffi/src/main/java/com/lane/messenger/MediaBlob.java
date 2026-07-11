package com.lane.messenger;

import java.util.Arrays;
import java.util.Objects;

/** Result of {@link LaneSession#fetchMedia(String)}. */
public final class MediaBlob {
    public final byte[] data;
    public final String fileName;
    public final String mimeType;
    public final String sha256Hex;

    public MediaBlob(byte[] data, String fileName, String mimeType, String sha256Hex) {
        this.data = data != null ? data : new byte[0];
        this.fileName = fileName != null ? fileName : "";
        this.mimeType = mimeType != null ? mimeType : "";
        this.sha256Hex = sha256Hex != null ? sha256Hex : "";
    }

    public int size() {
        return data.length;
    }

    @Override
    public String toString() {
        return "MediaBlob{fileName='" + fileName + "', mimeType='" + mimeType
                + "', size=" + data.length + ", sha256=" + sha256Hex + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MediaBlob)) return false;
        MediaBlob that = (MediaBlob) o;
        return Arrays.equals(data, that.data)
                && Objects.equals(fileName, that.fileName)
                && Objects.equals(mimeType, that.mimeType)
                && Objects.equals(sha256Hex, that.sha256Hex);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(fileName, mimeType, sha256Hex);
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }
}
