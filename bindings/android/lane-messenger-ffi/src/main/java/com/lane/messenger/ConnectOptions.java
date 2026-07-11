package com.lane.messenger;

import java.util.Objects;

/** Options for {@link LaneSession#connect(ConnectOptions)}. */
public final class ConnectOptions {
    public final String host;
    public final int port;
    public final boolean useTls;
    public final String userId;
    public final String deviceId;
    public final String authToken;
    public final String clientVersion;
    public final long resumeAfterSeq;
    public final long pingIntervalSecs;

    private ConnectOptions(Builder b) {
        this.host = Objects.requireNonNull(b.host, "host");
        this.port = b.port;
        this.useTls = b.useTls;
        this.userId = Objects.requireNonNull(b.userId, "userId");
        this.deviceId = Objects.requireNonNull(b.deviceId, "deviceId");
        this.authToken = Objects.requireNonNull(b.authToken, "authToken");
        this.clientVersion = b.clientVersion != null ? b.clientVersion : "java-ffi";
        this.resumeAfterSeq = b.resumeAfterSeq;
        this.pingIntervalSecs = b.pingIntervalSecs > 0 ? b.pingIntervalSecs : 30L;
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("port out of range: " + port);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String host = "127.0.0.1";
        private int port = 9000;
        private boolean useTls = true;
        private String userId;
        private String deviceId;
        private String authToken;
        private String clientVersion = "java-ffi";
        private long resumeAfterSeq = 0;
        private long pingIntervalSecs = 30;

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder useTls(boolean useTls) {
            this.useTls = useTls;
            return this;
        }

        public Builder userId(String userId) {
            this.userId = userId;
            return this;
        }

        public Builder deviceId(String deviceId) {
            this.deviceId = deviceId;
            return this;
        }

        public Builder authToken(String authToken) {
            this.authToken = authToken;
            return this;
        }

        public Builder clientVersion(String clientVersion) {
            this.clientVersion = clientVersion;
            return this;
        }

        public Builder resumeAfterSeq(long resumeAfterSeq) {
            this.resumeAfterSeq = resumeAfterSeq;
            return this;
        }

        public Builder pingIntervalSecs(long pingIntervalSecs) {
            this.pingIntervalSecs = pingIntervalSecs;
            return this;
        }

        public ConnectOptions build() {
            return new ConnectOptions(this);
        }
    }
}
