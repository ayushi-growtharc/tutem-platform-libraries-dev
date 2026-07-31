package com.tutem.platform.socket.session;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Represents one connected client's session.
 * Created on connect, destroyed on disconnect.
 */
@Data
@Builder
public class SocketSession {

    private final String sessionId;
    private final String userId;       // set by SocketAuthenticationHook; null if auth disabled
    private final String remoteAddress;
    private final Instant connectedAt;

    @Builder.Default
    private Instant lastActiveAt = Instant.now();

    /** Service-specific per-session data (e.g. current rideId, room name) */
    @Builder.Default
    private final Map<String, Object> attributes = new ConcurrentHashMap<>();

    public void setAttribute(String key, Object value) {
        attributes.put(key, value);
    }

    public Object getAttribute(String key) {
        return attributes.get(key);
    }

    public void refreshActivity() {
        this.lastActiveAt = Instant.now();
    }
}
