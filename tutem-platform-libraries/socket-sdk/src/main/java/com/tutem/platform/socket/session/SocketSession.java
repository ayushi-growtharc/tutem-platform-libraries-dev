package com.tutem.platform.socket.session;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Represents one connected client's session.
 * Created on connect, destroyed on disconnect.
 *
 * <p>Thread-safety: instances are published to Netty worker threads (inbound messages)
 * and to the {@code socket-heartbeat} cleanup thread. {@code lastActiveAt} is therefore
 * {@code volatile} and {@code attributes} is a concurrent map.
 *
 * <p>Deliberately NOT a Lombok {@code @Data}: no {@code equals}/{@code hashCode} is
 * generated (they would span the mutable attributes map, breaking any Set/Map that
 * holds sessions) and no blanket setters exist (the identity fields are immutable).
 */
@Getter
@Builder
@ToString(of = {"sessionId", "userId", "role", "remoteAddress", "connectedAt"})
public class SocketSession {

    private final String sessionId;
    private final String userId;       // set by SocketAuthenticationHook; null if auth disabled
    private final String role;         // set by SocketAuthenticationHook; null if auth disabled
    private final String remoteAddress;
    private final Instant connectedAt;

    /**
     * Last time this session was seen alive. Written from Netty worker threads
     * (on inbound messages / connect) and read from the heartbeat cleanup thread.
     */
    @Builder.Default
    private volatile Instant lastActiveAt = Instant.now();

    /** Service-specific per-session data (e.g. current rideId, room name) */
    @Builder.Default
    private final Map<String, Object> attributes = new ConcurrentHashMap<>();

    public void setAttribute(String key, Object value) {
        if (key == null || value == null) {
            return; // ConcurrentHashMap forbids nulls; silently ignoring beats an NPE on the hot path
        }
        attributes.put(key, value);
    }

    public Object getAttribute(String key) {
        return key == null ? null : attributes.get(key);
    }

    /** Immutable snapshot of the per-session attributes. */
    public Map<String, Object> getAttributes() {
        return Collections.unmodifiableMap(new HashMap<>(attributes));
    }

    public void refreshActivity() {
        this.lastActiveAt = Instant.now();
    }
}
