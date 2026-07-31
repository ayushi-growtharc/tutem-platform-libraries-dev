package com.tutem.platform.socket.session;

import java.util.Collection;
import java.util.Optional;

/**
 * Manages the mapping between userId, sessionId, and SocketSession.
 *
 * Default implementation is in-memory (InMemorySessionManager).
 * Services can override with a Redis-backed implementation for
 * multi-pod deployments by providing their own SessionManager bean.
 */
public interface SessionManager {

    void save(SocketSession session);

    Optional<SocketSession> findBySessionId(String sessionId);

    Optional<SocketSession> findByUserId(String userId);

    void removeBySessionId(String sessionId);

    Collection<SocketSession> getAll();

    boolean isConnected(String userId);

    int getConnectedCount();
}
