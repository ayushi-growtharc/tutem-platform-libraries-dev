package com.tutem.platform.socket.session;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default in-memory SessionManager.
 * Suitable for single-pod deployments.
 *
 * For multi-pod deployments, provide a Redis-backed implementation:
 *
 *   @Bean
 *   @Primary
 *   public SessionManager redisSessionManager(...) {
 *       return new RedisSessionManager(...);
 *   }
 */
public class InMemorySessionManager implements SessionManager {

    // sessionId → SocketSession
    private final ConcurrentHashMap<String, SocketSession> bySessionId = new ConcurrentHashMap<>();

    // userId → sessionId (latest session wins — handles re-connects)
    private final ConcurrentHashMap<String, String> userIdToSessionId = new ConcurrentHashMap<>();

    @Override
    public void save(SocketSession session) {
        bySessionId.put(session.getSessionId(), session);
        if (session.getUserId() != null) {
            userIdToSessionId.put(session.getUserId(), session.getSessionId());
        }
    }

    @Override
    public Optional<SocketSession> findBySessionId(String sessionId) {
        return Optional.ofNullable(bySessionId.get(sessionId));
    }

    @Override
    public Optional<SocketSession> findByUserId(String userId) {
        String sessionId = userIdToSessionId.get(userId);
        if (sessionId == null) return Optional.empty();
        return Optional.ofNullable(bySessionId.get(sessionId));
    }

    @Override
    public void removeBySessionId(String sessionId) {
        SocketSession session = bySessionId.remove(sessionId);
        if (session != null && session.getUserId() != null) {
            userIdToSessionId.remove(session.getUserId(), sessionId);
        }
    }

    @Override
    public Collection<SocketSession> getAll() {
        return Collections.unmodifiableCollection(bySessionId.values());
    }

    @Override
    public boolean isConnected(String userId) {
        return userIdToSessionId.containsKey(userId);
    }

    @Override
    public int getConnectedCount() {
        return bySessionId.size();
    }
}
