package com.tutem.platform.socket.session;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default in-memory SessionManager.
 * Suitable for single-pod deployments.
 *
 * <p>{@code bySessionId} is the single source of truth for liveness;
 * {@code userIdToSessionIds} is only an index into it, so a partially applied
 * update can never make {@link #isConnected(String)} disagree with
 * {@link #findAllByUserId(String)}.
 *
 * <p>For multi-pod deployments, provide a Redis-backed implementation:
 *
 * <pre>
 *   &#64;Bean
 *   &#64;Primary
 *   public SessionManager redisSessionManager(...) {
 *       return new RedisSessionManager(...);
 *   }
 * </pre>
 *
 * <p><strong>WARNING - if you take that advice, also disable ghost eviction:</strong>
 *
 * <pre>
 *   app:
 *     socket:
 *       heartbeat:
 *         evict-ghost-sessions: false
 * </pre>
 *
 * The heartbeat sweep treats "the local Netty server does not know this client id" as proof
 * that the session is dead. That holds only while the store is pod-local, as it is here. With
 * a store shared by N pods it is false for every session owned by another pod, so all N pods
 * would delete each other's sessions on every pass and presence data would never survive.
 *
 * <p>Swapping this bean also does not make rooms or broadcasts cross pods:
 * {@code sendToRoom}/{@code broadcast} operate on the single local {@code SocketIOServer}
 * instance. Cross-pod fan-out needs a broker or netty-socketio's store factory.
 */
public class InMemorySessionManager implements SessionManager {

    /** sessionId -> SocketSession. Authoritative: presence here means "connected". */
    private final ConcurrentHashMap<String, SocketSession> bySessionId = new ConcurrentHashMap<>();

    /** userId -> set of sessionIds. Pure index; entries are pruned when the set empties. */
    private final ConcurrentHashMap<String, Set<String>> userIdToSessionIds = new ConcurrentHashMap<>();

    @Override
    public void save(SocketSession session) {
        if (session == null || session.getSessionId() == null) {
            return;
        }
        // Authoritative map first: a session is never indexed before it is reachable.
        SocketSession previous = bySessionId.put(session.getSessionId(), session);

        String userId = session.getUserId();
        // Re-saving a sessionId under a different userId (a re-auth / identity refresh) would
        // otherwise leave the id in the OLD user's index while bySessionId already points at
        // the new one, so findAllByUserId(oldUser) would hand out the new user's session and
        // sendToUser(oldUser, ...) would deliver to the wrong socket. Drop the stale entry.
        if (previous != null && previous.getUserId() != null
            && !previous.getUserId().equals(userId)) {
            unindex(previous.getUserId(), session.getSessionId());
        }
        if (userId != null) {
            userIdToSessionIds.compute(userId, (key, existing) -> {
                Set<String> ids = (existing != null) ? existing : ConcurrentHashMap.newKeySet();
                ids.add(session.getSessionId());
                return ids;
            });
        }
    }

    @Override
    public Optional<SocketSession> findBySessionId(String sessionId) {
        if (sessionId == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(bySessionId.get(sessionId));
    }

    @Override
    public Collection<SocketSession> findAllByUserId(String userId) {
        if (userId == null) {
            return List.of();
        }
        Set<String> sessionIds = userIdToSessionIds.get(userId);
        if (sessionIds == null || sessionIds.isEmpty()) {
            return List.of();
        }
        List<SocketSession> sessions = new ArrayList<>(sessionIds.size());
        for (String sessionId : sessionIds) {
            SocketSession session = bySessionId.get(sessionId);
            if (session != null) {
                sessions.add(session);
            }
        }
        return Collections.unmodifiableList(sessions);
    }

    @Override
    public void removeBySessionId(String sessionId) {
        if (sessionId == null) {
            return;
        }
        SocketSession session = bySessionId.remove(sessionId);
        if (session == null || session.getUserId() == null) {
            return;
        }
        unindex(session.getUserId(), sessionId);
    }

    /**
     * Drops a single sessionId from one user's index entry, pruning the entry when it empties.
     *
     * <p>Removes exactly this sessionId and never the whole user entry, so a concurrent
     * re-connect on another device cannot be clobbered.
     */
    private void unindex(String userId, String sessionId) {
        userIdToSessionIds.compute(userId, (key, ids) -> {
            if (ids == null) {
                return null;
            }
            ids.remove(sessionId);
            return ids.isEmpty() ? null : ids;
        });
    }

    @Override
    public Collection<SocketSession> getAll() {
        // Snapshot copy: callers iterate while Netty threads mutate the live map.
        return Collections.unmodifiableList(new ArrayList<>(bySessionId.values()));
    }

    @Override
    public boolean isConnected(String userId) {
        // Derived from the authoritative map, so isConnected() is never true while
        // findAllByUserId() is empty.
        return !findAllByUserId(userId).isEmpty();
    }

    @Override
    public int getConnectedCount() {
        return bySessionId.size();
    }
}
