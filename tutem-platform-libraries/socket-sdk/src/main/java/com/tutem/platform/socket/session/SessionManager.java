package com.tutem.platform.socket.session;

import java.util.Collection;
import java.util.Optional;

/**
 * Manages the mapping between userId, sessionId, and SocketSession.
 *
 * <p>A single user may have MANY concurrent sessions (phone + tablet + web),
 * so {@link #findAllByUserId(String)} is the authoritative user lookup and
 * outbound fan-out must use it.
 *
 * <p>Default implementation is in-memory (InMemorySessionManager).
 * Services can override with a Redis-backed implementation for
 * multi-pod deployments by providing their own SessionManager bean.
 *
 * <p><strong>WARNING - a shared/distributed implementation needs one extra setting.</strong>
 * A {@code SessionManager} backed by a store that several pods can see (Redis, Hazelcast, a
 * database) MUST be deployed with
 * {@code app.socket.heartbeat.evict-ghost-sessions=false}. The heartbeat sweep decides
 * whether a session is a "ghost" by asking the <em>local</em> Netty server whether it owns
 * that client; with a shared store the honest answer is "no" for every session owned by
 * another pod, so each pod would evict every other pod's sessions on every pass (default:
 * every 30s) and continuously destroy presence data.
 *
 * <p>Also note that replacing this bean makes <em>presence and user lookup</em> cross-pod,
 * and nothing else. Socket.IO rooms and broadcasts still live inside a single
 * {@code SocketIOServer} instance, so {@code sendToRoom}/{@code broadcast} reach only the
 * clients connected to the pod that issues them. Fanning those out across pods needs a
 * separate mechanism (a message broker, or netty-socketio's store factory).
 *
 * <p>Implementations must be thread-safe: they are called from Netty worker
 * threads and from the heartbeat cleanup thread.
 */
public interface SessionManager {

    void save(SocketSession session);

    Optional<SocketSession> findBySessionId(String sessionId);

    /**
     * All live sessions for a user, in no particular order.
     * Returns an empty collection (never {@code null}) when the user has none.
     */
    Collection<SocketSession> findAllByUserId(String userId);

    /**
     * @deprecated a user can have multiple concurrent sessions; this returns an
     *             arbitrary one of them and silently ignores the rest.
     *             Use {@link #findAllByUserId(String)}.
     */
    @Deprecated
    default Optional<SocketSession> findByUserId(String userId) {
        return findAllByUserId(userId).stream().findFirst();
    }

    void removeBySessionId(String sessionId);

    /** Immutable snapshot of all live sessions. */
    Collection<SocketSession> getAll();

    boolean isConnected(String userId);

    int getConnectedCount();
}
