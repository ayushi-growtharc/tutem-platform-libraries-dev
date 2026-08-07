package com.tutem.platform.socket.heartbeat;

import com.corundumstudio.socketio.SocketIOClient;
import com.corundumstudio.socketio.SocketIOServer;
import com.tutem.platform.socket.session.SessionManager;
import com.tutem.platform.socket.session.SocketSession;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Evicts ghost sessions — entries in the {@link SessionManager} whose underlying
 * Netty client no longer exists or whose channel is closed. Without this, session
 * stores accumulate dead entries and outbound fan-out wastes work on them.
 *
 * <p>Ghost eviction is driven by the transport's own view of the connection, not by
 * application-level activity — which makes it safe for the default, pod-local
 * {@code InMemorySessionManager}. It is NOT safe for a shared/distributed
 * {@code SessionManager}: this pod's Netty server does not own the other pods'
 * connections, so it would classify every remote session as a ghost and delete it.
 * Such deployments MUST set {@code app.socket.heartbeat.evict-ghost-sessions=false}.
 *
 * <p>Idle-based disconnection is a DIFFERENT, opt-in behaviour
 * ({@code app.socket.heartbeat.disconnect-stale-sessions=true}, default false).
 * {@code lastActiveAt} only advances on inbound messages, so a healthy
 * listen-only subscriber (the normal mobile client) looks "idle" forever —
 * disconnecting on that basis kills working connections.
 *
 * <p>Owns its own single daemon thread; it does not rely on Spring's
 * {@code @Scheduled}/{@code @EnableScheduling} infrastructure.
 */
@Slf4j
@RequiredArgsConstructor
public class HeartbeatManager implements InitializingBean, DisposableBean {

    private final SocketIOServer server;
    private final SessionManager sessionManager;
    private final int staleSessionSeconds;
    private final int cleanupIntervalSeconds;
    private final boolean disconnectStaleSessions;
    /** See {@code app.socket.heartbeat.evict-ghost-sessions}; false for shared stores. */
    private final boolean evictGhostSessions;

    private ScheduledExecutorService scheduler;

    @Override
    public void afterPropertiesSet() {
        long interval = Math.max(1L, cleanupIntervalSeconds);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "socket-heartbeat");
            thread.setDaemon(true);
            return thread;
        });
        // Never let a failure inside one pass kill the scheduled task.
        scheduler.scheduleWithFixedDelay(this::runCleanupPass, interval, interval, TimeUnit.SECONDS);
        log.info("Heartbeat cleanup started: every {}s (evictGhostSessions={}, "
                + "disconnectStaleSessions={}, staleSessionSeconds={})",
            interval, evictGhostSessions, disconnectStaleSessions, staleSessionSeconds);
        if (!evictGhostSessions) {
            log.info("Ghost-session eviction is disabled "
                + "(app.socket.heartbeat.evict-ghost-sessions=false) - correct for a shared or "
                + "distributed SessionManager, where this pod cannot judge another pod's sessions");
        }
    }

    @Override
    public void destroy() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
            log.info("Heartbeat cleanup stopped");
        }
    }

    private void runCleanupPass() {
        try {
            cleanupStaleSessions();
        } catch (Exception e) {
            log.error("Heartbeat cleanup pass failed: {}", e.getMessage(), e);
        } catch (Throwable t) {
            // A scheduled task that throws is silently cancelled forever — log and swallow.
            log.error("Heartbeat cleanup pass failed fatally: {}", t.getMessage(), t);
        }
    }

    /**
     * One cleanup pass. Public so it can be triggered manually (tests, actuator endpoint).
     */
    public void cleanupStaleSessions() {
        if (!evictGhostSessions && !disconnectStaleSessions) {
            // Nothing this pass could legally do.
            return;
        }
        // Only needed by the opt-in idle branch below; not computed otherwise.
        Instant idleThreshold = disconnectStaleSessions
            ? Instant.now().minus(Duration.ofSeconds(Math.max(1, staleSessionSeconds)))
            : null;
        int ghostsRemoved = 0;
        int idleDisconnected = 0;

        for (SocketSession session : sessionManager.getAll()) {
            String sessionId = session.getSessionId();
            if (sessionId == null) {
                continue;
            }

            UUID clientId = parseClientId(sessionId);
            if (clientId == null) {
                // Not a Netty session id (custom SessionManager) — this manager cannot
                // judge its liveness, so leave it alone rather than evict blindly.
                continue;
            }

            SocketIOClient client = resolveClient(clientId);

            // 1. Ghost session: transport is gone → the store entry is garbage.
            //    Only meaningful when the session store is pod-local; with a shared store
            //    "this server does not own the client" is the normal case, not a ghost.
            if (client == null || !client.isChannelOpen()) {
                if (!evictGhostSessions) {
                    if (log.isDebugEnabled()) {
                        log.debug("Not evicting sessionId={}: this server does not own the client and "
                            + "evict-ghost-sessions=false", sessionId);
                    }
                    continue;
                }
                sessionManager.removeBySessionId(sessionId);
                ghostsRemoved++;
                if (log.isDebugEnabled()) {
                    log.debug("Evicted ghost session: sessionId={} userId={}", sessionId, session.getUserId());
                }
                continue;
            }

            // 2. Opt-in only: forcibly disconnect clients with no INBOUND traffic for a while.
            if (disconnectStaleSessions) {
                Instant lastActiveAt = session.getLastActiveAt();
                if (lastActiveAt != null && lastActiveAt.isBefore(idleThreshold)) {
                    try {
                        client.disconnect();
                    } catch (Exception e) {
                        log.debug("Failed to disconnect idle sessionId={}: {}", sessionId, e.getMessage());
                    }
                    sessionManager.removeBySessionId(sessionId);
                    idleDisconnected++;
                    log.debug("Disconnected idle session: sessionId={} userId={} lastActive={}",
                        sessionId, session.getUserId(), lastActiveAt);
                }
            }
        }

        if (ghostsRemoved > 0 || idleDisconnected > 0) {
            log.info("Heartbeat cleanup: evicted {} ghost session(s), disconnected {} idle session(s)",
                ghostsRemoved, idleDisconnected);
        }
    }

    private UUID parseClientId(String sessionId) {
        try {
            return UUID.fromString(sessionId);
        } catch (IllegalArgumentException e) {
            log.debug("Session id is not a Netty client UUID, skipping: sessionId={}", sessionId);
            return null;
        }
    }

    private SocketIOClient resolveClient(UUID clientId) {
        try {
            return server.getClient(clientId);
        } catch (Exception e) {
            log.debug("Failed to resolve client for sessionId={}: {}", clientId, e.getMessage());
            return null;
        }
    }
}
