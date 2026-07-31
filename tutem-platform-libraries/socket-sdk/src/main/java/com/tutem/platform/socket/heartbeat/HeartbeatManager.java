package com.tutem.platform.socket.heartbeat;

import com.corundumstudio.socketio.SocketIOClient;
import com.corundumstudio.socketio.SocketIOServer;
import com.tutem.platform.socket.session.SessionManager;
import com.tutem.platform.socket.session.SocketSession;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

/**
 * Detects and cleans up ghost connections — clients that appear connected
 * in the session store but whose underlying TCP connection is dead.
 *
 * Runs on a fixed interval. Compares each session's lastActiveAt against
 * the configured stale threshold. Forcibly disconnects and cleans up stale sessions.
 *
 * Without this, Redis or in-memory session stores accumulate dead entries over time,
 * and MessageDispatcher.sendToUser() wastes time trying to push to dead connections.
 */
@Slf4j
@RequiredArgsConstructor
public class HeartbeatManager {

    private final SocketIOServer server;
    private final SessionManager sessionManager;
    private final int staleSessionSeconds;

    @Scheduled(fixedDelayString = "${app.socket.heartbeat.cleanup-interval-seconds:30}000")
    public void cleanupStaleSessions() {
        Instant threshold = Instant.now().minus(Duration.ofSeconds(staleSessionSeconds));
        int removed = 0;

        for (SocketSession session : sessionManager.getAll()) {
            if (session.getLastActiveAt().isBefore(threshold)) {
                try {
                    SocketIOClient client = server.getClient(UUID.fromString(session.getSessionId()));
                    if (client != null && client.isChannelOpen()) {
                        client.disconnect();
                    }
                } catch (Exception ignored) {}

                sessionManager.removeBySessionId(session.getSessionId());
                removed++;
                log.debug("Removed stale session: sessionId={} userId={} lastActive={}",
                    session.getSessionId(), session.getUserId(), session.getLastActiveAt());
            }
        }

        if (removed > 0) {
            log.info("Heartbeat cleanup: removed {} stale sessions", removed);
        }
    }
}
