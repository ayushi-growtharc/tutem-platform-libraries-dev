package com.tutem.platform.socket.connection;

import com.corundumstudio.socketio.SocketIOClient;
import com.corundumstudio.socketio.SocketIOServer;
import com.tutem.platform.socket.session.SessionManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.Optional;
import java.util.UUID;

/**
 * High-level API for managing connections.
 * Services use this to query connection state and manage rooms.
 *
 * Usage in a service:
 *   connectionManager.isConnected("userId");
 *   connectionManager.joinRoom("sessionId", "driverRoom");
 *   connectionManager.getClientsInRoom("driverRoom");
 */
@Slf4j
@RequiredArgsConstructor
public class ConnectionManager {

    private final SocketIOServer server;
    private final SessionManager sessionManager;

    public boolean isConnected(String userId) {
        return sessionManager.isConnected(userId);
    }

    public int getConnectedCount() {
        return sessionManager.getConnectedCount();
    }

    public Collection<SocketIOClient> getClientsInRoom(String room) {
        return server.getRoomOperations(room).getClients();
    }

    public void joinRoom(String sessionId, String room) {
        getClient(sessionId).ifPresent(client -> {
            client.joinRoom(room);
            log.debug("Session {} joined room {}", sessionId, room);
        });
    }

    public void leaveRoom(String sessionId, String room) {
        getClient(sessionId).ifPresent(client -> {
            client.leaveRoom(room);
            log.debug("Session {} left room {}", sessionId, room);
        });
    }

    /** Disconnect ALL of this user's live sessions (a user may be on several devices). */
    public void disconnect(String userId) {
        sessionManager.findAllByUserId(userId).forEach(session ->
            getClient(session.getSessionId()).ifPresent(SocketIOClient::disconnect)
        );
    }

    /**
     * Resolves the live Netty client for a session id, or empty when the id is null, is not a
     * Socket.IO client id, or names no live client. Mirrors
     * {@code MessageDispatcher.sendToSession}: a bad session id is a no-op with a log line,
     * never an exception thrown at the caller — a session store can legitimately hand back an
     * id this server does not own, or (with a custom {@code SessionManager}) a null one.
     */
    private Optional<SocketIOClient> getClient(String sessionId) {
        if (sessionId == null) {
            log.debug("getClient: null sessionId — skipping");
            return Optional.empty();
        }
        try {
            SocketIOClient client = server.getClient(UUID.fromString(sessionId));
            return Optional.ofNullable(client);
        } catch (IllegalArgumentException e) {
            log.warn("getClient: invalid sessionId={}", sessionId);
            return Optional.empty();
        }
    }
}
