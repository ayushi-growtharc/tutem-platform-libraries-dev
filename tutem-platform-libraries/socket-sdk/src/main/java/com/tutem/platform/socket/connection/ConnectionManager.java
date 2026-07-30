package com.tutem.platform.socket.connection;

import com.corundumstudio.socketio.SocketIOClient;
import com.corundumstudio.socketio.SocketIOServer;
import com.tutem.platform.socket.session.SessionManager;
import com.tutem.platform.socket.session.SocketSession;
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

    public void disconnect(String userId) {
        sessionManager.findByUserId(userId).ifPresent(session ->
            getClient(session.getSessionId()).ifPresent(SocketIOClient::disconnect)
        );
    }

    private Optional<SocketIOClient> getClient(String sessionId) {
        try {
            SocketIOClient client = server.getClient(UUID.fromString(sessionId));
            return Optional.ofNullable(client);
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }
}
