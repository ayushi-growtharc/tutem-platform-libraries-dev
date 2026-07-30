package com.tutem.platform.socket.dispatcher;

import com.corundumstudio.socketio.SocketIOClient;
import com.corundumstudio.socketio.SocketIOServer;
import com.tutem.platform.socket.session.SessionManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.UUID;

/**
 * Outbound message sender. Used by services to push events to connected clients.
 *
 * Usage:
 *   messageDispatcher.sendToUser("userId", "rideAssigned", payload);
 *   messageDispatcher.sendToRoom("driverRoom", "newRide", payload);
 *   messageDispatcher.broadcast("systemAlert", payload);
 */
@Slf4j
@RequiredArgsConstructor
public class MessageDispatcher {

    private final SocketIOServer server;
    private final SessionManager sessionManager;

    /** Push event to a specific user by userId. No-op if user is offline. */
    public void sendToUser(String userId, String event, Object payload) {
        sessionManager.findByUserId(userId).ifPresentOrElse(
            session -> sendToSession(session.getSessionId(), event, payload),
            () -> log.debug("sendToUser: userId={} is offline — skipping event={}", userId, event)
        );
    }

    /** Push event to a specific session by sessionId. */
    public void sendToSession(String sessionId, String event, Object payload) {
        try {
            SocketIOClient client = server.getClient(UUID.fromString(sessionId));
            if (client != null && client.isChannelOpen()) {
                if (payload != null) {
                    client.sendEvent(event, payload);
                } else {
                    client.sendEvent(event);
                }
                log.debug("Sent event={} to sessionId={}", event, sessionId);
            } else {
                log.debug("sendToSession: sessionId={} not found or disconnected", sessionId);
            }
        } catch (IllegalArgumentException e) {
            log.warn("sendToSession: invalid sessionId={}", sessionId);
        }
    }

    /** Push event to all clients in a room. */
    public void sendToRoom(String room, String event, Object payload) {
        if (payload != null) {
            server.getRoomOperations(room).sendEvent(event, payload);
        } else {
            server.getRoomOperations(room).sendEvent(event);
        }
        log.debug("Sent event={} to room={}", event, room);
    }

    /** Broadcast event to ALL connected clients. */
    public void broadcast(String event, Object payload) {
        if (payload != null) {
            server.getBroadcastOperations().sendEvent(event, payload);
        } else {
            server.getBroadcastOperations().sendEvent(event);
        }
        log.debug("Broadcast event={}", event);
    }
}
