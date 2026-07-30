package com.tutem.platform.socket.exception;

import com.corundumstudio.socketio.SocketIOClient;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * Default error handler — logs the error and sends an "error" event back to the client.
 */
@Slf4j
public class DefaultSocketErrorHandler implements SocketErrorHandler {

    @Override
    public void handle(SocketIOClient client, String event, Exception ex) {
        log.error("Unhandled error on event={}: {}", event, ex.getMessage(), ex);
        try {
            client.sendEvent("error", Map.of(
                "event", event,
                "message", ex.getMessage() != null ? ex.getMessage() : "Internal server error"
            ));
        } catch (Exception ignored) {
            // client may have disconnected
        }
    }
}
