package com.tutem.platform.socket.exception;

import com.corundumstudio.socketio.SocketIOClient;

/**
 * Implement this to customize error handling across all @OnMessage handlers.
 * If not provided, DefaultSocketErrorHandler is used.
 *
 * Example:
 *   @Component
 *   public class MyErrorHandler implements SocketErrorHandler {
 *       public void handle(SocketIOClient client, String event, Exception ex) {
 *           client.sendEvent("error", Map.of("message", ex.getMessage()));
 *           alertingService.notify(ex);
 *       }
 *   }
 */
public interface SocketErrorHandler {
    void handle(SocketIOClient client, String event, Exception ex);
}
