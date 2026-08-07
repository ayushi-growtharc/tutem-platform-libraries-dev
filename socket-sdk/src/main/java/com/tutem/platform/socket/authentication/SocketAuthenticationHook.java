package com.tutem.platform.socket.authentication;

import com.corundumstudio.socketio.HandshakeData;
import com.tutem.platform.socket.exception.SocketAuthException;

/**
 * Plug in your own authentication logic by implementing this interface
 * and registering it as a Spring bean.
 *
 * Called on every client connect BEFORE @OnConnect handlers.
 * Throw SocketAuthException to reject the connection.
 *
 * Example JWT implementation in a consuming service:
 *
 *   @Component
 *   public class JwtSocketAuthHook implements SocketAuthenticationHook {
 *       public SocketAuthContext authenticate(HandshakeData data) {
 *           String token = data.getSingleUrlParam("token");
 *           if (token == null) throw new SocketAuthException("Token missing");
 *           String userId = jwtService.extractUserId(token);
 *           String role   = jwtService.extractRole(token);
 *           return SocketAuthContext.of(userId, role);
 *       }
 *   }
 */
public interface SocketAuthenticationHook {

    /**
     * Authenticate the connecting client.
     *
     * @param handshakeData raw handshake data (query params, headers, etc.)
     * @return SocketAuthContext with userId and claims on success
     * @throws SocketAuthException to reject the connection
     */
    SocketAuthContext authenticate(HandshakeData handshakeData) throws SocketAuthException;
}
