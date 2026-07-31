package com.tutem.platform.socket.authentication;

import com.corundumstudio.socketio.HandshakeData;

/**
 * ALLOW-ALL fallback auth hook: every connection is accepted as anonymous
 * (no userId, no role).
 *
 * <p>It is only installed when {@code app.socket.auth.enabled=false}. With auth
 * enabled, the consuming service MUST provide its own
 * {@link SocketAuthenticationHook} bean; a context without a userId is rejected
 * by the connect path, so this hook cannot accidentally let traffic through.
 *
 * <p><strong>Never rely on this hook in production.</strong> Any client that can
 * reach the socket port becomes a fully privileged, unidentified session, and
 * because sessions have no userId they are also unreachable via
 * {@code MessageDispatcher.sendToUser}.
 */
public class DefaultSocketAuthenticationHook implements SocketAuthenticationHook {

    @Override
    public SocketAuthContext authenticate(HandshakeData handshakeData) {
        return SocketAuthContext.anonymous();
    }
}
