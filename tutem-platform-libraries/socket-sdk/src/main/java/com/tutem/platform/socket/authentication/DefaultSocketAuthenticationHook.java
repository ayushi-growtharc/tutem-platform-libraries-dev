package com.tutem.platform.socket.authentication;

import com.corundumstudio.socketio.HandshakeData;
import lombok.extern.slf4j.Slf4j;

/**
 * Default no-op auth hook used when app.socket.auth.enabled=false
 * or when the service has not provided its own SocketAuthenticationHook bean.
 * Allows all connections through as anonymous.
 */
@Slf4j
public class DefaultSocketAuthenticationHook implements SocketAuthenticationHook {

    @Override
    public SocketAuthContext authenticate(HandshakeData handshakeData) {
        return SocketAuthContext.anonymous();
    }
}
