package com.tutem.platform.socket.authentication;

import lombok.Builder;
import lombok.Data;

import java.util.Map;

/**
 * Result of a successful authentication.
 * Returned by SocketAuthenticationHook.authenticate().
 * Claims are stored in SocketSession.attributes for handler access.
 */
@Data
@Builder
public class SocketAuthContext {

    private final String userId;
    private final String role;

    @Builder.Default
    private final Map<String, Object> claims = Map.of();

    public static SocketAuthContext anonymous() {
        return SocketAuthContext.builder().build();
    }

    public static SocketAuthContext of(String userId, String role) {
        return SocketAuthContext.builder().userId(userId).role(role).build();
    }
}
