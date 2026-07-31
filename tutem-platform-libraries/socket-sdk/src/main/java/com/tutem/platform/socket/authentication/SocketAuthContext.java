package com.tutem.platform.socket.authentication;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;

import java.util.Map;

/**
 * Result of a successful authentication.
 * Returned by SocketAuthenticationHook.authenticate().
 * Claims are stored in SocketSession.attributes for handler access.
 *
 * <p>{@code claims} is deliberately excluded from {@link #toString()}: hooks typically build
 * this straight from a decoded JWT, so the map routinely holds the raw token, a phone number,
 * or other PII, and a consumer logging the context it just built would leak all of it.
 * {@code SocketSession} excludes its {@code attributes} for the same reason.
 */
@Data
@Builder
@ToString(of = {"userId", "role"})
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
