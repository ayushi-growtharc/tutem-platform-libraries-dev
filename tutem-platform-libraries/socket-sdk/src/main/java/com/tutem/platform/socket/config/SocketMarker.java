package com.tutem.platform.socket.config;

/**
 * Internal activation marker for the socket-sdk. Not part of the public API.
 *
 * <p>A bean of this type is registered by {@link SocketMarkerConfiguration}, which is
 * imported by {@code @EnableSocket}. {@code SocketAutoConfiguration} is
 * {@code @ConditionalOnBean(SocketMarker.class)}, so simply having socket-sdk on the
 * classpath does nothing: no Netty server is created and port 9090 is not opened until
 * a service explicitly opts in with {@code @EnableSocket}.
 *
 * <p>Registering the marker via {@code @Import} (rather than importing the
 * auto-configuration directly) also keeps all of the SDK's {@code @ConditionalOnMissingBean}
 * evaluation in the deferred auto-configuration phase, where consumer-provided beans such
 * as a custom {@code SessionManager} or {@code SocketAuthenticationHook} are already
 * visible and are therefore detected reliably.
 */
public class SocketMarker {
}
