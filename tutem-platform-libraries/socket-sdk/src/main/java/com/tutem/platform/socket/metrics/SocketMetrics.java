package com.tutem.platform.socket.metrics;

/**
 * SDK-owned metrics abstraction for the socket layer.
 *
 * <p>Deliberately free of any Micrometer type: actuator and micrometer-tracing are
 * {@code compileOnly} dependencies of this SDK, so no type that the runtime always
 * loads may mention them in a field, parameter, or return type. Consumers that put
 * actuator on their classpath get {@link MicrometerSocketMetrics}; everybody else
 * gets {@link NoOpSocketMetrics}.
 *
 * <p>Metrics published by the Micrometer implementation:
 * <ul>
 *   <li>{@code socket.connections.active} - gauge, currently connected clients</li>
 *   <li>{@code socket.connections.total} - counter, total connects since startup</li>
 *   <li>{@code socket.disconnections.total} - counter</li>
 *   <li>{@code socket.messages.received} - counter, tagged by event name</li>
 *   <li>{@code socket.errors.total} - counter, tagged by event name</li>
 *   <li>{@code socket.auth.failures} - counter</li>
 * </ul>
 *
 * <p>Implementations must be thread-safe: these methods are called from Netty
 * worker threads.
 */
public interface SocketMetrics {

    /** Record one successful client connection. */
    void incrementConnect();

    /** Record one client disconnection. */
    void incrementDisconnect();

    /** Record one rejected authentication attempt. */
    void incrementAuthFailure();

    /**
     * Record one inbound message.
     *
     * @param event the socket event name (client-supplied, so cardinality must be bounded
     *              by the implementation)
     */
    void incrementMessageReceived(String event);

    /**
     * Record one handler error.
     *
     * @param event the socket event name that failed
     */
    void incrementError(String event);
}
