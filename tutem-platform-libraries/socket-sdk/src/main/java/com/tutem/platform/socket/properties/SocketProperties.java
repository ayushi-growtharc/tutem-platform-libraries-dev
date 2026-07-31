package com.tutem.platform.socket.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

import java.util.ArrayList;
import java.util.List;

/**
 * All socket server settings, read from application.yml under {@code app.socket.*}.
 *
 * <pre>
 * app:
 *   socket:
 *     enabled: true
 *     port: 9090
 *     host: 0.0.0.0
 *     origin: https://app.example.com
 *     transports: [websocket, polling]
 *     worker-count: 100
 *     ping-timeout: 60000
 *     ping-interval: 25000
 *     auth:
 *       enabled: true
 *     metrics:
 *       enabled: true
 *     tracing:
 *       enabled: true
 *     heartbeat:
 *       stale-session-seconds: 120
 *       cleanup-interval-seconds: 30
 *       disconnect-stale-sessions: false
 *       evict-ghost-sessions: true   # MUST be false with a distributed SessionManager
 * </pre>
 */
@Data
@ConfigurationProperties(prefix = "app.socket")
public class SocketProperties {

    /**
     * Master switch for the SDK. When {@code false}, no socket beans are created and no
     * port is opened even if {@code @EnableSocket} is present. Handy for tests and for
     * profiles that do not need real-time traffic.
     *
     * <p>NOTE: this field is deliberately never read in Java code. It exists so that
     * {@code app.socket.enabled} appears in the generated configuration metadata; the
     * switch itself is enforced by {@code @ConditionalOnProperty} on
     * {@code SocketAutoConfiguration}, which runs long before this object is bound.
     * Do not "fix" it as an unused field.
     */
    private boolean enabled = true;

    /** TCP port the Socket.IO server listens on. */
    private int port = 9090;

    /** Bind address. {@code 0.0.0.0} listens on every interface. */
    private String host = "0.0.0.0";

    /** Netty boss (acceptor) thread count. */
    private int bossCount = 1;

    /** Netty worker thread count; roughly the concurrency ceiling for handler execution. */
    private int workerCount = 100;

    /**
     * Whether non-Socket.IO HTTP requests reaching this port are passed through to
     * custom handlers. Defaults to {@code false} so the socket port only speaks Socket.IO.
     */
    private boolean allowCustomRequests = false;

    /** Milliseconds a client may take to upgrade from polling to websocket. */
    private int upgradeTimeout = 10000;

    /** Milliseconds without a client pong before the connection is considered dead. */
    private int pingTimeout = 60000;

    /** Milliseconds between server pings. */
    private int pingInterval = 25000;

    /**
     * Transports the server accepts, in Socket.IO terms. Legal values are
     * {@code websocket} and {@code polling} (case-insensitive).
     *
     * <p>Defaults to both, because standard Socket.IO clients - including Flutter's
     * {@code socket_io_client} defaults - begin with long-polling and then upgrade.
     * Restricting this to {@code [websocket]} breaks those clients.
     */
    private List<String> transports = new ArrayList<>(List.of("websocket", "polling"));

    /**
     * Value netty-socketio puts in the {@code Access-Control-Allow-Origin} response header
     * of the HTTP handshake, e.g. {@code https://app.example.com}. When {@code null} (the
     * default) the header is not set.
     *
     * <p><strong>This is not access control.</strong> netty-socketio only echoes this value
     * back in a CORS header; it never compares the request's {@code Origin} against it and
     * never rejects a connection because of it. CORS is a browser-side policy, and it is not
     * enforced for the WebSocket transport at all - a native mobile client, a script or
     * {@code curl} connects regardless of what this is set to. Use
     * {@code app.socket.auth.enabled} plus a {@code SocketAuthenticationHook} to control who
     * may connect.
     */
    private String origin = null;

    /**
     * {@code SmartLifecycle} phase in which the socket server starts and stops.
     * The default puts startup after Spring Boot's web server and makes shutdown
     * participate in graceful shutdown ordering.
     */
    private int startupPhase = Integer.MAX_VALUE - 1024;

    @NestedConfigurationProperty
    private AuthProperties auth = new AuthProperties();

    @NestedConfigurationProperty
    private MetricsProperties metrics = new MetricsProperties();

    @NestedConfigurationProperty
    private TracingProperties tracing = new TracingProperties();

    @NestedConfigurationProperty
    private HeartbeatProperties heartbeat = new HeartbeatProperties();

    /** Connection authentication settings. */
    @Data
    public static class AuthProperties {
        /**
         * When {@code true} (the default), a {@code SocketAuthenticationHook} bean is
         * required - startup fails fast without one - and it is called on every connect;
         * a connection whose hook yields no {@code userId} is rejected.
         *
         * <p>Defaults to {@code true} because the alternative is shipping an open,
         * unauthenticated socket port. Set it to {@code false} only for local development
         * or tests, where anonymous connections are acceptable.
         */
        private boolean enabled = true;
    }

    /** Micrometer metrics settings. */
    @Data
    public static class MetricsProperties {
        /**
         * Publish socket metrics when Micrometer and a {@code MeterRegistry} bean are
         * available. When {@code false}, a no-op implementation is used.
         */
        private boolean enabled = true;
    }

    /** Distributed tracing settings. */
    @Data
    public static class TracingProperties {
        /**
         * Wrap handler invocations in spans when micrometer-tracing and a {@code Tracer}
         * bean are available. When {@code false}, a no-op implementation is used.
         */
        private boolean enabled = true;
    }

    /** Ghost-connection detection settings. */
    @Data
    public static class HeartbeatProperties {
        /** Sessions idle beyond this threshold (seconds) are considered stale. */
        private int staleSessionSeconds = 120;

        /** How often the stale-session sweep runs, in seconds. */
        private int cleanupIntervalSeconds = 30;

        /**
         * When {@code true}, the sweep also force-disconnects the underlying client of a
         * stale session instead of only evicting it from the session store.
         *
         * <p>Defaults to {@code false}: a session can look idle simply because the client
         * has nothing to say, and tearing down a healthy connection is worse than keeping
         * a slightly stale store entry.
         */
        private boolean disconnectStaleSessions = false;

        /**
         * When {@code true} (the default), the sweep evicts "ghost" sessions - store entries
         * whose Socket.IO client id is unknown to <em>this</em> pod's Netty server, or whose
         * channel is closed.
         *
         * <p><strong>This MUST be set to {@code false} whenever the {@code SessionManager}
         * bean is backed by a shared/distributed store</strong> (Redis, Hazelcast, a
         * database - anything where a pod can see sessions it does not own). Ghost detection
         * asks the local Netty server whether it owns the connection; with a shared store the
         * answer is legitimately "no" for every session belonging to another pod, so each pod
         * would delete every other pod's sessions on each pass and continuously destroy
         * presence data.
         *
         * <p>Leaving it {@code true} is correct - and desirable - for the default
         * {@code InMemorySessionManager}, where the store and the transport are the same pod.
         */
        private boolean evictGhostSessions = true;
    }
}
