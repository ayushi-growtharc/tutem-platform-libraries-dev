package com.tutem.platform.socket.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

/**
 * All socket server settings, read from application.yml under app.socket.*
 *
 * app:
 *   socket:
 *     port: 9090
 *     host: 0.0.0.0
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
 */
@Data
@ConfigurationProperties(prefix = "app.socket")
public class SocketProperties {

    private int port = 9090;
    private String host = "0.0.0.0";
    private int bossCount = 1;
    private int workerCount = 100;
    private boolean allowCustomRequests = true;
    private int upgradeTimeout = 10000;
    private int pingTimeout = 60000;
    private int pingInterval = 25000;

    @NestedConfigurationProperty
    private AuthProperties auth = new AuthProperties();

    @NestedConfigurationProperty
    private MetricsProperties metrics = new MetricsProperties();

    @NestedConfigurationProperty
    private TracingProperties tracing = new TracingProperties();

    @NestedConfigurationProperty
    private HeartbeatProperties heartbeat = new HeartbeatProperties();

    @Data
    public static class AuthProperties {
        /** When true, SocketAuthenticationHook bean is required and called on every connect */
        private boolean enabled = false;
    }

    @Data
    public static class MetricsProperties {
        private boolean enabled = true;
    }

    @Data
    public static class TracingProperties {
        private boolean enabled = true;
    }

    @Data
    public static class HeartbeatProperties {
        /** Sessions with no activity beyond this threshold are considered stale and removed */
        private int staleSessionSeconds = 120;
        /** How often to run the stale session cleanup (seconds) */
        private int cleanupIntervalSeconds = 30;
    }
}
