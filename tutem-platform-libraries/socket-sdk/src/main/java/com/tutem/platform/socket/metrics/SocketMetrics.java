package com.tutem.platform.socket.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import com.tutem.platform.socket.session.SessionManager;
import lombok.extern.slf4j.Slf4j;

/**
 * Exposes socket connection and message metrics via Micrometer.
 * Automatically available in Prometheus/Grafana when actuator is on the classpath.
 *
 * Metrics published:
 *   socket.connections.active      — gauge, current connected clients
 *   socket.connections.total       — counter, total connects since startup
 *   socket.disconnections.total    — counter
 *   socket.messages.received       — counter, tagged by event name
 *   socket.errors.total            — counter, tagged by event name
 *   socket.auth.failures           — counter
 */
@Slf4j
public class SocketMetrics {

    private final Counter connectCounter;
    private final Counter disconnectCounter;
    private final Counter authFailureCounter;
    private final MeterRegistry meterRegistry;
    private final boolean enabled;

    public SocketMetrics(MeterRegistry meterRegistry, SessionManager sessionManager, boolean enabled) {
        this.meterRegistry = meterRegistry;
        this.enabled = enabled;

        if (enabled) {
            this.connectCounter = Counter.builder("socket.connections.total")
                .description("Total socket connections since startup")
                .register(meterRegistry);

            this.disconnectCounter = Counter.builder("socket.disconnections.total")
                .description("Total socket disconnections since startup")
                .register(meterRegistry);

            this.authFailureCounter = Counter.builder("socket.auth.failures")
                .description("Total authentication failures")
                .register(meterRegistry);

            Gauge.builder("socket.connections.active", sessionManager, SessionManager::getConnectedCount)
                .description("Currently connected socket clients")
                .register(meterRegistry);

            log.info("Socket metrics enabled");
        } else {
            this.connectCounter = null;
            this.disconnectCounter = null;
            this.authFailureCounter = null;
        }
    }

    public void incrementConnect() {
        if (enabled) connectCounter.increment();
    }

    public void incrementDisconnect() {
        if (enabled) disconnectCounter.increment();
    }

    public void incrementAuthFailure() {
        if (enabled) authFailureCounter.increment();
    }

    public void incrementMessageReceived(String event) {
        if (enabled) {
            Counter.builder("socket.messages.received")
                .tag("event", event)
                .description("Total messages received per event")
                .register(meterRegistry)
                .increment();
        }
    }

    public void incrementError(String event) {
        if (enabled) {
            Counter.builder("socket.errors.total")
                .tag("event", event)
                .description("Total errors per event")
                .register(meterRegistry)
                .increment();
        }
    }
}
