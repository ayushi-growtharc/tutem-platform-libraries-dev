package com.tutem.platform.socket.metrics;

import com.tutem.platform.socket.session.SessionManager;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Micrometer-backed {@link SocketMetrics}.
 *
 * <p>Only loaded when actuator/micrometer-core is on the consumer's classpath, a
 * {@code MeterRegistry} bean exists, and {@code app.socket.metrics.enabled} is not
 * {@code false}. See {@code SocketAutoConfiguration}.
 *
 * <p>Counters are created once in the constructor and cached. Per-event counters are
 * memoised in a {@link ConcurrentHashMap} so the hot path never performs a registry
 * lookup or a meter re-registration. The number of distinct {@code event} tag values is
 * additionally capped at {@link #MAX_EVENT_TAGS}, with everything beyond that folded into a
 * shared {@code event="other"} series to bound time-series cardinality in Prometheus. That cap
 * is a backstop, not the primary bound: counters are only ever recorded for event names
 * registered at startup from {@code @OnMessage} annotations, never for arbitrary names supplied
 * by a client, so cardinality is already limited by the consumer's own code.
 */
@Slf4j
public class MicrometerSocketMetrics implements SocketMetrics {

    /**
     * Maximum number of distinct {@code event} tag values tracked per counter family.
     * Beyond this, events are attributed to {@link #OVERFLOW_EVENT}.
     */
    static final int MAX_EVENT_TAGS = 100;

    /** Tag value used once {@link #MAX_EVENT_TAGS} distinct events have been seen. */
    static final String OVERFLOW_EVENT = "other";

    private static final String UNKNOWN_EVENT = "unknown";

    private final MeterRegistry registry;

    private final Counter connectCounter;
    private final Counter disconnectCounter;
    private final Counter authFailureCounter;

    private final Map<String, Counter> messageCounters = new ConcurrentHashMap<>();
    private final Map<String, Counter> errorCounters = new ConcurrentHashMap<>();

    private final Counter messageOverflowCounter;
    private final Counter errorOverflowCounter;

    public MicrometerSocketMetrics(MeterRegistry registry, SessionManager sessionManager) {
        this.registry = registry;

        this.connectCounter = Counter.builder("socket.connections.total")
            .description("Total socket connections since startup")
            .register(registry);

        this.disconnectCounter = Counter.builder("socket.disconnections.total")
            .description("Total socket disconnections since startup")
            .register(registry);

        this.authFailureCounter = Counter.builder("socket.auth.failures")
            .description("Total socket authentication failures")
            .register(registry);

        this.messageOverflowCounter = Counter.builder("socket.messages.received")
            .tag("event", OVERFLOW_EVENT)
            .description("Total messages received per event")
            .register(registry);

        this.errorOverflowCounter = Counter.builder("socket.errors.total")
            .tag("event", OVERFLOW_EVENT)
            .description("Total errors per event")
            .register(registry);

        Gauge.builder("socket.connections.active", sessionManager, SessionManager::getConnectedCount)
            .description("Currently connected socket clients")
            .register(registry);

        log.info("Socket metrics enabled (Micrometer, event tag cardinality capped at {})", MAX_EVENT_TAGS);
    }

    @Override
    public void incrementConnect() {
        connectCounter.increment();
    }

    @Override
    public void incrementDisconnect() {
        disconnectCounter.increment();
    }

    @Override
    public void incrementAuthFailure() {
        authFailureCounter.increment();
    }

    @Override
    public void incrementMessageReceived(String event) {
        counterFor(messageCounters, messageOverflowCounter, "socket.messages.received",
            "Total messages received per event", event).increment();
    }

    @Override
    public void incrementError(String event) {
        counterFor(errorCounters, errorOverflowCounter, "socket.errors.total",
            "Total errors per event", event).increment();
    }

    /**
     * Returns the cached counter for {@code event}, registering it on first use.
     * Falls back to the shared overflow counter once the tag cap is reached.
     */
    private Counter counterFor(Map<String, Counter> cache, Counter overflow,
                               String meterName, String description, String event) {
        String key = (event == null || event.isEmpty()) ? UNKNOWN_EVENT : event;

        Counter cached = cache.get(key);
        if (cached != null) {
            return cached;
        }
        if (cache.size() >= MAX_EVENT_TAGS) {
            return overflow;
        }
        return cache.computeIfAbsent(key, e -> Counter.builder(meterName)
            .tag("event", e)
            .description(description)
            .register(registry));
    }
}
