package com.tutem.platform.socket.metrics;

import com.tutem.platform.socket.session.InMemorySessionManager;
import com.tutem.platform.socket.session.SocketSession;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MicrometerSocketMetricsTest {

    private SimpleMeterRegistry registry;
    private InMemorySessionManager sessionManager;
    private MicrometerSocketMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        sessionManager = new InMemorySessionManager();
        metrics = new MicrometerSocketMetrics(registry, sessionManager);
    }

    private long meterCount(String name) {
        return registry.getMeters().stream()
            .map(Meter::getId)
            .filter(id -> id.getName().equals(name))
            .count();
    }

    @Test
    @DisplayName("the base counters and the active-connections gauge are registered up front")
    void constructor_registersBaseMetersAndGauge() {
        sessionManager.save(SocketSession.builder().sessionId("s1").userId("u1").build());
        sessionManager.save(SocketSession.builder().sessionId("s2").userId("u2").build());

        assertThat(registry.get("socket.connections.total").counter()).isNotNull();
        assertThat(registry.get("socket.disconnections.total").counter()).isNotNull();
        assertThat(registry.get("socket.auth.failures").counter()).isNotNull();
        assertThat(registry.get("socket.connections.active").gauge().value()).isEqualTo(2.0);
    }

    @Test
    @DisplayName("connect / disconnect / auth-failure counters increment")
    void increment_baseCounters_areRecorded() {
        metrics.incrementConnect();
        metrics.incrementConnect();
        metrics.incrementDisconnect();
        metrics.incrementAuthFailure();

        assertThat(registry.get("socket.connections.total").counter().count()).isEqualTo(2.0);
        assertThat(registry.get("socket.disconnections.total").counter().count()).isEqualTo(1.0);
        assertThat(registry.get("socket.auth.failures").counter().count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("repeated increments for one event reuse the cached counter, not a new meter")
    void incrementMessageReceived_sameEvent_cachesTheCounter() {
        metrics.incrementMessageReceived("chat");
        long metersAfterFirst = registry.getMeters().size();

        metrics.incrementMessageReceived("chat");
        metrics.incrementMessageReceived("chat");

        assertThat(registry.getMeters()).hasSize((int) metersAfterFirst);
        assertThat(registry.get("socket.messages.received").tag("event", "chat")
            .counter().count()).isEqualTo(3.0);
    }

    @Test
    @DisplayName("error counters are tagged and cached per event")
    void incrementError_sameEvent_cachesTheCounter() {
        metrics.incrementError("joinRoom");
        long metersAfterFirst = registry.getMeters().size();
        metrics.incrementError("joinRoom");

        assertThat(registry.getMeters()).hasSize((int) metersAfterFirst);
        assertThat(registry.get("socket.errors.total").tag("event", "joinRoom")
            .counter().count()).isEqualTo(2.0);
    }

    @Test
    @DisplayName("a null or empty event name collapses into the 'unknown' tag")
    void incrementMessageReceived_nullOrEmptyEvent_usesUnknownTag() {
        metrics.incrementMessageReceived(null);
        metrics.incrementMessageReceived("");

        assertThat(registry.get("socket.messages.received").tag("event", "unknown")
            .counter().count()).isEqualTo(2.0);
    }

    @Test
    @DisplayName("high-cardinality event names collapse into the shared overflow series")
    void incrementMessageReceived_beyondTagCap_foldsIntoOverflowCounter() {
        for (int i = 0; i < MicrometerSocketMetrics.MAX_EVENT_TAGS; i++) {
            metrics.incrementMessageReceived("evt-" + i);
        }
        // The cap is now reached: every further distinct event must reuse "other".
        long metersAtCap = meterCount("socket.messages.received");

        metrics.incrementMessageReceived("evt-overflow-a");
        metrics.incrementMessageReceived("evt-overflow-b");

        assertThat(meterCount("socket.messages.received"))
            .as("no new time series may be created past the cap").isEqualTo(metersAtCap);
        assertThat(registry.get("socket.messages.received")
            .tag("event", MicrometerSocketMetrics.OVERFLOW_EVENT)
            .counter().count()).isEqualTo(2.0);
        // MAX_EVENT_TAGS distinct series plus the pre-registered overflow series.
        assertThat(metersAtCap).isEqualTo(MicrometerSocketMetrics.MAX_EVENT_TAGS + 1L);
    }

    @Test
    @DisplayName("high-cardinality error names collapse into the shared overflow series")
    void incrementError_beyondTagCap_foldsIntoOverflowCounter() {
        for (int i = 0; i < MicrometerSocketMetrics.MAX_EVENT_TAGS; i++) {
            metrics.incrementError("err-" + i);
        }
        long metersAtCap = meterCount("socket.errors.total");

        metrics.incrementError("err-overflow");

        assertThat(meterCount("socket.errors.total")).isEqualTo(metersAtCap);
        assertThat(registry.get("socket.errors.total")
            .tag("event", MicrometerSocketMetrics.OVERFLOW_EVENT)
            .counter().count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("the active-connections gauge tracks the session manager")
    void activeConnectionsGauge_followsSessionManager() {
        assertThat(registry.get("socket.connections.active").gauge().value()).isEqualTo(0.0);

        sessionManager.save(SocketSession.builder().sessionId("s1").userId("u1").build());
        assertThat(registry.get("socket.connections.active").gauge().value()).isEqualTo(1.0);

        sessionManager.removeBySessionId("s1");
        assertThat(registry.get("socket.connections.active").gauge().value()).isEqualTo(0.0);
    }
}
