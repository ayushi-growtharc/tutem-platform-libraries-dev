package com.tutem.platform.socket.session;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SocketSessionTest {

    @Test
    @DisplayName("builder populates identity fields and defaults lastActiveAt")
    void builder_defaults_populateLastActiveAtAndEmptyAttributes() {
        Instant before = Instant.now().minusMillis(1);
        SocketSession session = SocketSession.builder()
            .sessionId("s1").userId("u1").role("DRIVER").remoteAddress("/127.0.0.1:1234")
            .connectedAt(before)
            .build();

        assertThat(session.getSessionId()).isEqualTo("s1");
        assertThat(session.getUserId()).isEqualTo("u1");
        assertThat(session.getRole()).isEqualTo("DRIVER");
        assertThat(session.getRemoteAddress()).isEqualTo("/127.0.0.1:1234");
        assertThat(session.getConnectedAt()).isEqualTo(before);
        assertThat(session.getLastActiveAt()).isNotNull();
        assertThat(session.getAttributes()).isEmpty();
    }

    @Test
    @DisplayName("setAttribute stores values and silently ignores null key or value")
    void setAttribute_nullKeyOrValue_isIgnored() {
        SocketSession session = SocketSession.builder().sessionId("s1").build();

        session.setAttribute("rideId", "r-9");
        session.setAttribute(null, "ignored");
        session.setAttribute("alsoIgnored", null);

        assertThat(session.getAttribute("rideId")).isEqualTo("r-9");
        assertThat(session.getAttribute("alsoIgnored")).isNull();
        assertThat(session.getAttribute(null)).isNull();
        assertThat(session.getAttributes()).containsExactly(Map.entry("rideId", "r-9"));
    }

    @Test
    @DisplayName("getAttributes returns an immutable snapshot")
    void getAttributes_returnsImmutableSnapshot() {
        SocketSession session = SocketSession.builder().sessionId("s1").build();
        session.setAttribute("a", 1);

        Map<String, Object> snapshot = session.getAttributes();
        session.setAttribute("b", 2);

        assertThat(snapshot).hasSize(1);
        assertThat(session.getAttributes()).hasSize(2);
        assertThatThrownBy(() -> snapshot.put("c", 3))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    @DisplayName("refreshActivity advances lastActiveAt")
    void refreshActivity_advancesLastActiveAt() {
        SocketSession session = SocketSession.builder()
            .sessionId("s1")
            .lastActiveAt(Instant.now().minusSeconds(600))
            .build();
        Instant original = session.getLastActiveAt();

        session.refreshActivity();

        assertThat(session.getLastActiveAt()).isAfter(original);
    }

    @Test
    @DisplayName("an explicitly null lastActiveAt is preserved (the heartbeat must tolerate it)")
    void builder_explicitNullLastActiveAt_isPreserved() {
        SocketSession session = SocketSession.builder().sessionId("s1").lastActiveAt(null).build();

        assertThat(session.getLastActiveAt()).isNull();
    }

    @Test
    @DisplayName("toString exposes identity fields but not the mutable attribute map")
    void toString_excludesAttributes() {
        SocketSession session = SocketSession.builder()
            .sessionId("s1").userId("u1").role("RIDER").build();
        session.setAttribute("secretClaim", "do-not-log");

        assertThat(session.toString()).contains("s1", "u1", "RIDER");
        assertThat(session.toString()).doesNotContain("secretClaim");
    }
}
