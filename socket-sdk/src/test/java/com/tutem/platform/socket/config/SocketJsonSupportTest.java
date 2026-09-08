package com.tutem.platform.socket.config;

import com.corundumstudio.socketio.protocol.JsonSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the wire format of {@code java.time} values on outbound socket frames.
 *
 * <p>This exists because two separate things had to be true for a push carrying an {@code Instant}
 * to be usable, and getting only the first produced a subtler bug than getting neither:
 *
 * <ol>
 *   <li>netty-socketio's own {@code JsonSupport} must know about {@code java.time} at all.
 *       Its default does not - it builds a private, unconfigured {@code ObjectMapper} - so every
 *       payload with an {@code Instant} field threw and the frame was dropped from inside the
 *       library's send path.</li>
 *   <li>It must serialize those values as ISO-8601 strings. Registering {@code JavaTimeModule}
 *       fixes (1) but leaves Jackson's {@code WRITE_DATES_AS_TIMESTAMPS} at its default of
 *       {@code true}, which encodes an {@code Instant} as an epoch-seconds <em>decimal</em>
 *       ({@code 1787547213.722813700}). That is worse than the original failure in one respect:
 *       the push now succeeds, so nothing logs an error, while the client silently receives a
 *       number where the REST API returns {@code "2026-08-14T09:30:22Z"} for the same field.
 *       A client parsing one shape breaks on the other.</li>
 * </ol>
 *
 * <p>The platform's REST contract specifies ISO-8601 UTC, so that is the format the socket must
 * agree with. Anyone replacing the {@code JsonSupport} bean has to keep both properties; these two
 * assertions are what will tell them if they did not.
 */
class SocketJsonSupportTest {

    /** A fixed instant with sub-second precision, so a truncating encoder cannot pass by luck. */
    private static final Instant WHEN = Instant.parse("2026-08-14T09:30:22.123456789Z");

    private record PushPayload(String type, Instant occurredAt) {
    }

    /**
     * netty-socketio's {@code JsonSupport} interface only exposes frame-level read/write, so the
     * cleanest way to assert on the encoding of a payload object is to serialize it with the very
     * {@code ObjectMapper} the support holds. It is {@code protected final}, hence reflection - the
     * alternative would be standing up a real server and a client just to read one field.
     */
    private static String writeWithSupportMapper(JsonSupport support, Object value)
            throws Exception {
        // Walk up rather than getDeclaredField on the concrete class: the bean is a subclass of
        // JacksonJsonSupport, and the mapper is declared on the parent.
        Field field = null;
        for (Class<?> type = support.getClass(); type != null && field == null;
                type = type.getSuperclass()) {
            try {
                field = type.getDeclaredField("objectMapper");
            } catch (NoSuchFieldException ignored) {
                // Keep climbing.
            }
        }
        assertThat(field).as("JacksonJsonSupport should still hold an objectMapper").isNotNull();
        field.setAccessible(true);
        Object mapper = field.get(support);
        return (String) mapper.getClass()
                .getMethod("writeValueAsString", Object.class)
                .invoke(mapper, value);
    }

    @Test
    @DisplayName("an Instant serializes at all - the default JsonSupport cannot")
    void serializesJavaTimeWithoutThrowing() throws Exception {
        JsonSupport support = new SocketAutoConfiguration().socketJsonSupport();

        String json = writeWithSupportMapper(support, new PushPayload("offer.outcome", WHEN));

        assertThat(json).contains("occurredAt");
    }

    @Test
    @DisplayName("an Instant serializes as an ISO-8601 string, not an epoch-seconds decimal")
    void serializesInstantAsIsoStringNotEpochDecimal() throws Exception {
        JsonSupport support = new SocketAutoConfiguration().socketJsonSupport();

        String json = writeWithSupportMapper(support, new PushPayload("offer.outcome", WHEN));

        assertThat(json).contains("\"2026-08-14T09:30:22.123456789Z\"");
        // The failure this guards against, stated explicitly: 1787...  as a bare number.
        assertThat(json).doesNotContain("1787");
    }
}
