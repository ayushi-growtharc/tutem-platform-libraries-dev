package com.tutem.platform.socket.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tutem.platform.socket.exception.SocketException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JsonSocketMessageSerializerTest {

    private final JsonSocketMessageSerializer serializer =
        new JsonSocketMessageSerializer(new ObjectMapper());

    public static class Payload {
        public String room;
        public int count;
    }

    public static class Unserializable {
        public String getBoom() {
            throw new IllegalStateException("cannot read property");
        }
    }

    @Test
    @DisplayName("deserialize converts a loosely typed map into the declared payload type")
    void deserialize_mapPayload_convertsToTargetType() {
        Payload payload = serializer.deserialize(
            Map.of("room", "driverRoom", "count", 5), Payload.class);

        assertThat(payload).isNotNull();
        assertThat(payload.room).isEqualTo("driverRoom");
        assertThat(payload.count).isEqualTo(5);
    }

    @Test
    @DisplayName("deserialize returns the raw value untouched when it is already the target type")
    void deserialize_alreadyTargetType_returnsSameInstance() {
        Payload original = new Payload();
        original.room = "r";

        assertThat(serializer.deserialize(original, Payload.class)).isSameAs(original);
    }

    @Test
    @DisplayName("deserialize passes null and a null target type through")
    void deserialize_nullInputs_areTolerated() {
        assertThat(serializer.<Payload>deserialize(null, Payload.class)).isNull();
        assertThat(serializer.<String>deserialize("raw", null)).isEqualTo("raw");
    }

    @Test
    @DisplayName("deserialize throws SocketException on a malformed payload instead of returning null")
    void deserialize_malformedPayload_throwsSocketException() {
        // Regression guard: this used to swallow the failure and hand the handler a null payload.
        assertThatThrownBy(() -> serializer.deserialize(
            Map.of("room", "r", "count", "not-a-number"), Payload.class))
            .isInstanceOf(SocketException.class)
            .hasMessageContaining(Payload.class.getName())
            .hasCauseInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("deserialize throws SocketException when the raw value has an incompatible shape")
    void deserialize_incompatibleShape_throwsSocketException() {
        assertThatThrownBy(() -> serializer.deserialize("just-a-string", Payload.class))
            .isInstanceOf(SocketException.class)
            .hasMessageContaining("Failed to deserialize socket payload");
    }

    @Test
    @DisplayName("serialize produces JSON for a normal payload")
    void serialize_normalPayload_producesJson() {
        Payload payload = new Payload();
        payload.room = "r1";
        payload.count = 2;

        assertThat(serializer.serialize(payload)).contains("\"room\":\"r1\"", "\"count\":2");
        assertThat(serializer.serialize(null)).isEqualTo("null");
    }

    @Test
    @DisplayName("serialize throws SocketException naming the offending type")
    void serialize_unserializablePayload_throwsSocketException() {
        assertThatThrownBy(() -> serializer.serialize(new Unserializable()))
            .isInstanceOf(SocketException.class)
            .hasMessageContaining(Unserializable.class.getName());
    }
}
