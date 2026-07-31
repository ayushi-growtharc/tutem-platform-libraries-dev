package com.tutem.platform.socket.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Default JSON serializer using Jackson.
 * netty-socketio already deserializes incoming JSON — this handles
 * cases where manual conversion is needed (e.g. Map → typed object).
 */
@Slf4j
@RequiredArgsConstructor
public class JsonSocketMessageSerializer implements SocketMessageSerializer {

    private final ObjectMapper objectMapper;

    @Override
    public <T> T deserialize(Object raw, Class<T> targetType) {
        try {
            if (raw == null) return null;
            if (targetType.isInstance(raw)) return targetType.cast(raw);
            // Convert Map → typed object (common when netty-socketio deserializes to LinkedHashMap)
            return objectMapper.convertValue(raw, targetType);
        } catch (Exception e) {
            log.error("Deserialization failed for type {}: {}", targetType.getSimpleName(), e.getMessage());
            return null;
        }
    }

    @Override
    public String serialize(Object payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (Exception e) {
            log.error("Serialization failed: {}", e.getMessage());
            return "{}";
        }
    }
}
