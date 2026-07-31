package com.tutem.platform.socket.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tutem.platform.socket.exception.SocketException;
import lombok.RequiredArgsConstructor;

/**
 * Default JSON serializer using Jackson.
 * netty-socketio hands us a loosely typed value (usually a LinkedHashMap) —
 * this converts it to the handler's declared payload type.
 *
 * <p>Failures are NOT swallowed: a {@link SocketException} is thrown so the
 * dispatch path can run the normal error pipeline (metrics, {@code @OnError}
 * handlers, {@code SocketErrorHandler}) and the client learns the payload was
 * rejected instead of a handler silently receiving {@code null}.
 */
@RequiredArgsConstructor
public class JsonSocketMessageSerializer implements SocketMessageSerializer {

    private final ObjectMapper objectMapper;

    @Override
    public <T> T deserialize(Object raw, Class<T> targetType) {
        if (raw == null) {
            return null;
        }
        if (targetType == null || targetType.isInstance(raw)) {
            @SuppressWarnings("unchecked")
            T cast = (T) raw;
            return cast;
        }
        try {
            // Convert Map -> typed object (common when netty-socketio deserializes to LinkedHashMap)
            return objectMapper.convertValue(raw, targetType);
        } catch (Exception e) {
            throw new SocketException(
                "Failed to deserialize socket payload into " + targetType.getName(), e);
        }
    }

    @Override
    public String serialize(Object payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (Exception e) {
            throw new SocketException("Failed to serialize socket payload of type "
                + (payload != null ? payload.getClass().getName() : "null"), e);
        }
    }
}
