package com.tutem.platform.event.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tutem.platform.event.exception.EventPublishException;

public class JacksonEventSerializer implements EventSerializer {
    private final ObjectMapper objectMapper;

    public JacksonEventSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public String serialize(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (Exception ex) {
            throw new EventPublishException("Serialization failed", ex);
        }
    }

    @Override
    public <T> T deserialize(String json, Class<T> clazz) {
        try {
            return objectMapper.readValue(json, clazz);
        } catch (Exception ex) {
            throw new EventPublishException("Deserialization failed", ex);
        }
    }
}
