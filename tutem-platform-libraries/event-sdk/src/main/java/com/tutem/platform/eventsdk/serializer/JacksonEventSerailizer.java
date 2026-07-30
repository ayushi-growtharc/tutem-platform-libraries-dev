package com.tutem.platform.eventsdk.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JacksonEventSerailizer implements EventSerializer {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public String serialize(Object object) {
        try {
            return mapper.writeValueAsString(object);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public <T> T deserialize(String json, Class<T> clazz) {
        try {
            return mapper.readValue(json, clazz);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
