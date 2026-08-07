package com.tutem.platform.event.serializer;

import com.tutem.platform.event.model.DomainEvent;

public interface EventSerializer {
    String serialize(Object object);

    <T> T deserialize(String json, Class<T> clazz);
}
