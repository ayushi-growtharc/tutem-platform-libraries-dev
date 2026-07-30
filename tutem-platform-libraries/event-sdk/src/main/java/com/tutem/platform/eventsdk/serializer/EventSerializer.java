package com.tutem.platform.eventsdk.serializer;

import com.tutem.platform.eventsdk.model.DomainEvent;

public interface EventSerializer {
    String serialize(Object object);

    <T> T deserialize(String json, Class<T> clazz);
}
