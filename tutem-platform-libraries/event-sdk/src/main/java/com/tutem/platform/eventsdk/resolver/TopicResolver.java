package com.tutem.platform.eventsdk.resolver;

import com.tutem.platform.eventsdk.model.DomainEvent;

public interface TopicResolver {
    String resolve(DomainEvent event);
}
