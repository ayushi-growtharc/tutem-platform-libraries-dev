package com.tutem.platform.event.resolver;

import com.tutem.platform.event.model.DomainEvent;

public interface TopicResolver {
    String resolve(DomainEvent event);
}
