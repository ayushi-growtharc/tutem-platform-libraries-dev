package com.tutem.platform.event.publisher;

import com.tutem.platform.event.model.DomainEvent;

public interface EventPublisher {
    void publish(DomainEvent event);
}