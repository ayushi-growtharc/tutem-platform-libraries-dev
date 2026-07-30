package com.tutem.platform.eventsdk.publisher;

import com.tutem.platform.eventsdk.model.DomainEvent;

public interface EventPublisher {
    void publish(DomainEvent event);
}