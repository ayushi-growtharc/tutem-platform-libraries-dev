package com.tutem.platform.eventsdk.factory;

import com.tutem.platform.eventsdk.envelope.EventEnvelope;
import com.tutem.platform.eventsdk.model.DomainEvent;

public interface EnvelopeFactory {
    EventEnvelope<DomainEvent> create(
            DomainEvent event);
}
