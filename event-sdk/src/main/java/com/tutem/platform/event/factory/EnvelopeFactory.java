package com.tutem.platform.event.factory;

import com.tutem.platform.event.envelope.EventEnvelope;
import com.tutem.platform.event.model.DomainEvent;

public interface EnvelopeFactory {
    EventEnvelope<DomainEvent> create(
            DomainEvent event);
}
