package com.tutem.platform.event.factory;

import com.tutem.platform.event.envelope.EventEnvelope;
import com.tutem.platform.event.model.DomainEvent;
import com.tutem.platform.event.tracing.CorrelationContext;

import java.time.Instant;
import java.util.UUID;

public class DefaultEnvelopeFactory implements EnvelopeFactory{

    @Override
    public EventEnvelope<DomainEvent> create(DomainEvent event) {
        EventEnvelope<DomainEvent> envelope = new EventEnvelope<>();

        envelope.setEventId(UUID.randomUUID().toString());
        envelope.setEventType(event.eventType());
        envelope.setVersion(event.version());
        envelope.setTraceId(CorrelationContext.get());
        envelope.setTimestamp(Instant.now());
        envelope.setPayload(event);
        return envelope;

    }
}
