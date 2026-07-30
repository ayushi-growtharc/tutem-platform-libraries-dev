package com.tutem.platform.eventsdk.factory;

import com.tutem.platform.eventsdk.envelope.EventEnvelope;
import com.tutem.platform.eventsdk.model.DomainEvent;
import com.tutem.platform.eventsdk.tracing.CorrelationContext;

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
