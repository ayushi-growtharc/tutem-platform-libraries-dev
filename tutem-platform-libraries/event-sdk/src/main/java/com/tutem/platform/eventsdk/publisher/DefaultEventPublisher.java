package com.tutem.platform.eventsdk.publisher;

import com.tutem.platform.eventsdk.broker.MessageBroker;
import com.tutem.platform.eventsdk.factory.EnvelopeFactory;
import com.tutem.platform.eventsdk.model.DomainEvent;
import com.tutem.platform.eventsdk.producer.KafkaEventProducer;
import com.tutem.platform.eventsdk.resolver.TopicResolver;
import com.tutem.platform.eventsdk.serializer.EventSerializer;

public class DefaultEventPublisher implements EventPublisher {
    private final TopicResolver topicResolver;

    private final EventSerializer serializer;

    private final EnvelopeFactory envelopeFactory;

    private final KafkaEventProducer kafkaEventProducer;

    public DefaultEventPublisher(TopicResolver topicResolver,
                                 EventSerializer eventSerializer,
                                 EnvelopeFactory envelopeFactory,
                                 KafkaEventProducer kafkaEventProducer) {
        this.topicResolver = topicResolver;
        this.serializer = eventSerializer;
        this.envelopeFactory = envelopeFactory;
        this.kafkaEventProducer = kafkaEventProducer;
    }

    @Override
    public void publish(DomainEvent event) {
        var topic = topicResolver.resolve(event);
        var envelope = envelopeFactory.create(event);
        var json = serializer.serialize(envelope);
        kafkaEventProducer.publish(topic, event.key(), json);
    }
}
