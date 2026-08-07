package com.tutem.platform.event.publisher;

import com.tutem.platform.event.broker.MessageBroker;
import com.tutem.platform.event.factory.EnvelopeFactory;
import com.tutem.platform.event.model.DomainEvent;
import com.tutem.platform.event.producer.KafkaEventProducer;
import com.tutem.platform.event.resolver.TopicResolver;
import com.tutem.platform.event.serializer.EventSerializer;

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
