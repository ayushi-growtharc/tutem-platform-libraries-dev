package com.tutem.platform.eventsdk.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tutem.platform.eventsdk.factory.DefaultEnvelopeFactory;
import com.tutem.platform.eventsdk.factory.EnvelopeFactory;
import com.tutem.platform.eventsdk.producer.KafkaEventProducer;
import com.tutem.platform.eventsdk.publisher.DefaultEventPublisher;
import com.tutem.platform.eventsdk.publisher.EventPublisher;
import com.tutem.platform.eventsdk.resolver.DefaultTopicResolver;
import com.tutem.platform.eventsdk.resolver.TopicResolver;
import com.tutem.platform.eventsdk.serializer.EventSerializer;
import com.tutem.platform.eventsdk.serializer.JacksonEventSerializer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

@Configuration
@EnableConfigurationProperties(EventTopicProperties.class)
public class EventSdkAutoConfiguration {

    @Bean
    public EventSerializer eventSerializer(ObjectMapper objectMapper) {
        return new JacksonEventSerializer(objectMapper);
    }

    @Bean
    public EnvelopeFactory envelopeFactory() {
        return new DefaultEnvelopeFactory();
    }

    @Bean
    public TopicResolver topicResolver(EventTopicProperties topicProperties) {
        return new DefaultTopicResolver(topicProperties);
    }

    @Bean
    public KafkaEventProducer kafkaEventProducer(KafkaTemplate<String, String> kafkaTemplate) {
        return new KafkaEventProducer(kafkaTemplate);
    }

    @Bean
    public EventPublisher eventPublisher(TopicResolver topicResolver,
                                         EventSerializer eventSerializer,
                                         EnvelopeFactory envelopeFactory,
                                         KafkaEventProducer kafkaEventProducer) {
        return new DefaultEventPublisher(topicResolver, eventSerializer, envelopeFactory, kafkaEventProducer);
    }
}
