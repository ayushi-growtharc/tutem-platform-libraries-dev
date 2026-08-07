package com.tutem.platform.event.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tutem.platform.event.factory.DefaultEnvelopeFactory;
import com.tutem.platform.event.factory.EnvelopeFactory;
import com.tutem.platform.event.producer.KafkaEventProducer;
import com.tutem.platform.event.publisher.DefaultEventPublisher;
import com.tutem.platform.event.publisher.EventPublisher;
import com.tutem.platform.event.resolver.DefaultTopicResolver;
import com.tutem.platform.event.resolver.TopicResolver;
import com.tutem.platform.event.serializer.EventSerializer;
import com.tutem.platform.event.serializer.JacksonEventSerializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Wires every event-sdk component.
 *
 * <p>Registered through
 * {@code META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports}
 * and gated on Kafka being present, so putting event-sdk on the classpath of a service that
 * has no Kafka support configured is inert.
 *
 * <p>Ordered after {@link KafkaAutoConfiguration} and {@link JacksonAutoConfiguration}
 * because the {@code KafkaTemplate} and {@code ObjectMapper} beans it consumes come from
 * those. Every bean is {@code @ConditionalOnMissingBean}, so a consumer can replace any
 * single piece - a custom {@link TopicResolver}, say - without giving up the rest.
 */
@AutoConfiguration(after = { KafkaAutoConfiguration.class, JacksonAutoConfiguration.class })
@ConditionalOnClass(KafkaTemplate.class)
@EnableConfigurationProperties(EventTopicProperties.class)
public class EventSdkAutoConfiguration {

    /**
     * Falls back to a plain {@code ObjectMapper} when the context has none, which is the
     * case outside a Boot application (plain {@code AnnotationConfigApplicationContext},
     * most slice tests).
     */
    @Bean
    @ConditionalOnMissingBean
    public EventSerializer eventSerializer(ObjectProvider<ObjectMapper> objectMapper) {
        return new JacksonEventSerializer(objectMapper.getIfAvailable(ObjectMapper::new));
    }

    @Bean
    @ConditionalOnMissingBean
    public EnvelopeFactory envelopeFactory() {
        return new DefaultEnvelopeFactory();
    }

    @Bean
    @ConditionalOnMissingBean
    public TopicResolver topicResolver(EventTopicProperties topicProperties) {
        return new DefaultTopicResolver(topicProperties);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(KafkaTemplate.class)
    public KafkaEventProducer kafkaEventProducer(KafkaTemplate<String, String> kafkaTemplate) {
        return new KafkaEventProducer(kafkaTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(KafkaEventProducer.class)
    public EventPublisher eventPublisher(TopicResolver topicResolver,
                                         EventSerializer eventSerializer,
                                         EnvelopeFactory envelopeFactory,
                                         KafkaEventProducer kafkaEventProducer) {
        return new DefaultEventPublisher(topicResolver, eventSerializer, envelopeFactory, kafkaEventProducer);
    }
}
