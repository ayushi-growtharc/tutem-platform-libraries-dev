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
 * <p>Ordered after Boot's own Kafka and Jackson autoconfiguration, by class <b>name</b>
 * ({@code afterName}), not by class literal ({@code after}) - because the {@code KafkaTemplate}
 * and {@code ObjectMapper} beans this class consumes come from those, and which package they live
 * in depends on the consumer's Boot version:
 *
 * <ul>
 *   <li>Boot 3: {@code org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration} and
 *       {@code org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration}, both in
 *       the monolithic {@code spring-boot-autoconfigure} jar.
 *   <li>Boot 4: relocated to their own modules -
 *       {@code org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration} and
 *       {@code org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration}.
 * </ul>
 *
 * <p>event-sdk itself builds against Boot 3.5.4, so a class-literal {@code after = {
 * KafkaAutoConfiguration.class, JacksonAutoConfiguration.class }} referencing the Boot 3 packages
 * used to compile, but silently stopped working for any Boot 4 consumer: Spring resolves
 * {@code @AutoConfiguration(after = ...)} from ASM bytecode metadata without needing the
 * referenced class on the classpath, so a Boot-4-relocated class just isn't there under that
 * name - the ordering hint is dropped rather than failing loudly, and
 * {@code EventSdkAutoConfiguration} can then run before Boot 4's real Kafka autoconfiguration has
 * created a {@code KafkaTemplate} bean. The practical symptom: {@link EventPublisher} never gets
 * wired on Boot 4, with nothing but a `Failed to introspect meta-annotation` WARN buried in the
 * startup log to explain why. Listing both packages via {@code afterName} (a {@code String[]},
 * needs no class to exist at all) orders correctly against whichever one is actually present,
 * on either major version, without a hard compile-time dependency on Boot 4's module split.
 *
 * <p>Every bean here is {@code @ConditionalOnMissingBean}, so a consumer can replace any single
 * piece - a custom {@link TopicResolver}, say - without giving up the rest.
 */
@AutoConfiguration(afterName = {
        "org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration",     // Boot 3
        "org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration", // Boot 3
        "org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration",     // Boot 4
        "org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration"  // Boot 4
})
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
