package com.tutem.platform.event.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tutem.platform.event.factory.DefaultEnvelopeFactory;
import com.tutem.platform.event.factory.EnvelopeFactory;
import com.tutem.platform.event.model.DomainEvent;
import com.tutem.platform.event.producer.KafkaEventProducer;
import com.tutem.platform.event.publisher.DefaultEventPublisher;
import com.tutem.platform.event.publisher.EventPublisher;
import com.tutem.platform.event.resolver.DefaultTopicResolver;
import com.tutem.platform.event.resolver.TopicResolver;
import com.tutem.platform.event.serializer.EventSerializer;
import com.tutem.platform.event.serializer.JacksonEventSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Auto-configuration contract for the event-sdk.
 *
 * <p>This class exists because the SDK previously shipped its registration file as
 * {@code ...AutoConfiguration.imports.txt} — a name Spring never looks for — and the file
 * was empty besides, so {@link EventSdkAutoConfiguration} silently did nothing in every
 * consuming application. Nothing failed; the beans just were not there. The
 * {@link #registrationFileNamesTheAutoConfiguration()} test below is the guard against
 * that specific regression, and the rest pin the conditional behaviour.
 *
 * <p>{@code KafkaTemplate} is contributed as a mock throughout. The SDK only ever calls
 * {@code send()} on it from {@code KafkaEventProducer}, which these tests do not exercise,
 * so no broker is involved.
 */
class EventSdkAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(EventSdkAutoConfiguration.class));

    @Test
    @DisplayName("registers the full publishing chain when a KafkaTemplate is available")
    void registersFullChain() {
        runner.withUserConfiguration(KafkaTemplateConfiguration.class).run(context -> {
            assertThat(context).hasSingleBean(EventSerializer.class);
            assertThat(context).hasSingleBean(EnvelopeFactory.class);
            assertThat(context).hasSingleBean(TopicResolver.class);
            assertThat(context).hasSingleBean(KafkaEventProducer.class);
            assertThat(context).hasSingleBean(EventPublisher.class);

            assertThat(context).getBean(EventSerializer.class).isInstanceOf(JacksonEventSerializer.class);
            assertThat(context).getBean(EnvelopeFactory.class).isInstanceOf(DefaultEnvelopeFactory.class);
            assertThat(context).getBean(TopicResolver.class).isInstanceOf(DefaultTopicResolver.class);
            assertThat(context).getBean(EventPublisher.class).isInstanceOf(DefaultEventPublisher.class);
        });
    }

    /**
     * The registration file is what makes the SDK auto-configure at all. Assert its real
     * name and contents, because every other test here bypasses it by naming the
     * auto-configuration class directly.
     */
    @Test
    @DisplayName("META-INF/spring registration file exists and names the auto-configuration")
    void registrationFileNamesTheAutoConfiguration() throws Exception {
        var url = getClass().getClassLoader().getResource(
                "META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports");

        assertThat(url)
                .as("auto-configuration registration file must be on the classpath under its exact name")
                .isNotNull();
        assertThat(new String(url.openStream().readAllBytes()))
                .contains(EventSdkAutoConfiguration.class.getName());
    }

    @Test
    @DisplayName("backs off entirely when Kafka is not on the classpath")
    void backsOffWithoutKafka() {
        runner.withClassLoader(new FilteredClassLoader(KafkaTemplate.class))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean(EventPublisher.class);
                    assertThat(context).doesNotHaveBean(EventSerializer.class);
                    assertThat(context).doesNotHaveBean(TopicResolver.class);
                });
    }

    /**
     * Kafka on the classpath but no {@code KafkaTemplate} bean is the ordinary state of a
     * service that depends on event-sdk transitively without configuring Kafka. It must not
     * fail the context — the producer and publisher simply stay absent.
     */
    @Test
    @DisplayName("omits producer and publisher when no KafkaTemplate bean is defined")
    void omitsProducerWithoutKafkaTemplateBean() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(KafkaEventProducer.class);
            assertThat(context).doesNotHaveBean(EventPublisher.class);

            // The Kafka-independent half is still wired.
            assertThat(context).hasSingleBean(EventSerializer.class);
            assertThat(context).hasSingleBean(EnvelopeFactory.class);
        });
    }

    @Test
    @DisplayName("a consumer-defined TopicResolver replaces the default without losing the rest")
    void consumerTopicResolverWins() {
        runner.withUserConfiguration(KafkaTemplateConfiguration.class, CustomTopicResolverConfiguration.class)
                .run(context -> {
                    assertThat(context).hasSingleBean(TopicResolver.class);
                    assertThat(context).getBean(TopicResolver.class).isNotInstanceOf(DefaultTopicResolver.class);
                    assertThat(context).hasSingleBean(EventPublisher.class);
                });
    }

    @Test
    @DisplayName("binds event.topics.* onto EventTopicProperties")
    void bindsTopicProperties() {
        runner.withUserConfiguration(KafkaTemplateConfiguration.class)
                .withPropertyValues("event.topics.order-created=orders.v1")
                .run(context -> assertThat(context).getBean(EventTopicProperties.class)
                        .extracting(EventTopicProperties::getTopics)
                        .isEqualTo(java.util.Map.of("order-created", "orders.v1")));
    }

    /**
     * Outside a Boot application there is no {@code ObjectMapper} bean; the serializer must
     * fall back rather than fail the context.
     */
    @Test
    @DisplayName("falls back to a plain ObjectMapper when the context has none")
    void serializerFallsBackWithoutObjectMapperBean() {
        runner.run(context -> {
            assertThat(context).doesNotHaveBean(ObjectMapper.class);
            assertThat(context).hasSingleBean(EventSerializer.class);
        });
    }

    static class KafkaTemplateConfiguration {

        @Bean
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> kafkaTemplate() {
            return mock(KafkaTemplate.class);
        }
    }

    static class CustomTopicResolverConfiguration {

        @Bean
        TopicResolver topicResolver() {
            return (DomainEvent event) -> "custom.topic";
        }
    }
}
