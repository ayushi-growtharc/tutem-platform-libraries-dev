package com.tutem.platform.event;

import com.tutem.platform.event.support.SampleKafkaConsumer;
import com.tutem.platform.event.support.SampleKafkaProducer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaLocalRoundTripTest {
    private static EmbeddedKafkaBroker embeddedKafkaBroker;
    private static final String TOPIC = "local-event-sdk-demo";

    private SampleKafkaProducer producer;
    private SampleKafkaConsumer consumer;

    @BeforeAll
    static void startKafka() throws Exception {
        embeddedKafkaBroker = new EmbeddedKafkaKraftBroker(1, 1, TOPIC);
        embeddedKafkaBroker.afterPropertiesSet();

        try (AdminClient adminClient = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                embeddedKafkaBroker.getBrokersAsString()
        ))) {
            try {
                adminClient.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
            } catch (ExecutionException ex) {
                if (!(ex.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException)) {
                    throw ex;
                }
            }
        }
    }

    @AfterAll
    static void stopKafka() {
        if (embeddedKafkaBroker != null) {
            embeddedKafkaBroker.destroy();
        }
    }

    @BeforeEach
    void setUp() {
        producer = new SampleKafkaProducer(embeddedKafkaBroker.getBrokersAsString());
        consumer = new SampleKafkaConsumer(
                embeddedKafkaBroker.getBrokersAsString(),
                "event-sdk-test-group-" + UUID.randomUUID()
        );
    }

    @AfterEach
    void tearDown() {
        producer.close();
        consumer.close();
    }

    @Test
    void publishesAndConsumesPayload() throws Exception {
        List<String> payloads = List.of(
                "{\"eventType\":\"demo.event.1\",\"message\":\"sample-1\"}",
                "{\"eventType\":\"demo.event.2\",\"message\":\"sample-2\"}",
                "{\"eventType\":\"demo.event.3\",\"message\":\"sample-3\"}"
        );

        for (int i = 0; i < payloads.size(); i++) {
            String payload = payloads.get(i);
            producer.publish(TOPIC, "demo-key-" + i, payload);
            String received = consumer.consumeOne(TOPIC, Duration.ofSeconds(10));
            assertEquals(payload, received);
        }
    }
}
