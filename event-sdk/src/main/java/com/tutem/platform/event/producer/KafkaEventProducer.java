package com.tutem.platform.event.producer;

import com.tutem.platform.event.exception.EventPublishException;
import org.springframework.kafka.core.KafkaTemplate;

public class KafkaEventProducer {
    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaEventProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(String topic, String key, String payload) {
        try {
            kafkaTemplate.send(topic, key, payload);
        }  catch (Exception ex) {
            throw new EventPublishException("Kafka publish failed", ex);
        }
    }
}
