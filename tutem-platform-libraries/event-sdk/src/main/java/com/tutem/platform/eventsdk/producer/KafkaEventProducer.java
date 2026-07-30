package com.tutem.platform.eventsdk.producer;

import org.springframework.kafka.core.KafkaTemplate;

public class KafkaEventProducer {
    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaEventProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(String topic, String key, String payload) {
        kafkaTemplate.send(topic, key, payload);
    }
}
