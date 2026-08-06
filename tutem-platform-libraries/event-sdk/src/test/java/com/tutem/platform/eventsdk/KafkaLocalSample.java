package com.tutem.platform.eventsdk;

import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class KafkaLocalSample {
    public static void main(String[] args) throws Exception {
        String topic = "local-event-sdk-demo";
        EmbeddedKafkaKraftBroker kafka = new EmbeddedKafkaKraftBroker(1, 1, topic);
        kafka.afterPropertiesSet();

        try {
            System.out.println("Kafka broker started at " + kafka.getBrokersAsString());
            System.out.println("Use bootstrap servers: " + kafka.getBrokersAsString());
            System.out.println("Topic ready: " + topic);
            TimeUnit.SECONDS.sleep(30);
        } finally {
            kafka.destroy();
        }
    }
}
