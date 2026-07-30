package com.tutem.platform.eventsdk.broker;

public interface MessageBroker {
    void publish(String topic, String key, String message);
}
