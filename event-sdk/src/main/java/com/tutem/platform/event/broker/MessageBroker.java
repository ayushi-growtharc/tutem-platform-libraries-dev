package com.tutem.platform.event.broker;

public interface MessageBroker {
    void publish(String topic, String key, String message);
}
