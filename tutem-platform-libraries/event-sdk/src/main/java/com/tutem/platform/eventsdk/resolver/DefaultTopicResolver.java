package com.tutem.platform.eventsdk.resolver;

import com.tutem.platform.eventsdk.model.DomainEvent;

import java.util.HashMap;
import java.util.Map;

public class DefaultTopicResolver implements TopicResolver {
    private final Map<String, String> topics = new HashMap<>();

    public DefaultTopicResolver() {
        //ToDo: Replace hard code with YAML configuration
        topics.put("RideRequested", "ride.requested");
        topics.put("DriverAssigned", "driver.assigned");
    }

    @Override
    public String resolve(DomainEvent event) {
        return topics.get(event.eventType());
    }
}
