package com.tutem.platform.eventsdk.resolver;

import com.tutem.platform.eventsdk.config.EventTopicProperties;
import com.tutem.platform.eventsdk.model.DomainEvent;

import java.util.HashMap;
import java.util.Map;

public class DefaultTopicResolver implements TopicResolver {
    private  final EventTopicProperties topicProperties;

    public DefaultTopicResolver(EventTopicProperties topicProperties) {
        this.topicProperties = topicProperties;
    }

    @Override
    public String resolve(DomainEvent event) {
        String topic = topicProperties.getTopics().get(event.eventType());
        if(topic == null){
            throw new RuntimeException("Topic not configured for "  + event.eventType());
        }
        return topic;
    }
}
