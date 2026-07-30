package com.tutem.platform.eventsdk.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(EventTopicProperties.class)
public class EventSdkConfiguration {
}
