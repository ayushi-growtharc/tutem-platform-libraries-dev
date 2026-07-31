package com.tutem.platform.socket.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Registers the {@link SocketMarker} activation bean. Imported by
 * {@code @EnableSocket} and never referenced directly by consumers.
 *
 * @see SocketMarker
 */
@Configuration(proxyBeanMethods = false)
public class SocketMarkerConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public SocketMarker socketMarker() {
        return new SocketMarker();
    }
}
