package com.tutem.platform.socket.config;

import com.corundumstudio.socketio.Configuration;
import com.corundumstudio.socketio.SocketIOServer;
import com.corundumstudio.socketio.Transport;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tutem.platform.socket.authentication.DefaultSocketAuthenticationHook;
import com.tutem.platform.socket.authentication.SocketAuthenticationHook;
import com.tutem.platform.socket.connection.ConnectionManager;
import com.tutem.platform.socket.dispatcher.MessageDispatcher;
import com.tutem.platform.socket.dispatcher.MessageHandlerRegistry;
import com.tutem.platform.socket.exception.DefaultSocketErrorHandler;
import com.tutem.platform.socket.exception.SocketErrorHandler;
import com.tutem.platform.socket.heartbeat.HeartbeatManager;
import com.tutem.platform.socket.lifecycle.SocketServerLifecycle;
import com.tutem.platform.socket.metrics.SocketMetrics;
import com.tutem.platform.socket.properties.SocketProperties;
import com.tutem.platform.socket.serialization.JsonSocketMessageSerializer;
import com.tutem.platform.socket.serialization.SocketMessageSerializer;
import com.tutem.platform.socket.session.InMemorySessionManager;
import com.tutem.platform.socket.session.SessionManager;
import com.tutem.platform.socket.tracing.SocketTracing;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.tracing.Tracer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

@Slf4j
@org.springframework.context.annotation.Configuration
@EnableConfigurationProperties(SocketProperties.class)
@EnableScheduling
public class SocketAutoConfiguration {

    // ── Server ────────────────────────────────────────────────────────────────

    @Bean
    public SocketIOServer socketIOServer(SocketProperties props) {
        Configuration config = new Configuration();
        config.setHostname(props.getHost());
        config.setPort(props.getPort());
        config.setBossThreads(props.getBossCount());
        config.setWorkerThreads(props.getWorkerCount());
        config.setAllowCustomRequests(props.isAllowCustomRequests());
        config.setUpgradeTimeout(props.getUpgradeTimeout());
        config.setPingTimeout(props.getPingTimeout());
        config.setPingInterval(props.getPingInterval());
        config.setTransports(Transport.WEBSOCKET);
        log.info("Socket server configured → {}:{}", props.getHost(), props.getPort());
        return new SocketIOServer(config);
    }

    // ── Session ───────────────────────────────────────────────────────────────

    @Bean
    @ConditionalOnMissingBean(SessionManager.class)
    public SessionManager sessionManager() {
        return new InMemorySessionManager();
    }

    // ── Connection ────────────────────────────────────────────────────────────

    @Bean
    public ConnectionManager connectionManager(SocketIOServer server, SessionManager sessionManager) {
        return new ConnectionManager(server, sessionManager);
    }

    // ── Dispatcher ────────────────────────────────────────────────────────────

    @Bean
    public MessageDispatcher messageDispatcher(SocketIOServer server, SessionManager sessionManager) {
        return new MessageDispatcher(server, sessionManager);
    }

    // ── Serialization ─────────────────────────────────────────────────────────

    @Bean
    @ConditionalOnMissingBean(SocketMessageSerializer.class)
    public SocketMessageSerializer socketMessageSerializer(ObjectMapper objectMapper) {
        return new JsonSocketMessageSerializer(objectMapper);
    }

    // ── Authentication ────────────────────────────────────────────────────────

    @Bean
    @ConditionalOnMissingBean(SocketAuthenticationHook.class)
    public SocketAuthenticationHook socketAuthenticationHook(SocketProperties props) {
        if (props.getAuth().isEnabled()) {
            throw new IllegalStateException(
                "app.socket.auth.enabled=true but no SocketAuthenticationHook bean found. " +
                "Provide a @Component implementing SocketAuthenticationHook."
            );
        }
        return new DefaultSocketAuthenticationHook();
    }

    // ── Error handling ────────────────────────────────────────────────────────

    @Bean
    @ConditionalOnMissingBean(SocketErrorHandler.class)
    public SocketErrorHandler socketErrorHandler() {
        return new DefaultSocketErrorHandler();
    }

    // ── Metrics ───────────────────────────────────────────────────────────────

    @Bean
    public SocketMetrics socketMetrics(
        MeterRegistry meterRegistry,
        SessionManager sessionManager,
        SocketProperties props
    ) {
        return new SocketMetrics(meterRegistry, sessionManager, props.getMetrics().isEnabled());
    }

    // ── Tracing ───────────────────────────────────────────────────────────────

    @Bean
    public SocketTracing socketTracing(
        @Autowired(required = false) Tracer tracer,
        SocketProperties props
    ) {
        return new SocketTracing(tracer, props.getTracing().isEnabled());
    }

    // ── Heartbeat ─────────────────────────────────────────────────────────────

    @Bean
    public HeartbeatManager heartbeatManager(SocketIOServer server, SessionManager sessionManager,
                                              SocketProperties props) {
        return new HeartbeatManager(server, sessionManager,
            props.getHeartbeat().getStaleSessionSeconds());
    }

    // ── Registry + Lifecycle ──────────────────────────────────────────────────

    @Bean
    public MessageHandlerRegistry messageHandlerRegistry(
        SocketIOServer server,
        ApplicationContext applicationContext,
        SessionManager sessionManager,
        ApplicationEventPublisher eventPublisher,
        SocketAuthenticationHook authHook,
        SocketMetrics metrics,
        SocketTracing tracing,
        SocketMessageSerializer serializer
    ) {
        return new MessageHandlerRegistry(server, applicationContext, sessionManager,
            eventPublisher, authHook, metrics, tracing, serializer);
    }

    @Bean
    public SocketServerLifecycle socketServerLifecycle(
        SocketIOServer server,
        MessageHandlerRegistry registry,
        ApplicationEventPublisher eventPublisher
    ) {
        return new SocketServerLifecycle(server, registry, eventPublisher);
    }
}
