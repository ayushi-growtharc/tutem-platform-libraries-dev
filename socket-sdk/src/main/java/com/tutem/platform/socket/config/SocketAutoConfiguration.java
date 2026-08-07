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
import com.tutem.platform.socket.metrics.MicrometerSocketMetrics;
import com.tutem.platform.socket.metrics.NoOpSocketMetrics;
import com.tutem.platform.socket.metrics.SocketMetrics;
import com.tutem.platform.socket.properties.SocketProperties;
import com.tutem.platform.socket.serialization.JsonSocketMessageSerializer;
import com.tutem.platform.socket.serialization.SocketMessageSerializer;
import com.tutem.platform.socket.session.InMemorySessionManager;
import com.tutem.platform.socket.session.SessionManager;
import com.tutem.platform.socket.tracing.MicrometerSocketTracing;
import com.tutem.platform.socket.tracing.NoOpSocketTracing;
import com.tutem.platform.socket.tracing.SocketTracing;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.tracing.Tracer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Wires every socket-sdk component.
 *
 * <p>Activation is opt-in: this class is {@code @ConditionalOnBean(SocketMarker.class)},
 * and the marker bean is only registered by {@code @EnableSocket}. Having socket-sdk on
 * the classpath therefore never opens a socket port on a service that did not ask for one.
 * {@code app.socket.enabled=false} forces the whole SDK off even when {@code @EnableSocket}
 * is present.
 *
 * <p>Because activation goes through the marker, this stays a real
 * {@code @AutoConfiguration} - registered through
 * {@code META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports}
 * only - so all {@code @ConditionalOnMissingBean} checks run in the deferred
 * auto-configuration phase, after user beans are known. Every bean below is
 * {@code @ConditionalOnMissingBean}, so a consumer can replace any single component just by
 * declaring its own bean of that type.
 *
 * <p>Micrometer metrics and tracing are optional: actuator and micrometer-tracing are
 * {@code compileOnly} here. The Micrometer-backed implementations live in nested
 * configuration classes guarded by {@code @ConditionalOnClass}, so their types are never
 * loaded when the consumer does not have those libraries.
 *
 * <p><strong>Why {@code ObjectProvider} and not {@code @ConditionalOnBean}.</strong>
 * {@code @ConditionalOnBean(MeterRegistry.class)} would be evaluated when this
 * auto-configuration is processed, and Spring Boot's {@code AutoConfigurationSorter}
 * orders auto-configurations alphabetically before applying
 * {@code @AutoConfiguration(before/after)} hints - so
 * {@code com.tutem.platform.socket.config.SocketAutoConfiguration} is processed
 * <em>before</em> {@code org.springframework.boot.actuate.autoconfigure.metrics.*} and
 * {@code ...tracing.*}. At that point no {@code MeterRegistry} / {@code Tracer} bean
 * definition exists yet, the condition does not match, and the consumer silently ends up
 * with {@link NoOpSocketMetrics} / {@link NoOpSocketTracing} - no meters, no spans, no
 * warning. Injecting an {@code ObjectProvider} instead defers the lookup to bean
 * <em>instantiation</em> time, when every auto-configuration has contributed its
 * definitions, so ordering is irrelevant.
 *
 * <p>The {@code NoOp*} fallbacks on this class carry {@code @ConditionalOnMissingClass} as
 * well as {@code @ConditionalOnMissingBean}: they exist purely for the case where the
 * Micrometer types are absent from the classpath, and must never race the nested
 * configurations. No two {@code @Bean} methods anywhere in this class share a name, so
 * the SDK is safe under {@code spring.main.allow-bean-definition-overriding=false}.
 */
@Slf4j
@AutoConfiguration
@ConditionalOnBean(SocketMarker.class)
@ConditionalOnProperty(prefix = "app.socket", name = "enabled", matchIfMissing = true)
@EnableConfigurationProperties(SocketProperties.class)
public class SocketAutoConfiguration {

    // ---- Server ------------------------------------------------------------

    @Bean
    @ConditionalOnMissingBean
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
        config.setTransports(resolveTransports(props.getTransports()));

        if (props.getOrigin() != null && !props.getOrigin().isBlank()) {
            config.setOrigin(props.getOrigin());
        }

        log.info("Socket server configured on {}:{} transports={} origin={}",
            props.getHost(), props.getPort(), props.getTransports(),
            props.getOrigin() == null ? "*" : props.getOrigin());

        // Auth is ON by default, so reaching this branch means someone switched it off.
        if (!props.getAuth().isEnabled()) {
            log.warn("app.socket.auth.enabled=false was set explicitly - the socket port on {}:{} "
                    + "accepts anonymous connections. This is intended for local development "
                    + "only; re-enable auth and provide a SocketAuthenticationHook before "
                    + "exposing this service.",
                props.getHost(), props.getPort());
        }
        return new SocketIOServer(config);
    }

    /**
     * Maps the configured transport names onto netty-socketio's {@link Transport} enum.
     *
     * @throws IllegalArgumentException if the list is empty or names an unknown transport
     */
    static Transport[] resolveTransports(List<String> configured) {
        if (configured == null || configured.isEmpty()) {
            throw new IllegalArgumentException(
                "app.socket.transports must not be empty. Legal values: " + legalTransports());
        }
        List<Transport> resolved = new ArrayList<>(configured.size());
        for (String name : configured) {
            String normalized = name == null ? "" : name.trim().toUpperCase(Locale.ROOT);
            Transport transport = null;
            for (Transport candidate : Transport.values()) {
                if (candidate.name().equals(normalized)) {
                    transport = candidate;
                    break;
                }
            }
            if (transport == null) {
                throw new IllegalArgumentException(
                    "Unknown value '" + name + "' in app.socket.transports. Legal values: "
                        + legalTransports());
            }
            if (!resolved.contains(transport)) {
                resolved.add(transport);
            }
        }
        return resolved.toArray(new Transport[0]);
    }

    private static String legalTransports() {
        return Arrays.stream(Transport.values())
            .map(t -> t.name().toLowerCase(Locale.ROOT))
            .collect(Collectors.joining(", ", "[", "] (case-insensitive)"));
    }

    // ---- Session -----------------------------------------------------------

    @Bean
    @ConditionalOnMissingBean
    public SessionManager sessionManager() {
        return new InMemorySessionManager();
    }

    // ---- Connection --------------------------------------------------------

    @Bean
    @ConditionalOnMissingBean
    public ConnectionManager connectionManager(SocketIOServer server, SessionManager sessionManager) {
        return new ConnectionManager(server, sessionManager);
    }

    // ---- Dispatcher --------------------------------------------------------

    @Bean
    @ConditionalOnMissingBean
    public MessageDispatcher messageDispatcher(SocketIOServer server, SessionManager sessionManager) {
        return new MessageDispatcher(server, sessionManager);
    }

    // ---- Serialization -----------------------------------------------------

    @Bean
    @ConditionalOnMissingBean
    public SocketMessageSerializer socketMessageSerializer(ObjectMapper objectMapper) {
        return new JsonSocketMessageSerializer(objectMapper);
    }

    // ---- Authentication ----------------------------------------------------

    /**
     * The SDK never invents an identity: with auth enabled (the default) a
     * {@link SocketAuthenticationHook} bean is mandatory and its absence fails startup.
     */
    @Bean
    @ConditionalOnMissingBean
    public SocketAuthenticationHook socketAuthenticationHook(SocketProperties props) {
        if (props.getAuth().isEnabled()) {
            throw new IllegalStateException(
                "app.socket.auth.enabled=true (the default) but no SocketAuthenticationHook bean "
                    + "was found, so every connection would be rejected. Either provide a "
                    + "@Component implementing SocketAuthenticationHook, or set "
                    + "app.socket.auth.enabled=false to accept anonymous connections "
                    + "(local development only - it leaves the socket port unauthenticated)."
            );
        }
        return new DefaultSocketAuthenticationHook();
    }

    // ---- Error handling ----------------------------------------------------

    @Bean
    @ConditionalOnMissingBean
    public SocketErrorHandler socketErrorHandler() {
        return new DefaultSocketErrorHandler();
    }

    // ---- Metrics / tracing fallbacks --------------------------------------
    // Only for consumers without Micrometer on the classpath at all. When the Micrometer
    // types ARE present, MicrometerMetricsConfiguration / MicrometerTracingConfiguration
    // below own the bean and decide - at instantiation time - between the Micrometer and
    // the NoOp implementation. Distinct method names keep every @Bean name unique.

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnMissingClass("io.micrometer.core.instrument.MeterRegistry")
    public SocketMetrics noOpSocketMetrics() {
        return new NoOpSocketMetrics();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnMissingClass("io.micrometer.tracing.Tracer")
    public SocketTracing noOpSocketTracing() {
        return new NoOpSocketTracing();
    }

    // ---- Heartbeat ---------------------------------------------------------

    @Bean
    @ConditionalOnMissingBean
    public HeartbeatManager heartbeatManager(SocketIOServer server,
                                             SessionManager sessionManager,
                                             SocketProperties props) {
        SocketProperties.HeartbeatProperties heartbeat = props.getHeartbeat();
        return new HeartbeatManager(server, sessionManager,
            heartbeat.getStaleSessionSeconds(),
            heartbeat.getCleanupIntervalSeconds(),
            heartbeat.isDisconnectStaleSessions(),
            heartbeat.isEvictGhostSessions());
    }

    // ---- Registry + Lifecycle ---------------------------------------------

    @Bean
    @ConditionalOnMissingBean
    public MessageHandlerRegistry messageHandlerRegistry(
        SocketIOServer server,
        ApplicationContext applicationContext,
        SessionManager sessionManager,
        ApplicationEventPublisher eventPublisher,
        SocketAuthenticationHook authHook,
        SocketMetrics metrics,
        SocketTracing tracing,
        SocketMessageSerializer serializer,
        SocketErrorHandler errorHandler,
        SocketProperties socketProperties
    ) {
        return new MessageHandlerRegistry(server, applicationContext, sessionManager,
            eventPublisher, authHook, metrics, tracing, serializer, errorHandler,
            socketProperties);
    }

    @Bean
    @ConditionalOnMissingBean
    public SocketServerLifecycle socketServerLifecycle(
        SocketIOServer server,
        MessageHandlerRegistry registry,
        ApplicationEventPublisher eventPublisher,
        SocketProperties props
    ) {
        return new SocketServerLifecycle(server, registry, eventPublisher, props.getStartupPhase());
    }

    // ---- Optional: Micrometer metrics -------------------------------------

    /**
     * Owns the {@link SocketMetrics} bean whenever micrometer-core is on the classpath.
     * The class body - and therefore every Micrometer type it mentions - is only loaded
     * once {@code @ConditionalOnClass} has matched.
     *
     * <p>Whether a {@code MeterRegistry} bean actually exists is resolved through an
     * {@link ObjectProvider} at instantiation time rather than by
     * {@code @ConditionalOnBean}: see the enclosing class's javadoc for why that condition
     * cannot see actuator's bean definitions.
     */
    @org.springframework.context.annotation.Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(MeterRegistry.class)
    static class MicrometerMetricsConfiguration {

        @Bean
        @ConditionalOnMissingBean
        SocketMetrics socketMetrics(ObjectProvider<MeterRegistry> registries,
                                    SessionManager sessionManager,
                                    SocketProperties props) {
            if (!props.getMetrics().isEnabled()) {
                log.info("Socket metrics disabled (app.socket.metrics.enabled=false)");
                return new NoOpSocketMetrics();
            }
            MeterRegistry registry;
            try {
                registry = registries.getIfAvailable();
            } catch (BeansException e) {
                // e.g. several MeterRegistry beans and none marked @Primary.
                log.warn("Could not resolve a unique MeterRegistry bean, socket metrics disabled: {}",
                    e.getMessage());
                return new NoOpSocketMetrics();
            }
            if (registry == null) {
                log.info("Micrometer is on the classpath but no MeterRegistry bean is present - "
                    + "socket metrics disabled");
                return new NoOpSocketMetrics();
            }
            return new MicrometerSocketMetrics(registry, sessionManager);
        }
    }

    // ---- Optional: Micrometer tracing -------------------------------------

    /**
     * Owns the {@link SocketTracing} bean whenever micrometer-tracing is on the classpath.
     *
     * <p>{@code Tracer} being on the classpath is not enough - only a bridge such as Brave
     * or OpenTelemetry actually contributes the bean - so the {@link ObjectProvider} may
     * legitimately be empty, in which case tracing falls back to a no-op.
     */
    @org.springframework.context.annotation.Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(Tracer.class)
    static class MicrometerTracingConfiguration {

        @Bean
        @ConditionalOnMissingBean
        SocketTracing socketTracing(ObjectProvider<Tracer> tracers, SocketProperties props) {
            if (!props.getTracing().isEnabled()) {
                log.info("Socket tracing disabled (app.socket.tracing.enabled=false)");
                return new NoOpSocketTracing();
            }
            Tracer tracer;
            try {
                tracer = tracers.getIfAvailable();
            } catch (BeansException e) {
                log.warn("Could not resolve a unique Tracer bean, socket tracing disabled: {}",
                    e.getMessage());
                return new NoOpSocketTracing();
            }
            if (tracer == null) {
                log.info("micrometer-tracing is on the classpath but no Tracer bean is present "
                    + "(no tracing bridge configured) - socket tracing disabled");
                return new NoOpSocketTracing();
            }
            return new MicrometerSocketTracing(tracer);
        }
    }
}
