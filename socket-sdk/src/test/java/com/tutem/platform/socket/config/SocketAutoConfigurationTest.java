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
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.tracing.Tracer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.autoconfigure.metrics.CompositeMeterRegistryAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.metrics.export.simple.SimpleMetricsExportAutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Auto-configuration contract for the socket-sdk.
 *
 * <p><strong>Why no real port is ever bound.</strong> {@code ApplicationContextRunner}
 * refreshes a real {@code ApplicationContext}, and {@code SocketServerLifecycle} is a
 * {@code SmartLifecycle} with {@code isAutoStartup() == true} — so refreshing the context
 * would call {@code SocketIOServer.start()} and bind Netty. Every test here therefore
 * contributes its own mock {@code SocketIOServer} bean via {@link MockServerConfiguration}.
 * That is legal because the SDK's {@code socketIOServer} bean is
 * {@code @ConditionalOnMissingBean}, and it is the cleanest option: a mock
 * {@code SocketServerLifecycle} would instead hide the real lifecycle bean and stop these
 * tests from proving it is wired at all.
 *
 * <p>The one exception is {@link #socketIOServer_invalidTransport_failsNamingTheBadValue()},
 * which deliberately lets the SDK build the real server bean — that test asserts the
 * context fails while constructing it, so {@code start()} is never reached.
 */
class SocketAutoConfigurationTest {

    /**
     * Marker + ObjectMapper + mock server: the "@EnableSocket was used" baseline.
     *
     * <p>{@code app.socket.auth.enabled=false} is part of the baseline because the default is
     * now {@code true}, which makes a {@code SocketAuthenticationHook} bean mandatory and
     * would fail-fast every test that is not about auth. The default itself, and the
     * fail-fast, are asserted separately below.
     */
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(SocketAutoConfiguration.class))
        .withUserConfiguration(MarkerConfiguration.class, ObjectMapperConfiguration.class,
            MockServerConfiguration.class)
        .withPropertyValues("app.socket.auth.enabled=false");

    // ---------------------------------------------------------------- activation

    @Test
    @DisplayName("no SocketMarker bean -> the SDK contributes no beans at all")
    void autoConfiguration_withoutMarker_registersNothing() {
        new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(SocketAutoConfiguration.class))
            .withUserConfiguration(ObjectMapperConfiguration.class)
            .run(context -> {
                assertThat(context).hasNotFailed();
                assertThat(context).doesNotHaveBean(SocketProperties.class);
                assertThat(context).doesNotHaveBean(SocketIOServer.class);
                assertThat(context).doesNotHaveBean(SessionManager.class);
                assertThat(context).doesNotHaveBean(MessageDispatcher.class);
                assertThat(context).doesNotHaveBean(MessageHandlerRegistry.class);
                assertThat(context).doesNotHaveBean(SocketServerLifecycle.class);
                assertThat(context).doesNotHaveBean(HeartbeatManager.class);
                assertThat(context).doesNotHaveBean(SocketMetrics.class);
                assertThat(context).doesNotHaveBean(SocketTracing.class);
            });
    }

    @Test
    @DisplayName("app.socket.enabled=false -> the SDK contributes no beans even with the marker")
    void autoConfiguration_disabledProperty_registersNothing() {
        runner.withPropertyValues("app.socket.enabled=false").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(SessionManager.class);
            assertThat(context).doesNotHaveBean(MessageHandlerRegistry.class);
            assertThat(context).doesNotHaveBean(SocketServerLifecycle.class);
            assertThat(context).doesNotHaveBean(SocketProperties.class);
        });
    }

    @Test
    @DisplayName("marker present -> the full component set is wired")
    void autoConfiguration_withMarker_wiresEveryComponent() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(SocketProperties.class);
            assertThat(context).hasSingleBean(SessionManager.class);
            assertThat(context).hasSingleBean(ConnectionManager.class);
            assertThat(context).hasSingleBean(MessageDispatcher.class);
            assertThat(context).hasSingleBean(SocketMessageSerializer.class);
            assertThat(context).hasSingleBean(SocketAuthenticationHook.class);
            assertThat(context).hasSingleBean(SocketErrorHandler.class);
            assertThat(context).hasSingleBean(SocketMetrics.class);
            assertThat(context).hasSingleBean(SocketTracing.class);
            assertThat(context).hasSingleBean(HeartbeatManager.class);
            assertThat(context).hasSingleBean(MessageHandlerRegistry.class);
            assertThat(context).hasSingleBean(SocketServerLifecycle.class);
        });
    }

    // ------------------------------------------------- metrics / tracing fallbacks

    @Test
    @DisplayName("Micrometer classes absent from the classpath -> context starts, both are NoOp")
    void autoConfiguration_withoutMicrometerClasses_startsWithNoOpImplementations() {
        // The tests below only prove "no MeterRegistry/Tracer BEAN": actuator and
        // micrometer-tracing are on the *test* classpath even though they are compileOnly for
        // consumers. This one proves "no CLASS" - i.e. that the compileOnly isolation holds and
        // nothing in the always-loaded configuration path touches a Micrometer type.
        runner.withClassLoader(new FilteredClassLoader(MeterRegistry.class, Tracer.class))
            .run(context -> {
                assertThat(context).hasNotFailed();
                assertThat(context.getStartupFailure()).isNull();
                assertThat(context).hasSingleBean(SocketMetrics.class);
                assertThat(context).hasSingleBean(SocketTracing.class);
                assertThat(context).getBean(SocketMetrics.class).isInstanceOf(NoOpSocketMetrics.class);
                assertThat(context).getBean(SocketTracing.class).isInstanceOf(NoOpSocketTracing.class);
                // The rest of the SDK is unaffected.
                assertThat(context).hasSingleBean(MessageHandlerRegistry.class);
                assertThat(context).hasSingleBean(SocketServerLifecycle.class);
            });
    }

    @Test
    @DisplayName("the real actuator auto-configurations -> SocketMetrics is Micrometer-backed")
    void socketMetrics_withRealActuatorAutoConfigurations_isMicrometerBacked() {
        // THE regression guard for the headline bug. The MeterRegistry here comes from Spring
        // Boot's own auto-configuration, not from a user @Configuration, which is what a real
        // app looks like. AutoConfigurationSorter orders by class name first, so
        // com.tutem...SocketAutoConfiguration is processed BEFORE
        // org.springframework.boot.actuate.autoconfigure.metrics.*: with
        // @ConditionalOnBean(MeterRegistry.class) no registry definition exists yet, the
        // condition does not match and the app silently gets NoOpSocketMetrics. Resolving the
        // registry through an ObjectProvider at instantiation time makes the order irrelevant.
        new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                SimpleMetricsExportAutoConfiguration.class,
                CompositeMeterRegistryAutoConfiguration.class,
                MetricsAutoConfiguration.class,
                SocketAutoConfiguration.class))
            .withUserConfiguration(MarkerConfiguration.class, ObjectMapperConfiguration.class,
                MockServerConfiguration.class)
            .withPropertyValues("app.socket.auth.enabled=false")
            .run(context -> {
                assertThat(context).hasNotFailed();
                assertThat(context).hasSingleBean(MeterRegistry.class);
                assertThat(context).hasSingleBean(SocketMetrics.class);
                assertThat(context).getBean(SocketMetrics.class)
                    .isInstanceOf(MicrometerSocketMetrics.class);
            });
    }

    @Test
    @DisplayName("no @Bean method name is declared twice (bean-definition overriding disabled)")
    void autoConfiguration_withBeanDefinitionOverridingDisabled_startsCleanly() {
        // socketMetrics/socketTracing used to be declared twice each (nested + enclosing).
        // Every @Bean method now has a unique name; this pins that the wiring stays clean
        // under the setting production apps use, so a future edit that drops one of the
        // conditions guarding these beans surfaces here instead of at deploy time.
        runner.withUserConfiguration(MeterRegistryConfiguration.class, TracerConfiguration.class)
            .withAllowBeanDefinitionOverriding(false)
            .run(context -> {
                assertThat(context).hasNotFailed();
                assertThat(context).hasSingleBean(SocketMetrics.class);
                assertThat(context).hasSingleBean(SocketTracing.class);
                assertThat(context).getBean(SocketMetrics.class)
                    .isInstanceOf(MicrometerSocketMetrics.class);
                assertThat(context).getBean(SocketTracing.class)
                    .isInstanceOf(MicrometerSocketTracing.class);
            });
    }

    @Test
    @DisplayName("no MeterRegistry and no Tracer bean -> context starts and both fall back to NoOp")
    void autoConfiguration_withoutMicrometerBeans_startsWithNoOpImplementations() {
        // Regression guard: this used to fail the whole context with
        // NoSuchBeanDefinitionException for MeterRegistry / Tracer.
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getStartupFailure()).isNull();
            assertThat(context).doesNotHaveBean(MeterRegistry.class);
            assertThat(context).doesNotHaveBean(Tracer.class);
            assertThat(context).getBean(SocketMetrics.class).isInstanceOf(NoOpSocketMetrics.class);
            assertThat(context).getBean(SocketTracing.class).isInstanceOf(NoOpSocketTracing.class);
        });
    }

    @Test
    @DisplayName("MeterRegistry bean present -> SocketMetrics is the Micrometer implementation")
    void socketMetrics_withMeterRegistry_isMicrometerBacked() {
        runner.withUserConfiguration(MeterRegistryConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).getBean(SocketMetrics.class)
                .isInstanceOf(MicrometerSocketMetrics.class);
            assertThat(context).hasSingleBean(SocketMetrics.class);
        });
    }

    @Test
    @DisplayName("app.socket.metrics.enabled=false -> NoOp metrics even with a MeterRegistry")
    void socketMetrics_metricsDisabled_fallsBackToNoOp() {
        runner.withUserConfiguration(MeterRegistryConfiguration.class)
            .withPropertyValues("app.socket.metrics.enabled=false")
            .run(context -> {
                assertThat(context).hasNotFailed();
                assertThat(context).getBean(SocketMetrics.class)
                    .isInstanceOf(NoOpSocketMetrics.class);
            });
    }

    @Test
    @DisplayName("Tracer bean present -> SocketTracing is the Micrometer implementation")
    void socketTracing_withTracer_isMicrometerBacked() {
        runner.withUserConfiguration(TracerConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).getBean(SocketTracing.class)
                .isInstanceOf(MicrometerSocketTracing.class);
            assertThat(context).hasSingleBean(SocketTracing.class);
        });
    }

    @Test
    @DisplayName("app.socket.tracing.enabled=false -> NoOp tracing even with a Tracer")
    void socketTracing_tracingDisabled_fallsBackToNoOp() {
        runner.withUserConfiguration(TracerConfiguration.class)
            .withPropertyValues("app.socket.tracing.enabled=false")
            .run(context -> {
                assertThat(context).hasNotFailed();
                assertThat(context).getBean(SocketTracing.class)
                    .isInstanceOf(NoOpSocketTracing.class);
            });
    }

    // ------------------------------------------------------------ consumer overrides

    @Test
    @DisplayName("a consumer SessionManager bean replaces the SDK default")
    void sessionManager_userBeanPresent_overridesSdkDefault() {
        runner.withUserConfiguration(CustomSessionManagerConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(SessionManager.class);
            assertThat(context).getBean(SessionManager.class)
                .isNotInstanceOf(InMemorySessionManager.class);
        });
    }

    @Test
    @DisplayName("a consumer SocketAuthenticationHook bean replaces the SDK default")
    void socketAuthenticationHook_userBeanPresent_overridesSdkDefault() {
        runner.withUserConfiguration(CustomAuthHookConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(SocketAuthenticationHook.class);
            assertThat(context).getBean(SocketAuthenticationHook.class)
                .isNotInstanceOf(DefaultSocketAuthenticationHook.class);
        });
    }

    @Test
    @DisplayName("a consumer SocketErrorHandler bean replaces the SDK default")
    void socketErrorHandler_userBeanPresent_overridesSdkDefault() {
        runner.withUserConfiguration(CustomErrorHandlerConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(SocketErrorHandler.class);
            assertThat(context).getBean(SocketErrorHandler.class)
                .isNotInstanceOf(DefaultSocketErrorHandler.class);
        });
    }

    @Test
    @DisplayName("a consumer SocketMessageSerializer bean replaces the SDK default")
    void socketMessageSerializer_userBeanPresent_overridesSdkDefault() {
        runner.withUserConfiguration(CustomSerializerConfiguration.class).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(SocketMessageSerializer.class);
            assertThat(context).getBean(SocketMessageSerializer.class)
                .isNotInstanceOf(JsonSocketMessageSerializer.class);
        });
    }

    // --------------------------------------------------------------- fail-fast auth

    @Test
    @DisplayName("auth.enabled=true with no hook bean -> startup fails naming SocketAuthenticationHook")
    void socketAuthenticationHook_authEnabledWithoutHook_failsFast() {
        runner.withPropertyValues("app.socket.auth.enabled=true").run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure())
                .hasStackTraceContaining("SocketAuthenticationHook")
                .hasStackTraceContaining("app.socket.auth.enabled=true");
        });
    }

    @Test
    @DisplayName("auth is on by default, so no hook bean fails startup with actionable guidance")
    void socketAuthenticationHook_defaultAuthEnabledWithoutHook_failsFastWithGuidance() {
        // Fix 11: the default flipped to true. A consumer that provides no hook must fail
        // loudly at startup rather than silently serve an unauthenticated socket port, and the
        // message must name both ways out.
        new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(SocketAutoConfiguration.class))
            .withUserConfiguration(MarkerConfiguration.class, ObjectMapperConfiguration.class,
                MockServerConfiguration.class)
            .run(context -> {
                assertThat(context).hasFailed();
                assertThat(context.getStartupFailure())
                    .hasStackTraceContaining("SocketAuthenticationHook")
                    .hasStackTraceContaining("app.socket.auth.enabled=false");
            });
    }

    @Test
    @DisplayName("auth on by default + a hook bean -> startup succeeds with auth enabled")
    void socketAuthenticationHook_defaultAuthEnabledWithHook_startsWithAuthOn() {
        new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(SocketAutoConfiguration.class))
            .withUserConfiguration(MarkerConfiguration.class, ObjectMapperConfiguration.class,
                MockServerConfiguration.class, CustomAuthHookConfiguration.class)
            .run(context -> {
                assertThat(context).hasNotFailed();
                assertThat(context.getBean(SocketProperties.class).getAuth().isEnabled()).isTrue();
            });
    }

    @Test
    @DisplayName("auth.enabled=true with a consumer hook bean -> startup succeeds")
    void socketAuthenticationHook_authEnabledWithHook_startsSuccessfully() {
        runner.withUserConfiguration(CustomAuthHookConfiguration.class)
            .withPropertyValues("app.socket.auth.enabled=true")
            .run(context -> {
                assertThat(context).hasNotFailed();
                assertThat(context.getBean(SocketProperties.class).getAuth().isEnabled()).isTrue();
            });
    }

    // ------------------------------------------------------------ property binding

    @Test
    @DisplayName("SocketProperties exposes the documented defaults")
    void socketProperties_noOverrides_usesDocumentedDefaults() {
        // A dedicated runner with NO property overrides at all - the shared `runner` disables
        // auth, which would hide the auth.enabled default asserted below. A hook bean is
        // supplied so the (now default-on) auth fail-fast does not trip.
        new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(SocketAutoConfiguration.class))
            .withUserConfiguration(MarkerConfiguration.class, ObjectMapperConfiguration.class,
                MockServerConfiguration.class, CustomAuthHookConfiguration.class)
            .run(context -> {
            SocketProperties props = context.getBean(SocketProperties.class);
            assertThat(props.isEnabled()).isTrue();
            assertThat(props.getPort()).isEqualTo(9090);
            assertThat(props.getHost()).isEqualTo("0.0.0.0");
            assertThat(props.getTransports()).containsExactly("websocket", "polling");
            assertThat(props.getOrigin()).isNull();
            assertThat(props.isAllowCustomRequests()).isFalse();
            assertThat(props.getBossCount()).isEqualTo(1);
            assertThat(props.getWorkerCount()).isEqualTo(100);
            assertThat(props.getUpgradeTimeout()).isEqualTo(10000);
            assertThat(props.getPingTimeout()).isEqualTo(60000);
            assertThat(props.getPingInterval()).isEqualTo(25000);
            assertThat(props.getStartupPhase()).isEqualTo(Integer.MAX_VALUE - 1024);
            // Secure by default: an SDK that ships an unauthenticated open port is worse than
            // one that refuses to start without a SocketAuthenticationHook.
            assertThat(props.getAuth().isEnabled()).isTrue();
            assertThat(props.getMetrics().isEnabled()).isTrue();
            assertThat(props.getTracing().isEnabled()).isTrue();
            assertThat(props.getHeartbeat().getStaleSessionSeconds()).isEqualTo(120);
            assertThat(props.getHeartbeat().getCleanupIntervalSeconds()).isEqualTo(30);
            assertThat(props.getHeartbeat().isDisconnectStaleSessions()).isFalse();
            assertThat(props.getHeartbeat().isEvictGhostSessions()).isTrue();
        });
    }

    @Test
    @DisplayName("app.socket.* properties bind onto SocketProperties")
    void socketProperties_explicitValues_areBound() {
        runner.withPropertyValues(
            "app.socket.port=59123",
            "app.socket.host=127.0.0.1",
            "app.socket.transports=polling,websocket",
            "app.socket.origin=https://app.example.com",
            "app.socket.allow-custom-requests=true",
            "app.socket.boss-count=2",
            "app.socket.worker-count=7",
            "app.socket.upgrade-timeout=1234",
            "app.socket.ping-timeout=4321",
            "app.socket.ping-interval=999",
            "app.socket.startup-phase=42",
            "app.socket.heartbeat.stale-session-seconds=17",
            "app.socket.heartbeat.cleanup-interval-seconds=5",
            "app.socket.heartbeat.disconnect-stale-sessions=true",
            "app.socket.heartbeat.evict-ghost-sessions=false"
        ).run(context -> {
            SocketProperties props = context.getBean(SocketProperties.class);
            assertThat(props.getPort()).isEqualTo(59123);
            assertThat(props.getHost()).isEqualTo("127.0.0.1");
            assertThat(props.getTransports()).containsExactly("polling", "websocket");
            assertThat(props.getOrigin()).isEqualTo("https://app.example.com");
            assertThat(props.isAllowCustomRequests()).isTrue();
            assertThat(props.getBossCount()).isEqualTo(2);
            assertThat(props.getWorkerCount()).isEqualTo(7);
            assertThat(props.getUpgradeTimeout()).isEqualTo(1234);
            assertThat(props.getPingTimeout()).isEqualTo(4321);
            assertThat(props.getPingInterval()).isEqualTo(999);
            assertThat(props.getStartupPhase()).isEqualTo(42);
            assertThat(props.getHeartbeat().getStaleSessionSeconds()).isEqualTo(17);
            assertThat(props.getHeartbeat().getCleanupIntervalSeconds()).isEqualTo(5);
            assertThat(props.getHeartbeat().isDisconnectStaleSessions()).isTrue();
            assertThat(props.getHeartbeat().isEvictGhostSessions()).isFalse();
        });
    }

    @Test
    @DisplayName("startup-phase binds through to SocketServerLifecycle.getPhase()")
    void socketServerLifecycle_startupPhaseProperty_isApplied() {
        runner.withPropertyValues("app.socket.startup-phase=4242").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(SocketServerLifecycle.class).getPhase()).isEqualTo(4242);
        });
    }

    // ------------------------------------------------------------------- transports

    @Test
    @DisplayName("an unknown app.socket.transports value fails startup naming the bad value")
    void socketIOServer_invalidTransport_failsNamingTheBadValue() {
        // No mock SocketIOServer here on purpose: the SDK's own socketIOServer bean must run
        // so transport resolution is exercised. It throws while *constructing* the bean, so
        // SocketIOServer.start() is never reached and no port is bound.
        new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(SocketAutoConfiguration.class))
            .withUserConfiguration(MarkerConfiguration.class, ObjectMapperConfiguration.class)
            // Auth off so the ONLY possible startup failure is transport resolution.
            .withPropertyValues("app.socket.auth.enabled=false",
                "app.socket.transports=carrier-pigeon")
            .run(context -> {
                assertThat(context).hasFailed();
                assertThat(context.getStartupFailure())
                    .hasStackTraceContaining("carrier-pigeon")
                    .hasStackTraceContaining("app.socket.transports");
            });
    }

    @Test
    @DisplayName("resolveTransports accepts any case, de-duplicates and preserves order")
    void resolveTransports_mixedCaseWithDuplicates_normalizesAndDeduplicates() {
        assertThat(SocketAutoConfiguration.resolveTransports(
            java.util.List.of("POLLING", " websocket ", "polling")))
            .containsExactly(Transport.POLLING, Transport.WEBSOCKET);
    }

    @Test
    @DisplayName("resolveTransports rejects an empty or null list")
    void resolveTransports_emptyList_throws() {
        assertThatThrownBy(() -> SocketAutoConfiguration.resolveTransports(java.util.List.of()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("app.socket.transports must not be empty");
        assertThatThrownBy(() -> SocketAutoConfiguration.resolveTransports(null))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("resolveTransports rejects an unknown name and lists the legal values")
    void resolveTransports_unknownName_throwsNamingTheValue() {
        assertThatThrownBy(() -> SocketAutoConfiguration.resolveTransports(
            java.util.List.of("websocket", "smoke-signal")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("smoke-signal")
            .hasMessageContaining("websocket")
            .hasMessageContaining("polling");
    }

    // ------------------------------------------------------------------- test config

    @org.springframework.context.annotation.Configuration(proxyBeanMethods = false)
    static class MarkerConfiguration {
        @Bean
        SocketMarker socketMarker() {
            return new SocketMarker();
        }
    }

    @org.springframework.context.annotation.Configuration(proxyBeanMethods = false)
    static class ObjectMapperConfiguration {
        @Bean
        ObjectMapper objectMapper() {
            return new ObjectMapper();
        }
    }

    /** Keeps Netty from binding: see the class javadoc. */
    @org.springframework.context.annotation.Configuration(proxyBeanMethods = false)
    static class MockServerConfiguration {
        @Bean
        SocketIOServer socketIOServer() {
            SocketIOServer server = mock(SocketIOServer.class);
            Configuration configuration = new Configuration();
            configuration.setHostname("127.0.0.1");
            configuration.setPort(0);
            when(server.getConfiguration()).thenReturn(configuration);
            return server;
        }
    }

    @org.springframework.context.annotation.Configuration(proxyBeanMethods = false)
    static class MeterRegistryConfiguration {
        @Bean
        MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }
    }

    @org.springframework.context.annotation.Configuration(proxyBeanMethods = false)
    static class TracerConfiguration {
        @Bean
        Tracer tracer() {
            return mock(Tracer.class);
        }
    }

    @org.springframework.context.annotation.Configuration(proxyBeanMethods = false)
    static class CustomSessionManagerConfiguration {
        @Bean
        SessionManager sessionManager() {
            return mock(SessionManager.class);
        }
    }

    @org.springframework.context.annotation.Configuration(proxyBeanMethods = false)
    static class CustomAuthHookConfiguration {
        @Bean
        SocketAuthenticationHook socketAuthenticationHook() {
            return mock(SocketAuthenticationHook.class);
        }
    }

    @org.springframework.context.annotation.Configuration(proxyBeanMethods = false)
    static class CustomErrorHandlerConfiguration {
        @Bean
        SocketErrorHandler socketErrorHandler() {
            return mock(SocketErrorHandler.class);
        }
    }

    @org.springframework.context.annotation.Configuration(proxyBeanMethods = false)
    static class CustomSerializerConfiguration {
        @Bean
        SocketMessageSerializer socketMessageSerializer() {
            return mock(SocketMessageSerializer.class);
        }
    }
}
