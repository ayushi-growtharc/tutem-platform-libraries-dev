package com.tutem.platform.socket.lifecycle;

import com.corundumstudio.socketio.Configuration;
import com.corundumstudio.socketio.SocketIOServer;
import com.tutem.platform.socket.dispatcher.MessageHandlerRegistry;
import com.tutem.platform.socket.event.SocketServerStartedEvent;
import com.tutem.platform.socket.event.SocketServerStoppedEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.springframework.context.ApplicationEventPublisher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SocketServerLifecycleTest {

    private static final int PHASE = 12345;
    private static final int PORT = 59321;

    private SocketIOServer server;
    private MessageHandlerRegistry registry;
    private ApplicationEventPublisher eventPublisher;
    private SocketServerLifecycle lifecycle;

    @BeforeEach
    void setUp() {
        server = mock(SocketIOServer.class);
        Configuration configuration = new Configuration();
        configuration.setHostname("127.0.0.1");
        configuration.setPort(PORT);
        when(server.getConfiguration()).thenReturn(configuration);

        registry = mock(MessageHandlerRegistry.class);
        eventPublisher = mock(ApplicationEventPublisher.class);
        lifecycle = new SocketServerLifecycle(server, registry, eventPublisher, PHASE);
    }

    @Test
    @DisplayName("start registers handlers BEFORE opening the port")
    void start_registersHandlersBeforeStartingServer() {
        lifecycle.start();

        InOrder order = inOrder(registry, server);
        order.verify(registry).registerAll();
        order.verify(server).start();
        assertThat(lifecycle.isRunning()).isTrue();
    }

    @Test
    @DisplayName("start publishes SocketServerStartedEvent carrying the configured port")
    void start_publishesStartedEventWithPort() {
        lifecycle.start();

        ArgumentCaptor<SocketServerStartedEvent> captor =
            ArgumentCaptor.forClass(SocketServerStartedEvent.class);
        verify(eventPublisher).publishEvent(captor.capture());
        assertThat(captor.getValue().getPort()).isEqualTo(PORT);
    }

    @Test
    @DisplayName("a second start is a no-op")
    void start_calledTwice_isIdempotent() {
        lifecycle.start();
        lifecycle.start();

        verify(registry, times(1)).registerAll();
        verify(server, times(1)).start();
        verify(eventPublisher, times(1)).publishEvent(any(SocketServerStartedEvent.class));
    }

    @Test
    @DisplayName("isAutoStartup is true and getPhase returns the injected phase")
    void smartLifecycleContract_isAutoStartupAndPhase() {
        assertThat(lifecycle.isAutoStartup()).isTrue();
        assertThat(lifecycle.getPhase()).isEqualTo(PHASE);
        assertThat(lifecycle.isRunning()).isFalse();
    }

    @Test
    @DisplayName("stop after a successful start stops the server and publishes the stopped event")
    void stop_afterStart_stopsServerAndPublishesEvent() {
        lifecycle.start();
        lifecycle.stop();

        verify(server).stop();
        verify(eventPublisher).publishEvent(any(SocketServerStoppedEvent.class));
        assertThat(lifecycle.isRunning()).isFalse();
    }

    @Test
    @DisplayName("a second stop is a no-op")
    void stop_calledTwice_isIdempotent() {
        lifecycle.start();
        lifecycle.stop();
        lifecycle.stop();

        verify(server, times(1)).stop();
        verify(eventPublisher, times(1)).publishEvent(any(SocketServerStoppedEvent.class));
    }

    @Test
    @DisplayName("stop never propagates a failure from server.stop(), and publishes no stopped event")
    void stop_serverStopThrows_doesNotPropagateAndPublishesNoStoppedEvent() {
        // The server may still be listening after a failed stop(), so claiming it stopped
        // would mislead every listener.
        doThrow(new IllegalStateException("netty already down")).when(server).stop();
        lifecycle.start();

        assertThatCode(() -> lifecycle.stop()).doesNotThrowAnyException();

        verify(eventPublisher, never()).publishEvent(any(SocketServerStoppedEvent.class));
        assertThat(lifecycle.isRunning()).isFalse();
    }

    @Test
    @DisplayName("a throwing SocketServerStoppedEvent listener does not break shutdown")
    void stop_stoppedEventListenerThrows_doesNotPropagate() {
        // Listener beans can already be destroyed while the context is closing.
        doThrow(new IllegalStateException("listener bean already destroyed"))
            .when(eventPublisher).publishEvent(any(SocketServerStoppedEvent.class));
        lifecycle.start();

        assertThatCode(() -> lifecycle.stop()).doesNotThrowAnyException();

        verify(server).stop();
        assertThat(lifecycle.isRunning()).isFalse();
    }

    @Test
    @DisplayName("a failed start rethrows, resets running, and a following stop does not throw")
    void stop_afterFailedStart_doesNotThrow() {
        doThrow(new IllegalStateException("scan failed")).when(registry).registerAll();

        assertThatThrownBy(() -> lifecycle.start())
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("scan failed");

        assertThat(lifecycle.isRunning()).isFalse();
        verify(server, never()).start();
        verify(eventPublisher, never()).publishEvent(any(SocketServerStartedEvent.class));

        assertThatCode(() -> lifecycle.stop()).doesNotThrowAnyException();
        verify(server, never()).stop();
    }

    @Test
    @DisplayName("a failure inside server.start() is rethrown and leaves the component not running")
    void start_serverStartThrows_resetsRunningFlag() {
        doThrow(new IllegalStateException("port already bound")).when(server).start();

        assertThatThrownBy(() -> lifecycle.start()).isInstanceOf(IllegalStateException.class);

        assertThat(lifecycle.isRunning()).isFalse();
        verify(registry).registerAll();
        verify(eventPublisher, never()).publishEvent(any(SocketServerStartedEvent.class));
        // A retry is allowed after a failed start.
        assertThatThrownBy(() -> lifecycle.start()).isInstanceOf(IllegalStateException.class);
        verify(server, times(2)).start();
    }

    @Test
    @DisplayName("a failure inside server.start() still releases the server's thread groups")
    void start_serverStartThrows_stopsServerToReleaseEventLoops() {
        doThrow(new IllegalStateException("port already bound")).when(server).start();

        assertThatThrownBy(() -> lifecycle.start()).isInstanceOf(IllegalStateException.class);

        // start() creates the boss/worker NioEventLoopGroups before it binds, so the failure
        // path must stop the server or ~100 non-daemon threads outlive the failed startup and
        // the JVM never exits. running is already false, so stop() cannot do this later.
        verify(server).stop();
        assertThat(lifecycle.isRunning()).isFalse();
    }

    @Test
    @DisplayName("a stop() that also throws does not mask the original startup failure")
    void start_serverStartThrowsAndStopThrows_originalExceptionWins() {
        doThrow(new IllegalStateException("port already bound")).when(server).start();
        doThrow(new NullPointerException("groups were never created")).when(server).stop();

        assertThatThrownBy(() -> lifecycle.start())
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("port already bound")
            .satisfies(thrown -> assertThat(thrown.getSuppressed())
                .singleElement()
                .isInstanceOf(NullPointerException.class));

        assertThat(lifecycle.isRunning()).isFalse();
    }
}
