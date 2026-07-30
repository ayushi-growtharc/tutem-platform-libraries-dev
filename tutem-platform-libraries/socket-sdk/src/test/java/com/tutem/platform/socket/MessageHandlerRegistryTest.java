package com.tutem.platform.socket;

import com.corundumstudio.socketio.AckRequest;
import com.corundumstudio.socketio.SocketIOClient;
import com.corundumstudio.socketio.SocketIOServer;
import com.tutem.platform.socket.annotations.OnConnect;
import com.tutem.platform.socket.annotations.OnDisconnect;
import com.tutem.platform.socket.annotations.OnMessage;
import com.tutem.platform.socket.authentication.DefaultSocketAuthenticationHook;
import com.tutem.platform.socket.dispatcher.MessageHandlerRegistry;
import com.tutem.platform.socket.metrics.SocketMetrics;
import com.tutem.platform.socket.session.InMemorySessionManager;
import com.tutem.platform.socket.session.SessionManager;
import com.tutem.platform.socket.tracing.SocketTracing;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class MessageHandlerRegistryTest {

    static class SamplePayload {
        public String room;
    }

    static class SampleHandler {
        @OnConnect
        public void onConnect(SocketIOClient client) {}

        @OnDisconnect
        public void onDisconnect(SocketIOClient client) {}

        @OnMessage("joinRoom")
        public void onJoin(SocketIOClient client, SamplePayload payload, AckRequest ack) {}

        @OnMessage("ping")
        public void onPing(SocketIOClient client) {}
    }

    @Test
    void shouldRegisterAllAnnotatedMethods() {
        SocketIOServer server = mock(SocketIOServer.class);
        ApplicationContext context = mock(ApplicationContext.class);
        ApplicationEventPublisher publisher = mock(ApplicationEventPublisher.class);
        SessionManager sessionManager = new InMemorySessionManager();
        SocketMetrics metrics = mock(SocketMetrics.class);
        SocketTracing tracing = new SocketTracing(null, false);

        SampleHandler handler = new SampleHandler();
        when(context.getBeanDefinitionNames()).thenReturn(new String[]{"sampleHandler"});
        when(context.getBean("sampleHandler")).thenReturn(handler);

        MessageHandlerRegistry registry = new MessageHandlerRegistry(
            server, context, sessionManager, publisher,
            new DefaultSocketAuthenticationHook(), metrics, tracing
        );
        registry.registerAll();

        verify(server).addConnectListener(any());
        verify(server).addDisconnectListener(any());
        verify(server).addEventListener(eq("joinRoom"), eq(SamplePayload.class), any());
        verify(server).addEventListener(eq("ping"), any(), any());
    }
}
