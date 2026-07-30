package com.tutem.platform.socket.lifecycle;

import com.corundumstudio.socketio.SocketIOServer;
import com.tutem.platform.socket.dispatcher.MessageHandlerRegistry;
import com.tutem.platform.socket.event.SocketServerStartedEvent;
import com.tutem.platform.socket.event.SocketServerStoppedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;

/**
 * Ties the Socket.IO server to the Spring application lifecycle.
 *
 * Order:
 *  1. Spring loads all beans
 *  2. ContextRefreshedEvent fires
 *  3. MessageHandlerRegistry scans and registers all @OnMessage handlers
 *  4. Server starts — only NOW accepts connections
 *  5. On ContextClosedEvent → server stops cleanly
 */
@Slf4j
@RequiredArgsConstructor
public class SocketServerLifecycle {

    private final SocketIOServer server;
    private final MessageHandlerRegistry registry;
    private final ApplicationEventPublisher eventPublisher;
    private boolean started = false;

    @EventListener(ContextRefreshedEvent.class)
    public void onContextReady() {
        if (started) return;
        registry.registerAll();
        server.start();
        started = true;
        int port = server.getConfiguration().getPort();
        log.info("Socket server started on port {}", port);
        eventPublisher.publishEvent(new SocketServerStartedEvent(this, port));
    }

    @EventListener(ContextClosedEvent.class)
    public void onContextClosed() {
        server.stop();
        log.info("Socket server stopped");
        eventPublisher.publishEvent(new SocketServerStoppedEvent(this));
    }
}
