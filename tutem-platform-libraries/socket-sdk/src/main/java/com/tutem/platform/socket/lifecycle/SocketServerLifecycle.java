package com.tutem.platform.socket.lifecycle;

import com.corundumstudio.socketio.SocketIOServer;
import com.tutem.platform.socket.dispatcher.MessageHandlerRegistry;
import com.tutem.platform.socket.event.SocketServerStartedEvent;
import com.tutem.platform.socket.event.SocketServerStoppedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.SmartLifecycle;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Ties the Socket.IO server to the Spring application lifecycle via {@link SmartLifecycle}.
 *
 * <p>Startup order:
 * <ol>
 *   <li>Spring finishes creating all singletons</li>
 *   <li>{@code LifecycleProcessor} starts phases in ascending order; the web server starts
 *       in its own phase, then this component starts in
 *       {@code app.socket.startup-phase}</li>
 *   <li>{@link MessageHandlerRegistry#registerAll()} scans and registers every handler</li>
 *   <li>{@code SocketIOServer.start()} - only now are client connections accepted</li>
 *   <li>{@link SocketServerStartedEvent} is published</li>
 * </ol>
 *
 * <p>Shutdown runs the same phases in reverse, so the socket port closes as part of the
 * ordered shutdown rather than at an arbitrary point during context close.
 *
 * <p>Using {@code SmartLifecycle} instead of {@code @EventListener(ContextRefreshedEvent)}
 * matters for two reasons: {@code ContextRefreshedEvent} carries no ordering relative to the
 * HTTP server, and it can fire more than once for a single context. {@link #start()} is
 * additionally guarded so a repeat call is a no-op, and {@link #stop()} never throws, so a
 * failed startup or a double stop cannot break context close.
 *
 * <p>{@link SocketServerStoppedEvent} is published only after {@code SocketIOServer.stop()}
 * actually succeeded: if the server failed to stop it may still be listening, and announcing
 * a shutdown that did not happen would mislead listeners.
 */
@Slf4j
public class SocketServerLifecycle implements SmartLifecycle {

    private final SocketIOServer server;
    private final MessageHandlerRegistry registry;
    private final ApplicationEventPublisher eventPublisher;
    private final int phase;

    private final AtomicBoolean running = new AtomicBoolean(false);

    public SocketServerLifecycle(SocketIOServer server,
                                 MessageHandlerRegistry registry,
                                 ApplicationEventPublisher eventPublisher,
                                 int phase) {
        this.server = server;
        this.registry = registry;
        this.eventPublisher = eventPublisher;
        this.phase = phase;
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        try {
            // Handlers must be registered BEFORE the port opens, otherwise a client can
            // send an event that nothing is listening for.
            registry.registerAll();
        } catch (RuntimeException e) {
            // Nothing to release: the server was never touched.
            throw failStart(e);
        }
        try {
            server.start();
        } catch (RuntimeException e) {
            // SocketIOServer.start() creates its boss/worker NioEventLoopGroups BEFORE it
            // binds, so a bind failure (port already taken, port out of range) leaves ~100
            // live non-daemon threads. running is back to false by now, which makes stop() a
            // no-op, so nothing would ever release them — and non-daemon threads stop a
            // failed-startup JVM from exiting at all, turning a crash-loop into a hung pod.
            try {
                server.stop();
            } catch (RuntimeException suppressed) {
                // stop() throws if start() failed before creating the groups; that is
                // expected here and must never replace the real startup failure.
                e.addSuppressed(suppressed);
            }
            throw failStart(e);
        }
        int port = server.getConfiguration().getPort();
        log.info("Socket server started on port {}", port);
        eventPublisher.publishEvent(new SocketServerStartedEvent(this, port));
    }

    /**
     * Marks the component not running, logs the startup failure, and returns the exception so
     * the caller can {@code throw} it (keeping the compiler aware the path terminates).
     */
    private RuntimeException failStart(RuntimeException e) {
        running.set(false);
        log.error("Socket server failed to start on {}:{}: {}",
            server.getConfiguration().getHostname(),
            server.getConfiguration().getPort(), e.getMessage(), e);
        return e;
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        try {
            server.stop();
            log.info("Socket server stopped");
        } catch (RuntimeException e) {
            // Never propagate: this runs during context close, and a failure here would
            // mask the real shutdown cause and abort the remaining shutdown work. The
            // stopped event is NOT published — the server may well still be listening, so
            // telling listeners it stopped would be a lie.
            log.warn("Socket server stop failed: {}", e.getMessage(), e);
            return;
        }
        try {
            eventPublisher.publishEvent(new SocketServerStoppedEvent(this));
        } catch (RuntimeException e) {
            // Listener beans may already be destroyed at this point in context close, and a
            // listener failure must not break the rest of shutdown.
            log.warn("A SocketServerStoppedEvent listener failed during shutdown: {}",
                e.getMessage(), e);
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public int getPhase() {
        return phase;
    }
}
