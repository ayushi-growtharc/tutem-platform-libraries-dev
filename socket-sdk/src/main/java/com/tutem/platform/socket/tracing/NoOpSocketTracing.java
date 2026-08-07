package com.tutem.platform.socket.tracing;

/**
 * Default {@link SocketTracing} used when micrometer-tracing is absent from the classpath,
 * when no {@code Tracer} bean exists, or when {@code app.socket.tracing.enabled=false}.
 *
 * <p>Runs the work directly, adding no span and no allocation.
 */
public class NoOpSocketTracing implements SocketTracing {

    @Override
    public void startSpan(String name, Runnable work) {
        work.run();
    }
}
