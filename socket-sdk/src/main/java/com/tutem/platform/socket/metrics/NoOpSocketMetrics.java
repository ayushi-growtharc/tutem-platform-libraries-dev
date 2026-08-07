package com.tutem.platform.socket.metrics;

/**
 * Default {@link SocketMetrics} used when Micrometer is absent from the classpath,
 * when no {@code MeterRegistry} bean exists, or when {@code app.socket.metrics.enabled=false}.
 *
 * <p>Every method is empty, so the hot path costs a single (JIT-inlinable) virtual call.
 * This keeps the SDK free of {@code null} metrics references and of
 * {@code if (metricsEnabled)} branches scattered through the dispatcher.
 */
public class NoOpSocketMetrics implements SocketMetrics {

    @Override
    public void incrementConnect() {
        // no-op
    }

    @Override
    public void incrementDisconnect() {
        // no-op
    }

    @Override
    public void incrementAuthFailure() {
        // no-op
    }

    @Override
    public void incrementMessageReceived(String event) {
        // no-op
    }

    @Override
    public void incrementError(String event) {
        // no-op
    }
}
