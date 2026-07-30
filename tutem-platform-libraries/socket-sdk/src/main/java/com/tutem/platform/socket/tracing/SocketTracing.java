package com.tutem.platform.socket.tracing;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import lombok.extern.slf4j.Slf4j;

/**
 * Wraps each @OnMessage handler invocation in an OpenTelemetry span.
 * Each socket event becomes a traceable unit in your observability dashboard.
 *
 * When tracing is disabled or Tracer is not on the classpath, the handler
 * is called directly with no overhead.
 */
@Slf4j
public class SocketTracing {

    private final Tracer tracer;
    private final boolean enabled;

    public SocketTracing(Tracer tracer, boolean enabled) {
        this.tracer = tracer;
        this.enabled = enabled && tracer != null;
    }

    /**
     * Run the given action inside a named trace span.
     * span name format: "socket.{eventName}" (e.g. "socket.joinTrackingRoom")
     */
    public void startSpan(String spanName, Runnable action) {
        if (!enabled) {
            action.run();
            return;
        }

        Span span = tracer.nextSpan().name(spanName).start();
        try (Tracer.SpanInScope scope = tracer.withSpan(span)) {
            action.run();
        } catch (Exception e) {
            span.error(e);
            throw e;
        } finally {
            span.end();
        }
    }
}
