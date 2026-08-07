package com.tutem.platform.socket.tracing;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import lombok.extern.slf4j.Slf4j;

/**
 * Micrometer-Tracing backed {@link SocketTracing}: wraps each socket handler invocation
 * in its own span, so a client event and every downstream call it triggers show up as one
 * trace in Jaeger/Zipkin.
 *
 * <p>Only loaded when micrometer-tracing is on the consumer's classpath, a {@code Tracer}
 * bean exists (i.e. a tracing bridge is configured), and {@code app.socket.tracing.enabled}
 * is not {@code false}. See {@code SocketAutoConfiguration}.
 */
@Slf4j
public class MicrometerSocketTracing implements SocketTracing {

    private final Tracer tracer;

    public MicrometerSocketTracing(Tracer tracer) {
        this.tracer = tracer;
        log.info("Socket tracing enabled (Micrometer Tracing)");
    }

    @Override
    public void startSpan(String name, Runnable work) {
        Span span = tracer.nextSpan().name(name).start();
        try (Tracer.SpanInScope ignored = tracer.withSpan(span)) {
            work.run();
        } catch (RuntimeException | Error e) {
            span.error(e);
            throw e;
        } finally {
            span.end();
        }
    }
}
