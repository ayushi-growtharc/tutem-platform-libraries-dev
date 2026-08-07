package com.tutem.platform.socket.tracing;

/**
 * SDK-owned tracing abstraction for the socket layer.
 *
 * <p>Deliberately free of any Micrometer/OpenTelemetry type: micrometer-tracing is a
 * {@code compileOnly} dependency of this SDK, so no type the runtime always loads may
 * mention it. Consumers with a {@code Tracer} bean get {@link MicrometerSocketTracing};
 * everybody else gets {@link NoOpSocketTracing}.
 */
public interface SocketTracing {

    /**
     * Run {@code work} inside a span named {@code name}.
     *
     * <p>Implementations must propagate any exception thrown by {@code work} to the caller
     * unchanged (after recording it on the span) and must always close the span.
     *
     * @param name span name, e.g. {@code "socket.joinTrackingRoom"}
     * @param work the work to instrument; never {@code null}
     */
    void startSpan(String name, Runnable work);
}
