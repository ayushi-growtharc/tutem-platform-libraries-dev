package com.tutem.platform.event.tracing;

public final class CorrelationContext {
    private static final ThreadLocal<String> TRACE_ID = new ThreadLocal<>();

    private CorrelationContext() {}

    public static void set(String traceId) {
        TRACE_ID.set(traceId);
    }

    public static String get() {
        return TRACE_ID.get();
    }

    public static void clear() {
        TRACE_ID.remove();
    }
}
