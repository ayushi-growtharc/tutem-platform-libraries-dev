package com.tutem.platform.eventsdk.envelope;

import java.time.Instant;

public class EventEnvelope<T> {
    private String eventId;
    private String eventType;
    private String version;
    private String traceId;
    private String source;
    private Instant timestamp;
    private T payload;

    public EventEnvelope() {}

    public String getEventId() {
        return eventId;
    }
    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getEventType() {
        return eventType;
    }
    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getVersion() {
        return  version;
    }
    public void setVersion(String version) {
        this.version = version;
    }

    public String getTraceId() {
        return traceId;
    }
    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public String getSource() {
        return source;
    }
    public void setSource(String source) {
        this.source = source;
    }

    public Instant getTimestamp() {
        return timestamp;
    }
    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public T getPayload() { return payload; }
    public void setPayload(T payload) { this.payload = payload; }
}