package  com.tutem.platform.event-sdk.model;

public interface DomainEvent {
    String eventType();
    String version();
}