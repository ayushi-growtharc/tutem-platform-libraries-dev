package  com.tutem.platform.libraries.event-sdk.model;

public interface DomainEvent {
    String eventType();
    String version();
}