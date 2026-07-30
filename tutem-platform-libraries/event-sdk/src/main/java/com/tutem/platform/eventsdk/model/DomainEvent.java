package  com.tutem.platform.eventsdk.model;

public interface DomainEvent {
    String eventType();
    String version();
    String key();
}