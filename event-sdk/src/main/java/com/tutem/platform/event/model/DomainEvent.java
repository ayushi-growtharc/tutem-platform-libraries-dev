package  com.tutem.platform.event.model;

public interface DomainEvent {
    String eventType();
    String version();
    String key();
}