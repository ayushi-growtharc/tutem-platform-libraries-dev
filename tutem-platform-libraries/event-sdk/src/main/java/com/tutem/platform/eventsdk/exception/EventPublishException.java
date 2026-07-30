package com.tutem.platform.eventsdk.exception;

public class EventPublishException extends RuntimeException {
    public EventPublishException(String message,  Throwable cause) {
        super(message, cause);
    }
}
