package com.tutem.platform.event.exception;

public class EventPublishException extends RuntimeException {
    public EventPublishException(String message,  Throwable cause) {
        super(message, cause);
    }
}
