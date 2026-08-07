package com.tutem.platform.socket.exception;

/**
 * Base runtime exception for all socket-sdk failures.
 * Thrown by the SDK itself (serialization, dispatch) and available to
 * consuming services that want their handler failures to route through
 * the SDK's error pipeline.
 */
public class SocketException extends RuntimeException {

    public SocketException(String message) {
        super(message);
    }

    public SocketException(String message, Throwable cause) {
        super(message, cause);
    }
}
