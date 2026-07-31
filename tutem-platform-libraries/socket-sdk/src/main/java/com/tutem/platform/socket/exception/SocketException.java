package com.tutem.platform.socket.exception;

public class SocketException extends RuntimeException {
    public SocketException(String message) { super(message); }
    public SocketException(String message, Throwable cause) { super(message, cause); }
}
