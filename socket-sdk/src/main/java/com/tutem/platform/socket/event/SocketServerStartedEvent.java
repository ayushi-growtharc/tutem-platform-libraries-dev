package com.tutem.platform.socket.event;

import lombok.Getter;
import org.springframework.context.ApplicationEvent;

@Getter
public class SocketServerStartedEvent extends ApplicationEvent {
    private final int port;

    public SocketServerStartedEvent(Object source, int port) {
        super(source);
        this.port = port;
    }
}
