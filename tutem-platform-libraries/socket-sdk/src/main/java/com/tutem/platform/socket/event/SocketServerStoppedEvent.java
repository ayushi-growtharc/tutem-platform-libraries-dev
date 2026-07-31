package com.tutem.platform.socket.event;

import org.springframework.context.ApplicationEvent;

public class SocketServerStoppedEvent extends ApplicationEvent {
    public SocketServerStoppedEvent(Object source) { super(source); }
}
