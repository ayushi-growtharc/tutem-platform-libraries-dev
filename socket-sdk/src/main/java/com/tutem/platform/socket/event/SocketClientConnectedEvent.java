package com.tutem.platform.socket.event;

import com.tutem.platform.socket.session.SocketSession;
import lombok.Getter;
import org.springframework.context.ApplicationEvent;

@Getter
public class SocketClientConnectedEvent extends ApplicationEvent {
    private final SocketSession session;

    public SocketClientConnectedEvent(Object source, SocketSession session) {
        super(source);
        this.session = session;
    }
}
