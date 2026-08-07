package com.tutem.platform.socket.event;

import com.tutem.platform.socket.session.SocketSession;
import lombok.Getter;
import org.springframework.context.ApplicationEvent;

@Getter
public class SocketClientDisconnectedEvent extends ApplicationEvent {
    private final SocketSession session;

    public SocketClientDisconnectedEvent(Object source, SocketSession session) {
        super(source);
        this.session = session;
    }
}
