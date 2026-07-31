package com.tutem.platform.socket.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method as a handler for an incoming socket message.
 *
 * Supported signatures:
 *   @OnMessage("joinRoom")
 *   public void handle(SocketIOClient client, MyPayload payload, AckRequest ack)
 *
 *   @OnMessage("joinRoom")
 *   public void handle(SocketIOClient client, MyPayload payload)
 *
 *   @OnMessage("joinRoom")
 *   public void handle(MyPayload payload)
 *
 * The payload class is auto-detected — it is the parameter that is
 * not SocketIOClient or AckRequest.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface OnMessage {

    /** The event name the client sends (e.g. "joinRoom", "trackDriver") */
    String value();
}
