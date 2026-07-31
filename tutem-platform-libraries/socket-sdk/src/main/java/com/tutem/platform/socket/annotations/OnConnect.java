package com.tutem.platform.socket.annotations;

import java.lang.annotation.*;

/**
 * Marks a method to be called when a client connects.
 * Called AFTER authentication succeeds.
 *
 * Supported signatures:
 *   @OnConnect
 *   public void onConnect(SocketIOClient client, SocketSession session)
 *
 *   @OnConnect
 *   public void onConnect(SocketIOClient client)
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface OnConnect {
}
