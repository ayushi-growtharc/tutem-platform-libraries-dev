package com.tutem.platform.socket.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method to be called when a client disconnects.
 *
 * Supported signatures:
 *   @OnDisconnect
 *   public void onDisconnect(SocketIOClient client, SocketSession session)
 *
 *   @OnDisconnect
 *   public void onDisconnect(SocketIOClient client)
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface OnDisconnect {
}
