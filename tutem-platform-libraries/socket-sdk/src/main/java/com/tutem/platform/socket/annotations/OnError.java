package com.tutem.platform.socket.annotations;

import java.lang.annotation.*;

/**
 * Marks a method to be called when an unhandled exception occurs in any @OnMessage handler.
 *
 * Supported signature:
 *   @OnError
 *   public void onError(SocketIOClient client, String event, Exception ex)
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface OnError {
}
