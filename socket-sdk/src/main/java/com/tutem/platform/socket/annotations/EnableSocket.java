package com.tutem.platform.socket.annotations;

import com.tutem.platform.socket.config.SocketMarkerConfiguration;
import org.springframework.context.annotation.Import;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Add to your Spring Boot application class to activate the socket-sdk.
 *
 * <pre>
 * &#64;SpringBootApplication
 * &#64;EnableSocket
 * public class MyServiceApplication { ... }
 * </pre>
 *
 * <p>This imports only a tiny marker bean; the real wiring lives in
 * {@code SocketAutoConfiguration}, which is {@code @ConditionalOnBean(SocketMarker.class)}
 * and therefore runs in Spring Boot's deferred auto-configuration phase. Consequence:
 * having socket-sdk on the classpath alone never opens a socket port, and any bean you
 * define yourself (e.g. {@code SessionManager}, {@code SocketAuthenticationHook}) reliably
 * takes precedence over the SDK default.
 *
 * <p>Set {@code app.socket.enabled=false} to switch the SDK off without removing this
 * annotation (useful in tests and in profiles that do not need the socket server).
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(SocketMarkerConfiguration.class)
public @interface EnableSocket {
}
