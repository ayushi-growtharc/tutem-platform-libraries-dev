package com.tutem.platform.socket.annotations;

import com.tutem.platform.socket.config.SocketAutoConfiguration;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * Add to your Spring Boot application class to activate the socket-sdk.
 *
 * @SpringBootApplication
 * @EnableSocket
 * public class MyServiceApplication { ... }
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(SocketAutoConfiguration.class)
public @interface EnableSocket {
}
