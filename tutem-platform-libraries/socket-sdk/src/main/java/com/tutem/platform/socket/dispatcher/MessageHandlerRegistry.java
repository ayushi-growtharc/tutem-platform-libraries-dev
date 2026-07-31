package com.tutem.platform.socket.dispatcher;

import com.corundumstudio.socketio.AckRequest;
import com.corundumstudio.socketio.SocketIOClient;
import com.corundumstudio.socketio.SocketIOServer;
import com.tutem.platform.socket.annotations.OnConnect;
import com.tutem.platform.socket.annotations.OnDisconnect;
import com.tutem.platform.socket.annotations.OnError;
import com.tutem.platform.socket.annotations.OnMessage;
import com.tutem.platform.socket.authentication.SocketAuthContext;
import com.tutem.platform.socket.authentication.SocketAuthenticationHook;
import com.tutem.platform.socket.event.*;
import com.tutem.platform.socket.exception.SocketAuthException;
import com.tutem.platform.socket.metrics.SocketMetrics;
import com.tutem.platform.socket.session.SessionManager;
import com.tutem.platform.socket.session.SocketSession;
import com.tutem.platform.socket.serialization.SocketMessageSerializer;
import com.tutem.platform.socket.tracing.SocketTracing;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Scans all Spring beans for socket annotations and registers them with the server.
 * Also manages the full lifecycle per connection: auth → session → events → disconnect.
 */
@Slf4j
@RequiredArgsConstructor
public class MessageHandlerRegistry {

    private final SocketIOServer server;
    private final ApplicationContext applicationContext;
    private final SessionManager sessionManager;
    private final ApplicationEventPublisher eventPublisher;
    private final SocketAuthenticationHook authHook;
    private final SocketMetrics metrics;
    private final SocketTracing tracing;
    private final SocketMessageSerializer serializer;

    @SuppressWarnings("unchecked")
    public void registerAll() {
        List<HandlerMethod> connectHandlers    = new ArrayList<>();
        List<HandlerMethod> disconnectHandlers = new ArrayList<>();
        List<HandlerMethod> errorHandlers      = new ArrayList<>();

        for (String beanName : applicationContext.getBeanDefinitionNames()) {
            Object bean = applicationContext.getBean(beanName);

            for (Method method : bean.getClass().getMethods()) {

                if (method.isAnnotationPresent(OnMessage.class)) {
                    String event = method.getAnnotation(OnMessage.class).value();
                    Class<?> payloadType = resolvePayloadType(method);

                    // always register with Object.class — we do our own deserialization
                    // to avoid netty-socketio's deserializer failing on custom types
                    server.addEventListener(event, Object.class,
                        (client, data, ack) -> {
                            Object converted = (payloadType != null && data != null)
                                ? serializer.deserialize(data, payloadType) : data;
                            handleMessage(bean, method, client, converted, ack, event);
                        });

                    log.info("Registered @OnMessage(\"{}\") → {}.{}", event,
                        bean.getClass().getSimpleName(), method.getName());
                }

                if (method.isAnnotationPresent(OnConnect.class)) {
                    connectHandlers.add(new HandlerMethod(bean, method));
                    log.info("Registered @OnConnect → {}.{}", bean.getClass().getSimpleName(), method.getName());
                }

                if (method.isAnnotationPresent(OnDisconnect.class)) {
                    disconnectHandlers.add(new HandlerMethod(bean, method));
                    log.info("Registered @OnDisconnect → {}.{}", bean.getClass().getSimpleName(), method.getName());
                }

                if (method.isAnnotationPresent(OnError.class)) {
                    errorHandlers.add(new HandlerMethod(bean, method));
                    log.info("Registered @OnError → {}.{}", bean.getClass().getSimpleName(), method.getName());
                }
            }
        }

        // Wire connect: auth → session → Spring event → @OnConnect handlers
        server.addConnectListener(client -> {
            // 1. Authenticate
            SocketAuthContext authContext = null;
            try {
                authContext = authHook.authenticate(client.getHandshakeData());
            } catch (SocketAuthException e) {
                log.warn("Auth rejected for session {}: {}", client.getSessionId(), e.getMessage());
                client.disconnect();
                metrics.incrementAuthFailure();
                return;
            }

            // 2. Create and save session
            SocketSession session = SocketSession.builder()
                .sessionId(client.getSessionId().toString())
                .userId(authContext != null ? authContext.getUserId() : null)
                .remoteAddress(client.getRemoteAddress() != null
                    ? client.getRemoteAddress().toString() : "unknown")
                .connectedAt(Instant.now())
                .build();

            if (authContext != null) {
                authContext.getClaims().forEach(session::setAttribute);
            }

            sessionManager.save(session);
            metrics.incrementConnect();
            eventPublisher.publishEvent(new SocketClientConnectedEvent(this, session));
            log.debug("Client connected: sessionId={} userId={}", session.getSessionId(), session.getUserId());

            // 3. Call @OnConnect handlers
            for (HandlerMethod hm : connectHandlers) {
                invokeLifecycleMethod(hm.bean, hm.method, client, session, errorHandlers);
            }
        });

        // Wire disconnect: session cleanup → Spring event → @OnDisconnect handlers
        server.addDisconnectListener(client -> {
            String sessionId = client.getSessionId().toString();
            SocketSession session = sessionManager.findBySessionId(sessionId).orElse(null);

            sessionManager.removeBySessionId(sessionId);
            metrics.incrementDisconnect();

            if (session != null) {
                eventPublisher.publishEvent(new SocketClientDisconnectedEvent(this, session));
            }

            log.debug("Client disconnected: sessionId={}", sessionId);

            for (HandlerMethod hm : disconnectHandlers) {
                invokeLifecycleMethod(hm.bean, hm.method, client, session, errorHandlers);
            }
        });
    }

    private void handleMessage(Object bean, Method method, SocketIOClient client,
                                Object data, AckRequest ack, String event) {
        String spanName = "socket." + event;
        tracing.startSpan(spanName, () -> {
            try {
                sessionManager.findBySessionId(client.getSessionId().toString())
                    .ifPresent(SocketSession::refreshActivity);

                metrics.incrementMessageReceived(event);
                invokeWithPayload(bean, method, client, data, ack);
            } catch (Exception e) {
                metrics.incrementError(event);
                log.error("Error handling event={} in {}.{}: {}",
                    event, bean.getClass().getSimpleName(), method.getName(), e.getMessage(), e);
            }
        });
    }

    private void invokeLifecycleMethod(Object bean, Method method, SocketIOClient client,
                                        SocketSession session, List<HandlerMethod> errorHandlers) {
        try {
            Class<?>[] params = method.getParameterTypes();
            if (params.length == 0) {
                method.invoke(bean);
            } else if (params.length == 1) {
                method.invoke(bean, client);
            } else {
                method.invoke(bean, client, session);
            }
        } catch (Exception e) {
            log.error("Error in lifecycle handler {}.{}: {}",
                bean.getClass().getSimpleName(), method.getName(), e.getMessage(), e);
            invokeErrorHandlers(errorHandlers, client, "connect/disconnect", e);
        }
    }

    private void invokeWithPayload(Object bean, Method method,
                                    SocketIOClient client, Object data, AckRequest ack) throws Exception {
        Class<?>[] params = method.getParameterTypes();
        if (params.length == 0) {
            method.invoke(bean);
        } else if (params.length == 1) {
            // single param could be the client (no-payload handler) or a payload
            method.invoke(bean, params[0] == SocketIOClient.class ? client : data);
        } else if (params.length == 2) {
            method.invoke(bean, params[0] == SocketIOClient.class ? client : data,
                              params[0] == SocketIOClient.class ? data : ack);
        } else if (params.length == 3) {
            method.invoke(bean, client, data, ack);
        }
    }

    private void invokeErrorHandlers(List<HandlerMethod> handlers, SocketIOClient client,
                                      String event, Exception ex) {
        for (HandlerMethod hm : handlers) {
            try {
                hm.method.invoke(hm.bean, client, event, ex);
            } catch (Exception e) {
                log.error("Error in @OnError handler: {}", e.getMessage());
            }
        }
    }

    private Class<?> resolvePayloadType(Method method) {
        for (Parameter param : method.getParameters()) {
            Class<?> type = param.getType();
            if (!type.equals(SocketIOClient.class) && !type.equals(AckRequest.class)) {
                return type;
            }
        }
        return null;
    }

    private record HandlerMethod(Object bean, Method method) {}
}
