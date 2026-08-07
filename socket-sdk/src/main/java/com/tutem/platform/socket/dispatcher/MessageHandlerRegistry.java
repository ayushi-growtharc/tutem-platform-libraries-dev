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
import com.tutem.platform.socket.event.SocketClientConnectedEvent;
import com.tutem.platform.socket.event.SocketClientDisconnectedEvent;
import com.tutem.platform.socket.exception.SocketErrorHandler;
import com.tutem.platform.socket.metrics.SocketMetrics;
import com.tutem.platform.socket.properties.SocketProperties;
import com.tutem.platform.socket.serialization.SocketMessageSerializer;
import com.tutem.platform.socket.session.SessionManager;
import com.tutem.platform.socket.session.SocketSession;
import com.tutem.platform.socket.tracing.SocketTracing;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Scans Spring beans for socket annotations and registers them with the server.
 * Also owns the full lifecycle per connection: auth -> session -> events -> disconnect.
 *
 * <p>Scanning rules (deliberate):
 * <ul>
 *   <li>Only non-abstract, non-lazy, singleton bean definitions are considered, and the bean
 *       <em>type</em> is resolved before any instance is created — scanning must never
 *       force-instantiate {@code @Lazy} beans or blow up on request/prototype-scoped ones.</li>
 *   <li>Annotations are looked up on the AOP <em>target</em> class, so handlers behind a JDK
 *       dynamic proxy ({@code @Transactional}, {@code @Async}, {@code @Validated}) are found;
 *       invocation still goes through the proxy so the advice runs.</li>
 *   <li>Handler signatures are validated once, at startup. An ambiguous or unbindable
 *       {@code @OnMessage}, {@code @OnConnect}, {@code @OnDisconnect} or {@code @OnError}
 *       method is reported as an ERROR and NOT registered, rather than failing per message
 *       at traffic rate (or handing a handler a silent {@code null}).</li>
 *   <li>{@link #registerAll()} is single-shot: netty-socketio's {@code Namespace} appends
 *       listeners to queues that {@code SocketIOServer.stop()} does not clear, so a second
 *       registration would double every handler invocation.</li>
 * </ul>
 */
@Slf4j
@RequiredArgsConstructor
public class MessageHandlerRegistry {

    private static final String CONNECT_EVENT = "connect";
    private static final String DISCONNECT_EVENT = "disconnect";

    private final SocketIOServer server;
    private final ApplicationContext applicationContext;
    private final SessionManager sessionManager;
    private final ApplicationEventPublisher eventPublisher;
    private final SocketAuthenticationHook authHook;
    private final SocketMetrics metrics;
    private final SocketTracing tracing;
    private final SocketMessageSerializer serializer;
    private final SocketErrorHandler errorHandler;
    private final SocketProperties properties;

    /** Event names already reported as "client asked for an ack the handler cannot send". */
    private final Set<String> ackGapReported = ConcurrentHashMap.newKeySet();

    /** Guards against a second {@link #registerAll()}; see the method javadoc. */
    private final AtomicBoolean registered = new AtomicBoolean(false);

    /**
     * Populated during {@link #registerAll()}; read from Netty threads afterwards.
     *
     * <p>Copy-on-write rather than {@code ArrayList}: registration normally finishes before
     * {@code server.start()} creates the worker threads, which would supply the
     * happens-before for free — but a consumer may inject an already-started
     * {@code SocketIOServer} of its own (the bean is {@code @ConditionalOnMissingBean}), and
     * then registration races live connections over these very lists.
     */
    private final List<LifecycleHandler> connectHandlers = new CopyOnWriteArrayList<>();
    private final List<LifecycleHandler> disconnectHandlers = new CopyOnWriteArrayList<>();
    private final List<LifecycleHandler> errorHandlers = new CopyOnWriteArrayList<>();

    // ------------------------------------------------------------------ scanning

    /**
     * Scans every eligible bean and registers its handlers with the server. Called once,
     * from {@code SocketServerLifecycle.start()}, before the port opens.
     *
     * <p>Registration is <strong>single-shot</strong>. netty-socketio's {@code Namespace}
     * appends every listener to a {@code ConcurrentLinkedQueue} and
     * {@code SocketIOServer.stop()} never clears those queues, so a
     * {@code context.stop(); context.start()} cycle would leave the previous listeners in
     * place: each {@code @OnMessage} would fire twice and each connect would save the session
     * twice. Clearing this class's own lists cannot undo that, so a repeat call logs at WARN
     * and does nothing. Restarting the socket server requires a fresh application context.
     */
    public void registerAll() {
        if (!registered.compareAndSet(false, true)) {
            log.warn("registerAll() called again - socket handlers are already registered and "
                + "re-registration is not supported (netty-socketio never removes listeners, so "
                + "every handler would fire twice). Ignoring. Restarting the socket server "
                + "requires a new application context, not Lifecycle stop/start.");
            return;
        }

        ConfigurableListableBeanFactory beanFactory =
            (applicationContext instanceof ConfigurableApplicationContext configurableContext)
                ? configurableContext.getBeanFactory() : null;

        for (String beanName : applicationContext.getBeanDefinitionNames()) {
            try {
                scanBean(beanName, beanFactory);
            } catch (Exception e) {
                // One unresolvable bean must never abort application startup.
                log.warn("Skipped bean '{}' while scanning for socket handlers: {}",
                    beanName, e.toString());
            }
        }

        registerConnectListener();
        registerDisconnectListener();
    }

    private void scanBean(String beanName, ConfigurableListableBeanFactory beanFactory) {
        if (!isEligible(beanName, beanFactory)) {
            return;
        }

        Class<?> declaredType = applicationContext.getType(beanName);
        if (declaredType == null || !mayDeclareHandlers(declaredType)) {
            return;
        }

        // Only now — for a type that actually declares (or may declare) handlers — do we
        // touch the instance.
        Object bean = applicationContext.getBean(beanName);
        Class<?> targetClass = AopProxyUtils.ultimateTargetClass(bean);

        Map<Method, HandlerAnnotations> annotated = MethodIntrospector.selectMethods(
            targetClass, (MethodIntrospector.MetadataLookup<HandlerAnnotations>)
                MessageHandlerRegistry::findHandlerAnnotations);

        if (annotated.isEmpty() && isOpaqueJdkProxy(bean)) {
            // AopProxyUtils.ultimateTargetClass() can only see through Spring's own proxies
            // (they implement Advised). A hand-rolled or third-party java.lang.reflect.Proxy
            // hides its target, so any annotation on the target class is invisible here and
            // the bean would be skipped without a trace.
            log.warn("Bean '{}' is a JDK dynamic proxy ({}) that does not implement "
                    + "org.springframework.aop.framework.Advised, so its target class cannot be "
                    + "resolved and socket annotations on that target are invisible. If it is "
                    + "meant to declare socket handlers, declare them on an interface the proxy "
                    + "exposes, or register the unproxied bean.",
                beanName, bean.getClass().getName());
            return;
        }

        for (Map.Entry<Method, HandlerAnnotations> entry : annotated.entrySet()) {
            try {
                // Resolve the method that is actually invocable on the (possibly proxied) bean,
                // so JDK-proxy advice is not bypassed.
                Method invocable = AopUtils.selectInvocableMethod(entry.getKey(), bean.getClass());
                ReflectionUtils.makeAccessible(invocable);
                registerMethod(bean, targetClass, invocable, entry.getValue());
            } catch (Exception e) {
                // e.g. an annotated method that the exposed proxy type does not expose:
                // report it and keep registering the bean's remaining handlers.
                log.error("Could not register socket handler {}.{}: {}",
                    targetClass.getName(), entry.getKey().getName(), e.toString());
            }
        }
    }

    /**
     * Skips bean definitions that must not be instantiated during scanning:
     * abstract templates, {@code @Lazy} beans (instantiating them defeats the point),
     * and non-singleton scopes (prototype / request / session — {@code getBean} outside
     * an active scope throws).
     */
    private boolean isEligible(String beanName, ConfigurableListableBeanFactory beanFactory) {
        if (beanFactory == null) {
            // Non-configurable context (rare; mostly tests): the best we can do is refuse
            // beans explicitly declared prototype.
            return !applicationContext.isPrototype(beanName);
        }
        BeanDefinition definition = beanFactory.getBeanDefinition(beanName);
        if (definition.isAbstract() || definition.isLazyInit() || !definition.isSingleton()) {
            if (log.isDebugEnabled()) {
                log.debug("Not scanning bean '{}' for socket handlers (abstract={}, lazy={}, scope={})",
                    beanName, definition.isAbstract(), definition.isLazyInit(), definition.getScope());
            }
            return false;
        }
        return true;
    }

    /**
     * True for a {@code java.lang.reflect.Proxy} that is not one of Spring's: Spring's JDK
     * proxies always implement {@link Advised}, which is what lets
     * {@code AopProxyUtils.ultimateTargetClass()} reach the annotated target class.
     */
    private static boolean isOpaqueJdkProxy(Object bean) {
        return Proxy.isProxyClass(bean.getClass()) && !(bean instanceof Advised);
    }

    /**
     * True when the resolved bean type declares a socket annotation, or when the type alone
     * cannot answer the question (interface-based JDK proxy) and the instance must be inspected.
     */
    private boolean mayDeclareHandlers(Class<?> declaredType) {
        if (declaredType.isInterface() || Proxy.isProxyClass(declaredType)) {
            return true;
        }
        Class<?> userClass = ClassUtils.getUserClass(declaredType);
        for (Method method : ReflectionUtils.getAllDeclaredMethods(userClass)) {
            if (findHandlerAnnotations(method) != null) {
                return true;
            }
        }
        return false;
    }

    private static HandlerAnnotations findHandlerAnnotations(Method method) {
        OnMessage onMessage = AnnotatedElementUtils.findMergedAnnotation(method, OnMessage.class);
        boolean onConnect = AnnotatedElementUtils.hasAnnotation(method, OnConnect.class);
        boolean onDisconnect = AnnotatedElementUtils.hasAnnotation(method, OnDisconnect.class);
        boolean onError = AnnotatedElementUtils.hasAnnotation(method, OnError.class);
        if (onMessage == null && !onConnect && !onDisconnect && !onError) {
            return null;
        }
        return new HandlerAnnotations(onMessage, onConnect, onDisconnect, onError);
    }

    private void registerMethod(Object bean, Class<?> targetClass, Method method,
                               HandlerAnnotations annotations) {
        if (Modifier.isStatic(method.getModifiers())) {
            log.error("Ignoring socket handler {}.{}: static methods are not supported",
                targetClass.getName(), method.getName());
            return;
        }

        if (annotations.onMessage() != null) {
            registerMessageHandler(bean, targetClass, method, annotations.onMessage().value());
        }
        if (annotations.onConnect()) {
            registerLifecycleHandler(connectHandlers, "@OnConnect", bean, targetClass, method);
        }
        if (annotations.onDisconnect()) {
            registerLifecycleHandler(disconnectHandlers, "@OnDisconnect", bean, targetClass, method);
        }
        if (annotations.onError()) {
            if (!isValidErrorHandler(method)) {
                log.error("Ignoring @OnError {}.{}: expected signature "
                        + "(SocketIOClient, String event, Exception ex)",
                    targetClass.getName(), method.getName());
            } else {
                errorHandlers.add(new LifecycleHandler(bean, method));
                log.info("Registered @OnError -> {}.{}", targetClass.getSimpleName(), method.getName());
            }
        }
    }

    /**
     * Validates and registers an {@code @OnConnect} / {@code @OnDisconnect} method.
     *
     * <p>Validation happens here, at startup, for the same reason {@code @OnMessage} binding
     * does: the only values the SDK can supply to a lifecycle method are the
     * {@link SocketIOClient} and the {@link SocketSession}. Any other parameter type used to
     * be bound to {@code null} silently, which surfaced as an NPE inside consumer code on the
     * first connection, and an {@code Object}-typed parameter used to receive the session by
     * accident. Legal shapes are any combination (in any order) of {@code SocketIOClient} and
     * {@code SocketSession}, including none of them.
     */
    private void registerLifecycleHandler(List<LifecycleHandler> target, String annotationName,
                                          Object bean, Class<?> targetClass, Method method) {
        for (Class<?> parameterType : method.getParameterTypes()) {
            if (parameterType != SocketIOClient.class && parameterType != SocketSession.class) {
                log.error("Ignoring {} {}.{}: parameter of type {} cannot be bound - a {} method may "
                        + "only declare SocketIOClient and/or SocketSession parameters, in any "
                        + "order, or no parameters at all",
                    annotationName, targetClass.getName(), method.getName(),
                    parameterType.getName(), annotationName);
                return;
            }
        }
        target.add(new LifecycleHandler(bean, method));
        log.info("Registered {} -> {}.{}", annotationName, targetClass.getSimpleName(), method.getName());
    }

    private void registerMessageHandler(Object bean, Class<?> targetClass, Method method, String event) {
        if (event == null || event.isBlank()) {
            log.error("Ignoring @OnMessage on {}.{}: event name must not be blank",
                targetClass.getName(), method.getName());
            return;
        }

        MessageHandler handler = buildBinding(bean, targetClass, method, event);
        if (handler == null) {
            return; // already reported at ERROR
        }

        // Always registered as Object.class: the SDK does its own deserialization so that
        // netty-socketio's deserializer cannot fail opaquely on custom payload types, and
        // so conversion failures land in this class's error pipeline.
        server.addEventListener(event, Object.class,
            (client, data, ack) -> handleMessage(handler, client, data, ack));

        log.info("Registered @OnMessage(\"{}\") -> {}.{}({})", event,
            targetClass.getSimpleName(), method.getName(),
            handler.payloadType() != null ? handler.payloadType().getSimpleName() : "no payload");
    }

    /**
     * Builds the positional argument plan once, at startup. Any parameter order works:
     * each parameter is filled from its own type. Returns {@code null} (after logging an
     * ERROR) when the signature cannot be bound unambiguously.
     */
    private MessageHandler buildBinding(Object bean, Class<?> targetClass, Method method, String event) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length == 0) {
            log.error("Ignoring @OnMessage(\"{}\") on {}.{}: no parameters to bind — declare at least "
                    + "one of SocketIOClient, AckRequest or a payload type",
                event, targetClass.getName(), method.getName());
            return null;
        }

        ArgumentSource[] plan = new ArgumentSource[parameterTypes.length];
        Class<?> payloadType = null;
        int payloadCount = 0;
        boolean acceptsAck = false;

        for (int i = 0; i < parameterTypes.length; i++) {
            Class<?> parameterType = parameterTypes[i];
            if (SocketIOClient.class.isAssignableFrom(parameterType)) {
                plan[i] = ArgumentSource.CLIENT;
            } else if (AckRequest.class.isAssignableFrom(parameterType)) {
                plan[i] = ArgumentSource.ACK;
                acceptsAck = true;
            } else {
                plan[i] = ArgumentSource.PAYLOAD;
                payloadType = parameterType;
                payloadCount++;
            }
        }

        if (payloadCount > 1) {
            log.error("Ignoring @OnMessage(\"{}\") on {}.{}: {} parameters are neither SocketIOClient "
                    + "nor AckRequest, so the payload parameter is ambiguous — a handler may declare "
                    + "at most one payload parameter",
                event, targetClass.getName(), method.getName(), payloadCount);
            return null;
        }

        return new MessageHandler(bean, method, targetClass, event, plan, payloadType, acceptsAck);
    }

    private boolean isValidErrorHandler(Method method) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        return parameterTypes.length == 3
            && SocketIOClient.class.isAssignableFrom(parameterTypes[0])
            && parameterTypes[1] == String.class
            && parameterTypes[2].isAssignableFrom(Exception.class);
    }

    // ------------------------------------------------------------------ lifecycle wiring

    private void registerConnectListener() {
        server.addConnectListener(client -> {
            boolean authRequired = isAuthRequired();

            // 1. Authenticate — fail CLOSED on anything that goes wrong, including
            //    RuntimeExceptions from a JWT library (expired/malformed token, NPE).
            SocketAuthContext authContext;
            try {
                authContext = authHook.authenticate(client.getHandshakeData());
            } catch (Exception e) {
                log.warn("Auth rejected for session {}: {}", client.getSessionId(), e.toString());
                metrics.incrementAuthFailure();
                safeDisconnect(client);
                return;
            }

            if (authRequired && !hasUsableIdentity(authContext)) {
                log.warn("Auth rejected for session {}: authentication hook returned no userId "
                    + "while app.socket.auth.enabled=true", client.getSessionId());
                metrics.incrementAuthFailure();
                safeDisconnect(client);
                return;
            }

            UUID clientId = client.getSessionId();
            if (clientId == null) {
                log.warn("Rejecting connection with no session id");
                safeDisconnect(client);
                return;
            }

            // 2. Create and save the session
            SocketSession session = SocketSession.builder()
                .sessionId(clientId.toString())
                .userId(authContext != null ? authContext.getUserId() : null)
                .role(authContext != null ? authContext.getRole() : null)
                .remoteAddress(client.getRemoteAddress() != null
                    ? client.getRemoteAddress().toString() : "unknown")
                .connectedAt(Instant.now())
                .build();

            if (authContext != null && authContext.getClaims() != null) {
                authContext.getClaims().forEach(session::setAttribute);
            }
            // A freshly connected client must never look stale to the heartbeat.
            session.refreshActivity();

            sessionManager.save(session);
            metrics.incrementConnect();
            publishQuietly(new SocketClientConnectedEvent(this, session), CONNECT_EVENT);
            if (log.isDebugEnabled()) {
                log.debug("Client connected: sessionId={} userId={} role={}",
                    session.getSessionId(), session.getUserId(), session.getRole());
            }

            // 3. @OnConnect handlers
            for (LifecycleHandler handler : connectHandlers) {
                invokeLifecycleMethod(handler, client, session, CONNECT_EVENT);
            }
        });
    }

    private void registerDisconnectListener() {
        server.addDisconnectListener(client -> {
            UUID clientId = client.getSessionId();
            String sessionId = clientId != null ? clientId.toString() : null;

            SocketSession session = (sessionId != null)
                ? sessionManager.findBySessionId(sessionId).orElse(null) : null;

            if (sessionId != null) {
                sessionManager.removeBySessionId(sessionId);
            }
            metrics.incrementDisconnect();

            if (session != null) {
                publishQuietly(new SocketClientDisconnectedEvent(this, session),
                    DISCONNECT_EVENT);
            }
            if (log.isDebugEnabled()) {
                log.debug("Client disconnected: sessionId={}", sessionId);
            }

            for (LifecycleHandler handler : disconnectHandlers) {
                invokeLifecycleMethod(handler, client, session, DISCONNECT_EVENT);
            }
        });
    }

    // ------------------------------------------------------------------ dispatch

    private void handleMessage(MessageHandler handler, SocketIOClient client,
                              Object data, AckRequest ack) {
        String event = handler.event();
        // startSpan itself is outside the inner try, so a tracer failure (misconfigured
        // exporter, corrupt context) would skip the handler AND the error pipeline, surfacing
        // only in netty-socketio's log. Route it through handleFailure like any other fault.
        try {
            tracing.startSpan("socket." + event, () -> dispatch(handler, client, data, ack));
        } catch (Exception e) {
            handleFailure(client, event, e);
        }
    }

    /** The traced body of {@link #handleMessage}; runs inside the span. */
    private void dispatch(MessageHandler handler, SocketIOClient client,
                          Object data, AckRequest ack) {
        String event = handler.event();
        try {
            SocketSession session = resolveSession(client);
            if (session == null && isAuthRequired()) {
                if (log.isDebugEnabled()) {
                    log.debug("Dropping event={} from sessionId={}: no authenticated session",
                        event, client.getSessionId());
                }
                safeDisconnect(client);
                return;
            }
            if (session != null) {
                session.refreshActivity();
            }

            metrics.incrementMessageReceived(event);
            reportAckGapIfAny(handler, ack, event);

            Object payload = (handler.payloadType() != null)
                ? serializer.deserialize(data, handler.payloadType()) : data;

            invoke(handler, client, payload, ack);
        } catch (Exception e) {
            handleFailure(client, event, e);
        }
    }

    /**
     * Publishes a lifecycle event without letting a listener failure abort the caller.
     *
     * <p>{@code publishEvent} is synchronous, so a consumer {@code @EventListener} runs on the
     * Netty thread and its exception would propagate out of the connect/disconnect listener.
     * netty-socketio catches that and hands it to its own exception listener, which only logs
     * — so every {@code @OnConnect}/{@code @OnDisconnect} handler after this point would be
     * skipped, the SDK's {@code @OnError} pipeline bypassed, and the client left looking
     * healthy while the consumer's room joins never happened.
     */
    private void publishQuietly(ApplicationEvent event, String phase) {
        try {
            eventPublisher.publishEvent(event);
        } catch (RuntimeException e) {
            log.warn("A {} listener failed for {}: {}", phase,
                event.getClass().getSimpleName(), e.getMessage(), e);
        }
    }

    private SocketSession resolveSession(SocketIOClient client) {
        UUID clientId = client.getSessionId();
        return (clientId == null) ? null
            : sessionManager.findBySessionId(clientId.toString()).orElse(null);
    }

    private void invoke(MessageHandler handler, SocketIOClient client,
                        Object payload, AckRequest ack) throws Exception {
        ArgumentSource[] plan = handler.plan();
        Object[] args = new Object[plan.length];
        for (int i = 0; i < plan.length; i++) {
            args[i] = switch (plan[i]) {
                case CLIENT -> client;
                case ACK -> ack;
                case PAYLOAD -> payload;
            };
        }
        try {
            handler.method().invoke(handler.bean(), args);
        } catch (InvocationTargetException e) {
            throw unwrap(e);
        }
    }

    private void invokeLifecycleMethod(LifecycleHandler handler, SocketIOClient client,
                                       SocketSession session, String event) {
        try {
            // Every parameter type was validated at registration time, so it is exactly
            // SocketIOClient or exactly SocketSession — nothing can silently bind to null.
            Class<?>[] parameterTypes = handler.method().getParameterTypes();
            Object[] args = new Object[parameterTypes.length];
            for (int i = 0; i < parameterTypes.length; i++) {
                args[i] = (parameterTypes[i] == SocketIOClient.class) ? client : session;
            }
            try {
                handler.method().invoke(handler.bean(), args);
            } catch (InvocationTargetException e) {
                throw unwrap(e);
            }
        } catch (Exception e) {
            // Same pipeline as message failures, with the real event name.
            handleFailure(client, event, e);
        }
    }

    /**
     * The single error path for handler failures: metrics, then every {@code @OnError}
     * handler, then the {@link SocketErrorHandler} (which by default notifies the client).
     */
    private void handleFailure(SocketIOClient client, String event, Exception exception) {
        try {
            metrics.incrementError(event);
        } catch (Exception e) {
            log.warn("Failed to record error metric for event={}: {}", event, e.toString());
        }

        for (LifecycleHandler handler : errorHandlers) {
            try {
                handler.method().invoke(handler.bean(), client, event, exception);
            } catch (Exception e) {
                Throwable cause = (e instanceof InvocationTargetException ite && ite.getCause() != null)
                    ? ite.getCause() : e;
                log.error("@OnError handler {}.{} itself failed: {}",
                    handler.bean().getClass().getSimpleName(), handler.method().getName(),
                    cause.toString(), cause);
            }
        }

        try {
            errorHandler.handle(client, event, exception);
        } catch (Exception e) {
            log.error("SocketErrorHandler failed while handling event={}: {}", event, e.toString(), e);
        }
    }

    /**
     * Acks are never sent automatically (that would change message semantics), but a client
     * waiting for an ack that can never arrive is otherwise invisible — report it once per event.
     */
    private void reportAckGapIfAny(MessageHandler handler, AckRequest ack, String event) {
        if (handler.acceptsAck() || ack == null) {
            return;
        }
        try {
            if (ack.isAckRequested() && ackGapReported.add(event)) {
                log.debug("Client requested an ack for event={} but {}.{} declares no AckRequest "
                        + "parameter — no ack will be sent",
                    event, handler.targetClass().getSimpleName(), handler.method().getName());
            }
        } catch (Exception e) {
            log.debug("Could not inspect ack state for event={}: {}", event, e.toString());
        }
    }

    // ------------------------------------------------------------------ helpers

    private boolean isAuthRequired() {
        return properties != null && properties.getAuth() != null && properties.getAuth().isEnabled();
    }

    private boolean hasUsableIdentity(SocketAuthContext authContext) {
        // A session with no userId can never be reached by sendToUser, so with auth enabled
        // it is treated as a failed authentication.
        return authContext != null
            && authContext.getUserId() != null
            && !authContext.getUserId().isBlank();
    }

    private void safeDisconnect(SocketIOClient client) {
        try {
            client.disconnect();
        } catch (Exception e) {
            log.debug("Failed to disconnect client {}: {}", client.getSessionId(), e.toString());
        }
    }

    private static Exception unwrap(InvocationTargetException e) {
        Throwable cause = e.getCause();
        if (cause instanceof Exception exception) {
            return exception;
        }
        if (cause != null) {
            return new IllegalStateException(cause.getMessage(), cause);
        }
        return e;
    }

    private enum ArgumentSource { CLIENT, ACK, PAYLOAD }

    private record HandlerAnnotations(OnMessage onMessage, boolean onConnect,
                                      boolean onDisconnect, boolean onError) {}

    private record LifecycleHandler(Object bean, Method method) {}

    private record MessageHandler(Object bean, Method method, Class<?> targetClass, String event,
                                  ArgumentSource[] plan, Class<?> payloadType, boolean acceptsAck) {}
}
