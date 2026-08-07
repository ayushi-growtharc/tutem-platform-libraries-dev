package com.tutem.platform.socket.dispatcher;

import com.corundumstudio.socketio.AckRequest;
import com.corundumstudio.socketio.HandshakeData;
import com.corundumstudio.socketio.SocketIOClient;
import com.corundumstudio.socketio.SocketIOServer;
import com.corundumstudio.socketio.listener.ConnectListener;
import com.corundumstudio.socketio.listener.DataListener;
import com.corundumstudio.socketio.listener.DisconnectListener;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tutem.platform.socket.annotations.OnConnect;
import com.tutem.platform.socket.annotations.OnDisconnect;
import com.tutem.platform.socket.annotations.OnError;
import com.tutem.platform.socket.annotations.OnMessage;
import com.tutem.platform.socket.authentication.SocketAuthContext;
import com.tutem.platform.socket.authentication.SocketAuthenticationHook;
import com.tutem.platform.socket.event.SocketClientConnectedEvent;
import com.tutem.platform.socket.event.SocketClientDisconnectedEvent;
import com.tutem.platform.socket.exception.SocketAuthException;
import com.tutem.platform.socket.exception.SocketErrorHandler;
import com.tutem.platform.socket.exception.SocketException;
import com.tutem.platform.socket.metrics.SocketMetrics;
import com.tutem.platform.socket.properties.SocketProperties;
import com.tutem.platform.socket.serialization.JsonSocketMessageSerializer;
import com.tutem.platform.socket.serialization.SocketMessageSerializer;
import com.tutem.platform.socket.session.SessionManager;
import com.tutem.platform.socket.session.SocketSession;
import com.tutem.platform.socket.tracing.NoOpSocketTracing;
import org.aopalliance.intercept.MethodInterceptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ConfigurableApplicationContext;

import java.lang.reflect.Proxy;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Scanning, registration, parameter binding, auth and error-path contract of
 * {@link MessageHandlerRegistry}.
 */
class MessageHandlerRegistryTest {

    private SocketIOServer server;
    private ApplicationContext context;
    private SessionManager sessionManager;
    private ApplicationEventPublisher eventPublisher;
    private SocketAuthenticationHook authHook;
    private SocketMetrics metrics;
    private SocketMessageSerializer serializer;
    private SocketErrorHandler errorHandler;
    private SocketProperties properties;

    private SocketIOClient client;
    private AckRequest ack;
    private UUID clientId;

    @BeforeEach
    void setUp() {
        server = mock(SocketIOServer.class);
        context = mock(ApplicationContext.class);
        sessionManager = mock(SessionManager.class);
        eventPublisher = mock(ApplicationEventPublisher.class);
        authHook = mock(SocketAuthenticationHook.class);
        metrics = mock(SocketMetrics.class);
        errorHandler = mock(SocketErrorHandler.class);
        properties = new SocketProperties();
        // app.socket.auth.enabled now defaults to true. Most tests here exercise the
        // anonymous baseline (no session in the store, no identity from the hook), so the
        // default is switched off explicitly; the auth tests below turn it back on.
        properties.getAuth().setEnabled(false);

        // A real Jackson serializer behind a mock, so individual tests can make
        // deserialization blow up without hand-rolling conversion for the happy path.
        SocketMessageSerializer realSerializer = new JsonSocketMessageSerializer(new ObjectMapper());
        serializer = mock(SocketMessageSerializer.class);
        doAnswer(invocation -> realSerializer.deserialize(invocation.getArgument(0),
            invocation.<Class<Object>>getArgument(1)))
            .when(serializer).deserialize(any(), ArgumentMatchers.<Class<Object>>any());

        // Optional#empty rather than Mockito's null default: the SDK calls .orElse(null).
        when(sessionManager.findBySessionId(anyString())).thenReturn(Optional.empty());

        clientId = UUID.randomUUID();
        client = mock(SocketIOClient.class);
        when(client.getSessionId()).thenReturn(clientId);
        when(client.getHandshakeData()).thenReturn(new HandshakeData());
        ack = mock(AckRequest.class);
    }

    private MessageHandlerRegistry newRegistry(ApplicationContext applicationContext) {
        return new MessageHandlerRegistry(server, applicationContext, sessionManager, eventPublisher,
            authHook, metrics, new NoOpSocketTracing(), serializer, errorHandler, properties);
    }

    /** Stubs {@code context} as if the given beans were the whole bean factory. */
    private MessageHandlerRegistry registryWith(Map<String, Object> beans) {
        when(context.getBeanDefinitionNames()).thenReturn(beans.keySet().toArray(new String[0]));
        beans.forEach((name, bean) -> {
            doReturn(bean.getClass()).when(context).getType(name);
            when(context.getBean(name)).thenReturn(bean);
        });
        return newRegistry(context);
    }

    private MessageHandlerRegistry registryWith(String name, Object bean) {
        Map<String, Object> beans = new LinkedHashMap<>();
        beans.put(name, bean);
        return registryWith(beans);
    }

    private DataListener<Object> listenerFor(String event) {
        @SuppressWarnings("unchecked")
        ArgumentCaptor<DataListener<Object>> captor =
            ArgumentCaptor.forClass(DataListener.class);
        verify(server).addEventListener(eq(event), eq(Object.class), captor.capture());
        return captor.getValue();
    }

    private ConnectListener connectListener() {
        ArgumentCaptor<ConnectListener> captor = ArgumentCaptor.forClass(ConnectListener.class);
        verify(server).addConnectListener(captor.capture());
        return captor.getValue();
    }

    private DisconnectListener disconnectListener() {
        ArgumentCaptor<DisconnectListener> captor = ArgumentCaptor.forClass(DisconnectListener.class);
        verify(server).addDisconnectListener(captor.capture());
        return captor.getValue();
    }

    /** Asserts no listener was registered for {@code event} under ANY payload class. */
    private void assertNoListenerFor(String event) {
        verify(server, never()).addEventListener(eq(event),
            ArgumentMatchers.<Class<Object>>any(), ArgumentMatchers.<DataListener<Object>>any());
    }

    // ------------------------------------------------------------------- registration

    @Test
    @DisplayName("registerAll registers @OnMessage with Object.class, not the payload type")
    void registerAll_annotatedBean_registersEventsAsObjectClass() {
        registryWith("sampleHandler", new SampleHandler()).registerAll();

        verify(server).addConnectListener(any());
        verify(server).addDisconnectListener(any());
        verify(server).addEventListener(eq("joinRoom"), eq(Object.class), any());
        verify(server).addEventListener(eq("ping"), eq(Object.class), any());
        // The SDK deserializes itself; netty must never be handed the payload type.
        verify(server, never()).addEventListener(eq("joinRoom"), eq(Payload.class), any());
    }

    @Test
    @DisplayName("scanning never instantiates a bean whose type declares no socket annotation")
    void registerAll_beanWithoutSocketAnnotations_isNeverInstantiated() {
        when(context.getBeanDefinitionNames())
            .thenReturn(new String[]{"expensiveBean", "sampleHandler"});
        doReturn(String.class).when(context).getType("expensiveBean");
        doReturn(SampleHandler.class).when(context).getType("sampleHandler");
        when(context.getBean("sampleHandler")).thenReturn(new SampleHandler());

        newRegistry(context).registerAll();

        verify(context, never()).getBean("expensiveBean");
        verify(server).addEventListener(eq("joinRoom"), eq(Object.class), any());
    }

    @Test
    @DisplayName("lazy, prototype and abstract bean definitions are skipped without instantiation")
    void registerAll_lazyPrototypeOrAbstractDefinition_isSkipped() {
        ConfigurableApplicationContext configurableContext =
            mock(ConfigurableApplicationContext.class);
        ConfigurableListableBeanFactory beanFactory = mock(ConfigurableListableBeanFactory.class);
        when(configurableContext.getBeanFactory()).thenReturn(beanFactory);
        when(configurableContext.getBeanDefinitionNames()).thenReturn(
            new String[]{"lazyHandler", "prototypeHandler", "abstractHandler", "eagerHandler"});

        RootBeanDefinition lazy = new RootBeanDefinition(SampleHandler.class);
        lazy.setLazyInit(true);
        RootBeanDefinition prototype = new RootBeanDefinition(SampleHandler.class);
        prototype.setScope(BeanDefinition.SCOPE_PROTOTYPE);
        RootBeanDefinition abstractDefinition = new RootBeanDefinition();
        abstractDefinition.setAbstract(true);
        RootBeanDefinition eager = new RootBeanDefinition(SampleHandler.class);

        when(beanFactory.getBeanDefinition("lazyHandler")).thenReturn(lazy);
        when(beanFactory.getBeanDefinition("prototypeHandler")).thenReturn(prototype);
        when(beanFactory.getBeanDefinition("abstractHandler")).thenReturn(abstractDefinition);
        when(beanFactory.getBeanDefinition("eagerHandler")).thenReturn(eager);
        doReturn(SampleHandler.class).when(configurableContext).getType("eagerHandler");
        when(configurableContext.getBean("eagerHandler")).thenReturn(new SampleHandler());

        newRegistry(configurableContext).registerAll();

        verify(configurableContext, never()).getBean("lazyHandler");
        verify(configurableContext, never()).getBean("prototypeHandler");
        verify(configurableContext, never()).getBean("abstractHandler");
        verify(configurableContext, never()).getType("lazyHandler");
        // Only the single eager bean's handler was registered.
        verify(server).addEventListener(eq("joinRoom"), eq(Object.class), any());
    }

    @Test
    @DisplayName("a bean whose type cannot be resolved does not abort registration of later beans")
    void registerAll_beanTypeResolutionThrows_stillRegistersRemainingHandlers() {
        when(context.getBeanDefinitionNames())
            .thenReturn(new String[]{"brokenBean", "sampleHandler"});
        doThrow(new IllegalStateException("cannot resolve bean class"))
            .when(context).getType("brokenBean");
        doReturn(SampleHandler.class).when(context).getType("sampleHandler");
        when(context.getBean("sampleHandler")).thenReturn(new SampleHandler());

        newRegistry(context).registerAll();

        verify(server).addEventListener(eq("joinRoom"), eq(Object.class), any());
        verify(server).addEventListener(eq("ping"), eq(Object.class), any());
        verify(server).addConnectListener(any());
    }

    @Test
    @DisplayName("an ambiguous two-payload @OnMessage and a zero-arg @OnMessage are not registered")
    void registerAll_unbindableSignatures_areRejectedAtStartup() {
        registryWith("badHandler", new BadSignatureHandler()).registerAll();

        assertNoListenerFor("ambiguousPayload");
        assertNoListenerFor("zeroArg");
        assertNoListenerFor("  ");
        // The well-formed handler on the same bean is still registered - and it is the ONLY one.
        verify(server).addEventListener(eq("valid"), eq(Object.class), any());
        verify(server, times(1)).addEventListener(anyString(), eq(Object.class),
            ArgumentMatchers.<DataListener<Object>>any());
    }

    @Test
    @DisplayName("a second registerAll() registers nothing (netty never removes listeners)")
    void registerAll_calledTwice_doesNotDoubleRegister() {
        // Regression guard: netty-socketio's Namespace appends to ConcurrentLinkedQueues and
        // SocketIOServer.stop() never clears them, so a Lifecycle stop/start used to fire
        // every @OnMessage twice and save each session twice.
        MessageHandlerRegistry registry = registryWith("sampleHandler", new SampleHandler());

        registry.registerAll();
        registry.registerAll();

        verify(server, times(1)).addConnectListener(any());
        verify(server, times(1)).addDisconnectListener(any());
        verify(server, times(1)).addEventListener(eq("joinRoom"), eq(Object.class), any());
        verify(server, times(1)).addEventListener(eq("ping"), eq(Object.class), any());
    }

    @Test
    @DisplayName("a non-Spring JDK proxy hiding its target is reported, not silently skipped")
    void registerAll_opaqueJdkProxyBean_isSkippedWithoutRegisteringAnything() {
        // A plain java.lang.reflect.Proxy (no Advised): the target's annotations are invisible,
        // so nothing can be registered. The SDK must still log a WARN naming the bean - this
        // test pins the behaviour (no registration, no exception, scanning continues).
        OpaqueProxyTarget target = new OpaqueProxyTarget();
        Object proxy = Proxy.newProxyInstance(getClass().getClassLoader(),
            new Class<?>[]{OpaqueProxyApi.class},
            (p, method, args) -> method.invoke(target, args));
        assertThat(Proxy.isProxyClass(proxy.getClass())).isTrue();
        assertThat(proxy).isNotInstanceOf(org.springframework.aop.framework.Advised.class);

        Map<String, Object> beans = new LinkedHashMap<>();
        beans.put("opaqueProxy", proxy);
        beans.put("sampleHandler", new SampleHandler());
        registryWith(beans).registerAll();

        assertNoListenerFor("opaque");
        // Scanning was not aborted by the proxy bean.
        verify(server).addEventListener(eq("joinRoom"), eq(Object.class), any());
    }

    // ------------------------------------------------------ lifecycle signature validation

    @Test
    @DisplayName("@OnConnect/@OnDisconnect accept SocketIOClient and SocketSession in any order")
    void registerAll_lifecycleSignatures_acceptedShapesAreAllInvoked() {
        when(authHook.authenticate(any())).thenReturn(SocketAuthContext.of("u1", "RIDER"));
        LifecycleShapeHandler handler = new LifecycleShapeHandler();
        registryWith("lifecycleShapeHandler", handler).registerAll();

        connectListener().onConnect(client);

        assertThat(handler.noArgs).isEqualTo(1);
        assertThat(handler.clientOnly).isSameAs(client);
        assertThat(handler.sessionOnly).isNotNull();
        assertThat(handler.reversedClient).isSameAs(client);
        assertThat(handler.reversedSession).isNotNull();

        disconnectListener().onDisconnect(client);
        assertThat(handler.disconnectClient).isSameAs(client);
    }

    @Test
    @DisplayName("an @OnConnect with an unbindable parameter is rejected at registration")
    void registerAll_lifecycleWithUnbindableParameter_isNotRegistered() {
        // Object used to receive the session by accident; String used to receive a silent null
        // and NPE inside consumer code on the very first connection.
        when(authHook.authenticate(any())).thenReturn(SocketAuthContext.of("u1", "RIDER"));
        BadLifecycleHandler handler = new BadLifecycleHandler();
        registryWith("badLifecycleHandler", handler).registerAll();

        connectListener().onConnect(client);
        disconnectListener().onDisconnect(client);

        assertThat(handler.objectParamCalls).isZero();
        assertThat(handler.stringParamCalls).isZero();
        assertThat(handler.disconnectCalls).isZero();
        // A rejected handler must not be reported through the error pipeline either: it was
        // never registered, so no connection-time failure happens at all.
        verify(errorHandler, never()).handle(any(), anyString(), any());
    }

    // --------------------------------------------------------------- parameter binding

    @Test
    @DisplayName("(SocketIOClient, Payload, AckRequest) binds all three positionally")
    void handleMessage_canonicalSignature_bindsClientPayloadAndAck() throws Exception {
        BindingHandler handler = new BindingHandler();
        registryWith("bindingHandler", handler).registerAll();

        listenerFor("canonical").onData(client, Map.of("room", "r1", "count", 3), ack);

        assertThat(handler.invoked).isEqualTo("canonical");
        assertThat(handler.client).isSameAs(client);
        assertThat(handler.ack).isSameAs(ack);
        assertThat(handler.payload).isNotNull();
        assertThat(handler.payload.room).isEqualTo("r1");
        assertThat(handler.payload.count).isEqualTo(3);
    }

    @Test
    @DisplayName("(Payload) binds the deserialized payload only")
    void handleMessage_payloadOnlySignature_bindsPayload() throws Exception {
        BindingHandler handler = new BindingHandler();
        registryWith("bindingHandler", handler).registerAll();

        listenerFor("payloadOnly").onData(client, Map.of("room", "r2", "count", 7), ack);

        assertThat(handler.invoked).isEqualTo("payloadOnly");
        assertThat(handler.payload.room).isEqualTo("r2");
        assertThat(handler.client).isNull();
        assertThat(handler.ack).isNull();
    }

    @Test
    @DisplayName("(Payload, SocketIOClient) - non-canonical order binds by type, not position")
    void handleMessage_reversedSignature_bindsByType() throws Exception {
        BindingHandler handler = new BindingHandler();
        registryWith("bindingHandler", handler).registerAll();

        listenerFor("reversed").onData(client, Map.of("room", "r3", "count", 1), ack);

        assertThat(handler.invoked).isEqualTo("reversed");
        assertThat(handler.payload.room).isEqualTo("r3");
        assertThat(handler.client).isSameAs(client);
    }

    @Test
    @DisplayName("(SocketIOClient, AckRequest) with no payload parameter is bound and invoked")
    void handleMessage_noPayloadSignature_bindsClientAndAck() throws Exception {
        BindingHandler handler = new BindingHandler();
        registryWith("bindingHandler", handler).registerAll();

        listenerFor("noPayload").onData(client, Map.of("ignored", true), ack);

        assertThat(handler.invoked).isEqualTo("noPayload");
        assertThat(handler.client).isSameAs(client);
        assertThat(handler.ack).isSameAs(ack);
        assertThat(handler.payload).isNull();
        // No payload type declared -> the serializer must not be consulted at all.
        verify(serializer, never()).deserialize(any(), ArgumentMatchers.<Class<Object>>any());
    }

    @Test
    @DisplayName("an inbound message refreshes the session and increments the received counter")
    void handleMessage_knownSession_refreshesActivityAndCountsMessage() throws Exception {
        SocketSession session = SocketSession.builder()
            .sessionId(clientId.toString()).userId("u1").build();
        when(sessionManager.findBySessionId(clientId.toString())).thenReturn(Optional.of(session));

        BindingHandler handler = new BindingHandler();
        registryWith("bindingHandler", handler).registerAll();

        listenerFor("payloadOnly").onData(client, Map.of("room", "r", "count", 0), ack);

        assertThat(handler.invoked).isEqualTo("payloadOnly");
        verify(metrics).incrementMessageReceived("payloadOnly");
    }

    // ------------------------------------------------------------------------ auth

    @Test
    @DisplayName("a plain RuntimeException from the auth hook fails closed")
    void onConnect_authHookThrowsRuntimeException_disconnectsAndSavesNoSession() {
        when(authHook.authenticate(any())).thenThrow(new RuntimeException("jwt library exploded"));
        registryWith("sampleHandler", new SampleHandler()).registerAll();

        connectListener().onConnect(client);

        verify(client).disconnect();
        verify(metrics).incrementAuthFailure();
        verify(sessionManager, never()).save(any());
        verify(metrics, never()).incrementConnect();
        verify(eventPublisher, never()).publishEvent(any(SocketClientConnectedEvent.class));
    }

    @Test
    @DisplayName("a SocketAuthException from the auth hook fails closed")
    void onConnect_authHookThrowsSocketAuthException_disconnectsAndSavesNoSession() {
        when(authHook.authenticate(any())).thenThrow(new SocketAuthException("token missing"));
        registryWith("sampleHandler", new SampleHandler()).registerAll();

        connectListener().onConnect(client);

        verify(client).disconnect();
        verify(metrics).incrementAuthFailure();
        verify(sessionManager, never()).save(any());
    }

    @Test
    @DisplayName("auth enabled + hook returns a null userId -> connection rejected")
    void onConnect_authEnabledAndNullUserId_isRejected() {
        properties.getAuth().setEnabled(true);
        when(authHook.authenticate(any())).thenReturn(SocketAuthContext.anonymous());
        registryWith("sampleHandler", new SampleHandler()).registerAll();

        connectListener().onConnect(client);

        verify(client).disconnect();
        verify(metrics).incrementAuthFailure();
        verify(sessionManager, never()).save(any());
    }

    @Test
    @DisplayName("auth enabled + hook returns a blank userId -> connection rejected")
    void onConnect_authEnabledAndBlankUserId_isRejected() {
        properties.getAuth().setEnabled(true);
        when(authHook.authenticate(any())).thenReturn(SocketAuthContext.of("   ", "DRIVER"));
        registryWith("sampleHandler", new SampleHandler()).registerAll();

        connectListener().onConnect(client);

        verify(client).disconnect();
        verify(metrics).incrementAuthFailure();
        verify(sessionManager, never()).save(any());
    }

    @Test
    @DisplayName("SocketAuthContext.of(userId, role) -> the saved session carries the role")
    void onConnect_authContextWithRole_savesSessionWithRole() {
        properties.getAuth().setEnabled(true);
        when(authHook.authenticate(any())).thenReturn(SocketAuthContext.of("user-42", "DRIVER"));
        registryWith("sampleHandler", new SampleHandler()).registerAll();

        connectListener().onConnect(client);

        ArgumentCaptor<SocketSession> captor = ArgumentCaptor.forClass(SocketSession.class);
        verify(sessionManager).save(captor.capture());
        SocketSession saved = captor.getValue();
        assertThat(saved.getSessionId()).isEqualTo(clientId.toString());
        assertThat(saved.getUserId()).isEqualTo("user-42");
        assertThat(saved.getRole()).isEqualTo("DRIVER");
        assertThat(saved.getLastActiveAt()).isNotNull();
        verify(metrics).incrementConnect();
        verify(eventPublisher).publishEvent(any(SocketClientConnectedEvent.class));
        verify(client, never()).disconnect();
    }

    @Test
    @DisplayName("a throwing SocketClientConnectedEvent listener still runs @OnConnect handlers")
    void onConnect_eventListenerThrows_onConnectHandlersStillRun() {
        // publishEvent is synchronous, so a consumer @EventListener runs on this thread. If its
        // failure escaped, netty-socketio would swallow it and every @OnConnect handler after
        // this point would be skipped — the consumer's room joins would never happen and the
        // client would still look healthy.
        doThrow(new IllegalStateException("listener blew up"))
            .when(eventPublisher).publishEvent(any(SocketClientConnectedEvent.class));
        SampleHandler handler = new SampleHandler();
        registryWith("sampleHandler", handler).registerAll();

        connectListener().onConnect(client);

        assertThat(handler.connectCalls).isEqualTo(1);
        assertThat(handler.connectedSession).isNotNull();
        verify(client, never()).disconnect();
    }

    @Test
    @DisplayName("a throwing SocketClientDisconnectedEvent listener still runs @OnDisconnect")
    void onDisconnect_eventListenerThrows_onDisconnectHandlersStillRun() {
        doThrow(new IllegalStateException("listener blew up"))
            .when(eventPublisher).publishEvent(any(SocketClientDisconnectedEvent.class));
        SampleHandler handler = new SampleHandler();
        registryWith("sampleHandler", handler).registerAll();
        when(sessionManager.findBySessionId(clientId.toString()))
            .thenReturn(Optional.of(SocketSession.builder()
                .sessionId(clientId.toString()).userId("user-42").build()));

        disconnectListener().onDisconnect(client);

        assertThat(handler.disconnectCalls).isEqualTo(1);
    }

    @Test
    @DisplayName("auth-context claims land in the session attributes")
    void onConnect_authContextWithClaims_copiesClaimsIntoSessionAttributes() {
        when(authHook.authenticate(any())).thenReturn(SocketAuthContext.builder()
            .userId("user-7").role("RIDER").claims(Map.<String, Object>of("tenant", "acme")).build());
        registryWith("sampleHandler", new SampleHandler()).registerAll();

        connectListener().onConnect(client);

        ArgumentCaptor<SocketSession> captor = ArgumentCaptor.forClass(SocketSession.class);
        verify(sessionManager).save(captor.capture());
        assertThat(captor.getValue().getAttribute("tenant")).isEqualTo("acme");
    }

    @Test
    @DisplayName("@OnConnect handlers run after the session is saved")
    void onConnect_lifecycleHandlers_areInvokedWithClientAndSession() {
        when(authHook.authenticate(any())).thenReturn(SocketAuthContext.of("u1", "RIDER"));
        SampleHandler handler = new SampleHandler();
        registryWith("sampleHandler", handler).registerAll();

        connectListener().onConnect(client);

        assertThat(handler.connectCalls).isEqualTo(1);
        assertThat(handler.connectedSession).isNotNull();
        assertThat(handler.connectedSession.getUserId()).isEqualTo("u1");
    }

    @Test
    @DisplayName("@OnDisconnect removes the session, counts the disconnect and invokes handlers")
    void onDisconnect_knownSession_removesAndNotifies() {
        SocketSession session = SocketSession.builder()
            .sessionId(clientId.toString()).userId("u1").build();
        when(sessionManager.findBySessionId(clientId.toString())).thenReturn(Optional.of(session));
        SampleHandler handler = new SampleHandler();
        registryWith("sampleHandler", handler).registerAll();

        disconnectListener().onDisconnect(client);

        verify(sessionManager).removeBySessionId(clientId.toString());
        verify(metrics).incrementDisconnect();
        assertThat(handler.disconnectCalls).isEqualTo(1);
    }

    @Test
    @DisplayName("auth enabled + no session for an inbound message -> handler skipped, client dropped")
    void handleMessage_authEnabledAndNoSession_skipsHandlerAndDisconnects() throws Exception {
        properties.getAuth().setEnabled(true);
        BindingHandler handler = new BindingHandler();
        registryWith("bindingHandler", handler).registerAll();

        listenerFor("canonical").onData(client, Map.of("room", "r", "count", 1), ack);

        assertThat(handler.invoked).isNull();
        verify(client).disconnect();
        verify(metrics, never()).incrementMessageReceived(anyString());
    }

    // ----------------------------------------------------------------- error pipeline

    @Test
    @DisplayName("a throwing handler routes through metrics, every @OnError and the SocketErrorHandler")
    void handleMessage_handlerThrows_runsFullErrorPipeline() throws Exception {
        ThrowingHandler throwing = new ThrowingHandler();
        SecondErrorObserver observer = new SecondErrorObserver();
        Map<String, Object> beans = new LinkedHashMap<>();
        beans.put("throwingHandler", throwing);
        beans.put("secondErrorObserver", observer);
        registryWith(beans).registerAll();

        listenerFor("boom").onData(client, Map.of("room", "r", "count", 1), ack);

        verify(metrics).incrementError("boom");
        assertThat(throwing.errorEvent).isEqualTo("boom");
        assertThat(throwing.errorException).isInstanceOf(IllegalStateException.class);
        assertThat(observer.errorEvent).isEqualTo("boom");
        verify(errorHandler).handle(eq(client), eq("boom"), any(IllegalStateException.class));
    }

    @Test
    @DisplayName("a deserialization failure routes through the same error pipeline")
    void handleMessage_serializerThrows_runsFullErrorPipeline() throws Exception {
        doThrow(new SocketException("Failed to deserialize socket payload"))
            .when(serializer).deserialize(any(), ArgumentMatchers.<Class<Object>>any());

        ThrowingHandler throwing = new ThrowingHandler();
        registryWith("throwingHandler", throwing).registerAll();

        listenerFor("ok").onData(client, Map.of("room", "r"), ack);

        verify(metrics).incrementError("ok");
        assertThat(throwing.errorEvent).isEqualTo("ok");
        assertThat(throwing.errorException).isInstanceOf(SocketException.class);
        verify(errorHandler).handle(eq(client), eq("ok"), any(SocketException.class));
        assertThat(throwing.okCalls).isZero();
    }

    @Test
    @DisplayName("an @OnError handler that itself throws does not stop the SocketErrorHandler")
    void handleMessage_onErrorHandlerThrows_stillCallsSocketErrorHandler() throws Exception {
        registryWith("hostileErrorHandler", new HostileErrorObserver()).registerAll();

        listenerFor("boom").onData(client, Map.of("room", "r"), ack);

        verify(errorHandler).handle(eq(client), eq("boom"), any(IllegalStateException.class));
    }

    @Test
    @DisplayName("an @OnError with the wrong signature is rejected and never invoked")
    void registerAll_malformedOnError_isNotRegistered() throws Exception {
        MalformedErrorObserver observer = new MalformedErrorObserver();
        registryWith("malformedErrorObserver", observer).registerAll();

        listenerFor("boom").onData(client, Map.of("room", "r"), ack);

        assertThat(observer.calls).isZero();
        verify(errorHandler).handle(eq(client), eq("boom"), any(Exception.class));
    }

    // ------------------------------------------------------------------- proxied bean

    @Test
    @DisplayName("a JDK dynamic proxy handler (Spring AOP) is discovered and invoked through the proxy")
    void registerAll_jdkDynamicProxyBean_isDiscoveredAndAdviceRuns() throws Exception {
        // Built with Spring's ProxyFactory so the proxy is exactly what @Transactional /
        // @Async produce: a JDK dynamic proxy implementing the interface plus Advised, which
        // is what lets AopProxyUtils.ultimateTargetClass() see the annotated target class.
        ProxiedHandlerImpl target = new ProxiedHandlerImpl();
        AtomicInteger adviceCalls = new AtomicInteger();
        ProxyFactory proxyFactory = new ProxyFactory(target);
        proxyFactory.setInterfaces(ProxiedHandlerApi.class);
        proxyFactory.addAdvice((MethodInterceptor) invocation -> {
            adviceCalls.incrementAndGet();
            return invocation.proceed();
        });
        Object proxy = proxyFactory.getProxy();
        assertThat(Proxy.isProxyClass(proxy.getClass()))
            .as("test fixture must be a JDK dynamic proxy").isTrue();

        registryWith("proxiedHandler", proxy).registerAll();

        listenerFor("proxied").onData(client, Map.of("room", "proxy-room", "count", 9), ack);

        assertThat(target.invocations).isEqualTo(1);
        assertThat(target.received).isNotNull();
        assertThat(target.received.room).isEqualTo("proxy-room");
        assertThat(adviceCalls.get())
            .as("invocation must go through the proxy so advice still runs").isEqualTo(1);
    }

    // ---------------------------------------------------------------------- fixtures

    public static class Payload {
        public String room;
        public int count;
    }

    static class SampleHandler {
        int connectCalls;
        int disconnectCalls;
        SocketSession connectedSession;

        @OnConnect
        public void onConnect(SocketIOClient client, SocketSession session) {
            connectCalls++;
            connectedSession = session;
        }

        @OnDisconnect
        public void onDisconnect(SocketIOClient client, SocketSession session) {
            disconnectCalls++;
        }

        @OnMessage("joinRoom")
        public void onJoin(SocketIOClient client, Payload payload, AckRequest ack) {
        }

        @OnMessage("ping")
        public void onPing(SocketIOClient client) {
        }
    }

    static class BindingHandler {
        String invoked;
        SocketIOClient client;
        Payload payload;
        AckRequest ack;

        @OnMessage("canonical")
        public void canonical(SocketIOClient client, Payload payload, AckRequest ack) {
            this.invoked = "canonical";
            this.client = client;
            this.payload = payload;
            this.ack = ack;
        }

        @OnMessage("payloadOnly")
        public void payloadOnly(Payload payload) {
            this.invoked = "payloadOnly";
            this.payload = payload;
        }

        @OnMessage("reversed")
        public void reversed(Payload payload, SocketIOClient client) {
            this.invoked = "reversed";
            this.payload = payload;
            this.client = client;
        }

        @OnMessage("noPayload")
        public void noPayload(SocketIOClient client, AckRequest ack) {
            this.invoked = "noPayload";
            this.client = client;
            this.ack = ack;
        }
    }

    static class BadSignatureHandler {
        @OnMessage("ambiguousPayload")
        public void twoPayloads(Payload first, Payload second) {
        }

        @OnMessage("zeroArg")
        public void zeroArg() {
        }

        @OnMessage("  ")
        public void blankEventName(Payload payload) {
        }

        @OnMessage("valid")
        public void valid(Payload payload) {
        }
    }

    static class ThrowingHandler {
        int okCalls;
        String errorEvent;
        Exception errorException;

        @OnMessage("boom")
        public void boom(Payload payload) {
            throw new IllegalStateException("handler failed");
        }

        @OnMessage("ok")
        public void ok(Payload payload) {
            okCalls++;
        }

        @OnError
        public void onError(SocketIOClient client, String event, Exception ex) {
            errorEvent = event;
            errorException = ex;
        }
    }

    static class SecondErrorObserver {
        String errorEvent;

        @OnError
        public void onError(SocketIOClient client, String event, Exception ex) {
            errorEvent = event;
        }
    }

    static class HostileErrorObserver {
        @OnMessage("boom")
        public void boom(Payload payload) {
            throw new IllegalStateException("handler failed");
        }

        @OnError
        public void onError(SocketIOClient client, String event, Exception ex) {
            throw new IllegalStateException("@OnError is broken too");
        }
    }

    static class MalformedErrorObserver {
        int calls;

        @OnMessage("boom")
        public void boom(Payload payload) {
            throw new IllegalStateException("handler failed");
        }

        /** Wrong signature: no event name parameter. */
        @OnError
        public void onError(SocketIOClient client, Exception ex) {
            calls++;
        }
    }

    /** Every legal lifecycle shape: none, one, both, and reversed. */
    static class LifecycleShapeHandler {
        int noArgs;
        SocketIOClient clientOnly;
        SocketSession sessionOnly;
        SocketIOClient reversedClient;
        SocketSession reversedSession;
        SocketIOClient disconnectClient;

        @OnConnect
        public void connectedNoArgs() {
            noArgs++;
        }

        @OnConnect
        public void connectedClientOnly(SocketIOClient client) {
            clientOnly = client;
        }

        @OnConnect
        public void connectedSessionOnly(SocketSession session) {
            sessionOnly = session;
        }

        @OnConnect
        public void connectedReversed(SocketSession session, SocketIOClient client) {
            reversedSession = session;
            reversedClient = client;
        }

        @OnDisconnect
        public void disconnected(SocketIOClient client) {
            disconnectClient = client;
        }
    }

    /** Every rejected lifecycle shape. */
    static class BadLifecycleHandler {
        int objectParamCalls;
        int stringParamCalls;
        int disconnectCalls;

        /** Object used to silently receive the SocketSession. */
        @OnConnect
        public void connectedWithObject(Object session) {
            objectParamCalls++;
        }

        /** Unrecognised type: used to be bound to null. */
        @OnConnect
        public void connectedWithString(SocketIOClient client, String nothing) {
            stringParamCalls++;
        }

        @OnDisconnect
        public void disconnectedWithPayload(Payload payload) {
            disconnectCalls++;
        }
    }

    public interface OpaqueProxyApi {
        void handleOpaque(Payload payload);
    }

    /** The annotation lives on the target class, which a non-Advised proxy hides. */
    public static class OpaqueProxyTarget implements OpaqueProxyApi {
        @OnMessage("opaque")
        @Override
        public void handleOpaque(Payload payload) {
        }
    }

    public interface ProxiedHandlerApi {
        void handleProxied(Payload payload);
    }

    public static class ProxiedHandlerImpl implements ProxiedHandlerApi {
        Payload received;
        int invocations;

        @OnMessage("proxied")
        @Override
        public void handleProxied(Payload payload) {
            received = payload;
            invocations++;
        }
    }
}
