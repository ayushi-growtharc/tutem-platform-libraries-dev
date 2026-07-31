# socket-sdk — Architecture

Maintainer-facing document. For consumer usage see [README.md](README.md).

`socket-sdk` is a Spring Boot auto-configuration around
[netty-socketio](https://github.com/mrniko/netty-socketio) 2.0.12. It turns annotated Spring bean
methods into Socket.IO event listeners, owns the session model, and ties the netty server to the
Spring application lifecycle.

- Module: `tutem-platform-libraries/socket-sdk`, `include("socket-sdk")` in `settings.gradle.kts`
- `com.tutem.platform:socket-sdk:1.0-SNAPSHOT`, Java 21 toolchain, Gradle Kotlin DSL
- Root package: `com.tutem.platform.socket`

---

## 1. Design goals and layering

### Goals

1. **Zero surprise on the classpath.** Adding the jar must never open a port. Activation is an
   explicit `@EnableSocket` on the consumer's application class.
2. **Every component replaceable.** All twelve beans the auto-configuration declares are
   `@ConditionalOnMissingBean`, so a consumer can swap any single piece by declaring its own bean.
3. **Optional integrations stay optional.** Actuator and micrometer-tracing are `compileOnly`. A
   consumer without them must start cleanly, not fail on a `NoClassDefFoundError`. This is why
   `SocketMetrics` and `SocketTracing` are SDK-owned interfaces that mention no Micrometer type,
   with `NoOp*` and `Micrometer*` implementations.
4. **Fail fast at startup, not at traffic rate.** Handler signatures are validated once, during
   scanning. An ambiguous or unbindable `@OnMessage` method, or an `@OnConnect`/`@OnDisconnect`
   method with a parameter the SDK cannot supply, is logged as an `ERROR` and not registered.
5. **Secure by default, fail closed on authentication.** `app.socket.auth.enabled` defaults to
   `true`, so a consumer with no `SocketAuthenticationHook` fails at context refresh rather than
   opening an anonymous port; `false` is an explicit, `WARN`-logged local-dev opt-out. Any exception
   from the auth hook disconnects the client.
6. **One error path.** Handler exceptions, deserialization failures and lifecycle-handler failures
   all converge on the same pipeline. Scoped to `Exception`: an `Error` from a handler is not
   caught — see diagram 2c.
7. **Correct lifecycle ordering.** The socket port opens after the HTTP server and closes as part
   of ordered shutdown, via `SmartLifecycle`.

### Explicit non-goal: transport abstraction

The SDK deliberately does **not** hide netty-socketio. `SocketIOClient`, `AckRequest` and
`HandshakeData` appear in `SocketAuthenticationHook`, `SocketErrorHandler` and every handler
signature, which is why `netty-socketio` is declared `api` rather than `implementation`.

Two other dependencies are `api` for the same honesty reason: `spring-context`, because the four
event types extend `ApplicationEvent` and consumers write `@EventListener` methods for them; and
`jackson-databind`, because `JsonSocketMessageSerializer(ObjectMapper)` is a public constructor a
consumer calls. `spring-boot-autoconfigure` and `slf4j-api` stay `implementation`; actuator and
micrometer-tracing stay `compileOnly` and are the consumer's job to add.

Rationale: a facade over `SocketIOClient` would have to re-implement rooms, acks, handshake access
and per-client send operations, and every non-trivial handler needs at least one of them. The cost
is real and should be stated rather than hidden — swapping transports would break consumers, and
consuming services compile against netty-socketio. What the SDK provides instead is the Spring
wiring, session model, auth path, error pipeline and lifecycle ordering.

### Layering

```text
consumer beans  @OnConnect / @OnMessage / @OnDisconnect / @OnError, SocketAuthenticationHook
      ▲ invoked by                                   ▲ called by
      │                                              │
dispatch layer  MessageHandlerRegistry (inbound)  MessageDispatcher / ConnectionManager (outbound)
      │
support layer   SessionManager · SocketMessageSerializer · SocketMetrics · SocketTracing
                SocketErrorHandler · HeartbeatManager
      │
lifecycle       SocketServerLifecycle (SmartLifecycle)
      │
transport       SocketIOServer (netty-socketio)
      ▲
config          SocketAutoConfiguration ← SocketMarker ← @EnableSocket · SocketProperties
```

Dependencies point downward and inward. Nothing in the support layer knows about the dispatch
layer; `MessageHandlerRegistry` is the only class that both reads the session store and invokes
consumer code.

---

## 2. Diagrams

### 2a. Components and dependencies

```mermaid
graph TD
    subgraph consumer["Consumer service"]
        APP["SpringBootApplication + EnableSocket"]
        HANDLERS["Annotated handler beans"]
        HOOK["SocketAuthenticationHook impl"]
    end

    subgraph config["config / annotations / properties"]
        ENABLE["EnableSocket annotation"]
        MARKERCFG["SocketMarkerConfiguration"]
        MARKER["SocketMarker"]
        AUTOCFG["SocketAutoConfiguration"]
        PROPS["SocketProperties"]
    end

    subgraph lifecycle["lifecycle"]
        LC["SocketServerLifecycle (SmartLifecycle)"]
    end

    subgraph dispatcher["dispatcher"]
        REG["MessageHandlerRegistry"]
        DISP["MessageDispatcher"]
    end

    subgraph support["support"]
        SESS["SessionManager / InMemorySessionManager"]
        SER["SocketMessageSerializer"]
        MET["SocketMetrics"]
        TRC["SocketTracing"]
        ERR["SocketErrorHandler"]
        HB["HeartbeatManager"]
        CONN["ConnectionManager"]
        EVT["Spring events"]
    end

    SRV["SocketIOServer (netty-socketio)"]

    APP --> ENABLE
    ENABLE -->|@Import| MARKERCFG
    MARKERCFG --> MARKER
    MARKER -.->|@ConditionalOnBean| AUTOCFG
    PROPS --> AUTOCFG
    AUTOCFG --> SRV
    AUTOCFG --> LC
    AUTOCFG --> REG
    AUTOCFG --> DISP
    AUTOCFG --> SESS
    AUTOCFG --> SER
    AUTOCFG --> MET
    AUTOCFG --> TRC
    AUTOCFG --> ERR
    AUTOCFG --> HB
    AUTOCFG --> CONN

    LC --> REG
    LC --> SRV
    REG --> SRV
    REG --> SESS
    REG --> SER
    REG --> MET
    REG --> TRC
    REG --> ERR
    REG --> EVT
    REG --> HOOK
    REG --> HANDLERS
    DISP --> SESS
    DISP --> SRV
    CONN --> SESS
    CONN --> SRV
    HB --> SESS
    HB --> SRV
```

Note the two edges that bypass `SessionManager` entirely: `MessageDispatcher.sendToRoom` /
`broadcast` and `ConnectionManager.getClientsInRoom` talk to `SocketIOServer` room and broadcast
operations directly. That is the crux of section 6.

### 2b. Connect flow

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant N as SocketIOServer (netty)
    participant R as MessageHandlerRegistry
    participant H as SocketAuthenticationHook
    participant S as SessionManager
    participant M as SocketMetrics
    participant P as ApplicationEventPublisher
    participant U as OnConnect handlers

    C->>N: Socket.IO handshake (query params, headers)
    N->>R: connect listener (netty worker thread)
    R->>H: authenticate(client.getHandshakeData())

    alt hook throws any exception
        H--xR: exception
        R->>M: incrementAuthFailure()
        R->>C: disconnect()
    else auth.enabled and userId null/blank, or context null
        H-->>R: unusable SocketAuthContext
        R->>M: incrementAuthFailure()
        R->>C: disconnect()
    else no netty session id
        R->>C: disconnect()
    else success
        H-->>R: SocketAuthContext(userId, role, claims)
        R->>R: build SocketSession(sessionId, userId, role, remoteAddress, connectedAt)
        R->>R: copy claims into session attributes; refreshActivity()
        R->>S: save(session)
        R->>M: incrementConnect()
        R->>P: publish SocketClientConnectedEvent
        R->>U: invoke each @OnConnect (client, session bound by type)
        Note over R,U: a handler exception routes to the error pipeline with event="connect"
    end
```

Disconnect is the mirror image: look the session up by id, remove it from the store,
`incrementDisconnect()`, publish `SocketClientDisconnectedEvent` (only if a session was found),
then invoke `@OnDisconnect` handlers with a possibly-`null` session.

### 2c. Inbound message flow, including the error path

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant N as SocketIOServer
    participant R as MessageHandlerRegistry
    participant T as SocketTracing
    participant S as SessionManager
    participant M as SocketMetrics
    participant Z as SocketMessageSerializer
    participant U as Handler method
    participant E as OnError handlers
    participant EH as SocketErrorHandler

    C->>N: emit("joinRoom", data[, ack])
    N->>R: listener registered as Object.class
    R->>T: startSpan("socket.joinRoom", work)
    T->>R: run work (in span, or directly for NoOp)

    R->>S: findBySessionId(client.getSessionId())
    alt no session and auth.enabled
        R->>C: disconnect()
        Note over R: event dropped, no metrics recorded
    else proceed
        R->>S: session.refreshActivity() (if session found)
        R->>M: incrementMessageReceived("joinRoom")
        R->>R: log once if client wants an ack but handler takes no AckRequest
        R->>Z: deserialize(raw, payloadType)
        alt conversion fails
            Z--xR: SocketException
        else ok
            Z-->>R: typed payload
            R->>U: invoke with positional plan (CLIENT / ACK / PAYLOAD)
            U--xR: exception (unwrapped from InvocationTargetException)
        end
    end

    opt any exception above
        R->>M: incrementError("joinRoom")
        R->>E: invoke every @OnError(client, event, ex)
        Note over E: a throwing @OnError is logged; the loop continues
        R->>EH: handle(client, event, ex)
        EH->>C: sendEvent("error", {event, message})
    end
```

Three details that matter:

- **`handleMessage` catches `Exception`, not `Throwable`.** An `Error` thrown by a handler —
  `OutOfMemoryError`, `StackOverflowError`, `NoClassDefFoundError`, a failing `assert` — bypasses
  the error pipeline entirely: no `incrementError`, no `@OnError` handlers, no `SocketErrorHandler`,
  no `error` event to the client. It propagates out of the tracing span (which does record it, see
  `MicrometerSocketTracing`) and into netty-socketio's pipeline. This is a deliberate choice —
  catching `Error` and carrying on is usually wrong — but it means "one error path" is a statement
  about `Exception`, not about everything a handler can throw. Do not document the pipeline as
  total. (`HeartbeatManager.runCleanupPass`, by contrast, does catch `Throwable`, because a
  scheduled task that throws is cancelled forever.)
- Listeners are always registered with `Object.class`, never the payload type. netty-socketio's own
  deserializer would fail opaquely on custom types; doing the conversion in
  `SocketMessageSerializer` puts the failure inside the SDK's error pipeline.
- The tracing span wraps the whole body including the `catch`, so the error path is inside the span.
  `MicrometerSocketTracing` records the error on the span and rethrows; `handleFailure` runs before
  that, so a normal handler failure does not escape `startSpan` — only a failure of the error
  pipeline itself would.

### 2d. SmartLifecycle startup and shutdown ordering

```mermaid
sequenceDiagram
    autonumber
    participant SP as Spring context
    participant LP as LifecycleProcessor
    participant WS as WebServerStartStopLifecycle
    participant LC as SocketServerLifecycle
    participant R as MessageHandlerRegistry
    participant N as SocketIOServer

    Note over SP: all singletons created; HeartbeatManager's daemon thread already started<br/>in afterPropertiesSet()
    SP->>LP: finishRefresh()
    LP->>WS: start() — lower phase, HTTP port opens first
    LP->>LC: start() — phase app.socket.startup-phase (default Integer.MAX_VALUE - 1024)
    LC->>LC: running.compareAndSet(false, true) — repeat calls are a no-op
    LC->>R: registerAll()
    R->>R: registered.compareAndSet(false, true) — second call WARNs and returns
    R->>N: scan beans, addEventListener / addConnect+DisconnectListener
    LC->>N: start() — only now are connections accepted
    LC->>SP: publish SocketServerStartedEvent(port)

    Note over LP: shutdown runs phases in DESCENDING order
    SP->>LP: close()
    LP->>LC: stop() — highest phase stops first
    LC->>LC: running.compareAndSet(true, false) — repeat calls are a no-op

    alt server.stop() succeeds
        LC->>N: stop()
        LC->>SP: publish SocketServerStoppedEvent
        Note over LC,SP: a throwing listener is caught and WARNed — shutdown continues
    else server.stop() throws
        N--xLC: RuntimeException
        LC->>LC: log WARN, return — NO SocketServerStoppedEvent
        Note over LC: the port may still be listening, so claiming "stopped" would be a lie
    end

    LP->>WS: stop() — HTTP server stops after the socket port
    SP->>SP: destroy singletons (HeartbeatManager.destroy() shuts the scheduler down)
```

Why `SmartLifecycle` rather than `@EventListener(ContextRefreshedEvent.class)`:
`ContextRefreshedEvent` carries no ordering relative to the web server and can fire more than once
for a single context. `SmartLifecycle` gives an explicit, configurable phase in both directions.

The component is defensive in both directions:

- `start()` guards on an `AtomicBoolean` `compareAndSet`, so a second call does nothing. On a
  `RuntimeException` it resets the flag, logs host/port, and rethrows — so startup still fails
  loudly, but a later `stop()` sees `running == false` and is a clean no-op.
- `stop()` guards the same way and never propagates: a throw during context close would mask the
  real shutdown cause and abort the remaining shutdown work. It does, however, **publish
  `SocketServerStoppedEvent` only on the success path.** If `server.stop()` throws, the failure is
  logged at `WARN` and the method returns without publishing — the port may well still be listening,
  and a listener that reacted to a "stopped" event by, say, deregistering from service discovery or
  flipping a health flag would be acting on a false premise. Publishing it unconditionally (the
  previous behaviour) made the event mean "stop was attempted", which is not what its name says.
  Event publication is separately wrapped, because listener beans may already be destroyed at this
  point in context close.
- Handlers are registered *before* `server.start()`. Reversing that would let a client emit an
  event in the window where nothing is listening.
- `registerAll()` has its own single-shot guard inside `MessageHandlerRegistry` (see section 3), so
  `SocketServerLifecycle`'s idempotence is belt-and-braces rather than the only line of defence.

---

## 3. Package-by-package reference

Legend: **API** = consumers depend on it; **SPI** = consumers implement or replace it;
**internal** = do not depend on it from outside the SDK.

### `annotations`

| Type | Kind | Notes |
|---|---|---|
| `@EnableSocket` | API | `@Import(SocketMarkerConfiguration.class)` on the application class. The only activation switch. |
| `@OnMessage(String value)` | API | Event name is required and must be non-blank. |
| `@OnConnect`, `@OnDisconnect` | API | Method-level, parameters bound by type. |
| `@OnError` | API | Method-level, one fixed signature. |

All are `@Retention(RUNTIME)`, `@Target(METHOD)` (except `@EnableSocket`, which is `TYPE`), and
`@Documented`. `@OnMessage` is looked up with `AnnotatedElementUtils.findMergedAnnotation`, so
meta-annotated composed annotations work.

### `config`

| Type | Kind | Notes |
|---|---|---|
| `SocketAutoConfiguration` | internal | The only entry in `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`. Declares all twelve SDK beans plus two nested optional configurations. |
| `SocketMarker` | internal | Empty activation marker. Explicitly *not* public API. |
| `SocketMarkerConfiguration` | internal | Registers the marker; imported by `@EnableSocket`. |
| `SocketAutoConfiguration.MicrometerMetricsConfiguration` | internal | Package-private nested `@Configuration(proxyBeanMethods = false)`, `@ConditionalOnClass(MeterRegistry.class)` only. Owns the `SocketMetrics` bean whenever micrometer-core is on the classpath, and decides Micrometer-vs-NoOp at instantiation time through an `ObjectProvider`. |
| `SocketAutoConfiguration.MicrometerTracingConfiguration` | internal | Same shape, `@ConditionalOnClass(Tracer.class)`, `ObjectProvider<Tracer>`. |

`resolveTransports(List<String>)` is package-private static and unit-testable: it maps configured
names onto netty-socketio's `Transport` enum case-insensitively, collapses duplicates, and throws
`IllegalArgumentException` naming the legal values for an empty list or an unknown name.

### `properties`

`SocketProperties` (`@ConfigurationProperties("app.socket")`, Lombok `@Data`) with four
`@NestedConfigurationProperty` groups: `AuthProperties`, `MetricsProperties`, `TracingProperties`,
`HeartbeatProperties`. **API** for configuration purposes; the class itself is injected into SDK
beans only. `spring-boot-configuration-processor` runs as an `annotationProcessor`, so IDE
completion and `spring-configuration-metadata.json` come for free. Full key reference lives in the
README.

Two defaults are load-bearing and worth knowing as a maintainer:

- **`auth.enabled` defaults to `true`.** `socketAuthenticationHook(...)` throws
  `IllegalStateException` when it is asked to supply the fallback while auth is on, so a consumer
  with no hook fails at context refresh instead of opening an anonymous port. `false` is the
  explicit local-dev escape hatch and logs a `WARN` naming host and port on every boot.
- **`heartbeat.evict-ghost-sessions` defaults to `true`.** Correct for `InMemorySessionManager`,
  destructive for a shared store — see section 6.

**`origin` is not a security control**, and must not be documented as one. It is passed to
`Configuration.setOrigin`, whose only reader in netty-socketio 2.0.12 is
`EncoderHandler.addOriginHeaders`: it writes the `Access-Control-Allow-Origin` and
`Access-Control-Allow-Credentials` response headers on polling responses.
`AuthorizeHandler.authorize` reads the *request's* `Origin` header, stashes it as a channel
attribute for that handler to echo, and derives the `xdomain` flag on `HandshakeData` from it — it
never compares it to the configured value and never rejects a connection on it. The only handshake
rejections there are the `AuthorizationListener`, an unknown or unconfigured `transport`, and a
missing session id. Browsers also do not apply CORS to the WebSocket transport at all. The
javadoc on the field says all of this; keep it that way.

### `session`

| Type | Kind | Notes |
|---|---|---|
| `SessionManager` | SPI | `findAllByUserId` is the authoritative user lookup. `findByUserId` is a `@Deprecated` default that delegates to `findAllByUserId().stream().findFirst()`. Implementations must be thread-safe. |
| `InMemorySessionManager` | internal (default impl) | Two `ConcurrentHashMap`s. |
| `SocketSession` | API | Lombok `@Getter @Builder`, `@ToString` over the identity fields only. |

`InMemorySessionManager` invariant: `bySessionId` is the single source of truth for liveness;
`userIdToSessionIds` is only an index into it. `save` writes the authoritative map first, so a
session is never indexed before it is reachable. `removeBySessionId` removes exactly one session id
from the user's set (never the whole user entry) via `compute`, so a concurrent reconnect on
another device is not clobbered; the user entry is pruned when its set empties. `isConnected` is
derived from `findAllByUserId`, so it can never be `true` while the lookup returns empty.
`getAll` returns an unmodifiable snapshot copy, because callers iterate while netty threads mutate.

`SocketSession` is deliberately not a Lombok `@Data`: no `equals`/`hashCode` (they would span the
mutable attributes map and break any `Set`/`Map` holding sessions) and no blanket setters (identity
fields are final). `getAttributes()` overrides the generated getter to return an unmodifiable copy.

### `connection`

`ConnectionManager` — **API**. Connection-state queries and room membership. `disconnect(userId)`
fans out over `findAllByUserId`, so all of a user's devices go. `getClientsInRoom` delegates to
`SocketIOServer.getRoomOperations(room).getClients()` and is therefore pod-local. Session ids that
are not parseable UUIDs yield `Optional.empty()` rather than throwing.

### `dispatcher`

| Type | Kind | Notes |
|---|---|---|
| `MessageDispatcher` | API | Outbound sends. `sendToUser` fans out to all of a user's sessions; `sendToSession` checks `isChannelOpen()`; `sendToRoom`/`broadcast` go straight to netty-socketio. A `null` payload sends the event with no data. |
| `MessageHandlerRegistry` | internal | The largest and most subtle class in the SDK. |

`MessageHandlerRegistry` responsibilities, in order:

1. **Scanning** (`registerAll`, called once from `SocketServerLifecycle.start()`). Iterates
   `getBeanDefinitionNames()`. Each bean is wrapped in its own try/catch — one unresolvable bean
   must never abort application startup.

   `registerAll()` is **single-shot**, guarded by an `AtomicBoolean` `compareAndSet`; a repeat call
   logs a `WARN` and returns. This is not defensive decoration: netty-socketio's `Namespace` appends
   every listener to a `ConcurrentLinkedQueue` and `SocketIOServer.stop()` never clears those
   queues, so a `context.stop(); context.start()` cycle would leave the previous listeners attached
   — each `@OnMessage` firing twice, each connect saving the session twice. Clearing this class's
   own `connectHandlers`/`disconnectHandlers`/`errorHandlers` lists cannot undo registrations that
   live inside netty-socketio, so refusing is the only correct answer. **Restart-in-place is
   unsupported**; restarting the socket server requires a fresh application context.
2. **Eligibility** (`isEligible`). Skips `abstract`, `@Lazy` and non-singleton bean definitions:
   instantiating a `@Lazy` bean defeats the point, and `getBean` on a request/prototype-scoped
   definition outside an active scope throws. Without a `ConfigurableListableBeanFactory` (rare,
   mostly tests) it falls back to refusing only explicit prototypes.
3. **Type-first inspection** (`mayDeclareHandlers`). The bean *type* is resolved before any
   instance is created; only a type that declares (or, being an interface/JDK proxy, may declare) a
   handler annotation causes `getBean`.
4. **Proxy handling.** Annotations are read from `AopProxyUtils.ultimateTargetClass(bean)`, so
   handlers behind a Spring AOP proxy are found; the invoked method comes from
   `AopUtils.selectInvocableMethod(method, bean.getClass())`, so `@Transactional`/`@Async` advice
   still runs.

   `ultimateTargetClass` can only see through proxies that implement Spring's `Advised`. A bean
   that is a bare `java.lang.reflect.Proxy` built by hand or by a third-party library hides its
   target, so annotations on the target class are invisible and the bean is not registered.
   `isOpaqueJdkProxy(bean)` (`Proxy.isProxyClass(...) && !(bean instanceof Advised)`) detects
   exactly that case, and when no annotations were found on such a bean it is reported at `WARN`
   naming the bean and its proxy class rather than being skipped in silence. This is a real
   limitation, not a fixed bug — the WARN only makes it diagnosable.
5. **`@OnMessage` binding plan** (`buildBinding`). Builds an `ArgumentSource[]` once per handler:
   `SocketIOClient` → `CLIENT`, `AckRequest` → `ACK`, anything else → `PAYLOAD`. Zero parameters or
   more than one payload parameter is an `ERROR` log and no registration. Static methods and blank
   event names likewise.
6. **`@OnConnect` / `@OnDisconnect` validation** (`registerLifecycleHandler`). The only values the
   SDK can supply to a lifecycle method are the `SocketIOClient` and the `SocketSession`, so the
   parameter list is validated against exactly those two types — any combination, any order, or
   none. Anything else is an `ERROR` log and no registration.

   Note this check uses **reference equality** on the parameter type
   (`parameterType != SocketIOClient.class`), not `isAssignableFrom` as `@OnMessage` binding does.
   A subtype of `SocketIOClient` is therefore rejected here. That is stricter than necessary but
   consistent and cheap to reason about; `invokeLifecycleMethod` still binds with
   `isAssignableFrom`, so the two agree on everything validation lets through.

   Previously there was no validation: an unrecognised parameter type was bound to `null`, which
   surfaced as an NPE inside consumer code on the first connection, and an `Object`-typed parameter
   received the session by accident because `Object.class.isAssignableFrom(SocketSession.class)` is
   true. Both now fail loudly at startup.
6. **Dispatch** (`handleMessage`) and **the error pipeline** (`handleFailure`) — see diagram 2c.
7. **Ack-gap reporting.** Acks are never auto-sent. When a client requests one for an event whose
   handler declares no `AckRequest`, that is logged once per event name, tracked in a
   `ConcurrentHashMap.newKeySet()`.

`ArgumentSource`, `HandlerAnnotations`, `LifecycleHandler` and `MessageHandler` are private nested
enum/records.

### `authentication`

| Type | Kind | Notes |
|---|---|---|
| `SocketAuthenticationHook` | SPI | `SocketAuthContext authenticate(HandshakeData) throws SocketAuthException`. Exposes a netty-socketio type by design. |
| `SocketAuthContext` | API | Lombok `@Data @Builder`; `userId`, `role`, `claims` (`@Builder.Default Map.of()`). Factories: `anonymous()`, `of(userId, role)`. |
| `DefaultSocketAuthenticationHook` | internal (fallback) | Allow-all, returns `anonymous()`. Installed only when `auth.enabled=false` — the factory method throws `IllegalStateException` when auth is enabled and no consumer hook exists. |

`role` is stored on `SocketSession`; `claims` entries are copied into the session's attributes.

### `serialization`

| Type | Kind | Notes |
|---|---|---|
| `SocketMessageSerializer` | SPI | `<T> T deserialize(Object raw, Class<T>)`, `String serialize(Object)`. |
| `JsonSocketMessageSerializer` | internal (default impl) | Jackson `convertValue`, using the consumer's `ObjectMapper` bean. |

Failures are **not** swallowed: both directions throw `SocketException`, so a bad payload runs the
normal error pipeline instead of a handler receiving `null`. `deserialize` short-circuits on `null`
raw values and on a `targetType` that already `isInstance(raw)`.

### `exception`

| Type | Kind | Notes |
|---|---|---|
| `SocketException` | API | Base `RuntimeException` for SDK failures. |
| `SocketAuthException` | API | Extends `SocketException`; thrown by hooks to reject a connection. |
| `SocketErrorHandler` | SPI | `void handle(SocketIOClient, String event, Exception)`. |
| `DefaultSocketErrorHandler` | internal (default impl) | Logs at `ERROR`, then `client.sendEvent("error", Map.of("event", …, "message", …))`, swallowing send failures because the client may already be gone. |

### `heartbeat`

`HeartbeatManager` — internal, `InitializingBean` + `DisposableBean`. Owns a single daemon thread
named `socket-heartbeat` created in `afterPropertiesSet()` and shut down in `destroy()`; the SDK
deliberately does not require `@EnableScheduling` in the consumer. `scheduleWithFixedDelay` with a
wrapper that catches `Exception` *and* `Throwable`, because a scheduled task that throws is
silently cancelled forever.

`cleanupStaleSessions()` returns immediately when both `evict-ghost-sessions` and
`disconnect-stale-sessions` are `false` — there is nothing a pass could legally do. The idle
threshold is only computed when the idle branch is enabled.

Per pass, per session: skip null session ids; skip ids that are not netty client UUIDs (a custom
`SessionManager` may use other ids, and this manager cannot judge their liveness); when
`server.getClient(id)` is `null` or the channel is closed, evict — but **only if
`evict-ghost-sessions=true`**, otherwise log at `DEBUG` and move on; and, only when
`disconnect-stale-sessions=true`, disconnect and evict sessions whose `lastActiveAt` predates the
idle threshold.

Note the ordering consequence: a session the local server does not own never reaches the idle
branch. With `evict-ghost-sessions=false` such entries are left entirely alone, which is exactly
what a shared store needs.

**`app.socket.heartbeat.evict-ghost-sessions` exists because ghost detection is
local-server-scoped.** "Ghost" means `server.getClient(uuid)` returned nothing *on this JVM*, which
with a shared `SessionManager` is the normal state of every session owned by another pod. With the
flag left `true` in a multi-pod deployment, each pod deletes every other pod's sessions on each
pass and presence data never survives a cycle. The flag is the documented mitigation; a pod-aware
sweep (owning-pod id on the session, skip foreign) is the better long-term fix and is why
`HeartbeatManager` is `@ConditionalOnMissingBean` like everything else. `afterPropertiesSet()` logs
the flag's value on every boot, plus an extra `INFO` line when eviction is off, so the state is
visible.

### `metrics` / `tracing`

| Type | Kind | Notes |
|---|---|---|
| `SocketMetrics` | SPI | Six increment methods. Mentions no Micrometer type anywhere. |
| `NoOpSocketMetrics` | internal | Empty methods — one JIT-inlinable virtual call on the hot path, and no `if (enabled)` branches in the dispatcher. |
| `MicrometerSocketMetrics` | internal | Counters built once in the constructor; per-event counters memoised in a `ConcurrentHashMap`; `event` tag cardinality capped at `MAX_EVENT_TAGS = 100`, overflowing into `event="other"`; empty/null event names become `event="unknown"`. Also registers the `socket.connections.active` gauge over `SessionManager::getConnectedCount`. |
| `SocketTracing` | SPI | `void startSpan(String name, Runnable work)`. Contract: propagate `work`'s exception unchanged after recording it, always close the span. |
| `NoOpSocketTracing` | internal | `work.run()`. |
| `MicrometerSocketTracing` | internal | `tracer.nextSpan().name(n).start()`, `try (SpanInScope)`, `span.error(e)` on `RuntimeException`/`Error`, `span.end()` in `finally`. |

The cardinality cap exists because event names are client-supplied; without it a hostile or buggy
client could create unbounded Prometheus time series.

### `event`

`SocketServerStartedEvent(port)`, `SocketServerStoppedEvent`, `SocketClientConnectedEvent(session)`,
`SocketClientDisconnectedEvent(session)` — all **API**, all plain `ApplicationEvent` subclasses
with Lombok `@Getter`. They are the loose-coupling seam for consumers that want presence tracking
without writing a handler.

### `lifecycle`

`SocketServerLifecycle` — internal, `SmartLifecycle`. See diagram 2d.

---

## 4. Auto-configuration and the conditional-bean model

### Activation: why a marker bean

```text
@EnableSocket
  └─ @Import(SocketMarkerConfiguration)
       └─ @Bean SocketMarker
            └─ satisfies @ConditionalOnBean(SocketMarker.class) on SocketAutoConfiguration
```

Two properties fall out of this, and both were the point of the design:

1. **The jar does nothing until you opt in.** `SocketAutoConfiguration` is registered *only* via
   `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`. Without a
   `SocketMarker` bean it does not match, so no `SocketIOServer` is constructed and port 9090 is
   not bound in a service that merely has the SDK on its classpath transitively.
2. **`@ConditionalOnMissingBean` evaluates reliably.** The obvious alternative —
   `@EnableSocket` directly `@Import`ing the configuration — would run it in the regular
   configuration-parsing phase, where `@ConditionalOnMissingBean` is evaluated against a
   partially-populated bean registry and consumer beans may not yet be visible. Routing activation
   through a marker keeps the real wiring in Spring Boot's *deferred* auto-configuration phase,
   which runs after user configuration, which is where `@ConditionalOnMissingBean` is meant to run.

Class-level conditions on `SocketAutoConfiguration`:

| Annotation | Effect |
|---|---|
| `@AutoConfiguration` | deferred phase, `proxyBeanMethods = false` semantics |
| `@ConditionalOnBean(SocketMarker.class)` | requires `@EnableSocket` |
| `@ConditionalOnProperty(prefix = "app.socket", name = "enabled", matchIfMissing = true)` | `app.socket.enabled=false` force-disables everything; absent means enabled |
| `@EnableConfigurationProperties(SocketProperties.class)` | binds `app.socket.*` without the consumer needing `@ConfigurationPropertiesScan` |

### Every bean is `@ConditionalOnMissingBean`

All twelve on the enclosing class: `socketIOServer`, `sessionManager`, `connectionManager`,
`messageDispatcher`, `socketMessageSerializer`, `socketAuthenticationHook`, `socketErrorHandler`,
`noOpSocketMetrics`, `noOpSocketTracing`, `heartbeatManager`, `messageHandlerRegistry`,
`socketServerLifecycle`. Plus the two nested Micrometer bean methods, `socketMetrics` and
`socketTracing`. A consumer overrides any of them by declaring its own bean of the same type;
`@Primary` is unnecessary.

**No two `@Bean` methods anywhere in the class share a method name**, so no bean definition ever
overrides another and the SDK is safe under
`spring.main.allow-bean-definition-overriding=false` (Spring Boot's default). This is why the
fallbacks are named `noOpSocketMetrics` / `noOpSocketTracing` rather than `socketMetrics` /
`socketTracing`.

### The optional-integration mechanism

`SocketMetrics` and `SocketTracing` being SDK-owned interfaces is what makes `compileOnly` safe.
No type the runtime always loads may mention a Micrometer class in a field, parameter or return
type, or a consumer without actuator would hit `NoClassDefFoundError` during bean creation.

There are two independent questions, and they are answered by two different mechanisms because
only one of them can be answered by a condition.

#### 1. Is the Micrometer type on the classpath? → `@ConditionalOnClass` on a nested class

```text
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(MeterRegistry.class)      ← library present?
static class MicrometerMetricsConfiguration { @Bean SocketMetrics socketMetrics(...) }
```

Putting `@ConditionalOnClass` on the nested class means its body — and therefore every Micrometer
type it mentions — is never loaded when the library is absent. The `NoOp*` fallbacks on the
enclosing class carry the exact complement, `@ConditionalOnMissingClass("io.micrometer...")`, so
the nested configuration and the fallback are **mutually exclusive by construction**: exactly one
of them can ever match, whatever order Spring happens to process them in. The fallbacks exist
purely for the classpath-absent case.

#### 2. Does a `MeterRegistry` / `Tracer` bean exist? → `ObjectProvider`, not `@ConditionalOnBean`

This is the part that was **broken as shipped**, and the previous version of this document
documented the broken design as if it worked.

The nested configurations used to carry `@ConditionalOnBean(MeterRegistry.class)` and
`@ConditionalOnProperty(app.socket.metrics.enabled)`. `@ConditionalOnBean` is evaluated when the
auto-configuration is *processed*. Spring Boot's `AutoConfigurationSorter` orders auto-configuration
classes **alphabetically** before applying `@AutoConfiguration(before/after)` and
`@AutoConfigureOrder` hints, and

```text
com.tutem.platform.socket.config.SocketAutoConfiguration
  <  org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration
  <  org.springframework.boot.actuate.autoconfigure.tracing.*
```

so this class is always processed **first**. At that moment no `MeterRegistry` and no `Tracer` bean
definition exists yet, `@ConditionalOnBean` does not match, the nested configurations are skipped,
and the enclosing `NoOp*` fallbacks win. Result: a consumer with `spring-boot-starter-actuator`
silently got `NoOpSocketMetrics` — no meters, no spans, no warning, and nothing in the log to
suggest anything was wrong. The README claimed the opposite ("a consumer with them gets the
Micrometer implementations automatically").

The fix moves the bean-existence question out of the condition system and into bean
*instantiation*, where every auto-configuration has already contributed its definitions:

```text
@Bean @ConditionalOnMissingBean
SocketMetrics socketMetrics(ObjectProvider<MeterRegistry> registries,
                            SessionManager sessionManager,
                            SocketProperties props)
```

The method then resolves, in order:

| Situation | Result | Log |
|---|---|---|
| `app.socket.metrics.enabled=false` | `NoOpSocketMetrics` | `INFO` naming the property |
| `registries.getIfAvailable()` throws `BeansException` (several registries, none `@Primary`) | `NoOpSocketMetrics` | `WARN` with the resolution failure |
| `getIfAvailable()` returns `null` (library present, no bean) | `NoOpSocketMetrics` | `INFO` saying so explicitly |
| a registry resolved | `MicrometerSocketMetrics` | — |

`SocketTracing` is identical with `ObjectProvider<Tracer>`. The `null` branch is not a corner case
there: `Tracer` on the classpath genuinely does not imply a `Tracer` bean, because only a bridge
(Brave, OpenTelemetry) contributes one.

Three properties follow, and they are the point of the design:

1. **Order-independent.** Nothing depends on when this auto-configuration is sorted relative to
   actuator's, nor on the textual position of any `@Bean` method or member class in the file.
2. **Never silent.** Every path that ends in a no-op logs which path it took, so "where did my
   `socket.*` meters go" is answerable from the startup log.
3. **`enabled=false` is honoured in both branches** — via the fallback bean when Micrometer is
   absent, and via the explicit check inside the nested bean method when it is present.

> **Removed claim.** An earlier version of this section asserted that the `NoOp*` fallbacks worked
> because "Spring parses nested member configuration classes — and registers their bean definitions
> — before the enclosing class's own `@Bean` methods", and that "reordering the file would break
> this". That was wrong twice over. `ConfigurationClassParser.processMemberClasses` does run before
> the enclosing class's bean methods are collected, but it does so **regardless of textual
> position**, so reordering the file changes nothing — and in any case the two `@Bean` methods no
> longer share a name or a matching condition, so there is no race left to order. Correctness now
> comes from the `@ConditionalOnClass` / `@ConditionalOnMissingClass` complement, which is a
> structural guarantee rather than a parse-order coincidence.

---

## 5. Threading and thread-safety

### Which thread runs what

| Thread | Runs |
|---|---|
| Main / Spring startup thread | bean creation, `HeartbeatManager.afterPropertiesSet()`, `SocketServerLifecycle.start()` → `registerAll()` → `server.start()` |
| Netty boss threads (`boss-count`, default 1) | accepting connections |
| Netty worker threads (`worker-count`, default 100) | **everything consumer-facing**: connect/disconnect listeners, auth hook, `@OnConnect`/`@OnMessage`/`@OnDisconnect`/`@OnError` handlers, the `SocketErrorHandler`, session reads/writes, metrics increments, span creation, and the publishing of `SocketClientConnectedEvent`/`SocketClientDisconnectedEvent` |
| `socket-heartbeat` (single daemon thread) | `cleanupStaleSessions()` — reads `SessionManager.getAll()`, reads `lastActiveAt`, calls `removeBySessionId` and possibly `client.disconnect()` |
| Spring shutdown thread | `SocketServerLifecycle.stop()`, `HeartbeatManager.destroy()` |

Consequences for consumers: synchronous `@EventListener` methods on client connect/disconnect run
on a netty worker thread and block it, as do handler bodies. Blocking I/O in a handler consumes one
of the `worker-count` threads. Make slow listeners `@Async`.

### Concurrent structures

| Structure | Mechanism |
|---|---|
| `InMemorySessionManager.bySessionId` | `ConcurrentHashMap<String, SocketSession>`, authoritative |
| `InMemorySessionManager.userIdToSessionIds` | `ConcurrentHashMap<String, Set<String>>` where the value is `ConcurrentHashMap.newKeySet()`; mutated only inside `compute`, so index updates are atomic per user |
| `SocketSession.attributes` | `ConcurrentHashMap` (hence `setAttribute` silently ignoring nulls — the map forbids them, and an NPE on the hot path is worse) |
| `SocketSession.lastActiveAt` | **`volatile Instant`** — written by netty workers on connect and on every inbound message, read by the heartbeat thread. `Instant` is immutable, so a volatile reference is sufficient; no lock is needed |
| `MessageHandlerRegistry.ackGapReported` | `ConcurrentHashMap.newKeySet()`; `add` returning `true` is the once-only latch |
| `MicrometerSocketMetrics.messageCounters` / `errorCounters` | `ConcurrentHashMap`, `computeIfAbsent` |
| `SocketServerLifecycle.running` | `AtomicBoolean` with `compareAndSet` for idempotent start/stop |

### Publication of the handler lists

`connectHandlers`, `disconnectHandlers` and `errorHandlers` are plain `ArrayList`s. They are
populated exclusively during `registerAll()` on the startup thread, *before* `server.start()` opens
the port, and are only read afterwards from netty worker threads. `server.start()` provides the
happens-before edge. This is safe as written, but it is a structural invariant, not an enforced
one: any future code path that mutates these lists after startup would need a concurrent
collection.

### Snapshot semantics

`SessionManager.getAll()` and `SocketSession.getAttributes()` both return unmodifiable *copies*.
The heartbeat sweep therefore iterates a snapshot and may act on a session that has since
disconnected — which is harmless, because `removeBySessionId` is idempotent and
`resolveClient`/`disconnect` are both guarded.

---

## 6. Known limitations and the multi-pod scaling path

### Today: single pod

Three separate pieces of state are pod-local, and they are not the same problem:

| State | Where it lives | Used by |
|---|---|---|
| `userId` → sessions | `InMemorySessionManager` heap maps | `sendToUser`, `sendToSession`, `isConnected`, `getConnectedCount`, `disconnect(userId)`, the heartbeat sweep |
| Room membership | netty-socketio's internal store (default: in-memory `MemoryStoreFactory`) | `sendToRoom`, `getClientsInRoom`, `client.joinRoom`/`leaveRoom` |
| The set of connected clients | the `SocketIOServer` instance's own client registry | `broadcast`, `server.getClient(uuid)` |

With N replicas, each pod sees only its own share of all three. `sendToUser("u1", …)` executed on
pod A silently does nothing when `u1` is connected to pod B — `findAllByUserId` returns empty and
the call logs at `DEBUG`.

Practical mitigations that need no SDK change: run a single replica; use sticky sessions so a
user's connections land on one pod *and* originate outbound sends from that pod; or publish
outbound intents on your own message bus so every pod attempts the local send.

### What a `RedisSessionManager` would fix — and what it would not

A distributed `SessionManager` implementation (the interface is designed for exactly this
substitution) **would** fix:

- `findBySessionId` / `findAllByUserId` / `isConnected` / `getConnectedCount` becoming
  cluster-wide facts.
- Presence queries: "is this user online anywhere?"
- Cross-pod *routing decisions* — pod A could learn that `u1`'s session lives on pod B.

It **would not** fix delivery, and this is the part that is easy to get wrong:

- `MessageDispatcher.sendToSession` resolves the client with `server.getClient(uuid)` against the
  **local** `SocketIOServer`. Knowing that the session exists on another pod does not give pod A a
  channel to it. `sendToUser` would still find the session and then fail to deliver.
- `sendToRoom` and `broadcast` never touch `SessionManager` at all — they call
  `server.getRoomOperations(room)` and `server.getBroadcastOperations()`. A Redis session store is
  invisible to them.
- Likewise `ConnectionManager.getClientsInRoom`, `joinRoom`, `leaveRoom` and
  `disconnect(userId)`'s final `client.disconnect()`.
- The heartbeat sweep would become actively wrong: ghost eviction removes any session whose
  `server.getClient(id)` is `null`, which for a shared store means every pod evicts every *other*
  pod's sessions on its next pass — continuously, every `cleanup-interval-seconds`. **This is now
  guarded by `app.socket.heartbeat.evict-ghost-sessions`, which a distributed `SessionManager`
  MUST set to `false`.** That is a mitigation, not a fix: with it off, dead sessions are never
  reaped at all. The real fix is a pod-aware sweep — store an owning-pod id on the session and skip
  foreign ones — which means replacing the `HeartbeatManager` bean.
- Note also that `HeartbeatManager.parseClientId` skipping non-UUID session ids is **not** a
  safeguard for this case, and must not be presented as one. A distributed `SessionManager` will
  naturally reuse netty's UUID session ids — `MessageDispatcher.sendToSession` and
  `ConnectionManager` both parse the id as a UUID, so it has little choice — which means the ids
  parse fine on every pod and are evicted on every pod. The skip only protects stores that invent
  their own non-UUID id scheme, and such a store breaks outbound delivery instead.

### The actual multi-pod path

Cross-pod delivery is netty-socketio's own concern, and it has an answer: a Redisson-backed
`StoreFactory` plus `PubSubStore`, configured on the `Configuration` object that builds the
`SocketIOServer`.

```text
Configuration#setStoreFactory(new RedissonStoreFactory(redissonClient))
```

That makes room membership shared and turns `sendToRoom`/`broadcast` into cluster-wide operations,
because netty-socketio publishes the dispatch over Redis pub/sub and every pod delivers to its own
local clients. Neither the Redisson client nor the store factory is a dependency of this module
today, and the SDK's `socketIOServer` bean does not call `setStoreFactory` — but the bean is
`@ConditionalOnMissingBean`, so a consumer can already supply its own configured `SocketIOServer`.

A complete multi-pod story therefore needs **both** halves:

1. A distributed `SessionManager` — for identity, presence and `sendToUser` targeting.
2. netty-socketio's Redisson `StoreFactory`/`PubSubStore` — for rooms, broadcast and cross-pod
   delivery.

Plus `evict-ghost-sessions=false` (or the pod-aware sweep). Doing only (1) produces a store that
looks cluster-aware while delivery quietly stays pod-local, which is worse than the honest
single-pod behaviour.

### Other gaps

- **No remote artifact repository.** `maven-publish` *is* configured — a `MavenPublication` from
  `components["java"]`, plus `java { withSourcesJar() }`, publishing
  `com.tutem.platform:socket-sdk:1.0-SNAPSHOT` — but the `publishing` block has no `repositories {}`
  target yet (GitHub Packages / Nexus / Artifactory / CodeArtifact undecided). So
  `publishToMavenLocal` works and `publish` has nowhere to go. Cross-repo consumption is
  `publishToMavenLocal` + `mavenLocal()`, or a Gradle composite build via `includeBuild`.
- **Netty is silently downgraded in the consuming services.** netty-socketio 2.0.12 pins netty
  `4.1.114.Final`; the Spring Boot 3.3.3 BOM pins `4.1.112.Final`; both consuming services apply
  `io.spring.dependency-management` 1.1.6, which *forces* BOM versions rather than letting Gradle's
  conflict resolution pick the higher one. The consumer therefore runs netty-socketio on top of a
  netty two patch versions older than it was built against, with no warning. Escape hatch:
  `ext['netty.version'] = '4.1.114.Final'` in the consumer's `build.gradle`. Worth revisiting
  whenever either version moves.
- **No `RedisSessionManager` implementation** ships here, despite the javadoc example on
  `InMemorySessionManager`.
- **No automatic acks.** By design; the SDK only logs when a client wants one and the handler
  cannot send it.
- **No authorization model.** `role` is stored, never enforced.
- **No handler timeout or bulkheading.** A handler that blocks holds a netty worker thread for as
  long as it likes.
- **No reconnect/replay buffer.** Disconnect removes the session; nothing is queued.
- **No restart-in-place.** `registerAll()` is single-shot because netty-socketio never removes
  listeners. A `Lifecycle` stop/start cycle logs a `WARN` and leaves the server unusable rather
  than double-registering. Fixing this properly means either a listener-removal API upstream or
  rebuilding the `SocketIOServer`.
- **The error pipeline covers `Exception`, not `Throwable`.** An `Error` from a handler escapes into
  the netty pipeline unrecorded — see the notes under diagram 2c.
- **Handlers behind a non-Spring JDK proxy are invisible.** Detected and reported at `WARN`, but not
  supported. Spring's own AOP proxies are fine.
- **`origin` is not access control** and never was, despite having been documented as CORS
  lock-down. See the `properties` section.
- **Test coverage.** Tests live under `src/test/java/com/tutem/platform/socket/`, mirroring the
  main package layout. The areas most worth keeping covered are the ones with the least obvious
  behaviour: the auto-configuration's conditional matrix, `resolveTransports`, the
  `@OnMessage` binding/rejection rules, the error pipeline ordering, `InMemorySessionManager`'s
  index invariant, and the heartbeat's ghost-vs-idle distinction.
