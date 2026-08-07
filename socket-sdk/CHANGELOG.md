# Changelog — socket-sdk

All notable changes to this module. Format loosely follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

---

## [Unreleased] - Integration into tutem-platform-libraries

`socket-sdk` arrived as an unbuildable ZIP drop in a sibling directory. It is now a subproject of
`tutem-platform-libraries` (`include("socket-sdk")` in `settings.gradle.kts`), its Groovy
`build.gradle` was converted to `build.gradle.kts`, and it builds under Gradle 9.6.0 with the
Java 21 toolchain. Module coordinates: `com.tutem.platform:socket-sdk:1.0-SNAPSHOT`.

> ### ⚠ Breaking / behavioural changes
>
> Read this section if you used the ZIP version.
>
> 0. **`app.socket.auth.enabled` now defaults to `true` (was `false`).** This is the most
>    user-visible break in the list: a consumer that does not declare a `SocketAuthenticationHook`
>    bean **now fails at startup** with an `IllegalStateException` explaining both remedies.
>    Previously the same application started and quietly accepted anonymous connections on the
>    socket port.
>
>    Fix it one of two ways:
>    - **production** — declare a `SocketAuthenticationHook` bean (this is the point of the change);
>    - **local dev / tests** — set `app.socket.auth.enabled=false` explicitly, which restores the
>      old allow-all behaviour and logs a `WARN` naming the host and port on every boot.
>
>    Nothing consumes the SDK yet, so there is no migration cost today. The rationale is that the
>    previous default shipped an unauthenticated open port and relied on every consumer remembering
>    to turn authentication on; a default that fails loudly is strictly better than one that fails
>    silently.
> 1. **`SocketMetrics` and `SocketTracing` are now interfaces**, not concrete classes. Anything
>    that constructed or subclassed them must be updated. `NoOp*` and `Micrometer*` implementations
>    are provided and selected automatically.
> 2. **`MessageHandlerRegistry`'s constructor widened to 10 arguments** — it now also takes
>    `SocketMessageSerializer`, `SocketErrorHandler` and `SocketProperties`. Any code constructing
>    it directly will not compile.
> 3. **`app.socket.allow-custom-requests` now defaults to `false`** (was `true`). Non-Socket.IO
>    HTTP requests on the socket port are no longer passed through to custom handlers.
> 4. **`app.socket.heartbeat.disconnect-stale-sessions` defaults to `false`** — the heartbeat no
>    longer force-disconnects merely-idle clients. This was previously always-on and unconditional.
> 5. **`@EnableSocket` is now genuinely required.** The auto-configuration is gated on the marker
>    bean it registers. Having the jar on the classpath no longer starts a socket server.
> 6. **Ambiguous and zero-argument `@OnMessage` methods are rejected at startup** with an `ERROR`
>    log and are not registered. Previously such methods were registered and failed (or bound
>    wrongly) at traffic rate.
> 7. **Deserialization failures now throw** `SocketException` instead of handing the handler a
>    `null` payload.
> 8. **`app.socket.transports` defaults to `[websocket, polling]`**, where the server was
>    previously hardcoded to websocket-only.
> 9. **`@OnConnect` / `@OnDisconnect` parameter lists are validated at registration.** Only
>    `SocketIOClient` and/or `SocketSession` parameters are allowed, in any order, or none.
>    Anything else is logged as an `ERROR` and the method is **not registered**, so it is never
>    called. Previously an unrecognised parameter type was silently bound to `null` (an NPE in your
>    handler on the first connection) and an `Object`-typed parameter silently received the
>    `SocketSession`. If you relied on either, change the signature.
> 10. **`MessageHandlerRegistry.registerAll()` is single-shot.** A `Lifecycle` stop/start cycle no
>    longer re-registers handlers; the second call logs a `WARN` and does nothing.
>    **Restart-in-place is unsupported** — restarting the socket server requires a fresh
>    application context. Previously a restart double-registered every listener, so each
>    `@OnMessage` fired twice.
> 11. **`SocketServerStoppedEvent` is published only when the server actually stopped cleanly.** If
>    `SocketIOServer.stop()` throws, the failure is logged and no event is published, because the
>    port may still be listening. Previously it was published unconditionally, so the event meant
>    "stop was attempted" rather than "stopped".

### Added

- **Build integration.** `socket-sdk/build.gradle.kts` (Kotlin DSL) replacing the Groovy
  `build.gradle`; `include("socket-sdk")` in `settings.gradle.kts`; Java 21 toolchain; UTF-8
  compilation; JUnit Platform test task.
- **`SocketMarker` / `SocketMarkerConfiguration`** — the activation marker bean imported by
  `@EnableSocket`. `SocketAutoConfiguration` is `@ConditionalOnBean(SocketMarker.class)`, so the
  SDK is strictly opt-in and all `@ConditionalOnMissingBean` checks run in the deferred
  auto-configuration phase, where consumer beans are already visible.
- **`app.socket.enabled`** (default `true`) — master switch. `false` disables the whole SDK even
  with `@EnableSocket` present; useful in tests and in profiles that need no realtime traffic.
- **`app.socket.transports`** (default `[websocket, polling]`) with case-insensitive name
  resolution, duplicate collapsing, and a startup `IllegalArgumentException` naming the legal
  values for an empty list or an unknown name.
- **`app.socket.origin`** (default `null`) — sets the `Access-Control-Allow-Origin` /
  `Access-Control-Allow-Credentials` **response headers** for browser polling clients. It is not an
  access control and rejects no connection; see the `Fixed` section.
- **`app.socket.startup-phase`** (default `Integer.MAX_VALUE - 1024`) — the `SmartLifecycle` phase
  in which the socket server starts and stops.
- **`app.socket.heartbeat.disconnect-stale-sessions`** (default `false`) — opt-in idle disconnect.
- **`app.socket.heartbeat.evict-ghost-sessions`** (default `true`) — controls the ghost sweep.
  Ghost detection asks the *local* netty server whether it owns a session, so it must be set to
  `false` for any shared/distributed `SessionManager` spanning more than one pod; otherwise each
  pod evicts every other pod's sessions on every pass. `HeartbeatManager` logs the flag's value at
  startup, and the sweep short-circuits entirely when both heartbeat behaviours are disabled.
- **`maven-publish` with a sources jar.** A `MavenPublication` from `components["java"]` plus
  `java { withSourcesJar() }`, publishing `com.tutem.platform:socket-sdk:1.0-SNAPSHOT`.
  `./gradlew :socket-sdk:publishToMavenLocal` works. **No remote repository is configured yet**, so
  cross-repo consumption still needs either `publishToMavenLocal` + `mavenLocal()` in the consumer,
  or a Gradle composite build (`includeBuild`). README has the Groovy-DSL snippets for both.
- **`spring-context` and `jackson-databind` promoted to `api`.** They appear in public API
  signatures — the event types extend `ApplicationEvent`, and
  `JsonSocketMessageSerializer(ObjectMapper)` is a public constructor — so declaring them
  `implementation` was wrong. Actuator and micrometer-tracing remain `compileOnly` and must be
  added by the consumer if metrics/tracing are wanted.
- **A startup `WARN` for beans behind a non-Spring JDK proxy.** `AopProxyUtils.ultimateTargetClass`
  can only see through proxies implementing `Advised`; a hand-rolled `java.lang.reflect.Proxy` hides
  its target, so socket annotations on that target are invisible and the bean is not registered.
  That case is now reported by name instead of being skipped in silence. It remains a limitation,
  not a fixed bug.
- **`NoOpSocketMetrics` and `NoOpSocketTracing`** so a consumer without actuator or
  micrometer-tracing starts cleanly and gets zero-overhead no-ops.
- **`MicrometerSocketMetrics` / `MicrometerSocketTracing`** in nested configuration classes gated
  on `@ConditionalOnClass`, so their Micrometer types are never loaded when the libraries are
  absent. Whether a `MeterRegistry`/`Tracer` bean exists, and whether the corresponding `enabled`
  property is set, is resolved inside the bean method — see the `Fixed` section.
- **`SessionManager.findAllByUserId(String)`** — the authoritative user lookup, supporting multiple
  concurrent sessions per user (phone + tablet + web).
- **`SocketServerLifecycle`** as a `SmartLifecycle` with a configurable phase, replacing
  event-listener-based startup.
- **`socket.auth.failures`** metric, incremented on every rejected connection.
- **Ack-gap reporting** — when a client requests an ack for an event whose handler declares no
  `AckRequest` parameter, that is logged once per event name (the SDK still never auto-acks).
- **Startup validation** of `@OnError` signatures: anything other than
  `(SocketIOClient, String, Exception)` is logged as an `ERROR` and not registered.
- **Configuration metadata** — `spring-boot-configuration-processor` runs as an
  `annotationProcessor`, so `app.socket.*` keys get IDE completion and javadoc.
- **Documentation** — `README.md` rewritten as a consumer guide, `ARCHITECTURE.md` rewritten as a
  maintainer document with component/sequence/lifecycle diagrams, and this `CHANGELOG.md`.

### Changed

- **`SocketMetrics` and `SocketTracing` became interfaces** with no Micrometer types in any
  signature. This is what makes actuator and micrometer-tracing safely `compileOnly`.
- **`@OnMessage` parameter binding is positional-by-type.** Each parameter is filled from its own
  type (`SocketIOClient` → client, `AckRequest` → ack, anything else → payload), so any parameter
  order works.
- **`MessageDispatcher.sendToUser` and `ConnectionManager.disconnect(userId)` fan out** to every
  live session of the user instead of an arbitrary one — multi-device now works.
- **`SessionManager.findByUserId(String)` is `@Deprecated`** and is now a default method delegating
  to `findAllByUserId(...).stream().findFirst()`.
- **`InMemorySessionManager` restructured** around a single source of truth: `bySessionId` is
  authoritative for liveness and `userIdToSessionIds` is a pure index into it, updated atomically
  via `compute`. `removeBySessionId` removes exactly one session id rather than the whole user
  entry, so a concurrent reconnect on another device is not clobbered. `isConnected` is derived
  from `findAllByUserId`, so the two can no longer disagree.
- **Authentication fails closed.** Any exception from the hook — not only `SocketAuthException` —
  counts as an auth failure and disconnects the client. With `auth.enabled=true`, a `null` context
  or a `null`/blank `userId` is also a rejection, and an inbound message from a client with no
  stored session is refused rather than dispatched.
- **`app.socket.auth.enabled=true` now fails fast at startup** with a clear `IllegalStateException`
  when no `SocketAuthenticationHook` bean is present, and logs a `WARN` at startup when auth is
  disabled.
- **The heartbeat owns its own daemon thread** (`socket-heartbeat`, single-threaded, created in
  `afterPropertiesSet`, shut down in `destroy`). The library no longer imposes `@EnableScheduling`
  on consumers, and a failing pass can no longer silently cancel the schedule.
- **The heartbeat's primary job is ghost-session eviction** — entries whose netty client is gone or
  whose channel is closed — rather than idle disconnection. Session ids that are not netty client
  UUIDs are skipped instead of evicted blindly. Note that this skip is **not** a safeguard for a
  distributed `SessionManager`: such a store naturally reuses netty's UUID session ids, so the ids
  parse fine on every pod and are evicted on every pod. Use `evict-ghost-sessions=false` for that.
- **`app.socket.auth.enabled` defaults to `true`.** See breaking change 0. `AuthProperties`'s
  javadoc no longer claims the `false` default exists "for backward compatibility".
- **Every SDK bean is `@ConditionalOnMissingBean`** (previously 4 of them were), so any single
  component can be replaced by declaring a bean of the same type. No `@Primary` needed.
- **Handler scanning is safe and proxy-aware.** Only non-abstract, non-`@Lazy`, singleton bean
  definitions are considered; the bean *type* is resolved before any instance is created, so
  scanning never force-instantiates a `@Lazy` bean or throws on a request/prototype-scoped one.
  Annotations are read from the AOP target class and invocation goes through the proxy, so
  handlers on `@Transactional`/`@Async`/`@Validated` beans are found *and* keep their advice. An
  unresolvable bean is logged and skipped instead of aborting startup.
- **`SocketAuthContext.role` is stored on the session** and readable via `session.getRole()`.
- **`SocketSession` hardened for concurrent access**: `lastActiveAt` is `volatile`, `attributes` is
  a `ConcurrentHashMap`, `getAttributes()` returns an unmodifiable snapshot, and the class is
  deliberately not a Lombok `@Data` (no `equals`/`hashCode` over a mutable map, no blanket setters
  on identity fields).
- **`MicrometerSocketMetrics` bounds tag cardinality.** Event names are client-supplied, so
  distinct `event` tag values are capped at 100 per counter family, overflowing into
  `event="other"`; `null`/empty names become `event="unknown"`. Counters are memoised so the hot
  path performs no registry lookup.
- **`SocketServerLifecycle.start()`/`stop()` are idempotent** (`AtomicBoolean` `compareAndSet`).
  `start()` resets the flag and rethrows on failure so `stop()` after a failed start is a clean
  no-op; `stop()` never propagates, so a failure there cannot mask the real shutdown cause.
- Handlers are now registered **before** `server.start()` opens the port, closing the window in
  which a client could emit an event with no listener attached.
- Every `@OnMessage` listener is registered as `Object.class` and deserialized by the SDK's
  `SocketMessageSerializer`, so payload-conversion failures land in the SDK's error pipeline
  instead of failing opaquely inside netty-socketio.
- `netty-socketio` is declared `api` rather than `implementation`, honestly reflecting that
  consumer-facing signatures expose `SocketIOClient`, `AckRequest` and `HandshakeData`.

### Fixed

- **Micrometer metrics and tracing never activated, even with actuator on the classpath.** The
  nested configurations were gated on `@ConditionalOnBean(MeterRegistry.class)` /
  `@ConditionalOnBean(Tracer.class)`. Those conditions are evaluated when the auto-configuration is
  *processed*, and Spring Boot's `AutoConfigurationSorter` orders auto-configurations alphabetically
  before applying `before`/`after` hints — so `com.tutem.platform.socket.config.
  SocketAutoConfiguration` was always processed before
  `org.springframework.boot.actuate.autoconfigure.metrics.*`, when no `MeterRegistry` bean
  definition existed yet. The condition never matched and consumers silently got `NoOpSocketMetrics`
  / `NoOpSocketTracing`: no meters, no spans, no warning. The README documented the opposite.

  Bean existence is now resolved through `ObjectProvider<MeterRegistry>` / `ObjectProvider<Tracer>`
  *inside* the bean method, at instantiation time, when every auto-configuration has contributed its
  definitions — so ordering is irrelevant. The `enabled` properties are checked in the same place.
  Every fallback path (property off, no bean, ambiguous bean) logs which implementation you got, so
  the failure mode cannot recur silently. Classpath presence is still `@ConditionalOnClass` on the
  nested class, and the `NoOp*` fallbacks now carry the complementary
  `@ConditionalOnMissingClass`, making the two mutually exclusive by construction rather than by
  parse order.
- **Duplicate `@Bean` method names removed.** The `NoOp*` fallbacks were both named `socketMetrics` /
  `socketTracing`, colliding with the nested configurations' bean methods and relying on
  definition-override behaviour. They are now `noOpSocketMetrics` / `noOpSocketTracing`; no two
  `@Bean` methods in `SocketAutoConfiguration` share a name, so the SDK is safe under
  `spring.main.allow-bean-definition-overriding=false`.
- **The documented "ordering subtlety" was false.** `ARCHITECTURE.md` claimed the `NoOp*` fallbacks
  worked because Spring parses nested member classes before the enclosing class's `@Bean` methods,
  and that "reordering the file would break this". `ConfigurationClassParser.processMemberClasses`
  runs before bean-method collection **regardless of textual position**, so reordering changes
  nothing — and the duplicate bean names the claim was protecting no longer exist. The paragraph is
  gone.
- **`app.socket.origin` was documented as a CORS lock-down / security control. It is neither.**
  Verified against netty-socketio 2.0.12: `Configuration.getOrigin()` has exactly one reader,
  `EncoderHandler.addOriginHeaders`, which writes the `Access-Control-Allow-Origin` and
  `Access-Control-Allow-Credentials` **response headers** on HTTP (polling) responses.
  `AuthorizeHandler` reads the *request's* `Origin`, stashes it as a channel attribute for that
  handler to echo and derives `HandshakeData`'s `xdomain` flag from it, but never compares it to the
  configured value and **never rejects a connection because of it** — the only rejections there are
  the `AuthorizationListener`, an unknown/unconfigured transport, and a missing session id. Browsers
  do not enforce CORS for the WebSocket transport at all, and non-browser clients ignore response
  headers entirely. The README security checklist told readers to set it "to the real client origin"
  as step 2 of hardening; that step has been replaced with network-layer restriction, and the
  property is now documented as a response header only. Authentication and network policy are the
  only real controls.
- **`SocketErrorHandler` and `@OnError` were dead code** — nothing ever invoked them. Handler
  exceptions now route through a single pipeline: `SocketMetrics.incrementError(event)` → every
  `@OnError` handler → the `SocketErrorHandler` bean. Failures inside `@OnConnect`/`@OnDisconnect`
  handlers travel the same path with `event` set to `"connect"`/`"disconnect"`. A throwing
  `@OnError` handler is logged and does not stop the remaining handlers.
- **Metrics and tracing crashed the application context at startup** when actuator or
  micrometer-tracing was absent, because Micrometer types appeared in always-loaded signatures.
  With the interface + `NoOp*` split, a consumer without those libraries now genuinely starts fine,
  and `NoOpSocketTracing` really does run the handler directly.
- **Deserialization failures silently produced a `null` payload.** They now throw
  `SocketException` and run the error pipeline, so the client learns its payload was rejected.
- **Websocket-only transports broke standard Socket.IO clients**, which begin on long-polling and
  then upgrade (including Flutter `socket_io_client` defaults). Both transports are now accepted by
  default.
- **The heartbeat force-disconnected healthy clients.** `lastActiveAt` advances only on *inbound*
  messages, so a listen-only subscriber — the normal Flutter client — was torn down roughly every
  two minutes. Idle disconnection is now opt-in and off by default; ghost eviction, which is driven
  by the transport's own view of the connection, is what runs by default.
- **A freshly connected session could look stale** to the very first heartbeat pass;
  `refreshActivity()` is now called before the session is saved.
- **Startup could open the socket port before or independently of the HTTP server.**
  `SmartLifecycle` with a configurable phase makes the ordering explicit in both directions and
  lets shutdown participate in graceful shutdown. The previous `ContextRefreshedEvent` approach had
  no ordering guarantee and could fire more than once per context.
- **A single unresolvable bean aborted handler scanning** (and therefore startup). Each bean is now
  scanned in its own try/catch.
- **`static` and blank-event-name `@OnMessage` methods** were silently mis-registered; both are now
  reported and skipped.
- A scheduled heartbeat pass that threw was silently cancelled forever; the pass body now catches
  `Exception` and `Throwable` and logs.
- **`@OnConnect`/`@OnDisconnect` bound unsupported parameter types to `null`.** See breaking
  change 9 — validated at registration now.
- **A `Lifecycle` restart double-registered every handler.** See breaking change 10.
- **`SocketServerStoppedEvent` was published even when `SocketIOServer.stop()` failed.** See
  breaking change 11.
- **Documentation: the netty downgrade in the consuming services was undocumented.**
  netty-socketio 2.0.12 pins netty `4.1.114.Final`, but `tutem-user-service-dev` and
  `tutem-rider-service-dev` apply `io.spring.dependency-management` 1.1.6 with the Spring Boot
  3.3.3 BOM, which *forces* netty to `4.1.112.Final` rather than letting Gradle resolve to the
  higher version — a silent downgrade of the transport library underneath netty-socketio. README
  now documents it with the escape hatch (`ext['netty.version'] = '4.1.114.Final'`).
- **Documentation: the "publishing is not configured" text is replaced with the accurate picture.**
  `maven-publish` and a sources jar are configured and `publishToMavenLocal` works; what is still
  missing is a *remote* repository. Consumer-side snippets are given in Groovy DSL, since both
  consuming services use Groovy `build.gradle`.
- **Documentation: two known limitations that were previously unstated.** Handlers behind a
  hand-rolled (non-Spring) JDK proxy are not discovered — now logged at `WARN` rather than skipped
  in silence. And `MessageHandlerRegistry.handleMessage` catches `Exception`, not `Throwable`, so an
  `Error` from a handler bypasses the error pipeline entirely and propagates into the netty
  pipeline; the pipeline is not the total guarantee the docs implied.
- **Documentation: the heartbeat's "skipped rather than evicted blindly" reassurance was
  misleading** — it read as safety for exactly the multi-pod shared-store case that the ghost sweep
  destroys. Replaced with the `evict-ghost-sessions` requirement and an explanation of why the
  non-UUID skip does not help there.

### Removed

- **`socket-sdk-testapp` module and `TestAppApplication`** — these never existed. All references,
  including the entire "How to run the test app" section and its Postman event/REST endpoint
  tables, are gone from the documentation.
- **`RedisSessionManager` and `MessagePackSocketMessageSerializer`** — likewise nonexistent. Both
  are now listed under "Not implemented / roadmap" instead of being documented as available.
- **The fake published coordinate `com.tutem.platform:socket-sdk:1.0.0-SNAPSHOT`.** The module's
  real version is `1.0-SNAPSHOT`, and it is now genuinely publishable — see `Added`. What is still
  absent is a remote repository, so the coordinate resolves only from `mavenLocal()` or a composite
  build.
- **The claim that the SDK is protocol-agnostic** and that "your service never imports or touches
  netty-socketio". It is not, and it does. The trade-off is now documented explicitly.
- **The wrong startup log line and the wrong `platform-libraries` folder name** from the docs.
- The dependency on Spring's `@EnableScheduling` in consuming services.
