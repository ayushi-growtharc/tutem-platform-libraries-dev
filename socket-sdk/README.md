# socket-sdk

A Spring Boot library that gives a Tutem microservice a Socket.IO-compatible realtime server
(netty-socketio) driven by annotations: `@OnConnect`, `@OnMessage`, `@OnDisconnect`, `@OnError`,
plus a `MessageDispatcher` for pushing events out from anywhere in your code.

It handles the plumbing — server lifecycle, handshake authentication, a session store keyed by
user *and* device, payload deserialization, a single error pipeline, optional Micrometer metrics
and tracing. It does **not** hide netty-socketio: your handler signatures and extension points
take `SocketIOClient`, `AckRequest` and `HandshakeData` directly (see
[Relationship to netty-socketio](#relationship-to-netty-socketio)).

- Java 21, Spring Boot 3.3.x, Gradle 9.x (this build uses the Kotlin DSL)
- `com.tutem.platform:socket-sdk:1.0-SNAPSHOT` — a subproject of `tutem-platform-libraries`

---

## Contents

- [How to depend on it](#how-to-depend-on-it)
- [Quick start](#quick-start)
- [Configuration reference](#configuration-reference)
- [Security](#security)
- [Handler reference](#handler-reference)
- [Pushing events out](#pushing-events-out)
- [Spring events](#spring-events)
- [Metrics and tracing](#metrics-and-tracing)
- [Extension points](#extension-points)
- [Relationship to netty-socketio](#relationship-to-netty-socketio)
- [Limitations / not implemented](#limitations--not-implemented)

---

## How to depend on it

### Inside this build (`tutem-platform-libraries`)

`socket-sdk` is a Gradle subproject — `settings.gradle.kts` has `include("socket-sdk")`. Another
module in this build depends on it as a project:

```kotlin
// Kotlin DSL (build.gradle.kts)
dependencies {
    implementation(project(":socket-sdk"))
}
```

```groovy
// Groovy DSL (build.gradle)
dependencies {
    implementation project(':socket-sdk')
}
```

### From another repository (`tutem-user-service-dev`, `tutem-rider-service-dev`)

Publishing **is** now configured. `socket-sdk/build.gradle.kts` applies `maven-publish` with a
`MavenPublication` built `from(components["java"])` and `java { withSourcesJar() }`, so the
publication carries both the jar and a sources jar.

Coordinates: **`com.tutem.platform:socket-sdk:1.0-SNAPSHOT`**

**No remote repository is configured yet** — the `publishing` block has no `repositories {}` entry,
pending a decision on where to host (GitHub Packages / Nexus / Artifactory / CodeArtifact). So
`./gradlew :socket-sdk:publish` has nowhere to go. Cross-repo consumption therefore has exactly two
working paths today.

#### Option 1 — publish to the local Maven repository

In `tutem-platform-libraries`:

```bash
./gradlew :socket-sdk:publishToMavenLocal
```

That installs the jar, the sources jar and the POM into `~/.m2/repository`. Then in the consuming
service (both services use the **Groovy** DSL):

```groovy
// tutem-user-service-dev/build.gradle  (and tutem-rider-service-dev/build.gradle)
repositories {
    mavenLocal()      // must come before mavenCentral()
    mavenCentral()
}

dependencies {
    implementation 'com.tutem.platform:socket-sdk:1.0-SNAPSHOT'
}
```

The catch: `~/.m2` is per-machine. Nothing in CI or on a colleague's laptop will resolve this until
a remote repository exists, and you must re-run `publishToMavenLocal` after every SDK change.

#### Option 2 — Gradle composite build (no publishing step)

Preferred for local development, because SDK edits are picked up on the next build with no
intermediate install:

```groovy
// tutem-user-service-dev/settings.gradle
rootProject.name = 'tutem-user-service'
includeBuild('../tutem-platform-libraries-dev/tutem-platform-libraries')
```

```groovy
// tutem-user-service-dev/build.gradle
dependencies {
    implementation 'com.tutem.platform:socket-sdk:1.0-SNAPSHOT'
}
```

Gradle substitutes the module coordinate for the included build's `:socket-sdk` project
automatically, because the group and version match. The relative path above assumes the sibling
checkout layout used in this workspace — adjust it if yours differs.

### What comes along transitively

Three dependencies are `api`, so they land on the consumer's **compile** classpath. Each is there
because it appears in a signature a consumer writes against:

| Dependency | Why `api` |
|---|---|
| `com.corundumstudio.socketio:netty-socketio:2.0.12` | `SocketIOClient`, `AckRequest`, `HandshakeData` appear in every handler signature and in `SocketAuthenticationHook` / `SocketErrorHandler`. You cannot write a handler without them. |
| `org.springframework:spring-context:6.1.12` | `SocketClientConnectedEvent` and the other three events extend `org.springframework.context.ApplicationEvent`, so any `@EventListener` for them compiles against spring-context. |
| `com.fasterxml.jackson.core:jackson-databind:2.17.2` | `JsonSocketMessageSerializer(ObjectMapper)` is a public constructor a consumer calls to supply its own `ObjectMapper`. |

`spring-boot-autoconfigure` and `slf4j-api` are `implementation` — internal to the SDK.

Actuator and micrometer-tracing are `compileOnly` **in the SDK** and are therefore *not*
transitive. A consumer without them starts fine and gets `NoOpSocketMetrics` /
`NoOpSocketTracing`. A consumer that wants socket metrics or spans must add them itself — see
[Metrics and tracing](#metrics-and-tracing).

#### ⚠ Netty is silently downgraded in the consuming services

`netty-socketio:2.0.12` pins netty to **`4.1.114.Final`** (its pom sets
`<netty.version>4.1.114.Final</netty.version>` for `netty-buffer`, `netty-common`,
`netty-transport`, `netty-handler`, `netty-codec`, `netty-codec-http` and
`netty-transport-native-epoll`).

Both consuming services (`tutem-user-service-dev`, `tutem-rider-service-dev`) apply
`io.spring.dependency-management` 1.1.6 with the Spring Boot **3.3.3** plugin. The Spring Boot
3.3.3 BOM sets `<netty.version>4.1.112.Final</netty.version>`, and `io.spring.dependency-management`
applies BOM versions as *forced* Maven-style dependency management — it **overrides** the
transitive `4.1.114.Final` rather than resolving to the higher version the way Gradle's own
conflict resolution would.

Net effect: the transport library underneath netty-socketio is quietly rolled back two patch
versions in the consumer, with no warning. Pin it back if that matters:

```groovy
// consumer build.gradle — Groovy DSL
ext['netty.version'] = '4.1.114.Final'
```

`io.spring.dependency-management` reads that property and substitutes it for the BOM's value across
every netty module at once, so the whole netty stack stays consistent. Verify with
`./gradlew dependencies --configuration runtimeClasspath | grep netty`.

---

## Quick start

### 1. Opt in with `@EnableSocket`

Nothing happens until you do this. The jar on the classpath opens no port and creates no beans.

```java
import com.tutem.platform.socket.annotations.EnableSocket;

@SpringBootApplication
@EnableSocket
public class UserServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(UserServiceApplication.class, args);
    }
}
```

`@EnableSocket` imports a `SocketMarker` bean; `SocketAutoConfiguration` is
`@ConditionalOnBean(SocketMarker.class)` and `@ConditionalOnProperty(app.socket.enabled)` with
`matchIfMissing = true`. So: annotation present → SDK on; `app.socket.enabled=false` → SDK off
even with the annotation (handy in tests and in profiles that do not need realtime traffic).

### 2. Minimal `application.yml`

> **`app.socket.auth.enabled` defaults to `true`.** You must therefore do **one** of the two
> things below, or the application will not start.

**Option A — production, and the default.** Provide a `SocketAuthenticationHook` bean (step 3).
No `auth` configuration is needed at all:

```yaml
app:
  socket:
    port: 9090
```

**Option B — local dev / tests, no hook yet.** Turn authentication off explicitly:

```yaml
app:
  socket:
    port: 9090
    auth:
      enabled: false     # anonymous connections accepted — LOCAL DEV ONLY
```

If you do neither, startup fails fast:

```text
java.lang.IllegalStateException: app.socket.auth.enabled=true (the default) but no
SocketAuthenticationHook bean was found, so every connection would be rejected. Either provide a
@Component implementing SocketAuthenticationHook, or set app.socket.auth.enabled=false to accept
anonymous connections (local development only - it leaves the socket port unauthenticated).
```

That is intentional — the SDK will not silently open an unauthenticated port. With Option B the
SDK logs a `WARN` on every boot so the state cannot be mistaken for production-ready.

On a successful start you will see roughly:

```text
Socket server configured on 0.0.0.0:9090 transports=[websocket, polling] origin=*
Registered @OnMessage("joinRoom") -> RideSocketHandler.joinRoom(JoinRoomPayload)
Socket server started on port 9090
```

### 3. An authentication hook

```java
import com.corundumstudio.socketio.HandshakeData;
import com.tutem.platform.socket.authentication.SocketAuthContext;
import com.tutem.platform.socket.authentication.SocketAuthenticationHook;
import com.tutem.platform.socket.exception.SocketAuthException;

@Component
@RequiredArgsConstructor
public class JwtSocketAuthHook implements SocketAuthenticationHook {

    private final JwtService jwtService;

    @Override
    public SocketAuthContext authenticate(HandshakeData handshakeData) throws SocketAuthException {
        String token = handshakeData.getSingleUrlParam("token");
        if (token == null || token.isBlank()) {
            throw new SocketAuthException("Missing token");
        }
        return SocketAuthContext.of(jwtService.extractUserId(token), jwtService.extractRole(token));
    }
}
```

`SocketAuthContext` is a Lombok `@Builder` with `userId`, `role` and a `Map<String, Object> claims`
(default `Map.of()`). Every claim entry is copied into `SocketSession.attributes`. Use
`SocketAuthContext.of(userId, role)` for the common case, or the builder when you have claims:

```java
return SocketAuthContext.builder()
        .userId(userId)
        .role(role)
        .claims(Map.of("tenantId", tenantId))
        .build();
```

The connect path **fails closed**: any exception out of `authenticate` — not only
`SocketAuthException` — is counted as an auth failure and disconnects the client. With
`auth.enabled=true`, returning `null`, or a context whose `userId` is `null`/blank, is also a
rejection (a session with no `userId` can never be reached by `sendToUser`, so it is useless).

### 4. Handlers

```java
import com.corundumstudio.socketio.AckRequest;
import com.corundumstudio.socketio.SocketIOClient;
import com.tutem.platform.socket.annotations.*;
import com.tutem.platform.socket.session.SocketSession;

@Component
@RequiredArgsConstructor
public class RideSocketHandler {

    private final RideService rideService;

    @OnConnect
    public void onConnect(SocketIOClient client, SocketSession session) {
        log.info("connected userId={} role={}", session.getUserId(), session.getRole());
    }

    @OnMessage("joinRoom")
    public void joinRoom(SocketIOClient client, JoinRoomPayload payload, AckRequest ack) {
        client.joinRoom(payload.getRoomId());
        if (ack.isAckRequested()) {
            ack.sendAckData(Map.of("status", "joined"));
        }
    }

    @OnMessage("locationUpdate")
    public void locationUpdate(LocationPayload payload) {   // payload only is fine
        rideService.record(payload);
    }

    @OnDisconnect
    public void onDisconnect(SocketIOClient client, SocketSession session) {
        // session may be null if the client was never registered
    }

    @OnError
    public void onError(SocketIOClient client, String event, Exception ex) {
        log.warn("socket handler failed event={}", event, ex);
    }
}
```

Handlers are found on any non-abstract, non-`@Lazy`, singleton Spring bean — `@Component`,
`@Service`, `@Bean` method result, anything. Beans behind a **Spring** AOP proxy
(`@Transactional`, `@Async`, `@Validated`) work: annotations are read from the AOP target class,
and the invocation still goes through the proxy so the advice runs. A hand-rolled non-Spring JDK
proxy is the one case that does not work — see
[Limitations](#limitations--not-implemented).

### 5. Push events out from anywhere

```java
import com.tutem.platform.socket.dispatcher.MessageDispatcher;

@Service
@RequiredArgsConstructor
public class RideNotifier {

    private final MessageDispatcher dispatcher;

    public void rideAssigned(String userId, RideDto ride) {
        dispatcher.sendToUser(userId, "rideAssigned", ride);   // fans out to ALL the user's devices
    }

    public void rideUpdated(String roomId, RideDto ride) {
        dispatcher.sendToRoom(roomId, "rideUpdate", ride);
    }

    public void systemAlert(AlertDto alert) {
        dispatcher.broadcast("systemAlert", alert);
    }
}
```

---

## Configuration reference

Every key lives under `app.socket.`. Values below are the real defaults from
`SocketProperties.java`.

### Server

| Key | Type | Default | Meaning |
|---|---|---|---|
| `enabled` | `boolean` | `true` | Master switch. `false` disables the whole SDK even when `@EnableSocket` is present — no beans, no port. Useful in tests. |
| `port` | `int` | `9090` | TCP port the Socket.IO server listens on. Separate from the HTTP `server.port`. |
| `host` | `String` | `0.0.0.0` | Bind address. `0.0.0.0` listens on every interface. |
| `boss-count` | `int` | `1` | Netty boss (acceptor) thread count. |
| `worker-count` | `int` | `100` | Netty worker thread count. Handlers run on these threads, so this is the concurrency ceiling for handler execution. |
| `transports` | `List<String>` | `[websocket, polling]` | Transports the server accepts. Legal values: `websocket`, `polling` (case-insensitive, duplicates collapsed). An empty list or an unknown name throws `IllegalArgumentException` at startup. Keep `polling` unless you are certain: standard Socket.IO clients (including Flutter `socket_io_client` defaults) start on long-polling and then upgrade. |
| `origin` | `String` | `null` | **Sets a CORS response header. It is not access control and rejects nothing.** When set, netty-socketio emits `Access-Control-Allow-Origin: <value>` and `Access-Control-Allow-Credentials: true` on polling HTTP responses. When unset, it echoes the request's own `Origin` back (or `*` when there is none). No connection is ever refused because of this value — see [Security](#security). |
| `allow-custom-requests` | `boolean` | `false` | Whether non-Socket.IO HTTP requests arriving on this port are passed to custom handlers. `false` means the socket port only speaks Socket.IO. |
| `upgrade-timeout` | `int` (ms) | `10000` | How long a client may take to upgrade from polling to websocket. |
| `ping-timeout` | `int` (ms) | `60000` | Time without a client pong before the connection is considered dead. |
| `ping-interval` | `int` (ms) | `25000` | Interval between server pings. |
| `startup-phase` | `int` | `2147482623` (`Integer.MAX_VALUE - 1024`) | `SmartLifecycle` phase in which the socket server starts and stops. The default starts it after Spring Boot's web server and makes shutdown participate in graceful-shutdown ordering. Change it only if something must start after the socket port opens. |

### Authentication

| Key | Type | Default | Meaning |
|---|---|---|---|
| `auth.enabled` | `boolean` | **`true`** | When `true` (the default), a `SocketAuthenticationHook` bean is **required** — startup fails with `IllegalStateException` if there is none — it is called on every connect, a `null`/blank `userId` is rejected, and inbound messages from a client with no stored session are refused. Set it to `false` **only** for local development or tests: that installs the allow-all `DefaultSocketAuthenticationHook`, accepts every connection as anonymous, and logs a `WARN` at startup. See [Security](#security). |

### Metrics and tracing

| Key | Type | Default | Meaning |
|---|---|---|---|
| `metrics.enabled` | `boolean` | `true` | Publish socket metrics when micrometer-core is on the classpath **and** a `MeterRegistry` bean exists. `false` forces `NoOpSocketMetrics`. Every outcome is logged at startup, so you never have to guess which implementation you got. |
| `tracing.enabled` | `boolean` | `true` | Wrap each handler invocation in a span when micrometer-tracing is on the classpath **and** a `Tracer` bean exists. `false` forces `NoOpSocketTracing`. Also logged at startup. |

### Heartbeat

| Key | Type | Default | Meaning |
|---|---|---|---|
| `heartbeat.cleanup-interval-seconds` | `int` | `30` | How often the sweep runs. Clamped to a minimum of 1. |
| `heartbeat.stale-session-seconds` | `int` | `120` | Idle threshold, in seconds, used **only** when `disconnect-stale-sessions` is `true`. Clamped to a minimum of 1. |
| `heartbeat.disconnect-stale-sessions` | `boolean` | `false` | When `true`, the sweep also force-disconnects clients whose session has been idle past the threshold. Leave it `false` unless you know your clients send inbound traffic regularly — see [Heartbeat](#heartbeat-what-it-actually-does). |
| `heartbeat.evict-ghost-sessions` | `boolean` | `true` | When `true`, the sweep deletes store entries whose Socket.IO client id is unknown to **this pod's** netty server (or whose channel is closed). Correct for the default `InMemorySessionManager`. **Must be set to `false` if you replace `SessionManager` with a shared/distributed store across more than one pod** — otherwise every pod deletes every other pod's sessions on every pass. See the warning under [Heartbeat](#-ghost-eviction-is-local-server-scoped--it-breaks-a-shared-sessionmanager). |

### Everything at once

```yaml
app:
  socket:
    enabled: true
    port: 9090
    host: 0.0.0.0
    boss-count: 1
    worker-count: 100
    transports: [websocket, polling]
    origin: https://app.tutem.example   # CORS response header only — NOT access control

    allow-custom-requests: false
    upgrade-timeout: 10000
    ping-timeout: 60000
    ping-interval: 25000
    startup-phase: 2147482623
    auth:
      enabled: true
    metrics:
      enabled: true
    tracing:
      enabled: true
    heartbeat:
      cleanup-interval-seconds: 30
      stale-session-seconds: 120
      disconnect-stale-sessions: false
      evict-ghost-sessions: true      # set false with a shared/distributed SessionManager
```

---

## Security

**`app.socket.auth.enabled` defaults to `true`.** The SDK will not start an unauthenticated socket
server by accident: with the default and no `SocketAuthenticationHook` bean, context refresh fails
with `IllegalStateException`.

Setting `app.socket.auth.enabled=false` is the deliberate opt-out. It installs
`DefaultSocketAuthenticationHook`, which accepts every connection as anonymous — no `userId`, no
`role` — so anything that can reach the socket port gets a session. A `WARN` naming the host and
port is logged on every boot in that state. **Use it for local development and tests only.**

Any deployment reachable beyond localhost must:

1. Leave `app.socket.auth.enabled` at its default (`true`) and provide a
   `SocketAuthenticationHook` bean. Startup fails fast when the property is on and no hook is
   present, so this cannot be forgotten silently. **This is the only application-level access
   control the SDK has.** Never set it to `false` in a deployed environment.
2. Restrict who can reach the port at the network layer — Kubernetes `NetworkPolicy`, a security
   group, a gateway. The socket port is a second listener that your HTTP ingress rules do not
   cover.
3. Leave `app.socket.allow-custom-requests: false` (the default) so the socket port only serves
   Socket.IO and does not become a second, unguarded HTTP surface.

### `app.socket.origin` is **not** a security control

This was previously documented as a CORS lock-down. It is not. Setting it restricts nothing and
rejects nothing.

Verified against netty-socketio 2.0.12:

- `Configuration.getOrigin()` has exactly one reader: `EncoderHandler.addOriginHeaders`. It writes
  the `Access-Control-Allow-Origin` and `Access-Control-Allow-Credentials` **response headers** on
  HTTP (polling) responses. That is its entire effect.
- `AuthorizeHandler` reads the request's `Origin` header, stores it as a channel attribute (for the
  handler above to echo) and passes a `xdomain` boolean into `HandshakeData`. It **never compares it
  to the configured value and never refuses a connection because of it.** The only handshake
  rejections there come from the `AuthorizationListener`, an unknown/unsupported `transport`, and
  a missing session id.
- Browsers do not apply CORS to the WebSocket transport at all, so even the header has no effect
  once a client has upgraded.
- Any non-browser client — a Flutter app, `curl`, a script — ignores response headers entirely.

So: set `origin` if you want correct CORS headers for a browser doing long-polling with
credentials. Do not set it expecting it to keep anyone out. **Authentication (item 1) and network
policy (item 2) are the only real controls.**

Note also that with `origin` unset the server does not send a restrictive header — it echoes the
caller's own `Origin` back with `Access-Control-Allow-Credentials: true`, or `*` when the request
carries no `Origin`.

Two more properties of the auth path worth knowing:

- It fails closed. Any exception from the hook — `SocketAuthException`, an expired-JWT
  `RuntimeException`, an `NPE` — increments `socket.auth.failures` and disconnects the client.
- With auth enabled, an inbound message from a client that has no stored session (e.g. it
  connected before a restart) is dropped and the client is disconnected rather than dispatched.

`SocketAuthContext.role` is stored on the session and readable from handlers as
`session.getRole()`. The SDK performs no authorization of its own: role checks belong in your
handlers.

---

## Handler reference

### `@OnMessage("eventName")`

Parameter binding is **positional-by-type**, so any order works. Each parameter is filled
according to its own type:

| Parameter type | Bound to |
|---|---|
| `SocketIOClient` (or a subtype) | the connected client |
| `AckRequest` (or a subtype) | the ack request for this message |
| anything else | the deserialized payload |

All of these are equivalent in effect:

```java
@OnMessage("joinRoom") void a(SocketIOClient client, JoinRoomPayload p, AckRequest ack) { }
@OnMessage("joinRoom") void b(AckRequest ack, JoinRoomPayload p, SocketIOClient client) { }
@OnMessage("joinRoom") void c(JoinRoomPayload p) { }
@OnMessage("joinRoom") void d(SocketIOClient client) { }
```

Rejected **at startup** with an `ERROR` log, and simply not registered (the event then has no
listener at all — check your logs):

- **Zero parameters.** There is nothing to bind.
- **Two or more non-client, non-ack parameters.** The payload parameter is ambiguous.
- A blank event name, or a `static` method.

This is a behaviour change from earlier versions, which failed per message at traffic rate
instead. Validation now happens once, when handlers are scanned.

Deserialization: the SDK registers every listener as `Object.class` and converts the raw value
itself via `SocketMessageSerializer`. A conversion failure throws `SocketException` and travels
the normal error path — the handler is never invoked with a silently `null` payload.

Acks are never sent automatically; that would change message semantics. If a client requests an
ack for an event whose handler declares no `AckRequest` parameter, the SDK logs that once per
event name at `DEBUG` so the gap is at least visible.

### `@OnConnect` / `@OnDisconnect`

Called after authentication succeeds and the session has been saved (connect), and after the
session has been removed from the store (disconnect).

**Signatures are validated at registration.** The only two values the SDK can supply are the
client and the session, so the *only* legal parameters are `SocketIOClient` and `SocketSession` —
any combination, in any order, including no parameters at all:

```java
@OnConnect void a(SocketIOClient client, SocketSession session) { }
@OnConnect void b(SocketSession session, SocketIOClient client) { }
@OnConnect void c(SocketSession session) { }
@OnConnect void d() { }
```

Anything else is logged as an `ERROR` and **not registered** — the method is then never called at
all, so check your logs:

```java
@OnConnect void bad(SocketIOClient client, String userId) { }   // rejected: String
@OnConnect void alsoBad(Object thing) { }                        // rejected: Object
```

Note the types are matched **exactly**, not by assignability: a subtype of `SocketIOClient` is
rejected here (unlike in `@OnMessage`, where `SocketIOClient` subtypes bind fine). Declare the
parameter as `SocketIOClient`.

> **Behaviour change.** Previously any other parameter type was silently bound to `null`, and an
> `Object`-typed parameter silently received the `SocketSession`. The first surfaced as an NPE
> inside consumer code on the first connection; the second worked by accident. Both are now
> rejected at startup instead.

On disconnect the session may still be `null` — no session was found for that client — so
null-check it.

### `@OnError`

One signature, validated at startup:

```java
@OnError
public void onError(SocketIOClient client, String event, Exception ex) { }
```

Three parameters exactly: a `SocketIOClient` (or subtype), a `String` event name, and a type that
`Exception` can be assigned to (`Exception` — use this — or `Throwable`/`Object`). Anything else is
logged as an `ERROR` and not registered. All registered `@OnError` handlers are
called, in registration order, for every handler failure — including failures inside `@OnConnect`
and `@OnDisconnect` methods, where `event` is `"connect"` or `"disconnect"`.

### The error pipeline

A handler exception (or a deserialization failure) goes, in order:

1. `SocketMetrics.incrementError(event)`
2. every `@OnError` handler (a throwing `@OnError` handler is logged and does not stop the rest)
3. the `SocketErrorHandler` bean — `DefaultSocketErrorHandler` logs the error and sends an
   `error` event back to the client with `{ "event": ..., "message": ... }`

Replace step 3 by declaring your own `SocketErrorHandler` bean.

### Heartbeat: what it actually does

`HeartbeatManager` runs on its own daemon thread named `socket-heartbeat` — the SDK does **not**
require `@EnableScheduling` in your service. Every `cleanup-interval-seconds` it walks the session
store and:

- **Evicts ghost sessions** — entries whose netty client no longer exists or whose channel is
  closed. This is the primary job and is always safe, because it is driven by the transport's own
  view of the connection.
- **Optionally disconnects idle sessions** — only when
  `heartbeat.disconnect-stale-sessions=true`.

Why the idle disconnect is opt-in and defaults to `false`: `SocketSession.lastActiveAt` only
advances on *inbound* messages. A perfectly healthy listen-only subscriber — the normal Flutter
client that connects, subscribes and then waits for pushes — looks idle forever. With the old
always-on behaviour those clients were force-disconnected every two minutes. Turn it on only if
your clients are known to send traffic more often than `stale-session-seconds`.

`cleanupStaleSessions()` is public, so a test or an admin endpoint can trigger one pass directly.

#### ⚠ Ghost eviction is **local-server-scoped** — it breaks a shared `SessionManager`

"Ghost" means `server.getClient(uuid)` returned `null` (or a closed channel) on **this JVM's**
`SocketIOServer`. It does not mean "this session is dead"; it means "this pod does not own this
session".

With the default `InMemorySessionManager` that distinction does not exist, so the sweep is correct.
The moment you substitute a shared/distributed `SessionManager` — the Redis extension point that
[Extension points](#extension-points) and `ARCHITECTURE.md` describe — across more than one pod, it
becomes actively destructive: every pod sees every *other* pod's sessions in `getAll()`, finds no
local netty client for them, and evicts them. Every pod does this every
`cleanup-interval-seconds`, so the shared store is continuously torn down and presence data never
survives a cycle.

**A shared `SessionManager` across multiple pods therefore requires:**

```yaml
app:
  socket:
    heartbeat:
      evict-ghost-sessions: false
```

With that set, a session whose client is unknown locally is left alone (logged at `DEBUG`), and
`HeartbeatManager` logs an `INFO` at startup confirming ghost eviction is off. If both
`evict-ghost-sessions` and `disconnect-stale-sessions` are `false`, the sweep returns immediately
and does no work at all. The alternative is a pod-aware sweep of your own — store an owning-pod id
on the session and skip foreign ones — by replacing the `HeartbeatManager` bean.

The previous version of this section said that session ids which are not netty client UUIDs "are
skipped rather than evicted blindly". That is true (`HeartbeatManager.parseClientId` returns `null`
for a non-UUID id and the entry is left alone) but it is **not** the safety property it sounds like:
a distributed `SessionManager` that reuses netty's UUID session ids — which is the natural design,
since `MessageDispatcher.sendToSession` parses the id as a UUID — parses fine on every pod and is
evicted on every pod. Do not read that skip as protection for the multi-pod case.

And note what turning the sweep off does **not** buy you: rooms, `broadcast` and
`getClientsInRoom` still do not cross pods, because they bypass `SessionManager` entirely and talk
to the local `SocketIOServer`. See [Limitations](#limitations--not-implemented).

---

## Pushing events out

`MessageDispatcher` (injectable anywhere):

| Method | Behaviour |
|---|---|
| `sendToUser(String userId, String event, Object payload)` | Looks up **all** live sessions for the user via `SessionManager.findAllByUserId` and sends to each — multi-device works. Logs at `DEBUG` and does nothing when the user has no live session. |
| `sendToSession(String sessionId, String event, Object payload)` | Sends to one session. No-ops when the id is `null`, unparseable, unknown, or the channel is closed. |
| `sendToRoom(String room, String event, Object payload)` | Sends to a netty-socketio room. |
| `broadcast(String event, Object payload)` | Sends to every connected client. |

A `null` payload sends the event with no data.

`ConnectionManager` for connection state and rooms:

| Method | Behaviour |
|---|---|
| `isConnected(String userId)` | `true` when the user has at least one live session. |
| `getConnectedCount()` | Number of live sessions (not distinct users). |
| `joinRoom(String sessionId, String room)` / `leaveRoom(String sessionId, String room)` | Room membership for one session; no-op if the client is gone. |
| `getClientsInRoom(String room)` | `Collection<SocketIOClient>` in the room. |
| `disconnect(String userId)` | Disconnects **all** of that user's live sessions. |

`SessionManager` if you need the session objects themselves: `findBySessionId`, `findAllByUserId`,
`getAll` (immutable snapshot), `removeBySessionId`, `isConnected`, `getConnectedCount`.

> `findByUserId(String)` is `@Deprecated`. It returns an arbitrary one of the user's sessions and
> ignores the rest. Use `findAllByUserId`.

`SocketSession` exposes `getSessionId()`, `getUserId()`, `getRole()`, `getRemoteAddress()`,
`getConnectedAt()`, `getLastActiveAt()`, `getAttribute(String)`, `setAttribute(String, Object)`
and `getAttributes()` (an immutable snapshot copy). `setAttribute` ignores `null` keys and values.

---

## Spring events

Published on the standard `ApplicationEventPublisher`, so `@EventListener` works:

| Event | When | Accessor |
|---|---|---|
| `SocketServerStartedEvent` | after handlers are registered and the port is open | `getPort()` |
| `SocketServerStoppedEvent` | after the server has stopped **cleanly** — if `SocketIOServer.stop()` throws, the failure is logged and this event is *not* published, because the port may still be listening | — |
| `SocketClientConnectedEvent` | after the session is saved, before `@OnConnect` handlers | `getSession()` |
| `SocketClientDisconnectedEvent` | after the session is removed, before `@OnDisconnect` handlers; only published when a session was found | `getSession()` |

```java
@EventListener
public void onSocketClientConnected(SocketClientConnectedEvent event) {
    presenceService.markOnline(event.getSession().getUserId());
}
```

Client connect/disconnect events are published on netty worker threads. Keep listeners fast or
make them `@Async`.

---

## Metrics and tracing

Both are interfaces owned by the SDK — `SocketMetrics` and `SocketTracing` — with a no-op and a
Micrometer implementation each. Actuator and micrometer-tracing are `compileOnly` in the SDK, so
**a consumer without them starts fine** and gets `NoOpSocketMetrics` / `NoOpSocketTracing`. No
Micrometer type is loaded in that case, and `NoOpSocketTracing.startSpan` just runs the work
directly.

### What you need to add

Nothing is transitive here. To get real meters:

```groovy
// consumer build.gradle
implementation 'org.springframework.boot:spring-boot-starter-actuator'
```

and for spans, additionally a **tracing bridge** — `micrometer-tracing` on its own contributes no
`Tracer` bean:

```groovy
implementation 'io.micrometer:micrometer-tracing-bridge-brave'   // or -bridge-otel
```

### How the implementation is chosen

`SocketAutoConfiguration` decides in two steps.

1. **Is the Micrometer type on the classpath?** `MeterRegistry` / `Tracer` presence is a
   `@ConditionalOnClass` check on a nested configuration class
   (`MicrometerMetricsConfiguration`, `MicrometerTracingConfiguration`). Because the check is on
   the nested class, its body — and every Micrometer type it names — is never loaded when the
   library is absent. If the class is absent, the enclosing configuration's
   `noOpSocketMetrics` / `noOpSocketTracing` fallback beans supply the no-ops; those carry
   `@ConditionalOnMissingClass` so they and the nested configuration can never both match.
2. **Does a `MeterRegistry` / `Tracer` bean actually exist?** Resolved *inside* the bean method
   through an `ObjectProvider<MeterRegistry>` / `ObjectProvider<Tracer>`, plus a check of
   `app.socket.metrics.enabled` / `app.socket.tracing.enabled`. Any of "property disabled", "no
   such bean", or "several candidates and none `@Primary`" yields the no-op implementation, and
   each case logs which one you got.

Every outcome is therefore visible in the startup log, e.g.:

```text
Micrometer is on the classpath but no MeterRegistry bean is present - socket metrics disabled
micrometer-tracing is on the classpath but no Tracer bean is present (no tracing bridge configured) - socket tracing disabled
Socket metrics disabled (app.socket.metrics.enabled=false)
```

> **Why not `@ConditionalOnBean(MeterRegistry.class)`?** Because it does not work here, and until
> now this README claimed it did. `@ConditionalOnBean` is evaluated when the auto-configuration is
> *processed*, and Spring Boot's `AutoConfigurationSorter` orders auto-configurations
> alphabetically before applying `@AutoConfiguration(before/after)` hints — so
> `com.tutem.platform.socket.config.SocketAutoConfiguration` is processed *before*
> `org.springframework.boot.actuate.autoconfigure.metrics.*`. At that moment no `MeterRegistry`
> bean definition exists, the condition does not match, and the consumer silently gets the no-op
> implementation: no meters, no spans, no warning. Resolving through `ObjectProvider` defers the
> lookup to bean *instantiation* time, after every auto-configuration has contributed its
> definitions, which makes the outcome independent of ordering. If you were on an earlier build
> and wondered where your `socket.*` meters went — that was this bug.

Metrics published by `MicrometerSocketMetrics`:

| Meter | Type | Tags |
|---|---|---|
| `socket.connections.active` | gauge | — |
| `socket.connections.total` | counter | — |
| `socket.disconnections.total` | counter | — |
| `socket.messages.received` | counter | `event` |
| `socket.errors.total` | counter | `event` |
| `socket.auth.failures` | counter | — |

Event names come from clients, so `event` tag cardinality is capped at 100 distinct values per
counter family; everything beyond that folds into `event="other"`. A `null`/empty event name is
recorded as `event="unknown"`.

Tracing wraps each inbound message in a span named `socket.<eventName>`.

---

## Extension points

Every bean the SDK declares is `@ConditionalOnMissingBean`. To replace one, declare your own bean
of that type — no `@Primary` needed. Because activation runs through the marker bean in the
deferred auto-configuration phase, your bean is always seen first.

| Type | SDK default | Replace it to… |
|---|---|---|
| `SocketAuthenticationHook` | `DefaultSocketAuthenticationHook` (allow-all, installed **only** when `auth.enabled=false`; with the default `true` the SDK's factory method throws `IllegalStateException` at startup) | authenticate the handshake (JWT, API key, …) — required in any deployed environment |
| `SessionManager` | `InMemorySessionManager` | change where sessions live. **A shared/distributed store also requires `app.socket.heartbeat.evict-ghost-sessions=false`** — read [Heartbeat](#-ghost-eviction-is-local-server-scoped--it-breaks-a-shared-sessionmanager) and [Limitations](#limitations--not-implemented) before reaching for Redis |
| `SocketErrorHandler` | `DefaultSocketErrorHandler` (logs, emits `error` to the client) | change what the client is told, add alerting |
| `SocketMessageSerializer` | `JsonSocketMessageSerializer` (Jackson, uses your `ObjectMapper` bean) | use another payload format |
| `SocketMetrics` | `NoOpSocketMetrics`, or `MicrometerSocketMetrics` when available | publish elsewhere, or change tag policy |
| `SocketTracing` | `NoOpSocketTracing`, or `MicrometerSocketTracing` when available | integrate a different tracing stack |
| `SocketIOServer` | built from `SocketProperties` | set a netty-socketio option the SDK does not expose |
| `MessageDispatcher` | `MessageDispatcher` | change outbound fan-out |
| `ConnectionManager` | `ConnectionManager` | change room/connection semantics |
| `HeartbeatManager` | `HeartbeatManager` | change the ghost/idle sweep |
| `MessageHandlerRegistry` | `MessageHandlerRegistry` | change scanning or dispatch (rarely a good idea) |
| `SocketServerLifecycle` | `SocketServerLifecycle` | change start/stop ordering (prefer `app.socket.startup-phase`) |

```java
@Configuration
public class SocketOverrides {

    @Bean
    public SocketErrorHandler socketErrorHandler(AlertService alerts) {
        return (client, event, ex) -> {
            alerts.notify(event, ex);
            client.sendEvent("error", Map.of("event", event, "message", "Request failed"));
        };
    }
}
```

---

## Relationship to netty-socketio

**This SDK does not abstract netty-socketio away, and it is not protocol-agnostic.**
netty-socketio types appear in the API you write against:

- `SocketAuthenticationHook.authenticate(HandshakeData)`
- `SocketErrorHandler.handle(SocketIOClient, String, Exception)`
- `@OnMessage` / `@OnConnect` / `@OnDisconnect` / `@OnError` parameters: `SocketIOClient`,
  `AckRequest`
- `MessageHandlerRegistry` registers listeners on `SocketIOServer` directly

That is why `netty-socketio` is an `api` dependency rather than `implementation`.

The trade-off is deliberate. Hiding `SocketIOClient` would mean re-inventing rooms, acks,
handshake access and per-client operations behind a thinner, lossier facade — and every real
handler eventually needs one of them. The cost is that swapping the transport would be a
breaking change for consumers, and that consuming services do compile against netty-socketio.
The SDK's value is the Spring wiring, session model, auth path, error pipeline and lifecycle
ordering — not transport independence.

---

## Limitations / not implemented

**Single-pod only.** Nothing in the SDK crosses pods:

- `InMemorySessionManager` holds sessions in `ConcurrentHashMap`s on the JVM heap. Pod A does not
  know about pod B's sessions, so `sendToUser` reaches only the sessions on the pod that runs it.
- `sendToRoom`, `broadcast` and `ConnectionManager.getClientsInRoom` go straight to
  netty-socketio's room/broadcast operations, which are pod-local. No Redisson `StoreFactory` /
  `PubSubStore` is configured, so rooms do not span pods either.
- Consequence: run one replica, or pin a client's connections to one pod (sticky sessions), or
  fan out through your own message bus. `ARCHITECTURE.md` spells out what a distributed store
  would and would not fix.

**No remote artifact repository.** `maven-publish` is configured and
`./gradlew :socket-sdk:publishToMavenLocal` works, but the `publishing` block declares no
`repositories {}` target, so there is nothing to publish *to*. Cross-repo and CI consumption need
either `mavenLocal()` per machine or a composite build — see
[How to depend on it](#from-another-repository-tutem-user-service-dev-tutem-rider-service-dev).

**No `RedisSessionManager`.** The `SessionManager` interface exists precisely so one can be
written, and the javadoc on `InMemorySessionManager` sketches the bean override — but no such
implementation ships here.

**No automatic acks.** If a client requests an ack, a handler must declare an `AckRequest`
parameter and call `sendAckData` itself. The SDK only logs the gap.

**No authorization model.** The SDK authenticates the handshake and stores `userId`/`role`. Any
per-event permission check is yours to write.

**No reconnection or replay semantics.** A disconnect removes the session; nothing is buffered
for a client that comes back.

**Restart-in-place is not supported.** netty-socketio's `Namespace` appends every listener to a
queue that `SocketIOServer.stop()` never clears, so a `context.stop(); context.start()` cycle would
leave the old listeners attached: every `@OnMessage` would fire twice and every connect would save
the session twice. `MessageHandlerRegistry.registerAll()` is therefore **single-shot** — a second
call logs a `WARN` and does nothing, rather than silently double-registering. Restarting the socket
server requires a fresh application context, not a `Lifecycle` stop/start.

**Handlers behind a hand-rolled (non-Spring) JDK proxy are not discovered.** Spring's own AOP
proxies are fine: annotations are read from `AopProxyUtils.ultimateTargetClass(bean)` and the
invocation goes through the proxy so `@Transactional`/`@Async`/`@Validated` advice still runs. But
`ultimateTargetClass` can only unwrap a proxy that implements Spring's `Advised` contract. A bean
that is a bare `java.lang.reflect.Proxy` built by hand (or by a third-party library) exposes only
its interfaces, so annotations declared on the implementation class are invisible and those
handlers are never registered.

This is now **logged at `WARN`** naming the bean and its proxy class, rather than being silently
skipped. Fix it by declaring the annotations on an interface the proxy exposes, or by registering
the bean unproxied.

**A handler that throws an `Error` escapes into the netty pipeline.**
`MessageHandlerRegistry.handleMessage` catches `Exception`, not `Throwable`. An
`OutOfMemoryError`, `StackOverflowError`, `NoClassDefFoundError` or a failing `assert` inside a
handler therefore bypasses the SDK's error pipeline entirely: no `socket.errors.total` increment,
no `@OnError` handler, no `error` event to the client. It propagates to netty-socketio's own
exception handling. This is deliberate to the extent that catching `Error` and continuing is
usually wrong — but it means the error pipeline is not a total guarantee, and a handler that can
throw `Error` should be wrapped by its author.

### Not implemented / roadmap (do not depend on these)

- A remote artifact repository (GitHub Packages / Nexus / Artifactory / CodeArtifact) so CI and
  other machines can resolve `com.tutem.platform:socket-sdk:1.0-SNAPSHOT` without
  `publishToMavenLocal`.
- A distributed `SessionManager` (Redis) *plus* netty-socketio's Redisson `StoreFactory` and
  `PubSubStore` — together they are what multi-pod actually requires.
- A runnable sample or test application. There is no `socket-sdk-testapp` module.
- Non-JSON `SocketMessageSerializer` implementations (MessagePack, Protobuf).
