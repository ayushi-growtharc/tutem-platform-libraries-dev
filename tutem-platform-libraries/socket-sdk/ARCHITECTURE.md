# socket-sdk — Architecture & Developer Guide

Production-grade, generic WebSocket infrastructure for TUTEM Java microservices.
Built on netty-socketio internally. Developer-facing API is protocol-agnostic.

---

## Table of Contents

1. [What problem does this solve?](#1-what-problem-does-this-solve)
2. [Big picture](#2-big-picture)
3. [Complete project structure](#3-complete-project-structure)
4. [Package-by-package explanation](#4-package-by-package-explanation)
5. [How a service uses this SDK from scratch](#5-how-a-service-uses-this-sdk-from-scratch)
6. [What belongs in the SDK vs your service](#6-what-belongs-in-the-sdk-vs-your-service)
7. [Replacing any SDK component](#7-replacing-any-sdk-component)
8. [Configuration reference](#8-configuration-reference)

---

## 1. What problem does this solve?

Any Java service that needs to talk to Flutter apps in real-time needs a WebSocket server.
Without the SDK, every service would have to:

- Set up netty-socketio from scratch
- Write boilerplate to register event handlers
- Build session management manually
- Handle auth, errors, metrics, tracing themselves — copy-pasted across services

The `socket-sdk` does all of that once. Any service imports it and writes business logic only.

---

## 2. Big picture

```
Flutter Rider App  ──┐
Flutter Driver App ──┤  WebSocket (port 9090)
                     ▼
           Java Service (e.g. tutem-realtime-service)
                imports socket-sdk
                     │
           ┌─────────┼──────────┐
           ▼         ▼          ▼
       @OnConnect  @OnMessage  @OnDisconnect
       (your code) (your code) (your code)
                     │
        SDK handles everything underneath:
        session mgmt, auth, metrics, tracing,
        heartbeat, error handling, serialization
```

The SDK sits invisibly between netty-socketio and your service code.
Your service never imports or touches netty-socketio directly.

---

## 3. Complete project structure

```
socket-sdk/
├── build.gradle
├── ARCHITECTURE.md                          ← this file
└── src/main/java/com/tutem/platform/socket/
    │
    ├── annotations/
    │     EnableSocket.java                  @EnableSocket
    │     OnMessage.java                     @OnMessage("eventName")
    │     OnConnect.java                     @OnConnect
    │     OnDisconnect.java                  @OnDisconnect
    │     OnError.java                       @OnError
    │
    ├── properties/
    │     SocketProperties.java              reads app.socket.* from application.yml
    │
    ├── config/
    │     SocketAutoConfiguration.java       wires all beans together
    │
    ├── session/
    │     SocketSession.java                 one connected client's session data
    │     SessionManager.java                interface: save/find/remove sessions
    │     InMemorySessionManager.java        default implementation (ConcurrentHashMap)
    │
    ├── connection/
    │     ConnectionManager.java             joinRoom, leaveRoom, isConnected, disconnect
    │
    ├── dispatcher/
    │     MessageDispatcher.java             sendToUser, sendToRoom, broadcast
    │     MessageHandlerRegistry.java        scans beans, wires @OnMessage handlers
    │
    ├── serialization/
    │     SocketMessageSerializer.java       interface: deserialize/serialize
    │     JsonSocketMessageSerializer.java   default Jackson implementation
    │
    ├── authentication/
    │     SocketAuthenticationHook.java      interface: plug in your JWT validation
    │     SocketAuthContext.java             result of auth: userId, role, claims
    │     DefaultSocketAuthenticationHook.java  no-op (allows all when auth disabled)
    │
    ├── heartbeat/
    │     HeartbeatManager.java              scheduled cleanup of ghost connections
    │
    ├── exception/
    │     SocketException.java               base exception
    │     SocketAuthException.java           thrown to reject a connection
    │     SocketErrorHandler.java            interface: handle errors from @OnMessage
    │     DefaultSocketErrorHandler.java     logs + sends "error" event to client
    │
    ├── metrics/
    │     SocketMetrics.java                 Micrometer counters and gauges
    │
    ├── tracing/
    │     SocketTracing.java                 OpenTelemetry span per @OnMessage call
    │
    ├── event/
    │     SocketServerStartedEvent.java      Spring event: server started
    │     SocketServerStoppedEvent.java      Spring event: server stopped
    │     SocketClientConnectedEvent.java    Spring event: client connected
    │     SocketClientDisconnectedEvent.java Spring event: client disconnected
    │
    └── lifecycle/
          SocketServerLifecycle.java         starts server after handlers registered,
                                             stops on Spring shutdown
```

---

## 4. Package-by-package explanation

---

### `annotations` — the only thing a developer touches daily

Five annotations. This is the entire public API a service developer uses.

```java
@EnableSocket          // activate the SDK — put on your application class once

@OnConnect             // called when a Flutter client connects
@OnDisconnect          // called when a Flutter client disconnects
@OnMessage("event")    // called when Flutter sends a specific named event
@OnError               // called when any @OnMessage handler throws an exception
```

Supported method signatures for `@OnMessage`:

```java
// payload only
@OnMessage("joinRoom")
public void handle(MyPayload payload)

// client + payload (most common)
@OnMessage("joinRoom")
public void handle(SocketIOClient client, MyPayload payload)

// client + payload + acknowledgement
@OnMessage("joinRoom")
public void handle(SocketIOClient client, MyPayload payload, AckRequest ack)
```

The payload class is auto-detected — it is the parameter that is not `SocketIOClient` or `AckRequest`.

---

### `properties` — all configuration in one place

`SocketProperties` reads `app.socket.*` from `application.yml`.

```yaml
app:
  socket:
    port: 9090                          # Flutter connects on this port
    host: 0.0.0.0                       # listen on all interfaces
    worker-count: 100                   # Netty worker threads
    ping-timeout: 60000                 # ms before silent client is dead
    ping-interval: 25000                # ms between pings
    auth:
      enabled: true                     # require SocketAuthenticationHook bean
    metrics:
      enabled: true                     # Micrometer metrics
    tracing:
      enabled: true                     # OpenTelemetry tracing
    heartbeat:
      stale-session-seconds: 120        # idle session threshold
      cleanup-interval-seconds: 30      # how often cleanup runs
```

---

### `session` — who is currently connected

**`SocketSession`** — represents one connected client:

```
sessionId     → raw Socket.IO connection UUID
userId        → set by SocketAuthenticationHook (who this person is)
remoteAddress → client IP
connectedAt   → when they connected
lastActiveAt  → when they last sent a message (updated on every @OnMessage call)
attributes    → Map<String, Object> for service-specific per-session data
```

**`SessionManager`** — interface with two mappings:
- `userId → sessionId` (find a user's connection by their ID)
- `sessionId → SocketSession` (get full session details)

```java
sessionManager.isConnected("userId");
sessionManager.findByUserId("userId");
sessionManager.findBySessionId("sessionId");
sessionManager.getConnectedCount();
```

**`InMemorySessionManager`** — default, uses `ConcurrentHashMap`.
Replace with a Redis-backed implementation for multi-pod deployments (see section 7).

---

### `connection` — room and connection management

`ConnectionManager` is the high-level API for managing connections and rooms.

Rooms are logical groupings of clients. Put a driver in room `"driverRoom_abc"`,
then push one message to everyone in that room instead of one by one.

```java
connectionManager.isConnected("userId");
connectionManager.joinRoom("sessionId", "driverRoom");
connectionManager.leaveRoom("sessionId", "driverRoom");
connectionManager.getClientsInRoom("driverRoom");
connectionManager.disconnect("userId");           // force-disconnect a user
```

---

### `dispatcher` — sending messages outbound

**`MessageDispatcher`** — inject this anywhere in your service to push events to Flutter:

```java
// push to one specific user
dispatcher.sendToUser("driverId", "newRideRequest", payload);

// push to all clients in a room
dispatcher.sendToRoom("driverRoom_abc", "surgeAlert", payload);

// push to every connected client
dispatcher.broadcast("systemAlert", payload);

// push to a specific session
dispatcher.sendToSession("sessionId", "event", payload);
```

If the user is offline, `sendToUser` silently does nothing — no error, no exception.

**`MessageHandlerRegistry`** — runs once at startup. Scans every Spring bean,
finds annotated methods, and calls `server.addEventListener()` automatically.
Also wires the full connection lifecycle:

```
connect → authenticate → create session → @OnConnect handlers → metrics
message → refresh session activity → start trace → call @OnMessage handler → metrics
disconnect → remove session → @OnDisconnect handlers → metrics
```

---

### `serialization` — converting JSON to Java objects

**`SocketMessageSerializer`** — interface:

```java
<T> T deserialize(Object raw, Class<T> targetType);
String serialize(Object payload);
```

**`JsonSocketMessageSerializer`** — default Jackson implementation.
Handles the common case where netty-socketio deserializes to `LinkedHashMap`
instead of your typed class — converts it properly via `ObjectMapper.convertValue()`.

Replace with binary serialization (MessagePack, Protobuf) via `@Primary` bean (see section 7).

---

### `authentication` — who is allowed to connect

The SDK calls `SocketAuthenticationHook.authenticate()` on every connect,
before any `@OnConnect` handler runs.

**`SocketAuthenticationHook`** — implement this in your service:

```java
@Component
public class JwtSocketAuthHook implements SocketAuthenticationHook {

    public SocketAuthContext authenticate(HandshakeData data) throws SocketAuthException {
        String token = data.getSingleUrlParam("token");
        if (token == null) throw new SocketAuthException("Token missing");

        String userId = jwtService.extractUserId(token);
        String role   = jwtService.extractRole(token);
        return SocketAuthContext.of(userId, role);
    }
}
```

Flutter sends the token as a query param on connect:

```dart
io.io("http://server:9090?token=eyJhbGci...", options)
```

**`SocketAuthContext`** — result of auth: `userId`, `role`, `claims` (extras from token).
Claims are stored in `SocketSession.attributes` for handlers to access.

**Throw `SocketAuthException`** to refuse the connection. SDK disconnects the client immediately.

**`DefaultSocketAuthenticationHook`** — used when `auth.enabled=false`.
Allows all connections through as anonymous. No implementation needed.

---

### `heartbeat` — cleaning up dead connections

**`HeartbeatManager`** runs on a fixed schedule (default every 30 seconds).

A ghost connection = a session that exists in memory/Redis but the underlying
TCP connection is dead (client crashed, phone went offline without clean disconnect).

For each session older than `stale-session-seconds`:
1. Force-disconnect the SocketIOClient
2. Remove from SessionManager

Without this, dead entries accumulate. `sendToUser()` wastes time on them.
`socket.connections.active` metric shows inflated numbers.

---

### `exception` — error handling

**`SocketException`** — base exception for all SDK errors.

**`SocketAuthException`** — throw inside `SocketAuthenticationHook` to reject a connection.

**`SocketErrorHandler`** — implement to customize what happens when a `@OnMessage` handler throws:

```java
@Component
public class MyErrorHandler implements SocketErrorHandler {
    public void handle(SocketIOClient client, String event, Exception ex) {
        alertingService.notify(ex);
        client.sendEvent("error", Map.of("message", "Something went wrong"));
    }
}
```

**`DefaultSocketErrorHandler`** — used if you don't provide your own.
Logs the exception and sends an `"error"` event back to the Flutter client.

---

### `metrics` — automatic Micrometer metrics

When `app.socket.metrics.enabled=true`, these are registered automatically:

| Metric | Type | Description |
|---|---|---|
| `socket.connections.active` | Gauge | Currently connected clients |
| `socket.connections.total` | Counter | Total connects since startup |
| `socket.disconnections.total` | Counter | Total disconnects since startup |
| `socket.messages.received` | Counter (tagged by event) | Messages per event name |
| `socket.errors.total` | Counter (tagged by event) | Errors per event name |
| `socket.auth.failures` | Counter | Failed authentication attempts |

Zero code in your service. These appear in Prometheus/Grafana automatically.

---

### `tracing` — OpenTelemetry span per message

When `app.socket.tracing.enabled=true`, every `@OnMessage` handler call
is wrapped in a named span:

```
span: "socket.joinTrackingRoom"
    ├── SessionManager.refreshActivity()
    ├── your handler method
    │     ├── MongoDB query
    │     └── Kafka publish
    └── span ends
```

Full trace visible in Jaeger/Zipkin — Flutter event → handler → downstream calls, all connected.

If the `Tracer` bean is not on the classpath, `SocketTracing` runs the handler
directly with zero overhead.

---

### `event` — Spring events for other beans to react to

The SDK publishes these `ApplicationEvent`s automatically:

```java
SocketServerStartedEvent       // server started — includes port
SocketServerStoppedEvent       // server stopped
SocketClientConnectedEvent     // includes SocketSession (userId, IP, connectedAt)
SocketClientDisconnectedEvent  // includes SocketSession
```

Any bean listens with `@EventListener`:

```java
@Component
public class AuditLogger {

    @EventListener
    public void onConnect(SocketClientConnectedEvent event) {
        SocketSession session = event.getSession();
        auditLog.record("CONNECT", session.getUserId(), session.getRemoteAddress());
    }

    @EventListener
    public void onDisconnect(SocketClientDisconnectedEvent event) {
        auditLog.record("DISCONNECT", event.getSession().getUserId());
    }
}
```

---

### `lifecycle` — startup and shutdown order

`SocketServerLifecycle` enforces this exact order:

```
1. Spring loads all beans
2. ContextRefreshedEvent fires
3. MessageHandlerRegistry scans all beans → registers all @OnMessage handlers
4. SocketIOServer.start() ← only NOW accepts Flutter connections
5. Publishes SocketServerStartedEvent

(application runs)

6. Spring shutdown → ContextClosedEvent
7. SocketIOServer.stop()
8. Publishes SocketServerStoppedEvent
```

Critical: server starts ONLY AFTER all handlers are registered.
If it started before scanning, a Flutter client could send an event with no handler listening.

---

### `config` — the wiring class

`SocketAutoConfiguration` creates every bean above and connects them.
A developer never touches this. It activates when `@EnableSocket` is on the application class.

Uses `@ConditionalOnMissingBean` for every replaceable component — if your service
provides its own bean, the SDK uses yours instead of the default (see section 7).

---

## 5. How a service uses this SDK from scratch

---

### Step 1 — Add dependency

In your service's `build.gradle`:

```groovy
implementation 'com.tutem.platform:socket-sdk:1.0.0-SNAPSHOT'
```

---

### Step 2 — Enable in application class

```java
@SpringBootApplication
@EnableSocket
public class MyServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(MyServiceApplication.class, args);
    }
}
```

---

### Step 3 — Configure application.yml

```yaml
app:
  socket:
    port: 9090
    worker-count: 100
    auth:
      enabled: true       # set false if no auth needed
    metrics:
      enabled: true
    tracing:
      enabled: true
```

---

### Step 4 — Provide auth (if auth.enabled=true)

```java
@Component
public class JwtSocketAuthHook implements SocketAuthenticationHook {

    private final JwtService jwtService;

    public SocketAuthContext authenticate(HandshakeData data) throws SocketAuthException {
        String token = data.getSingleUrlParam("token");
        if (token == null) throw new SocketAuthException("Missing token");
        return SocketAuthContext.of(
            jwtService.extractUserId(token),
            jwtService.extractRole(token)
        );
    }
}
```

---

### Step 5 — Write handlers

```java
@Component
@RequiredArgsConstructor
public class RoomHandler {

    private final SomeRepository repository;

    @OnConnect
    public void onConnect(SocketIOClient client, SocketSession session) {
        log.info("Connected: userId={}", session.getUserId());
    }

    @OnDisconnect
    public void onDisconnect(SocketIOClient client, SocketSession session) {
        log.info("Disconnected: userId={}", session.getUserId());
    }

    @OnMessage("joinRoom")
    public void onJoinRoom(SocketIOClient client, JoinRoomPayload payload, AckRequest ack) {
        client.joinRoom(payload.getRoom());
        // your business logic
    }

    @OnMessage("sendMessage")
    public void onSendMessage(SocketIOClient client, MessagePayload payload) {
        // your business logic
    }

    @OnError
    public void onError(SocketIOClient client, String event, Exception ex) {
        log.error("Error on event {}: {}", event, ex.getMessage());
        client.sendEvent("error", Map.of("event", event, "message", ex.getMessage()));
    }
}
```

---

### Step 6 — Push messages from anywhere in your service

```java
@Service
@RequiredArgsConstructor
public class NotificationService {

    private final MessageDispatcher dispatcher;

    public void notifyUser(String userId, Object payload) {
        dispatcher.sendToUser(userId, "notification", payload);
    }

    public void notifyRoom(String room, Object payload) {
        dispatcher.sendToRoom(room, "roomUpdate", payload);
    }
}
```

---

### Step 7 — Listen to lifecycle events (optional)

```java
@Component
public class ConnectionAudit {

    @EventListener
    public void onClientConnected(SocketClientConnectedEvent event) {
        SocketSession session = event.getSession();
        log.info("Client connected: userId={} from {}", session.getUserId(), session.getRemoteAddress());
    }

    @EventListener
    public void onClientDisconnected(SocketClientDisconnectedEvent event) {
        log.info("Client disconnected: userId={}", event.getSession().getUserId());
    }
}
```

---

## 6. What belongs in the SDK vs your service

**SDK — generic infrastructure, zero business knowledge:**

- Server setup and configuration
- Handler scanning and annotation processing
- Session lifecycle (create, store, remove)
- Auth hook interface (not the implementation)
- MessageDispatcher (send to user/room/broadcast)
- Serialization
- Heartbeat and ghost session cleanup
- Metrics and tracing
- Error handler interface and default
- Spring lifecycle events

**Your service — business logic only:**

```
my-service/
└── src/main/java/
    ├── handler/
    │     RoomHandler.java          @OnMessage("joinRoom")
    │     LocationHandler.java      @OnMessage("updateLocation")
    │     RideHandler.java          @OnMessage("getRideInfo")
    │
    ├── auth/
    │     JwtSocketAuthHook.java    implements SocketAuthenticationHook
    │
    ├── model/
    │     JoinRoomPayload.java      payload DTOs matching client event shapes
    │     LocationPayload.java
    │
    └── service/
          RideService.java          injects MessageDispatcher to push to Flutter
```

No netty-socketio imports anywhere in your service code. Only SDK annotations.

---

## 7. Replacing any SDK component

Every core component uses `@ConditionalOnMissingBean` — provide your own bean
and the SDK uses yours automatically.

**Replace SessionManager with Redis (for multi-pod deployment):**

```java
@Bean
@Primary
public SessionManager redisSessionManager(RedisTemplate<String, String> redis) {
    return new RedisSessionManager(redis);
}
```

**Replace serialization with MessagePack:**

```java
@Bean
@Primary
public SocketMessageSerializer messagePackSerializer() {
    return new MessagePackSocketMessageSerializer();
}
```

**Custom error handler:**

```java
@Bean
public SocketErrorHandler myErrorHandler(AlertingService alerting) {
    return (client, event, ex) -> {
        alerting.notify(ex);
        client.sendEvent("error", Map.of("message", "Internal error"));
    };
}
```

---

## 8. Configuration reference

| Property | Default | Description |
|---|---|---|
| `app.socket.port` | `9090` | Port Flutter connects to |
| `app.socket.host` | `0.0.0.0` | Bind address |
| `app.socket.worker-count` | `100` | Netty worker thread count |
| `app.socket.ping-timeout` | `60000` | Ms before silent client considered dead |
| `app.socket.ping-interval` | `25000` | Ms between server pings |
| `app.socket.auth.enabled` | `false` | Require SocketAuthenticationHook bean |
| `app.socket.metrics.enabled` | `true` | Expose Micrometer metrics |
| `app.socket.tracing.enabled` | `true` | Wrap handlers in OTel spans |
| `app.socket.heartbeat.stale-session-seconds` | `120` | Idle session threshold |
| `app.socket.heartbeat.cleanup-interval-seconds` | `30` | Cleanup schedule interval |
