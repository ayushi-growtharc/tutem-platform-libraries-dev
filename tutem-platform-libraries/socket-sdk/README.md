# socket-sdk

Production-grade WebSocket library for TUTEM Java microservices.
Any Spring Boot service imports this to get a full Socket.IO-compatible WebSocket server.

---

## Requirements

- Java 21
- Spring Boot 3.3.x
- Gradle 8.8+

---

## How to add to your service

### Step 1 — Add dependency in `build.gradle`

```groovy
implementation project(':socket-sdk')        // if in same repo
// OR
implementation 'com.tutem.platform:socket-sdk:1.0.0-SNAPSHOT'   // if published
```

### Step 2 — Add `@EnableSocket` to your main class

```java
@SpringBootApplication
@EnableSocket
public class MyServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(MyServiceApplication.class, args);
    }
}
```

### Step 3 — Add config to `application.yml`

```yaml
app:
  socket:
    port: 9090           # Flutter connects here
    auth:
      enabled: true      # set false to allow all connections without auth
```

### Step 4 — Provide auth hook (required if auth.enabled=true)

```java
@Component
public class JwtAuthHook implements SocketAuthenticationHook {
    public SocketAuthContext authenticate(HandshakeData data) throws SocketAuthException {
        String token = data.getSingleUrlParam("token");
        if (token == null) throw new SocketAuthException("Missing token");
        return SocketAuthContext.of(jwtService.extractUserId(token), "USER");
    }
}
```

### Step 5 — Write handlers

```java
@Component
public class MyHandler {

    @OnConnect
    public void onConnect(SocketIOClient client, SocketSession session) { }

    @OnMessage("joinRoom")
    public void joinRoom(SocketIOClient client, JoinRoomPayload payload) {
        client.joinRoom(payload.getRoomId());
    }

    @OnDisconnect
    public void onDisconnect(SocketIOClient client, SocketSession session) { }

    @OnError
    public void onError(SocketIOClient client, String event, Exception ex) { }
}
```

### Step 6 — Push events to Flutter from anywhere

```java
@Service
public class NotifyService {

    @Autowired MessageDispatcher dispatcher;

    public void notifyUser(String userId, Object data) {
        dispatcher.sendToUser(userId, "rideAssigned", data);
    }

    public void notifyRoom(String roomId, Object data) {
        dispatcher.sendToRoom(roomId, "rideUpdate", data);
    }

    public void notifyAll(Object data) {
        dispatcher.broadcast("systemAlert", data);
    }
}
```

---

## Full configuration reference

```yaml
app:
  socket:
    port: 9090                          # WebSocket port
    host: 0.0.0.0                       # bind address
    worker-count: 100                   # Netty threads
    ping-timeout: 60000                 # ms
    ping-interval: 25000                # ms
    auth:
      enabled: true
    metrics:
      enabled: true                     # Micrometer
    tracing:
      enabled: true                     # OpenTelemetry
    heartbeat:
      stale-session-seconds: 120
      cleanup-interval-seconds: 30
```

---

## How to run the test app

The `socket-sdk-testapp` module is a ready-made Spring Boot app to test the SDK.

### Prerequisites
- Java 21 installed
- IntelliJ IDEA (recommended — avoids Gradle daemon issues on Windows)

### Steps

1. Open `platform-libraries` folder in IntelliJ IDEA
2. Trust the project when prompted
3. Wait for Gradle sync to finish (bottom bar)
4. Set JDK to Java 21 if prompted (File → Project Structure → SDK)
5. Run `socket-sdk-testapp` → `TestAppApplication`
6. Console should show:
   ```
   SocketIO server started at port: 9090
   Started TestAppApplication
   ```

### Test with Postman

**Connect (Socket.IO):**
```
URL: http://localhost:9090?userId=user1
```

**Events you can send:**

| Event | Payload | Response |
|---|---|---|
| `ping` | none | `pong` → `{ status: "alive" }` |
| `joinRoom` | `{ "userId": "user1", "roomId": "testRoom" }` | ack `{ status: "joined" }` |
| `sendMessage` | `{ "from": "user1", "message": "hello" }` | `newMessage` broadcast |
| `getOnlineCount` | none | `onlineCount` → `{ count: N }` |

**REST endpoints (HTTP):**

```
GET  http://localhost:8080/test/status
POST http://localhost:8080/test/push/user/{userId}    body: { "event": "...", "data": "..." }
POST http://localhost:8080/test/push/room/{roomId}    body: { "event": "...", "data": "..." }
POST http://localhost:8080/test/broadcast             body: { "event": "...", "data": "..." }
```

---

## Package structure

```
com.tutem.platform.socket/
├── annotations/     @EnableSocket, @OnMessage, @OnConnect, @OnDisconnect, @OnError
├── config/          SocketAutoConfiguration — wires all beans
├── properties/      SocketProperties — reads app.socket.*
├── session/         SocketSession, SessionManager, InMemorySessionManager
├── connection/      ConnectionManager — rooms, disconnect
├── dispatcher/      MessageDispatcher — push to user/room/all
│                    MessageHandlerRegistry — scans @OnMessage annotations
├── authentication/  SocketAuthenticationHook interface + SocketAuthContext
├── serialization/   SocketMessageSerializer + JsonSocketMessageSerializer
├── exception/       SocketAuthException, SocketErrorHandler
├── heartbeat/       HeartbeatManager — cleans dead connections
├── metrics/         SocketMetrics — Micrometer
├── tracing/         SocketTracing — OpenTelemetry
├── event/           Spring events: Connected, Disconnected, Started, Stopped
└── lifecycle/       SocketServerLifecycle — startup/shutdown order
```

---

## Replacing default components

Every component uses `@ConditionalOnMissingBean` — provide your own bean to override:

```java
// Replace in-memory sessions with Redis for multi-pod scaling
@Bean @Primary
public SessionManager redisSessionManager(RedisTemplate<String,String> redis) {
    return new RedisSessionManager(redis);
}
```
