# tutem-platform-libraries

Two reusable Spring Boot libraries for Tutem services, published as
`com.tutem.platform:<module>:1.0-SNAPSHOT`:

- **event-sdk** — publishes domain events to Kafka behind an `EventPublisher` interface.
- **socket-sdk** — Socket.IO realtime server integration for Spring Boot.

These are **libraries, not applications**. Nothing here has a `main`; every change is judged by
what it does to a consuming service's classpath and bean graph.

## Build

```bash
./gradlew build                 # compile + test both modules
./gradlew :event-sdk:test       # one module
./gradlew publishToMavenLocal   # how consumers get these (no remote repo yet)
```

Java 21 toolchain, Gradle wrapper 9.6.0. Run from the repo root.

## Where build configuration lives

Do **not** add plugins, dependencies, or shared config to the root `build.gradle`. It is an
aggregator with no sources; anything declared there applies to the root project alone and
silently does nothing for the modules. A previous version of this repo fell into exactly that
trap.

- Shared config → `buildSrc/src/main/groovy/tutem.java-library-conventions.gradle`
  (plugins, group/version, toolchain, encoding, sources jar, `maven-publish`, JUnit platform).
- Versions → `gradle/libs.versions.toml`.
- Module `build.gradle` files → **their own dependencies only**.

Most catalog entries deliberately carry no version: the convention plugin applies the Spring
Boot BOM, so Spring, Jackson, Kafka, Micrometer, Lombok and JUnit are managed together. To move
the stack, bump `springBoot` in the catalog — never pin a version in a module.

## Conventions that matter here

**`api` vs `implementation` is a deliberate decision, not a habit.** A dependency is `api` only
when a consumer compiles against it — e.g. `spring-kafka` is `api` because
`KafkaEventProducer`'s public constructor takes a `KafkaTemplate`. Each `api` line carries a
comment saying why. Keep that up; getting it wrong leaks transitive deps to every service.

**Never put test-only libraries on `implementation`.** They ship to consumers' runtime
classpaths. Check the generated POM if unsure:
`./gradlew publishToMavenLocal && cat ~/.m2/repository/com/tutem/platform/<module>/1.0-SNAPSHOT/*.pom`

**Auto-configuration registration is easy to get silently wrong.** Both modules register through
`src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`.
The filename must be exact — a `.txt` suffix, a typo, or an empty file produces **no error at
all**, just an SDK that does nothing in every consuming app. `EventSdkAutoConfigurationTest`
guards this for event-sdk; add the same guard to any new module.

**Activation must be opt-in or conditional.** Putting an SDK on the classpath must never change
a service's behaviour by itself:
- socket-sdk requires `@EnableSocket` (marker bean + `@ConditionalOnBean`), so it never opens a
  port unasked.
- event-sdk is `@ConditionalOnClass(KafkaTemplate)`, so it stays inert without Kafka.

Every bean in both modules is `@ConditionalOnMissingBean` so consumers can replace one piece
without losing the rest. Preserve that when adding beans.

## event-sdk shape

`EventPublisher.publish(DomainEvent)` is the only entry point. Internally: `TopicResolver` maps
`eventType()` → topic (from `event.topics.*`), `EnvelopeFactory` wraps the event in an
`EventEnvelope`, `EventSerializer` renders JSON, `KafkaEventProducer` sends it keyed by
`event.key()`.

Known rough edges — worth fixing, don't be surprised by them:
- `DefaultEnvelopeFactory` never sets `source`, so it is always `null` on the wire.
- `KafkaEventProducer` wraps `kafkaTemplate.send()` in try/catch, but `send()` is async — broker
  failures land in the returned future, so publishing is effectively fire-and-forget.
- `DefaultTopicResolver` throws a raw `RuntimeException` at publish time for an unmapped event
  type; nothing validates topic config at startup.
- `CorrelationContext` is a bare `ThreadLocal`; `traceId` is `null` unless a caller sets it, and
  it does not cross threads.
- event-sdk does not depend on `jackson-datatype-jsr310`, yet `EventEnvelope.timestamp` is an
  `Instant`. Boot supplies a suitable `ObjectMapper`; the SDK's fallback mapper cannot serialize
  it.

## Consumers

`tutem-location-service` (sibling repo) consumes socket-sdk and is on **Spring Boot 4.1**, while
these libraries build against **3.5.4**. Boot 4 defaults to Jackson 3
(`tools.jackson.databind.ObjectMapper`), but both SDKs are written against classic Jackson 2
(`com.fasterxml.jackson.databind.ObjectMapper`). Consumers on Boot 4 must supply a Jackson 2
`ObjectMapper` bean — see that repo's `SocketJacksonConfig`. Keep this in mind before assuming a
change is source-compatible for consumers.

## Testing

JUnit 5 + AssertJ + Mockito via `spring-boot-starter-test`. Auto-configuration behaviour is
tested with `ApplicationContextRunner` (`FilteredClassLoader` for the classpath-absent cases) —
see `SocketAutoConfigurationTest` and `EventSdkAutoConfigurationTest` for the established
pattern. Prefer that over booting a real context.

`event-sdk` uses an embedded Kafka broker (`spring-kafka-test`) for round-trip tests; no external
broker is required.

## Style

Comments explain *why*, not what — especially for dependency scopes, conditionals, and anything
that looks arbitrary. socket-sdk is the reference for the level of explanation expected on
public extension points. Match the density of the file you are editing.
