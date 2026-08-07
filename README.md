# Tutem Platform Libraries

This repository contains reusable platform libraries for Tutem services.

## Overview

The project is organized as a multi-module Gradle build with two libraries:

- event-sdk: event publishing and Kafka integration helpers for producing domain events
- socket-sdk: Socket.IO-based realtime server support for Spring Boot applications

## Project structure

```text
.
├── build.gradle                 # aggregator only - no sources, no shared config
├── settings.gradle
├── gradle/
│   └── libs.versions.toml       # version catalog: the only place versions are pinned
├── buildSrc/
│   └── src/main/groovy/
│       └── tutem.java-library-conventions.gradle   # shared build config for every module
├── event-sdk/
│   └── src/main/java/...        # event publishing, serialization, topic resolution, Kafka producer
│   └── src/test/java/...        # auto-configuration contract, local Kafka tests and sample harness
└── socket-sdk/
    └── src/main/java/...        # Socket.IO server, annotations, lifecycle, auth, metrics, tracing
    └── src/test/java/...        # socket library tests
```

### Build conventions

Modules declare nothing but their own dependencies. Plugins, group and version, the Java 21
toolchain, UTF-8 encoding, the sources jar, `maven-publish` and the JUnit platform all come
from the `tutem.java-library-conventions` plugin in [buildSrc/](buildSrc/).

Third-party versions come from [gradle/libs.versions.toml](gradle/libs.versions.toml). Most
entries there carry no version on purpose: the convention plugin applies the Spring Boot BOM,
which manages Spring, Jackson, Kafka, Micrometer, Lombok and JUnit together. To move the whole
stack, bump `springBoot` in the catalog — not individual coordinates in a module.

## Modules

### event-sdk

The event SDK provides building blocks for publishing domain events to Kafka.
It includes:

- Kafka event producer abstraction
- JSON event serialization
- envelope creation for event payloads
- topic resolution from event metadata
- Spring Boot auto-configuration support

This module is suitable for services that need to publish cloud events or domain events in a consistent format.

### socket-sdk

The socket SDK provides a Spring Boot-friendly Socket.IO server integration.
It includes:

- annotation-based handlers such as @OnConnect, @OnMessage, @OnDisconnect, and @OnError
- session and connection lifecycle management
- authentication hooks
- metrics and tracing integration points
- dispatcher support for pushing realtime events from application code

## Prerequisites

- Java 21+
- Gradle 9+
- Internet access for resolving Maven dependencies

## Build and test

From the repository root, run:

```bash
./gradlew test
```

To run the module-specific tests:

```bash
./gradlew :event-sdk:test
./gradlew :socket-sdk:test
```

## Kafka local setup and testing

The event SDK includes a local Kafka-based test harness for validating producer/consumer behavior without needing a separate broker installation.

### What is included

- embedded Kafka test infrastructure for local tests
- a sample producer and consumer used by the test suite
- a round-trip test that publishes sample messages and verifies they are consumed

### Run the Kafka producer/consumer test

```bash
./gradlew :event-sdk:test --tests "com.tutem.platform.event.KafkaLocalRoundTripTest"
```

This test publishes sample payloads such as:

```json
{"eventType":"demo.event.1","message":"sample-1"}
{"eventType":"demo.event.2","message":"sample-2"}
{"eventType":"demo.event.3","message":"sample-3"}
```

and confirms they are received by the consumer.

### Local sample harness

A simple runnable sample class is also available for local experimentation:

```bash
./gradlew :event-sdk:test --tests "com.tutem.platform.event.KafkaLocalSample"
```

This starts an embedded Kafka broker locally so you can inspect the bootstrap server details and test your own producer/consumer flow.

## How to use the libraries

Both modules publish as `com.tutem.platform:<module>:1.0-SNAPSHOT`. Until a remote repository
is chosen, run `./gradlew publishToMavenLocal` and consume them from `mavenLocal()`.

### event-sdk usage

Auto-configuration is on by default, gated on Kafka: with `spring-kafka` configured, an
`EventPublisher` is wired for you. Without it the SDK stays inert, so depending on event-sdk
never forces Kafka onto a service. Map each event type to a topic:

```yaml
event:
  topics:
    order-created: orders.v1
```

Every bean is `@ConditionalOnMissingBean`, so defining your own `TopicResolver`,
`EventSerializer` or `EnvelopeFactory` replaces just that piece.

### socket-sdk usage

Activation is opt-in: annotate your application with `@EnableSocket`. Having socket-sdk on the
classpath never opens a port on a service that did not ask for one, and `app.socket.enabled=false`
forces it off entirely. See [socket-sdk/README.md](socket-sdk/README.md) for the full property list.

## Notes

- The repository is currently structured as a local multi-module build; no remote Maven repository has been chosen yet.
- The event SDK Kafka support is tested locally through the embedded test setup.
- For production deployments, you would typically swap the embedded/local setup for a real Kafka broker and configure the appropriate bootstrap servers and topics.

## Contribution

When adding new functionality:

1. Keep module boundaries clear.
2. Add or update tests for the affected module.
3. Prefer local, reproducible setups for development and verification.
