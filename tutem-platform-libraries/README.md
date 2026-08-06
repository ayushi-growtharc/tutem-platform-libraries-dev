# Tutem Platform Libraries

This repository contains reusable platform libraries for Tutem services.

## Overview

The project is organized as a multi-module Gradle build with two libraries:

- event-sdk: event publishing and Kafka integration helpers for producing domain events
- socket-sdk: Socket.IO-based realtime server support for Spring Boot applications

## Project structure

```text
.
├── build.gradle
├── settings.gradle
├── event-sdk/
│   └── src/main/java/...        # event publishing, serialization, topic resolution, Kafka producer
│   └── src/test/java/...        # local Kafka producer/consumer tests and sample harness
└── socket-sdk/
    └── src/main/java/...        # Socket.IO server, annotations, lifecycle, auth, metrics, tracing
    └── src/test/java/...        # socket library tests
```

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
./gradlew :event-sdk:test --tests "com.tutem.platform.eventsdk.KafkaLocalRoundTripTest"
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
./gradlew :event-sdk:test --tests "com.tutem.platform.eventsdk.KafkaLocalSample"
```

This starts an embedded Kafka broker locally so you can inspect the bootstrap server details and test your own producer/consumer flow.

## How to use the libraries

### event-sdk usage

Add the module to your build as a project dependency or via a published artifact once repository publishing is set up.

### socket-sdk usage

Use the Spring Boot auto-configuration support by enabling the socket integration in your application and configuring the required properties.

## Notes

- The repository is currently structured as a local multi-module build.
- The event SDK Kafka support is tested locally through the embedded test setup.
- For production deployments, you would typically swap the embedded/local setup for a real Kafka broker and configure the appropriate bootstrap servers and topics.

## Contribution

When adding new functionality:

1. Keep module boundaries clear.
2. Add or update tests for the affected module.
3. Prefer local, reproducible setups for development and verification.
