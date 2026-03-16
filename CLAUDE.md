# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run

```bash
# Build all modules
mvn clean install

# Build a specific module
mvn clean install -pl kafka-intro

# Run a module
mvn spring-boot:run -pl kafka-intro

# Start Kafka infrastructure (Kafka on :9092, Schema Registry on :8081, Kafdrop UI on :9000)
docker-compose up -d
```

## Testing

```bash
# All tests in a module
mvn test -pl kafka-intro

# Single test class
mvn test -pl kafka-intro -Dtest=TestedSenderTest

# Single test method
mvn test -pl kafka-intro -Dtest=TestedSenderTest#sends
```

Tests use JUnit 5 + Spring Boot Test + Mockito. The base class `IntegrationTest` (kafka-intro) spins up a real embedded Kafka context via `@SpringBootTest` and provides helpers for consumer lag assertions and partition assignment waiting.

## Project Structure

Multi-module Maven project (Java 21, Spring Boot 3.5.6, Spring Kafka):

| Module | Purpose |
|---|---|
| `kafka-intro` | Main training module — 18 feature packages covering core Kafka patterns |
| `kafka-streams` | Kafka Streams topologies (game, words, metrics, notifications, time) |
| `kafka-avro` | Avro schema registration and serialization with Schema Registry |
| `kafka-tx` | Transactional message processing patterns |
| `patterns/` | 50+ PlantUML diagrams illustrating each pattern |

## Architecture (kafka-intro)

Each sub-package under `victor.training.kafka.intro` represents a self-contained pattern:

- **inbox / outbox** — Reliable messaging with deduplication and saga coordination
- **seqno** — Sequence number tracking for ordering guarantees
- **ooo** — Out-of-order message resequencing
- **race** — Optimistic-locking-based race condition handling
- **poison** — Dead-letter topic and poison pill handling
- **dups** — Duplicate detection strategies
- **batch** — Batch consumer processing
- **shedlock** — Distributed scheduling with ShedLock
- **interceptor** — Producer/consumer interceptors for tracing

`Event` is the core message type with inner `Serializer`/`Deserializer` classes.

Consumer group ID defaults to `app` (configured in `application.yaml`); tests use a random group ID (`application-test.properties`) to avoid interference between test runs.

## Key Config

- `kafka-intro/src/main/resources/application.yaml` — Main config (Kafka bootstrap, H2 TCP at :9093, consumer concurrency=2)
- `kafka-intro/src/test/resources/application-test.properties` — Test overrides (random group ID, P6Spy SQL logging, scheduled jobs disabled)
