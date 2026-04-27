# Library Workshop Module — Design Spec

**Date:** 2026-04-27  
**Status:** Approved

## Overview

A new top-level Maven module `library` demonstrates event-driven architecture motivation through a year-2150 book library scenario. Customers check out books, which are added to their member account (max 5) and then teleported to their living room. The module starts with synchronous HTTP interactions, with Kafka stubs in place for progressive workshop conversion.

## Maven Structure

```
kafka/
  library/                        ← parent pom (packaging=pom, parent=victor.training:kafka:1.0)
    pom.xml
    requests.http                 ← IntelliJ HTTP client entry point
    checkout/                     ← Spring Boot app, port 8080
    member/                       ← Spring Boot app, port 8082
    teleport/                     ← Spring Boot app, port 8083
    reminder/                     ← Spring Boot app stub, port 8084
```

`library/pom.xml` declares all four as modules and inherits from the root `kafka` pom (gets Spring Boot, Spring Kafka, Lombok, etc. for free).

Each sub-module is a minimal Spring Boot app with H2 + JPA (where needed) and its own `application.properties` setting the server port.

## Services

### Checkout (port 8080)
- Entry point: `POST /checkout` with body `{ "userId": 1, "bookIds": [1, 6] }`
- Injects a `RestClient` configured for Member (`:8082`) and Teleport (`:8083`)
- Flow:
  1. Call `POST /members/{userId}/books` with `checkoutId` + `bookIds`
  2. Call `POST /teleport` with `userId` + `bookIds`
  3. If Teleport call throws: call `DELETE /members/{userId}/books` (compensation), then propagate failure

### Member (port 8082)
- `POST /members/{userId}/books` — adds books to user account
  - Idempotent: uses `checkoutId` (UUID, sent in request body) to skip duplicate operations
  - Enforces max-5-books-per-user rule; returns 409 if exceeded
- `DELETE /members/{userId}/books` — removes books (compensation endpoint)
- H2 + JPA: `MemberBook(userId, bookId, checkoutId)`

### Teleport (port 8083)
- `POST /teleport` — simulates book teleportation
- Configurable failure: `POST /teleport/toggle-fail` flips a flag to make the next call throw 500, enabling the compensation demo
- No database needed; stateless stub

### Reminder (port 8084)
- Empty Spring Boot app — just a `@SpringBootApplication` main class and `application.properties`
- Placeholder for future Kafka exercise: "notify users to return books due tomorrow"

## HTTP Entry Point

`library/requests.http` contains ready-to-run requests:

```http
### Checkout books (happy path)
POST http://localhost:8080/checkout
Content-Type: application/json

{ "userId": 1, "bookIds": [1, 6] }

### Make teleport fail on next call
POST http://localhost:8083/teleport/toggle-fail

### Checkout books (triggers compensation)
POST http://localhost:8080/checkout
Content-Type: application/json

{ "userId": 1, "bookIds": [2, 3] }
```

## Idempotency

`Checkout` generates a UUID `checkoutId` and sends it in the body to Member. Member stores `(checkoutId, userId, bookId)` with a unique constraint on `(checkoutId, bookId)`. A duplicate call with the same `checkoutId` is silently skipped.

## Kafka Stubs

Each service contains an `Events.java` class (in its root package) with:
- Event record definitions, e.g. `record BookCheckedOut(String checkoutId, long userId, List<Long> bookIds)`
- Commented-out `@KafkaListener` snippet showing what the consumer would look like
- Commented-out `KafkaTemplate.send(...)` call showing what the producer would look like

This gives participants a clear migration target without requiring any Kafka infrastructure to run the initial HTTP version.

## What Is NOT in Scope

- No service discovery / Eureka
- No API gateway
- No Docker setup (services run locally on fixed ports)
- No Kafka connectivity in the initial scaffold (Kafka stubs are comments only)
- Reminder service has no logic yet
