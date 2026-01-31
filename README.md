# Charks Outbox

[![CI](https://github.com/charks1/charks-outbox/actions/workflows/ci.yml/badge.svg)](https://github.com/charks1/charks-outbox/actions/workflows/ci.yml)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=charks1_charks-outbox&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=charks1_charks-outbox)
[![codecov](https://codecov.io/gh/charks1/charks-outbox/branch/main/graph/badge.svg)](https://codecov.io/gh/charks1/charks-outbox)
[![Maven Central](https://img.shields.io/maven-central/v/xyz.charks/charks-outbox-core.svg)](https://central.sonatype.com/artifact/xyz.charks/charks-outbox-core)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Java](https://img.shields.io/badge/Java-25-orange.svg)](https://openjdk.org/projects/jdk/25/)

A lightweight, framework-agnostic implementation of the [Transactional Outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html) for Java 25+.

## Why Charks Outbox?

In distributed systems, ensuring reliable message delivery while maintaining data consistency is challenging. The Transactional Outbox pattern solves this by storing events in the same transaction as your business data, then reliably delivering them to message brokers.

**Key benefits:**

- **Guaranteed delivery** - Events are persisted atomically with your data
- **At-least-once semantics** - Failed deliveries are automatically retried
- **Ordering guarantees** - Events are delivered in order per aggregate
- **Framework-agnostic** - Works with any Java application, not just Spring

## Features

- **Zero dependencies in core** - The core module has no external dependencies
- **Multiple storage backends** - JDBC, R2DBC, MongoDB
- **Multiple message brokers** - Kafka, RabbitMQ, Pulsar, SQS, NATS
- **Pluggable serialization** - JSON (Jackson), Avro, Protobuf
- **Observable** - Micrometer and OpenTelemetry support
- **Production-ready** - Distributed locking, retry policies, graceful shutdown
- **Spring Boot starter** - Auto-configuration for Spring Boot applications

## Requirements

- Java 25 or later
- A supported database (PostgreSQL, MySQL, Oracle, SQL Server)
- A supported message broker

## Installation

### Maven

```xml
<!-- Core (required) -->
<dependency>
    <groupId>xyz.charks</groupId>
    <artifactId>charks-outbox-core</artifactId>
    <version>1.0.0</version>
</dependency>

<!-- Storage backend (choose one) -->
<dependency>
    <groupId>xyz.charks</groupId>
    <artifactId>charks-outbox-jdbc</artifactId>
    <version>1.0.0</version>
</dependency>

<!-- Message broker (choose one) -->
<dependency>
    <groupId>xyz.charks</groupId>
    <artifactId>charks-outbox-kafka</artifactId>
    <version>1.0.0</version>
</dependency>

<!-- Serializer (choose one) -->
<dependency>
    <groupId>xyz.charks</groupId>
    <artifactId>charks-outbox-serializer-json</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Gradle

```kotlin
implementation("xyz.charks:charks-outbox-core:1.0.0")
implementation("xyz.charks:charks-outbox-jdbc:1.0.0")
implementation("xyz.charks:charks-outbox-kafka:1.0.0")
implementation("xyz.charks:charks-outbox-serializer-json:1.0.0")
```

### Spring Boot

```xml
<dependency>
    <groupId>xyz.charks</groupId>
    <artifactId>charks-outbox-spring-boot-starter</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Quick Start

### 1. Create the outbox table

```sql
CREATE TABLE outbox_events (
    id UUID PRIMARY KEY,
    aggregate_type VARCHAR(255) NOT NULL,
    aggregate_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    payload BYTEA NOT NULL,
    metadata JSONB,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    retry_count INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMP,
    next_retry_at TIMESTAMP
);

CREATE INDEX idx_outbox_status_created ON outbox_events (status, created_at);
```

### 2. Store events in your transaction

```java
@Transactional
public void placeOrder(Order order) {
    // Save your business entity
    orderRepository.save(order);

    // Store the event in the same transaction
    OutboxEvent event = OutboxEvent.builder()
        .aggregateType("Order")
        .aggregateId(order.getId())
        .eventType("OrderPlaced")
        .payload(serializer.serialize(new OrderPlacedEvent(order)))
        .build();

    outboxRepository.save(event);
}
```

### 3. Configure and start the relay

```java
// Configure the relay
RelayConfig config = RelayConfig.builder()
    .batchSize(100)
    .pollingInterval(Duration.ofSeconds(1))
    .retryPolicy(ExponentialBackoff.builder()
        .initialDelay(Duration.ofSeconds(1))
        .maxDelay(Duration.ofMinutes(5))
        .maxAttempts(10)
        .build())
    .build();

// Create and start the relay
OutboxRelay relay = new OutboxRelay(repository, connector, serializer, config);
relay.start();

// On shutdown
relay.stop();
```

### Spring Boot Configuration

```yaml
charks:
  outbox:
    enabled: true
    relay:
      batch-size: 100
      polling-interval: 1s
    retry:
      max-attempts: 10
      initial-delay: 1s
      max-delay: 5m
    kafka:
      bootstrap-servers: localhost:9092
      topic-prefix: events
```

## Modules

| Module | Description |
|--------|-------------|
| `charks-outbox-core` | Core API and domain model (zero dependencies) |
| `charks-outbox-jdbc` | JDBC repository implementation |
| `charks-outbox-r2dbc` | Reactive R2DBC repository implementation |
| `charks-outbox-mongodb` | MongoDB repository implementation |
| `charks-outbox-kafka` | Kafka broker connector |
| `charks-outbox-rabbitmq` | RabbitMQ broker connector |
| `charks-outbox-pulsar` | Apache Pulsar broker connector |
| `charks-outbox-sqs` | AWS SQS broker connector |
| `charks-outbox-nats` | NATS broker connector |
| `charks-outbox-serializer-json` | Jackson JSON serializer |
| `charks-outbox-serializer-avro` | Apache Avro serializer |
| `charks-outbox-serializer-protobuf` | Protocol Buffers serializer |
| `charks-outbox-micrometer` | Micrometer metrics integration |
| `charks-outbox-opentelemetry` | OpenTelemetry tracing integration |
| `charks-outbox-spring-boot-starter` | Spring Boot auto-configuration |
| `charks-outbox-quarkus` | Quarkus extension |
| `charks-outbox-test` | Testing utilities and mocks |

## Documentation

- [Getting Started Guide](docs/getting-started.md)
- [Configuration Reference](docs/configuration.md)
- [Architecture Overview](docs/architecture.md)
- [API Reference](https://charks.xyz/charks-outbox/apidocs/)

## Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
