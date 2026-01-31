# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.1](https://github.com/charks1/charks-outbox/compare/v1.0.0...v1.0.1) (2026-01-31)

### 🐛 Bug Fixes

* **ci:** add write permissions for GitHub release creation ([6a41dee](https://github.com/charks1/charks-outbox/commit/6a41dee))
* **ci:** use correct secret names for Maven Central credentials ([e11a754](https://github.com/charks1/charks-outbox/commit/e11a754))
* **javadoc:** add missing imports for Javadoc references ([c25302c](https://github.com/charks1/charks-outbox/commit/c25302c))

### 🔧 Miscellaneous

* prepare for 1.0.1 release ([c7af865](https://github.com/charks1/charks-outbox/commit/c7af865))
  * Add Javadoc to Quarkus constructors
  * Configure maven-release-plugin for automated releases
  * Fix SCM URLs to point to charks1 organization

## [1.0.0](https://github.com/charks1/charks-outbox/releases/tag/v1.0.0) (2026-01-31)

Initial release of Charks Outbox - a lightweight, framework-agnostic implementation of the Transactional Outbox pattern for Java 25+.

### 🚀 Features

#### Core

* **core:** add outbox domain model with sealed status hierarchy ([8c8991a](https://github.com/charks1/charks-outbox/commit/8c8991a))
* **core:** add repository and query abstractions ([b340e16](https://github.com/charks1/charks-outbox/commit/b340e16))
* **core:** add relay engine with pluggable retry policies ([5153180](https://github.com/charks1/charks-outbox/commit/5153180))

#### Storage Backends

* **jdbc:** add JDBC repository implementation ([a4f9dbd](https://github.com/charks1/charks-outbox/commit/a4f9dbd))
* **jdbc:** add Oracle and SQL Server dialect support ([d6040eb](https://github.com/charks1/charks-outbox/commit/d6040eb))
* **r2dbc:** add R2DBC reactive repository implementation ([911670c](https://github.com/charks1/charks-outbox/commit/911670c))
* **r2dbc:** implement R2DBC repository with reactive operations ([d9ff418](https://github.com/charks1/charks-outbox/commit/d9ff418))
* **mongodb:** add MongoDB repository implementation ([7909e15](https://github.com/charks1/charks-outbox/commit/7909e15))

#### Message Brokers

* **kafka:** add Kafka broker connector ([030a9a2](https://github.com/charks1/charks-outbox/commit/030a9a2))
* **rabbit:** add RabbitMQ broker connector ([e36f5be](https://github.com/charks1/charks-outbox/commit/e36f5be))
* **pulsar:** add Pulsar broker connector ([0a737e2](https://github.com/charks1/charks-outbox/commit/0a737e2))
* **sqs:** add AWS SQS broker connector ([f6d31d5](https://github.com/charks1/charks-outbox/commit/f6d31d5))
* **nats:** add NATS broker connector ([8692afe](https://github.com/charks1/charks-outbox/commit/8692afe))

#### Serializers

* **serializer-json:** add Jackson JSON serializer ([672fbab](https://github.com/charks1/charks-outbox/commit/672fbab))
* **serializer-avro:** add Apache Avro serializer ([6191779](https://github.com/charks1/charks-outbox/commit/6191779))
* **serializer-protobuf:** add Protocol Buffers serializer ([cbdeb62](https://github.com/charks1/charks-outbox/commit/cbdeb62))

#### Observability

* **micrometer:** add Micrometer metrics integration ([81ddcbc](https://github.com/charks1/charks-outbox/commit/81ddcbc))
* **opentelemetry:** add OpenTelemetry tracing integration ([e508ddc](https://github.com/charks1/charks-outbox/commit/e508ddc))

#### Frameworks

* **spring:** add Spring Boot auto-configuration ([ae7a171](https://github.com/charks1/charks-outbox/commit/ae7a171))
* **quarkus:** add Quarkus extension with CDI integration ([3bcf11f](https://github.com/charks1/charks-outbox/commit/3bcf11f))
* **test:** add in-memory repository and mock connector ([594473e](https://github.com/charks1/charks-outbox/commit/594473e))

### 🐛 Bug Fixes

* **jdbc:** use dialect-specific JSON placeholder for PostgreSQL JSONB ([c3240ee](https://github.com/charks1/charks-outbox/commit/c3240ee))
* **kafka:** add createdAt header for consistency with other brokers ([7e7b09a](https://github.com/charks1/charks-outbox/commit/7e7b09a))
* **mongodb:** throw exception when updating non-existent event ([1e23335](https://github.com/charks1/charks-outbox/commit/1e23335))
* **mongodb:** implement AutoCloseable interface ([47f37f3](https://github.com/charks1/charks-outbox/commit/47f37f3))
* **pulsar:** prevent test hanging with MockitoExtension and timeout ([ffe208d](https://github.com/charks1/charks-outbox/commit/ffe208d))
* **r2dbc:** handle RETRYABLE filter and nullable Archived.reason ([6de15f1](https://github.com/charks1/charks-outbox/commit/6de15f1))
* **r2dbc:** replace subscribe() with usingWhen for connection management ([dbc1ee4](https://github.com/charks1/charks-outbox/commit/dbc1ee4))
* **repository:** persist all status fields in updateStatus ([6323bfe](https://github.com/charks1/charks-outbox/commit/6323bfe))
* **sqs:** use QueueAttributeName enum for FIFO queue attributes ([973474a](https://github.com/charks1/charks-outbox/commit/973474a))
* **sqs:** handle closed client in isHealthy check ([6a7fb98](https://github.com/charks1/charks-outbox/commit/6a7fb98))
* add nullability annotations and remove redundant null checks ([deee63f](https://github.com/charks1/charks-outbox/commit/deee63f))
* address SonarCloud blocker and critical issues ([4d35f03](https://github.com/charks1/charks-outbox/commit/4d35f03))
* address SonarCloud major issues in production code ([94149a2](https://github.com/charks1/charks-outbox/commit/94149a2))
* address remaining SonarCloud major issues ([fb43612](https://github.com/charks1/charks-outbox/commit/fb43612))
* address SonarCloud issues across production and test code ([df49d0c](https://github.com/charks1/charks-outbox/commit/df49d0c))
* address code quality issues across modules ([fe4dd80](https://github.com/charks1/charks-outbox/commit/fe4dd80))

### ✅ Tests

* **spring:** add unit tests for Spring Boot auto-configuration ([d17fbc3](https://github.com/charks1/charks-outbox/commit/d17fbc3))
* **jdbc:** add OutboxTableSchema tests for Oracle and SQL Server dialects ([106faba](https://github.com/charks1/charks-outbox/commit/106faba))
* **mongodb:** mock UpdateResult in update test ([67978d8](https://github.com/charks1/charks-outbox/commit/67978d8))
* **brokers:** verify createdAt is transmitted in all broker IT tests ([9e2129a](https://github.com/charks1/charks-outbox/commit/9e2129a))
* **kafka,sqs:** add isHealthy after close tests ([0e35b8a](https://github.com/charks1/charks-outbox/commit/0e35b8a))
* add Testcontainers integration tests for R2DBC and MongoDB ([1a468ff](https://github.com/charks1/charks-outbox/commit/1a468ff))
* add Testcontainers integration tests for broker modules ([cb2f4e3](https://github.com/charks1/charks-outbox/commit/cb2f4e3))
* improve unit test coverage across modules ([885579e](https://github.com/charks1/charks-outbox/commit/885579e))
* fix SonarCloud code smells in test classes ([2a3786f](https://github.com/charks1/charks-outbox/commit/2a3786f))

### ♻️ Code Refactoring

* **core:** extract HeadersCodec and improve ReactiveOutboxRepository ([54ece48](https://github.com/charks1/charks-outbox/commit/54ece48))
* extract helper methods to reduce code duplication ([eb1e44f](https://github.com/charks1/charks-outbox/commit/eb1e44f))

### 📚 Documentation

* update README to reflect all implemented modules ([1463cf3](https://github.com/charks1/charks-outbox/commit/1463cf3))
* add SonarCloud quality gate badge ([0bea9c5](https://github.com/charks1/charks-outbox/commit/0bea9c5))
* fix badge URLs to use correct GitHub organization ([450a00c](https://github.com/charks1/charks-outbox/commit/450a00c))

### 👷 Continuous Integration

* add CI/CD workflows and documentation ([b194865](https://github.com/charks1/charks-outbox/commit/b194865))
* fix JaCoCo Java 25 support and upgrade CodeQL to v4 ([32ea388](https://github.com/charks1/charks-outbox/commit/32ea388))
* add separate job for integration tests with Testcontainers ([33b98cf](https://github.com/charks1/charks-outbox/commit/33b98cf))

### 🔧 Miscellaneous

* update central-publishing-maven-plugin to 0.9.0 ([e083edb](https://github.com/charks1/charks-outbox/commit/e083edb))
* **deps:** update dependencies for security fixes ([04fa620](https://github.com/charks1/charks-outbox/commit/04fa620))
* **kafka:** migrate from deprecated KafkaContainer ([4577d1d](https://github.com/charks1/charks-outbox/commit/4577d1d))
