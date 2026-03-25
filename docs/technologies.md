# Technologies

This document lists the main tools and technologies used in the project, why each one was chosen, and which alternatives were considered but not selected.

## Core Language And Runtime

### Java 17

## Build And Dependency Management

### Maven

## HTTP Service Framework

### Javalin

Chosen because:

- the project needs lightweight HTTP and WebSocket endpoints rather than a large enterprise framework
- Javalin keeps routing explicit and easy to follow

Alternatives considered:

- Spring Boot
- raw Jetty/Servlet APIs

Why they were not chosen:

- Spring Boot would have worked, but it adds more framework surface area than this project needs
- raw Servlet APIs would be more verbose and would slow down development without improving the architecture

## Messaging And Asynchronous Coordination

### RabbitMQ

Chosen because:

- the system needs asynchronous communication between upload, status, and processing paths
- RabbitMQ supports both event fan-out and work-queue patterns needed by the project
- it is easy to run locally and in Docker-based deployments

Alternatives considered:

- Apache Kafka

Why they were not chosen:

- Kafka is powerful but heavier than needed for this workload and local deployment model

## Object Storage

### MinIO

Chosen because:

- the system needs durable storage for uploaded chunks, processed segments, and HLS artifacts
- MinIO is S3-compatible, which keeps the storage interface realistic and portable
- it is simple to run locally for development and demos

Alternatives considered:

- AWS S3 directly

Why they were not chosen:

- AWS S3 would work well in production-like environments, but MinIO is easier for local development and course demonstration

## Relational Database

### PostgreSQL

Chosen because:

- the project needs durable structured state for upload status, video readiness, processing coordination, and recovery
- PostgreSQL supports transactions, row locking, and SQL semantics that are useful for distributed coordination
- it is reliable and familiar for local and containerized environments

Alternatives considered:

- MySQL
- SQLite
- MongoDB

Why they were not chosen:

- MySQL could support similar persistence needs, but PostgreSQL is a stronger fit for transaction-heavy coordination logic
- SQLite is not suitable as the shared coordination database for multi-node service replicas
- MongoDB is weaker for the row-locking and transactional coordination patterns used here

## Media Processing

### FFmpeg

## Containerization And Deployment

### Docker And Docker Compose

Chosen because:

- the system depends on several services that need reproducible local startup
- Docker Compose makes it easy to run Postgres, RabbitMQ, MinIO, and the application services together
- containerization reduces environment drift across machines

### Docker Swarm

Chosen because:

- the project needs a simple way to run multiple service replicas across nodes
- Docker Swarm is lighter-weight than Kubernetes and easier to integrate into the existing Docker-based workflow
- it is enough to demonstrate multi-node deployment and redundancy

## Logging And Testing

### Log4j / SLF4J

Chosen because:

- the system needs structured service logging across multiple Java modules
- these libraries are standard and integrate cleanly with Java services

### JUnit

Chosen because:

- it is the standard Java testing framework
- it integrates directly with Maven and the project’s module structure

## Summary

The technology choices favor:

- explainability over framework complexity
- local reproducibility over managed-service dependence
- durable shared infrastructure over ad hoc in-memory coordination
- tools that make distributed behavior visible and testable
