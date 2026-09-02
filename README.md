# IBM MQ Integration Gateway

[![CI](https://github.com/muvhulawamuthula/mq-integration-gateway/actions/workflows/ci.yml/badge.svg)](https://github.com/muvhulawamuthula/mq-integration-gateway/actions/workflows/ci.yml)
![Java](https://img.shields.io/badge/Java-21-blue)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.3-brightgreen)
![Messaging](https://img.shields.io/badge/IBM%20MQ-JMS-052FAD)

Spring Boot **queue → HTTP → reply** bridge for ISO 20022 payments.

Inbound **`pacs.008`** credit transfers arrive on IBM MQ (`PACS008.IN`). The gateway forwards
them to the [ISO 20022 Payment Message Processor](https://github.com/muvhulawamuthula/iso20022-processor)
over HTTP, then publishes the processor's **`pacs.002`** status report to `PACS002.OUT`. Failures
that cannot (or should not) be retried land on a **DLQ**, with bounded redelivery for transient
errors.

The service is **payload-agnostic**: it never parses the pacs.008 XML. Correlation uses JMS
headers; the message contract lives entirely in the processor.

> Real interbank traffic arrives on a queue, not over HTTP. This gateway is the adapter at that
> boundary — and its focus is **what happens when things go wrong**.

---

## Docs

| Document | What it covers |
|---|---|
| [Architecture](docs/architecture.md) | Component map, message flow, failure model, delivery guarantees, design decisions |
| [Getting started](docs/getting-started.md) | Local run, IBM MQ demo, configuration, tests, production checklist |

---

## Quick start

```bash
# Terminal 1 — companion processor on :8080
git clone https://github.com/muvhulawamuthula/iso20022-processor
cd iso20022-processor && mvn spring-boot:run

# Terminal 2 — gateway on :8081 (dev profile: embedded Artemis, no IBM MQ)
mvn spring-boot:run

# Verify
mvn test
```

Full walkthrough (including the Docker Compose IBM MQ demo): **[Getting started](docs/getting-started.md)**.

---

## Architecture at a glance

```
  bank / clearing       ┌────────────────────────────┐        ISO 20022 Payment
  network               │   MQ Integration Gateway    │        Processor (HTTP)
                        │                             │
  pacs.008  ──MQ──────► │  Pacs008Listener (tx JMS)   │ ──POST /pacs008──►  validate
  (PACS008.IN)          │        │                    │                     settle
                        │        ▼                    │ ◄──pacs.002 (200)── acknowledge
                        │  ProcessorClient (RestClient)│
  pacs.002  ◄──MQ────── │  ReplyPublisher              │
  (PACS002.OUT)         │        │                    │
                        │        ▼ (on failure)       │
  poison /  ◄──MQ────── │  DeadLetterRouter            │
  exhausted (DLQ)       └────────────────────────────┘
```

| Outcome | What the gateway does |
|---|---|
| Processor returns pacs.002 (incl. business `RJCT`) | Publish reply, commit consume |
| Transient failure (`5xx` / unreachable) | Roll back → redeliver; after `gateway.max-delivery-attempts` (default 3), route to DLQ |
| Permanent failure (`4xx` / non-text payload) | Route to DLQ immediately (tagged with `gateway_dlq_reason`), commit |

Delivery is **at-least-once**. Safety under redelivery comes from the processor's **`MsgId`
idempotency** — a duplicate pacs.008 is deduplicated, never paid twice.

Deep dive: **[Architecture](docs/architecture.md)**.

---

## Configuration (essentials)

| Property | Default | Notes |
|---|---|---|
| `spring.profiles.active` | `dev` | `dev` = embedded Artemis; `mq` = IBM MQ |
| `gateway.queues.inbound` / `reply` / `dead-letter` | `PACS008.IN` / `PACS002.OUT` / `DLQ` | Queue names |
| `gateway.processor.base-url` | `http://localhost:8080` | Env: `GATEWAY_PROCESSOR_BASEURL` (compose) |
| `gateway.processor.path` | `/api/v1/payments/pacs008` | POST endpoint |
| `gateway.max-delivery-attempts` | `3` | App-level poison limit via `JMSXDeliveryCount` |
| `gateway.concurrency` | `1-5` | JMS listener min-max consumers |
| `server.port` | `8081` | Actuator / Prometheus on same port |

Full table and IBM MQ connection settings: **[Getting started → Configuration](docs/getting-started.md#configuration)**.

---

## Observability

- Actuator + Prometheus: `http://localhost:8081/actuator/prometheus`
- Meters: `gateway.messages.replied`, `gateway.messages.dead_lettered`, `gateway.messages.transient_retries`, `gateway.processor.roundtrip` (p50/p95/p99)
- Every log line carries inbound `JMSMessageID` via MDC (`jmsMsgId=…`)

---

## Project structure

```
src/main/java/com/muvhulawa/gateway
├── GatewayApplication.java
├── config/         GatewayProperties, JmsConfig (transacted), EmbeddedBrokerConfig (!mq)
├── inbound/        Pacs008Listener — @JmsListener orchestration
├── downstream/     ProcessorClient, ProcessorResponse, Transient/Permanent exceptions
├── outbound/       ReplyPublisher, DeadLetterRouter
└── observability/  GatewayMetrics
src/main/resources
├── application.yml         defaults; active profile = dev
├── application-dev.yml     embedded Artemis
└── application-mq.yml      IBM MQ ConnectionFactory
mq/payments.mqsc            queue defs for the IBM MQ container
docker-compose.yml          IBM MQ + processor + gateway
docs/                       architecture & getting-started
```

---

## Tech stack

Java 21 · Spring Boot 3.3.4 · Spring JMS · IBM MQ (`mq-jms-spring-boot-starter` 3.5.1) ·
embedded ActiveMQ Artemis (dev/test) · RestClient · Micrometer / Prometheus · JUnit 5 ·
Docker · GitHub Actions
