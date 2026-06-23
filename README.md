# IBM MQ Integration Gateway

[![CI](https://github.com/muvhulawamuthula/mq-integration-gateway/actions/workflows/ci.yml/badge.svg)](https://github.com/muvhulawamuthula/mq-integration-gateway/actions/workflows/ci.yml)
![Java](https://img.shields.io/badge/Java-21-blue)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.3-brightgreen)
![Messaging](https://img.shields.io/badge/IBM%20MQ-JMS-052FAD)

A Spring Boot service that bridges **ISO 20022 `pacs.008`** credit-transfer messages from an **IBM MQ**
queue to a payment processor over HTTP, and publishes the resulting **`pacs.002`** status report back
on a reply queue — with transacted consumption, bounded redelivery, and a dead-letter queue for
poison messages.

> Companion ingress to the **[ISO 20022 Payment Message Processor](https://github.com/muvhulawamuthula/iso20022-processor)**.
> Real interbank traffic does not arrive over HTTP; it arrives on a message queue (IBM MQ, SWIFT, a
> real-time clearing bus). This service is the adapter that turns *"a message on a queue"* into *"a
> call to the payment API"* — and its focus is the part that actually matters at that boundary:
> **what happens when things go wrong.**

---

## Contents

- [Architecture](#architecture)
- [Message flow & failure semantics](#message-flow--failure-semantics)
- [Delivery guarantees](#delivery-guarantees)
- [Getting started](#getting-started)
- [End-to-end demo with real IBM MQ](#end-to-end-demo-with-real-ibm-mq)
- [Configuration](#configuration)
- [Observability](#observability)
- [Testing](#testing)
- [Design decisions & trade-offs](#design-decisions--trade-offs)
- [Production checklist](#production-checklist)
- [Project structure](#project-structure)
- [Tech stack](#tech-stack)

---

## Architecture

```
  bank / clearing       ┌────────────────────────────┐        ISO 20022 Payment
  network               │   MQ Integration Gateway    │        Processor (HTTP)
                        │                             │
  pacs.008  ──MQ──────► │  @JmsListener (transacted)  │ ──POST /pacs008──►  validate
  (PACS008.IN)          │        │                    │                     settle
                        │        ▼                    │ ◄──pacs.002 (200)── acknowledge
                        │  ProcessorClient (RestClient)│
  pacs.002  ◄──MQ────── │  ReplyPublisher              │
  (PACS002.OUT)         │        │                    │
                        │        ▼ (on failure)       │
  poison /  ◄──MQ────── │  DeadLetterRouter            │
  exhausted (DLQ)       └────────────────────────────┘
```

The gateway is intentionally **payload-agnostic**: it never parses the pacs.008 XML. The contract
with the processor is *"valid pacs.008 in, pacs.002 out"*, and request/reply correlation uses JMS
headers — so a change to the message version touches the processor, not the bridge.

---

## Message flow & failure semantics

The listener runs in a **transacted JMS session**, so message acknowledgement is bound to the
outcome of handling it. That single property yields three correct behaviours:

| Situation | Action | Rationale |
|---|---|---|
| **Success** — processor returns a pacs.002 (incl. a business `RJCT`) | Publish the reply, commit (consume) | A pacs.002 is a *successful* exchange. A business rejection is still an answer worth delivering, not a failure. |
| **Transient** failure — processor down / `5xx` / connection refused | Roll back → **redeliver**; after the delivery-count limit, route to the **DLQ** | Retrying may succeed. The downstream processor's **`MsgId` idempotency** makes redelivery safe — a duplicate pacs.008 is deduplicated, never paid twice. |
| **Permanent** failure — `4xx`, or a non-text payload | Route straight to the **DLQ** (tagged with a reason), commit | Retrying cannot help, and leaving the message on the queue would block everything behind it (head-of-line blocking). |

The transient/permanent distinction is the crux:

- A `pacs.002 RJCT` (a business "no") comes back as a normal `200` and is **published as a reply** —
  it is not a failure.
- Only a **transport-level** failure (`5xx`, I/O) is retried.
- Only a failure that *cannot* succeed on retry (`4xx`, malformed payload) is dead-lettered
  immediately.

Poison handling (the redelivery limit) is enforced **in application code** on `JMSXDeliveryCount`,
so it behaves identically on every broker — see [decision 4](#4-broker-agnostic-code-broker-specific-config).

---

## Delivery guarantees

The gateway is **at-least-once by design**. It acknowledges the inbound message only after a
successful forward *and* reply publish; a crash mid-flight redelivers the message. Correctness under
that redelivery comes from the **downstream processor being idempotent** on the pacs.008 `MsgId`.

This is a deliberate choice over attempting exactly-once across a queue and an HTTP call (a
distributed-systems trap). Exactly-once-*effect* is available via XA if a deployment truly needs it
(see [production checklist](#production-checklist)) — at a real throughput and operational cost.

---

## Getting started

### Prerequisites

- JDK 21+
- Maven 3.9+
- Docker (only for the [end-to-end demo](#end-to-end-demo-with-real-ibm-mq))

### Run locally (zero external infrastructure)

The `dev` profile (default) starts an **embedded ActiveMQ Artemis** broker in-process, so no IBM MQ
is needed to run or develop the gateway.

```bash
# 1. Start the processor (companion repo) on :8080
git clone https://github.com/muvhulawamuthula/iso20022-processor && \
  (cd iso20022-processor && mvn spring-boot:run)

# 2. Start the gateway on :8081
mvn spring-boot:run
```

Put a `pacs.008` on `PACS008.IN`; the `pacs.002` appears on `PACS002.OUT`, and failures land on `DLQ`.

```bash
mvn test     # 4 integration tests on an embedded broker (see Testing)
```

---

## End-to-end demo with real IBM MQ

`docker-compose.yml` stands up the whole chain — **IBM MQ + processor + gateway** (running the `mq`
profile) — and wires them together. It assumes the companion processor repo is checked out as a
sibling directory (`../iso20022-processor`).

```bash
docker compose up --build      # gateway waits until ibmmq and processor are healthy
```

`mq/payments.mqsc` is applied at queue-manager start-up: it creates `PACS008.IN`, `PACS002.OUT`, and
`DLQ` and grants the `app` user access.

**Drive it** using the MQ sample programs in bindings mode inside the broker container:

```bash
# Send a pacs.008 (single-line so amqsput puts it as one string message)
docker compose exec ibmmq bash -c \
  '/opt/mqm/samp/bin/amqsput PACS008.IN QM1 < /demo/valid-pacs008-oneline.xml'

# Read the pacs.002 reply the gateway published
docker compose exec ibmmq /opt/mqm/samp/bin/amqsget PACS002.OUT QM1
```

The reply comes back with group status `ACSC`. To watch a failure path, `docker compose stop
processor` and send again: the message is retried, then dead-lettered to `DLQ` (read it with
`amqsget DLQ QM1`) tagged with a `gateway_dlq_reason` property. The MQ web console is at
<https://localhost:9443/ibmmq/console> (admin / `passw0rd`).

> **Poison handling on IBM MQ.** The gateway bounds redelivery itself, so no MQ-specific config is
> required. If you would rather have IBM MQ's JMS client requeue poison messages natively, set a
> backout policy *and* grant the connecting user `+setall` on the backout queue (the requeue passes
> message context):
> ```
> ALTER QLOCAL(PACS008.IN) BOTHRESH(3) BOQNAME(DLQ)
> SET AUTHREC PROFILE(DLQ) OBJTYPE(QUEUE) PRINCIPAL('app') AUTHADD(SETALL)
> ```

---

## Configuration

All settings are externalised. Defaults are sensible for local development; production values are
supplied via environment variables (see `docker-compose.yml` and `application-mq.yml`).

| Property | Env (compose) | Default | Description |
|---|---|---|---|
| `spring.profiles.active` | `SPRING_PROFILES_ACTIVE` | `dev` | `dev` = embedded Artemis; `mq` = IBM MQ |
| `gateway.queues.inbound` | — | `PACS008.IN` | Inbound pacs.008 request queue |
| `gateway.queues.reply` | — | `PACS002.OUT` | Default reply queue (used when no `JMSReplyTo`) |
| `gateway.queues.dead-letter` | — | `DLQ` | Dead-letter queue for poison / exhausted messages |
| `gateway.processor.base-url` | `GATEWAY_PROCESSOR_BASEURL` | `http://localhost:8080` | Payment processor base URL |
| `gateway.processor.path` | — | `/api/v1/payments/pacs008` | pacs.008 submission endpoint |
| `gateway.concurrency` | — | `1-5` | JMS listener concurrency (min-max consumers) |
| `gateway.max-delivery-attempts` | — | `3` | Redeliveries before the gateway dead-letters a message |
| `ibm.mq.queue-manager` | `MQ_QMGR` | `QM1` | IBM MQ queue manager (`mq` profile) |
| `ibm.mq.channel` | `MQ_CHANNEL` | `DEV.APP.SVRCONN` | Server-connection channel |
| `ibm.mq.conn-name` | `MQ_CONN_NAME` | `localhost(1414)` | `host(port)` of the queue manager |
| `ibm.mq.user` / `ibm.mq.password` | `MQ_APP_USER` / `MQ_APP_PASSWORD` | `app` / — | Application credentials |

---

## Observability

Actuator is enabled with a Prometheus endpoint at `http://localhost:8081/actuator/prometheus`. The
meters are framed around an integration point — not request counts, but message outcomes:

| Meter | Tags | Meaning |
|---|---|---|
| `gateway.messages.replied` | `status`, `replay` | Replies published, by processor group status |
| `gateway.messages.dead_lettered` | `kind` | Dead-letters, by `permanent` / `non_text` / `exhausted` |
| `gateway.messages.transient_retries` | — | Transient failures that triggered a redelivery |
| `gateway.processor.roundtrip` | (timer, p50/p95/p99) | Forward → pacs.002 latency |

Every log line for a message carries its inbound `JMSMessageID` via MDC, so a single message is
traceable across the forward → reply / dead-letter hops.

---

## Testing

```bash
mvn test
```

Four integration tests boot the full Spring context against an **embedded Artemis** broker and a
stubbed processor (an in-JVM `HttpServer`, no extra dependencies), covering every branch of the
failure model:

| Test | Asserts |
|---|---|
| Valid message | Forwarded; pacs.002 published to the reply queue, correlated via `JMSCorrelationID` |
| Transient failure (`5xx`) | Retried, then dead-lettered with `gateway_dlq_reason=exhausted…`; no reply produced |
| Permanent failure (`4xx`) | Dead-lettered immediately with a reason; no retries |
| Non-text payload | Treated as poison and dead-lettered |

The same broker-agnostic code is verified against real IBM MQ via the
[compose demo](#end-to-end-demo-with-real-ibm-mq).

---

## Design decisions & trade-offs

#### 1. Payload-agnostic bridge
The gateway never parses the pacs.008 XML. Correlation uses JMS headers (`JMSCorrelationID`, falling
back to `JMSMessageID`), not anything dug out of the body. The message contract — and any future
version bump — lives entirely in the processor.

#### 2. Transacted consume, not auto-acknowledge
Auto-ack would lose a message the instant the processor hiccuped. A transacted session means a
failed forward rolls the consume back, so the message is still on the queue to retry.

#### 3. Idempotent downstream over exactly-once transport
At-least-once delivery plus an idempotent consumer is the robust, honest design for a queue→HTTP
boundary. The processor deduplicates on `MsgId`, which is what makes redelivery safe.

#### 4. Broker-agnostic code, broker-specific config
The same `@JmsListener` / `JmsTemplate` code runs on embedded Artemis (dev/test) and IBM MQ (prod);
only the `ConnectionFactory` differs, in profile config. Poison handling is enforced in application
code on `JMSXDeliveryCount` rather than relying on a broker feature — IBM MQ's JMS-client
poison-requeue intercepts at the queue's backout threshold and requires extra context authority, so
leaning on it would make behaviour broker-specific and the failure surface larger.

#### 5. JMS property names are valid Java identifiers
Status metadata is surfaced as `payment_status` / `idempotent_replay` JMS properties — not the
processor's hyphenated HTTP header names, which JMS forbids — so downstream consumers can route on
status without parsing XML.

---

## Production checklist

- [ ] **XA / two-phase commit** between the consume and the reply publish, if the reply and the ack
      must be atomic. The current design is at-least-once on purpose (decision 3).
- [ ] **Edge schema pre-check** to reject malformed payloads before a network hop, trading a little
      duplication of the processor's XSD gate for lower downstream load.
- [ ] **Circuit breaker** on processor health, so a sustained outage stops pulling from the queue
      rather than spinning on redelivery.
- [ ] **Secrets management** for `MQ_APP_PASSWORD` (the demo uses a plain env var).
- [ ] **TLS** on the MQ channel and the processor HTTP endpoint.

---

## Project structure

```
src/main/java/com/muvhulawa/gateway
├── GatewayApplication.java
├── config/         GatewayProperties, JmsConfig (transacted factory), EmbeddedBrokerConfig (dev/test)
├── inbound/        Pacs008Listener  — the @JmsListener orchestrating the flow
├── downstream/     ProcessorClient, ProcessorResponse, Transient/PermanentDownstreamException
├── outbound/       ReplyPublisher, DeadLetterRouter
└── observability/  GatewayMetrics
src/main/resources
├── application.yml         (defaults; active profile = dev)
├── application-dev.yml     (embedded Artemis)
└── application-mq.yml      (IBM MQ)
mq/payments.mqsc            queue definitions applied by the IBM MQ container
docker-compose.yml          IBM MQ + processor + gateway
```

---

## Tech stack

Java 21 · Spring Boot 3.3 · Spring JMS · IBM MQ (`mq-jms-spring-boot-starter`) · embedded ActiveMQ
Artemis (dev/test) · RestClient · Micrometer / Prometheus · JUnit 5 · Docker · GitHub Actions
