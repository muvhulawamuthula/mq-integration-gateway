# Architecture

How the IBM MQ Integration Gateway bridges ISO 20022 `pacs.008` traffic to the payment processor
and publishes `pacs.002` replies — including the failure model that makes the boundary safe.

Companion service: [iso20022-processor](https://github.com/muvhulawamuthula/iso20022-processor).

---

## System context

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

The gateway is intentionally **payload-agnostic**. It never parses pacs.008 XML. The contract with
the processor is *"valid pacs.008 in, pacs.002 out"*. Request/reply correlation uses JMS headers
(`JMSCorrelationID`, falling back to `JMSMessageID`), so a message-version bump touches the
processor, not this bridge.

---

## Components

| Package / type | Role |
|---|---|
| `inbound.Pacs008Listener` | `@JmsListener` on `${gateway.queues.inbound}`; orchestrates forward → reply / DLQ; enforces `JMSXDeliveryCount` poison limit |
| `config.JmsConfig` | `DefaultJmsListenerContainerFactory` with **`sessionTransacted(true)`** and configured concurrency |
| `config.GatewayProperties` | Binds `gateway.*` (queues, processor URL/path, concurrency, max delivery attempts) |
| `config.EmbeddedBrokerConfig` | Artemis address settings for `!mq` profiles (high `maxDeliveryAttempts` backstop; app guard is authoritative) |
| `downstream.ProcessorClient` | `RestClient` POST of XML; maps 2xx → response, 5xx/I/O → `TransientDownstreamException`, 4xx → `PermanentDownstreamException` |
| `downstream.ProcessorResponse` | Record from HTTP body + `X-Payment-Status` / `X-Idempotent-Replay` headers |
| `outbound.ReplyPublisher` | Publishes pacs.002 to `JMSReplyTo` or default reply queue; sets `payment_status`, `idempotent_replay` |
| `outbound.DeadLetterRouter` | Sends body to DLQ with `gateway_dlq_reason`, `gateway_original_msg_id`, `gateway_failed_at` |
| `observability.GatewayMetrics` | Micrometer counters/timer for replies, DLQ kinds, transient retries, processor round-trip |

Profiles:

- **`dev`** (default) — embedded ActiveMQ Artemis; `MQAutoConfiguration` excluded
- **`mq`** — IBM MQ `ConnectionFactory` via `mq-jms-spring-boot-starter`; Artemis auto-config excluded

The same listener/publisher code runs on both brokers.

---

## Message flow

1. A `TextMessage` arrives on `PACS008.IN` (or configured inbound queue).
2. Listener reads `JMSXDeliveryCount`. If `deliveries > gateway.max-delivery-attempts` (default **3**),
   route to DLQ with reason `exhausted after N redeliveries…` and **return** (session commits).
3. Non-`TextMessage` payloads are dead-lettered immediately (`non_text`).
4. Otherwise `ProcessorClient.submit(xml)` POSTs `application/xml` to
   `{base-url}{path}` (default `http://localhost:8080/api/v1/payments/pacs008`).
5. On success, `ReplyPublisher` sends the pacs.002 body, correlating via inbound
   `JMSCorrelationID` or `JMSMessageID`, and surfaces status as JMS properties.
6. On `TransientDownstreamException`, the exception is rethrown → **transaction rolls back** →
   broker redelivers.
7. On `PermanentDownstreamException`, message is dead-lettered and the method returns normally →
   **commit** (message leaves the inbound queue).

---

## Failure semantics

| Situation | Action | Rationale |
|---|---|---|
| **Success** — processor returns pacs.002 (incl. business `RJCT`) | Publish reply, commit | A pacs.002 is a successful *exchange*. Business rejection is still an answer worth delivering. |
| **Transient** — `5xx`, connection refused / I/O | Roll back → redeliver; after delivery-count limit → DLQ (`exhausted`) | Retry may succeed. Processor `MsgId` idempotency makes redelivery safe. |
| **Permanent** — `4xx`, or non-text payload | DLQ immediately (`permanent` / `non_text`), commit | Retry cannot help; leaving the message would cause head-of-line blocking. |

Critical distinctions encoded in code:

- A **`pacs.002 RJCT`** arrives as HTTP **200** and is published as a reply — not a failure.
- Only **transport-level** failures (`5xx`, `ResourceAccessException`) are retried.
- Only failures that cannot succeed on retry (`4xx`, malformed/non-text) are dead-lettered at once.

Poison handling is enforced **in application code** on `JMSXDeliveryCount`, so behaviour matches
across Artemis (dev/test) and IBM MQ (prod). On IBM MQ, `mq/payments.mqsc` sets a very high
`BOTHRESH` so the queue manager's native JMS-client poison-requeue does not intercept first.

---

## Delivery guarantees

The gateway is **at-least-once by design**:

- The inbound message is acknowledged only after a successful forward **and** reply publish (or an
  intentional DLQ route).
- A crash mid-flight rolls back the transacted session; the broker redelivers.
- Correctness under redelivery depends on the **downstream processor being idempotent** on the
  pacs.008 `MsgId`.

Exactly-once across a queue and an HTTP call is a distributed-systems trap. Exactly-once-*effect*
via XA remains an optional production hardening step (see the production checklist in
[Getting started](getting-started.md#production-checklist)).

---

## Reply and DLQ contracts

### Reply (`ReplyPublisher`)

- Destination: inbound `JMSReplyTo` if set; else `gateway.queues.reply` (`PACS002.OUT`).
- `JMSCorrelationID` set for sender matching.
- JMS properties (valid Java identifiers — no hyphens):
  - `payment_status` — from `X-Payment-Status` (default `UNKNOWN` if missing)
  - `idempotent_replay` — from `X-Idempotent-Replay`

### Dead letter (`DeadLetterRouter`)

- Destination: `gateway.queues.dead-letter` (`DLQ`).
- Properties:
  - `gateway_dlq_reason` — human-readable cause
  - `gateway_original_msg_id` — original `JMSMessageID`
  - `gateway_failed_at` — epoch millis

---

## Observability

Actuator exposes `health`, `info`, `metrics`, `prometheus` on port **8081**.

| Meter | Tags | Meaning |
|---|---|---|
| `gateway.messages.replied` | `status`, `replay` | Replies published by processor group status / idempotent replay |
| `gateway.messages.dead_lettered` | `kind` | `permanent` / `non_text` / `exhausted` |
| `gateway.messages.transient_retries` | — | Transient failures that triggered redelivery |
| `gateway.processor.roundtrip` | timer (p50/p95/p99) | Forward → pacs.002 latency |

Logging pattern includes MDC `jmsMsgId` so a single message is traceable across forward → reply/DLQ.

---

## Design decisions

### 1. Payload-agnostic bridge

No XML parsing in the gateway. Correlation and routing use JMS headers/properties. Version changes
to pacs.008 stay inside the processor.

### 2. Transacted consume, not auto-acknowledge

Auto-ack would lose a message as soon as the processor hiccuped. A transacted session binds
acknowledgement to handler outcome: thrown transient exceptions roll the consume back.

### 3. Idempotent downstream over exactly-once transport

At-least-once + idempotent consumer is the honest design for queue→HTTP. The processor deduplicates
on `MsgId`.

### 4. Broker-agnostic code, broker-specific config

`@JmsListener` / `JmsTemplate` code is shared. Only the `ConnectionFactory` (and which
auto-configuration is excluded) differs by profile. Application-level poison handling on
`JMSXDeliveryCount` avoids depending on IBM MQ's JMS-client backout requeue (which needs extra
context authority on the backout queue).

### 5. JMS property names are valid Java identifiers

Status metadata uses `payment_status` / `idempotent_replay`, not the processor's hyphenated HTTP
header names (illegal as JMS property names), so consumers can route on status without parsing XML.

---

## Package layout

```
com.muvhulawa.gateway
├── GatewayApplication          @SpringBootApplication + @EnableJms
├── config/                     properties, transacted listener factory, Artemis customizer
├── inbound/                    Pacs008Listener
├── downstream/                 HTTP client + exception taxonomy + response record
├── outbound/                   reply + DLQ publishers
└── observability/              Micrometer façade
```

Runtime artifacts: multi-stage `Dockerfile` (Maven build → Temurin 21 JRE, non-root `appuser`,
default profile `mq`), `docker-compose.yml` (IBM MQ + processor + gateway), `mq/payments.mqsc`.
