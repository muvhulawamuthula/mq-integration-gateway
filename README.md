# IBM MQ Integration Gateway

A Spring Boot service that bridges **ISO 20022 `pacs.008`** credit-transfer messages from an
**IBM MQ** queue to the [ISO 20022 Payment Message Processor](../iso20022-processor) over HTTP,
and publishes the resulting **`pacs.002`** status report back on a reply queue.

This is the companion ingress to the processor: real interbank traffic does not arrive over HTTP,
it arrives on a message queue (IBM MQ, SWIFT, a real-time clearing bus). The gateway is the
adapter that turns "a message on a queue" into "a call to the payment service" — and it is built to
demonstrate the part that actually matters at that boundary: **what happens when things go wrong.**

```
  bank / clearing       ┌───────────────────────────┐        ISO 20022 Payment
  network               │   MQ Integration Gateway   │        Processor (HTTP)
                        │                            │
  pacs.008  ──MQ──────► │  @JmsListener (transacted) │ ──POST /pacs008──►  validate
  (PACS008.IN)          │        │                   │                     settle
                        │        ▼                   │ ◄──pacs.002 (200)── acknowledge
                        │  ProcessorClient (HTTP)    │
  pacs.002  ◄──MQ────── │  ReplyPublisher            │
  (PACS002.OUT)         │        │                   │
                        │        ▼ (on failure)      │
  poison /  ◄──MQ────── │  DeadLetterRouter          │
  exhausted (DLQ)       └───────────────────────────┘
```

---

## The three outcomes (the part that matters)

The gateway runs its listener in a **transacted JMS session**, so acknowledgement is tied to the
outcome of handling the message. That single fact gives three correct behaviours:

| Situation | What the gateway does | Why |
|---|---|---|
| **Success** — processor returns a pacs.002 (incl. a business `RJCT`) | Publish the reply, commit (consume) | A pacs.002 is a *successful* exchange. A business rejection is still an answer worth delivering — not a failure. |
| **Transient** failure — processor down / 5xx / connection refused | Roll back → broker **redelivers**; after the backout threshold the broker dead-letters it | Retrying may succeed. The processor's **`MsgId` idempotency** makes redelivery safe — a duplicate pacs.008 is deduplicated, never paid twice. |
| **Permanent** failure — 4xx, or a non-text payload | Route straight to the **DLQ** with a reason tag, commit | Retrying cannot help, and leaving it on the queue would block every message behind it (head-of-line blocking). |

The transient/permanent split is the crux: a `pacs.002 RJCT` (business "no") comes back as a normal
`200` and is published as a reply; only a **transport-level** failure is retried; only a failure
that *cannot* succeed on retry is dead-lettered immediately.

> **Why redelivery is safe here.** The gateway is intentionally at-least-once: it acknowledges only
> after a successful forward + reply. A crash mid-flight redelivers the message, and the downstream
> processor's idempotency key absorbs the duplicate. This is the whole reason the two services are
> designed as a pair.

---

## Run it

The gateway runs out of the box on an **embedded ActiveMQ Artemis** broker — no IBM MQ needed to
develop or test it locally. In production it targets IBM MQ via the `mq` profile; the listener and
publisher code is broker-agnostic Spring JMS and does not change.

```bash
# 1. Start the processor (companion project) on :8080
cd ../iso20022-processor && mvn spring-boot:run

# 2. Start the gateway on :8081 (embedded broker, dev profile)
cd ../mq-integration-gateway && mvn spring-boot:run
```

Drop a `pacs.008` on `PACS008.IN` and the pacs.002 appears on `PACS002.OUT`; failures land on `DLQ`.

```bash
mvn test   # 4 integration tests on an embedded broker + a stubbed processor:
           # forward+reply with correlation, transient->redelivery->DLQ,
           # permanent->DLQ, and non-text poison->DLQ
```

### Run it in Docker (production / IBM MQ profile)

```bash
docker build -t mq-integration-gateway .
docker run -p 8081:8081 \
  -e MQ_QMGR=QM1 -e MQ_CHANNEL=DEV.APP.SVRCONN -e MQ_CONN_NAME='mqhost(1414)' \
  -e MQ_APP_USER=app -e MQ_APP_PASSWORD=*** \
  mq-integration-gateway     # SPRING_PROFILES_ACTIVE=mq is baked into the image
```

On the queue manager, give the inbound queue a backout policy so the broker itself moves poison
messages aside:

```
ALTER QLOCAL(PACS008.IN) BOTHRESH(3) BOQNAME(DLQ)
```

---

## End-to-end demo with real IBM MQ (`docker compose`)

`docker-compose.yml` stands up the whole chain — **IBM MQ**, the **processor**, and the **gateway**
running on the `mq` profile — and wires them together. It assumes the companion processor repo is a
sibling directory (`../iso20022-processor`).

```bash
docker compose up --build      # starts ibmmq, processor, gateway (gateway waits for both to be healthy)
```

`mq/payments.mqsc` is applied to the queue manager at start-up: it creates `PACS008.IN`,
`PACS002.OUT`, and `DLQ` and grants the `app` user access.

**Drive it** — put a pacs.008 on the inbound queue and read the pacs.002 reply, using the MQ sample
programs in bindings mode inside the broker container:

```bash
# 1. Send a pacs.008 (single-line so amqsput puts it as one string message)
docker compose exec ibmmq bash -c \
  '/opt/mqm/samp/bin/amqsput PACS008.IN QM1 < /demo/valid-pacs008-oneline.xml'

# 2. Read the pacs.002 reply the gateway published
docker compose exec ibmmq /opt/mqm/samp/bin/amqsget PACS002.OUT QM1
```

You'll see the `ACSC` pacs.002 come back on `PACS002.OUT`. To watch a failure path, stop the
processor (`docker compose stop processor`) and send again: the message is retried, then lands on
`DLQ` (read it with `amqsget DLQ QM1`) — tagged with `gateway_dlq_reason`. The MQ web console is at
<https://localhost:9443/ibmmq/console> (admin / `passw0rd`) if you prefer to watch queue depths.

Gateway and processor metrics are at `http://localhost:8081/actuator/prometheus` and
`http://localhost:8080/actuator/prometheus`.

---

## Design decisions

**1. The gateway is payload-agnostic.** It never parses the pacs.008 XML. The contract with the
processor is just "valid pacs.008 in, pacs.002 out", and correlation uses JMS headers
(`JMSCorrelationID`, falling back to `JMSMessageID`), not anything dug out of the body. Swapping the
message version touches the processor, not the bridge.

**2. Transacted consume, not auto-acknowledge.** Auto-ack would lose a message the instant the
processor hiccuped. A transacted session means a failed forward rolls the consume back so the
message is still there to retry.

**3. Idempotent downstream over exactly-once transport.** Exactly-once across a queue and an HTTP
call is a distributed-systems trap. The honest, robust design is at-least-once delivery plus an
idempotent consumer — which is exactly what the processor provides on `MsgId`.

**4. Broker-agnostic code, broker-specific config.** The same `@JmsListener` / `JmsTemplate` code
runs on embedded Artemis (dev/test) and IBM MQ (prod). Only the `ConnectionFactory` and the backout
policy differ, and those live in profile config — `application-dev.yml` vs `application-mq.yml`.

**5. JMS property names are valid Java identifiers.** Status metadata is surfaced as
`payment_status` / `idempotent_replay` JMS properties (not the processor's hyphenated HTTP header
names, which JMS forbids) so downstream consumers can route on status without parsing XML.

**6. Observability for an integration point.** Micrometer meters track the reply mix by status, the
dead-letter count by kind, the transient-retry rate, and processor round-trip latency — exposed at
`/actuator/prometheus`. Every log line carries the inbound `JMSMessageID` via MDC.

---

## What's next (honest)

- **XA / two-phase commit** between the consume and the reply publish, if you need the reply and the
  ack to be atomic. The current design is at-least-once on purpose (see decision 3); XA buys
  exactly-once-effect at a real throughput and operational cost.
- **Schema pre-check at the edge** to reject malformed payloads before a network hop, trading a
  little duplication of the processor's XSD gate for lower downstream load.
- **Backpressure** tied to processor health (circuit breaker) so a sustained outage stops pulling
  from the queue rather than spinning on redelivery.

---

## Stack

Java 21 · Spring Boot 3.3 · Spring JMS · IBM MQ (`mq-jms-spring-boot-starter`) · embedded ActiveMQ
Artemis (dev/test) · RestClient · Micrometer / Prometheus · JUnit 5 · Docker · GitHub Actions
