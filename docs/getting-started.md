# Getting started

Run and verify the IBM MQ Integration Gateway locally, then exercise the full chain against real
IBM MQ via Docker Compose.

---

## Prerequisites

- **JDK 21+**
- **Maven 3.9+**
- **Docker** (only for the [end-to-end IBM MQ demo](#end-to-end-demo-with-real-ibm-mq))
- Companion processor repo for local/HTTP demos:
  [iso20022-processor](https://github.com/muvhulawamuthula/iso20022-processor)

---

## Run locally (zero external broker)

The default **`dev`** profile starts an **embedded ActiveMQ Artemis** broker in-process. No IBM MQ
installation is required to develop or test.

```bash
# 1. Start the processor on :8080
git clone https://github.com/muvhulawamuthula/iso20022-processor
cd iso20022-processor && mvn spring-boot:run

# 2. Start the gateway on :8081 (from this repo)
mvn spring-boot:run
```

Queues pre-created by `application-dev.yml`: `PACS008.IN`, `PACS002.OUT`, `DLQ`.

Put a pacs.008 `TextMessage` on `PACS008.IN`; the pacs.002 appears on `PACS002.OUT`. Permanent /
exhausted failures land on `DLQ` with `gateway_dlq_reason`.

```bash
mvn test
```

---

## End-to-end demo with real IBM MQ

`docker-compose.yml` brings up **IBM MQ + processor + gateway** (`mq` profile). It expects the
companion processor repo checked out as a **sibling** directory:

```text
../iso20022-processor   ← build context for the processor service
./                      ← this gateway repo
```

```bash
docker compose up --build
# gateway waits until ibmmq and processor are healthy
```

`mq/payments.mqsc` runs at queue-manager start-up: defines `PACS008.IN`, `PACS002.OUT`, `DLQ`, and
grants the `app` user access. Inbound `BOTHRESH` is set very high so **application-level** poison
handling (not MQ's JMS-client backout requeue) is authoritative.

### Drive the demo

Use MQ sample programs inside the broker container (bindings mode). The sample pacs.008 is mounted
at `/demo/valid-pacs008-oneline.xml` (single-line so `amqsput` puts one string message):

```bash
# Send a pacs.008
docker compose exec ibmmq bash -c \
  '/opt/mqm/samp/bin/amqsput PACS008.IN QM1 < /demo/valid-pacs008-oneline.xml'

# Read the pacs.002 reply
docker compose exec ibmmq /opt/mqm/samp/bin/amqsget PACS002.OUT QM1
```

Expect group status **`ACSC`** on the happy path (`payment_status` JMS property on the reply).

Failure path: `docker compose stop processor`, send again — the message is retried, then
dead-lettered to `DLQ` (`amqsget DLQ QM1`) with `gateway_dlq_reason`.

MQ web console: <https://localhost:9443/ibmmq/console> — user `admin` / password `passw0rd`.

Gateway HTTP: `http://localhost:8081` (Actuator / Prometheus). Processor: `http://localhost:8080`.

### Optional: MQ-native poison requeue

The gateway already bounds redelivery in code. To prefer IBM MQ's JMS-client backout instead, set a
backout policy **and** grant the connecting user `+setall` on the backout queue (requeue passes
message context):

```
ALTER QLOCAL(PACS008.IN) BOTHRESH(3) BOQNAME(DLQ)
SET AUTHREC PROFILE(DLQ) OBJTYPE(QUEUE) PRINCIPAL('app') AUTHADD(SETALL)
```

---

## Configuration

Defaults favour local development; production values come from environment / profile
(`application-mq.yml`, `docker-compose.yml`).

| Property | Env (compose / typical) | Default | Description |
|---|---|---|---|
| `spring.profiles.active` | `SPRING_PROFILES_ACTIVE` | `dev` | `dev` = embedded Artemis; `mq` = IBM MQ |
| `gateway.queues.inbound` | — | `PACS008.IN` | Inbound pacs.008 queue |
| `gateway.queues.reply` | — | `PACS002.OUT` | Default reply queue when no `JMSReplyTo` |
| `gateway.queues.dead-letter` | — | `DLQ` | Dead-letter queue |
| `gateway.processor.base-url` | compose via `SPRING_APPLICATION_JSON` / `GATEWAY_PROCESSOR_BASEURL` | `http://localhost:8080` | Processor base URL |
| `gateway.processor.path` | — | `/api/v1/payments/pacs008` | Submission path |
| `gateway.concurrency` | — | `1-5` | Listener min-max consumers |
| `gateway.max-delivery-attempts` | — | `3` | App-level redeliveries before DLQ |
| `server.port` | — | `8081` | HTTP / Actuator port |
| `ibm.mq.queue-manager` | `MQ_QMGR` | `QM1` | Queue manager (`mq` profile) |
| `ibm.mq.channel` | `MQ_CHANNEL` | `DEV.APP.SVRCONN` | SVRCONN channel |
| `ibm.mq.conn-name` | `MQ_CONN_NAME` | `localhost(1414)` | `host(port)` |
| `ibm.mq.user` | `MQ_APP_USER` | `app` | Application user |
| `ibm.mq.password` | `MQ_APP_PASSWORD` | _(empty)_ | Application password |

Docker image (`Dockerfile`) defaults `SPRING_PROFILES_ACTIVE=mq`, exposes `8081`, runs as UID
`10001`, and health-checks TCP readiness on that port.

---

## Testing

```bash
mvn test
# CI: mvn -B verify (see .github/workflows/ci.yml)
```

`GatewayBridgeIntegrationTest` boots the full Spring context on the **`dev`** profile (embedded
Artemis) against an in-JVM JDK `HttpServer` stub processor — no external broker or Docker.

| Test | Asserts |
|---|---|
| Valid message | Forwarded; pacs.002 on reply queue; `JMSCorrelationID` preserved; `payment_status=ACSC` |
| Transient failure (`503`) | Retried then DLQ with `gateway_dlq_reason` containing `exhausted`; no reply |
| Permanent failure (`400`) | Immediate DLQ with rejection reason; no reply |
| Non-text payload | DLQ with reason containing `non-text` |

Broker-agnostic code is also exercised against real IBM MQ via the compose demo above.

---

## Observability quick check

With the gateway running:

```bash
curl -s http://localhost:8081/actuator/health
curl -s http://localhost:8081/actuator/prometheus | grep gateway.messages
```

Log lines include `jmsMsgId=<JMSMessageID>` via MDC for per-message tracing.

---

## Production checklist

- [ ] **XA / two-phase commit** between consume and reply publish if reply and ack must be atomic
      (current design is at-least-once on purpose — see [Architecture](architecture.md#delivery-guarantees)).
- [ ] **Edge schema pre-check** to reject malformed payloads before the network hop (optional load
      trade-off vs duplicating the processor's XSD gate).
- [ ] **Circuit breaker** on processor health so a sustained outage stops pulling rather than
      spinning on redelivery.
- [ ] **Secrets management** for `MQ_APP_PASSWORD` (compose demo uses a plain env var).
- [ ] **TLS** on the MQ channel and the processor HTTP endpoint.
- [ ] Confirm queue manager backout policy aligns with application `max-delivery-attempts` (demo
      MQSC keeps broker backout from racing the app guard).
