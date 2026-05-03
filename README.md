# Distributed Notification System

Event-driven notification pipeline built on the transactional outbox pattern. Guarantees at-least-once delivery from API ingestion through RabbitMQ to partitioned consumers, with Redis-backed idempotency and a cascading retry chain.

---

## Architecture Diagram

```
┌─────────────┐     POST /notify
│  API Client │─────────────────────────────────────────────────────┐
└─────────────┘                                                      │
                                                                     ▼
                                                          ┌─────────────────────┐
                                                          │   Producer Service  │
                                                          │  Zod validate       │
                                                          │  generate UUID      │
                                                          └──────────┬──────────┘
                                                                     │ INSERT status=PENDING
                                                                     ▼
                                                          ┌─────────────────────┐
                                                          │  PostgreSQL Outbox  │
                                                          └──────────┬──────────┘
                                                                     │ SELECT FOR UPDATE
                                                                     │ SKIP LOCKED LIMIT 50
                                                                     ▼
                                                          ┌─────────────────────┐
                                                          │   Outbox Poller     │
                                                          │  hash(eventId) % 2  │
                                                          └──────────┬──────────┘
                                                                     │ publish persistent
                                                                     ▼
                                                  ┌──────────────────────────────────┐
                                                  │  notifications.exchange (topic)   │
                                                  └────┬──────────────┬──────────────┘
                                                  email.1          email.2        sms.1
                                                       │              │              │
                                              ┌────────┘    ┌────────┘    ┌─────────┘
                                              ▼             ▼             ▼
                                        email.queue.1  email.queue.2  sms.queue.1
                                              │
                                              ▼ prefetch=10
                                        ┌──────────────┐
                                        │   Consumer   │
                                        │ Redis NX EX  │
                                        └──────┬───────┘
                                               │ NACK (requeue=false)
                                               ▼
                              ┌────────────────────────────────────┐
                              │  retry_1s → retry_5s → retry_30s  │
                              │  (TTL-based dead-letter chaining)  │
                              └──────────────────┬─────────────────┘
                                                 ▼
                                        notifications.dlq
```

---

## System Flow

1. Client sends `POST /notify` with `type` and `payload`.
2. Producer validates the body with Zod, generates a `correlationId` (UUID), writes a row to the `outbox` table with `status=PENDING`.
3. Outbox poller runs in a tight loop. Each iteration opens a Prisma interactive transaction and executes `SELECT ... FOR UPDATE SKIP LOCKED LIMIT 50` to claim a batch of pending events without blocking other poller instances.
4. For each row, the poller computes `hash(eventId) % 2` to assign an email partition (`email.1` or `email.2`), or routes to `sms.1` for SMS events. It publishes the message as `persistent` JSON to `notifications.exchange` with `correlationId` in the AMQP native property.
5. Successfully published events are batch-updated to `status=PROCESSED` within the same transaction. Failed publishes stay `PENDING` for the next iteration. If the batch was full (50), the loop continues immediately; otherwise it sleeps 1 second.
6. The consumer (bound to `email.queue.1`) calls `channel.prefetch(10)` to limit in-flight messages, then for each delivery: attempts `SET idempotency:{eventId} 1 EX 86400 NX` in Redis. A `null` return means duplicate — message is ACKed and dropped. An `OK` return means new — message is processed and ACKed.
7. Any unhandled error during processing triggers `channel.nack(msg, false, false)` with `requeue=false`, handing control to the dead-letter exchange.

---

## Key Design Decisions

### Outbox Pattern
The producer writes to the database and nothing else. There is no direct RabbitMQ publish in the API path. This eliminates the dual-write problem: if the broker is unavailable, events persist in the database and are delivered once the broker recovers. The outbox table is the source of truth for every event that must be delivered.

### At-Least-Once Delivery
`SELECT FOR UPDATE SKIP LOCKED` inside a transaction ensures only one poller instance processes a row at a time. The lock is held until the transaction commits, so concurrent pollers never duplicate work. However, if the poller crashes after publishing but before committing the `PROCESSED` update, the row remains `PENDING` and will be republished on the next poll — making delivery at-least-once, not exactly-once. Idempotent consumers handle the duplicate.

### Idempotency via Redis
```
SET idempotency:{eventId} 1 EX 86400 NX
```
`NX` makes the set atomic and conditional. The first consumer to process a given `eventId` succeeds; any subsequent attempt (duplicate delivery) returns `null` and is ACKed immediately. The 24-hour expiry balances storage cost against the realistic redelivery window.

### Partitioning Strategy
Two email queues (`email.queue.1`, `email.queue.2`) allow independent consumer scaling per partition. Routing is deterministic: `hash(eventId) % 2` maps each event to a fixed queue, preserving per-event ordering within a partition. This is a lightweight alternative to the consistent-hash exchange plugin, with no additional infrastructure dependency.

### Retry and DLQ Design
Main queues declare `x-dead-letter-exchange` pointing to `notifications.retry.exchange`. On NACK with `requeue=false`, the broker routes the message through a TTL-based cascade:

| Queue       | TTL   | Dead-letters to |
|-------------|-------|-----------------|
| retry_1s    | 1 s   | retry_5s        |
| retry_5s    | 5 s   | retry_30s       |
| retry_30s   | 30 s  | notifications.dlq |
| notifications.dlq | — | (terminal)    |

All retry queues are durable with persistent messages. The DLQ has no further dead-letter exchange, preventing infinite loops.

---

## Tech Stack

| Component      | Technology                    | Purpose                          |
|----------------|-------------------------------|----------------------------------|
| API            | Express 4, Zod                | Request ingestion, validation    |
| ORM            | Prisma 5 + PostgreSQL 16      | Outbox persistence, typed queries |
| Message broker | RabbitMQ 3.13                 | Event routing, retry topology    |
| Cache          | Redis 7, ioredis              | Idempotency key store            |
| Language       | TypeScript 5 (strict)         | Type safety across all services  |
| Runtime        | Node.js 20 (Debian/bullseye)  | glibc required for Prisma engine |
| Logging        | pino                          | Structured JSON logs             |
| Containers     | Docker Compose                | Full local environment           |

---

## Project Structure

```
.
├── docker-compose.yml          # Full local environment definition
├── tsconfig.base.json          # Shared TS compiler options (strict mode)
├── package.json                # npm workspaces root
└── packages/
    ├── shared/                 # Internal package: types, logger, config,
    │   └── src/                # RabbitMQ topology setup, constants
    │       ├── types.ts
    │       ├── logger.ts
    │       ├── config.ts
    │       ├── constants.ts
    │       ├── topology.ts
    │       └── index.ts
    ├── producer/               # HTTP API service
    │   ├── prisma/schema.prisma
    │   └── src/
    │       ├── routes/notify.ts
    │       ├── db.ts
    │       └── index.ts
    ├── outbox-poller/          # Standalone polling process
    │   ├── prisma/schema.prisma
    │   └── src/
    │       ├── poller.ts       # Batch loop, transaction, FOR UPDATE SKIP LOCKED
    │       ├── publisher.ts    # Partition routing, AMQP publish
    │       ├── db.ts
    │       └── index.ts
    └── consumer/               # Message consumer
        └── src/
            ├── redis.ts        # Idempotency helpers
            └── index.ts        # consume, prefetch, ack/nack logic
```

---

## Local Setup

**Prerequisites:** Docker Desktop running.

```bash
# Clone and start all services
git clone <repo>
cd notification-system
docker compose up --build
```

Services started:
- RabbitMQ broker + management UI: `localhost:15672` (guest/guest)
- PostgreSQL: `localhost:5432`
- Redis: `localhost:6379`
- Producer API: `localhost:3000`
- Outbox poller and consumer start automatically

**For local development without Docker (app code only):**

```bash
# Start only infra
docker compose up rabbitmq postgres redis -d

cp .env.example .env
npm install
npm run build -w @notification/shared

# Generate Prisma clients
npx prisma generate --schema packages/producer/prisma/schema.prisma
npx prisma generate --schema packages/outbox-poller/prisma/schema.prisma

# Push schema
npx prisma db push --schema packages/producer/prisma/schema.prisma

# Run services in separate terminals
npm run dev:producer
npm run dev:poller
npm run dev:consumer
```

---

## API Usage

**Endpoint:** `POST /notify`

```bash
# Email notification
curl -X POST http://localhost:3000/notify \
  -H "Content-Type: application/json" \
  -d '{
    "type": "email",
    "payload": {
      "to": "user@example.com",
      "subject": "Your order shipped",
      "body": "Order #12345 is on its way."
    }
  }'

# Response
{
  "eventId": "b3d2f1a0-...",
  "correlationId": "e7c9a2b1-..."
}

# SMS notification
curl -X POST http://localhost:3000/notify \
  -H "Content-Type: application/json" \
  -d '{
    "type": "sms",
    "payload": {
      "to": "+919876543210",
      "message": "Your OTP is 482910"
    }
  }'
```

**Request schema:**
```typescript
{
  type: "email" | "sms"       // required
  payload: Record<string, unknown>  // arbitrary JSON, required
}
```

**Response:** `202 Accepted` with `eventId` and `correlationId`. The event is durably stored — delivery is guaranteed even if downstream services are temporarily unavailable.

---

## Failure Handling

### Broker unavailable at publish time
The outbox poller catches publish errors per-event. Failed events remain `PENDING` in the database. On the next poll iteration (within 1 second), they are retried. No event is lost.

### Consumer processing failure
Any exception during message processing triggers:
```
channel.nack(msg, false, false)  // requeue=false → broker routes to DLX
```
The dead-letter cascade activates:
```
email.queue.1 (fail)
  → retry_1s  (wait 1s, TTL expires)
  → retry_5s  (wait 5s, TTL expires)
  → retry_30s (wait 30s, TTL expires)
  → notifications.dlq (terminal — requires manual intervention or DLQ processor)
```
Total retry window before DLQ: ~36 seconds across 3 attempts.

### Duplicate delivery
If a message is delivered twice (poller crash between publish and DB commit), the consumer's Redis idempotency check prevents double processing. The duplicate is ACKed and discarded silently.

### Service restart
All queues and exchanges are `durable`. All messages are `persistent`. A full broker restart recovers all in-flight messages. Consumer and poller reconnect with exponential backoff (max 10 attempts, capped at 10 s).

---

## Scalability Notes

| What to scale     | How                                                        |
|-------------------|------------------------------------------------------------|
| Consumer throughput | Run multiple consumer instances, each consuming from the same queue — RabbitMQ distributes messages via round-robin |
| Email capacity    | Add `email.queue.3`, bind to `email.3` routing key, add consumer instances, update poller partition count |
| Poller throughput | Run multiple poller instances — `SKIP LOCKED` ensures each claims a non-overlapping batch |
| API throughput    | Producer is stateless; scale horizontally behind a load balancer |
| Redis availability | Replace single Redis node with Redis Cluster or Redis Sentinel |

Current bottleneck: the outbox poller is single-threaded per instance. With a 50-row batch and ~1 ms publish latency, throughput is approximately 500–1000 events/second per poller instance. For higher throughput, run multiple poller instances in parallel — `SKIP LOCKED` handles contention correctly.

---

## Tradeoffs

**Outbox adds latency.** Every event incurs at minimum one poll interval (~1 s) before delivery. For near-real-time notifications this is acceptable; for sub-100 ms SLAs it is not.

**At-least-once, not exactly-once.** Exactly-once delivery requires distributed transactions or two-phase commit, both of which add significant complexity. The system trades that complexity for idempotent consumers with a 24-hour deduplication window.

**Hash-based partitioning without consistent hashing.** Partition count is hardcoded (`% 2`). Adding a third email queue requires a code change and a rolling deploy. A consistent-hash exchange plugin would eliminate this but introduces an infrastructure dependency.

**Prisma interactive transactions hold a DB connection.** The poller batches 50 rows inside a single transaction, holding a connection for the duration of all publishes (~50–100 ms). Under high concurrency, this can exhaust the connection pool. Mitigated by sizing the pool appropriately and limiting poller instances.

**Redis idempotency key expiry.** The 24-hour TTL means a message delivered more than 24 hours after its first processing will be treated as new. This is a deliberate tradeoff between storage cost and correctness over extremely long redelivery windows.

**No publisher confirms.** The poller uses a standard AMQP channel, not a confirm channel. If RabbitMQ acknowledges the publish at the TCP/socket level but fails to persist before a broker crash, the message is lost. Confirm channels would eliminate this at the cost of ~2x publish latency.

---

## Future Improvements

- **DLQ reprocessor** — scheduled job to replay DLQ messages after manual review or automatic backoff
- **Publisher confirms** — switch poller to `createConfirmChannel()` for guaranteed broker-side persistence
- **OpenTelemetry tracing** — propagate `correlationId` as a trace context through producer → poller → consumer
- **Prometheus + Grafana** — expose queue depth, publish rate, consumer lag, retry counts
- **Circuit breaker on poller** — stop publishing if broker error rate exceeds threshold; avoids thundering herd on reconnect
- **Prisma migrate** — replace `prisma db push` with versioned migration files for production deployments
- **Consistent-hash exchange** — true partition rebalancing when adding/removing consumer groups without code changes
- **Redis Cluster** — eliminate single point of failure in idempotency store
