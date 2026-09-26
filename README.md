# Streaming Event Processor (Go)

A Go service that receives events from multiple producers via Kafka,
validates them against declarative JSON Schema contracts, and persists valid
events to a Redis Stream for low-latency pickup by a downstream Sender
service.

Go has no bindings for Spark, so this is a plain Kafka consumer rather than
a distributed cluster job: horizontal scaling comes from running multiple
`processor` replicas under the same `KAFKA_GROUP_ID` — Kafka rebalances
partitions across them automatically.

## Architecture

```
Producer A ─┐
Producer B ─┼──▶ Kafka topic "events" ──▶ Event Processor ──▶ Redis Stream "events:ready" ──▶ Sender (not included)
Producer C ─┘      (partitioned)        (Receive→Validate→Persist)   │
                                                                       └──▶ Redis Stream "events:dlq"
```

- **Receive**: `segmentio/kafka-go` consumer group reader, manual offset
  commits (`FetchMessage` + `CommitMessages`). Offsets are only committed
  after a message has been fully persisted or dead-lettered, so a crash
  mid-message causes re-delivery on restart, not silent loss.
- **Validate**: two-stage. First the event envelope (`event_id`,
  `event_type`, `tenant_id`, `target_client_id`, `occurred_at`, `payload`)
  is checked. Then the full event is checked against the JSON Schema
  registered for its `event_type` in `schemas/`. Adding a new event type is
  just dropping in a new schema file — no code changes.
- **Persist**: `redis/go-redis`, `XADD` to `events:ready`. An idempotency
  guard (`SETNX` on `event_id`) prevents duplicate persistence if a message
  is redelivered.
- **Failure handling**: anything that fails validation, or is unparseable,
  is written to `events:dlq` with a reason, and the Kafka offset still
  advances — one bad event never blocks the partition.

## Project layout

```
go/streaming-event-processor/
├── docker-compose.yml         # kafka (KRaft), redis, processor
├── Dockerfile                  # multi-stage: golang build -> alpine runtime
├── go.mod / go.sum
├── schemas/                    # declarative event contracts, one file per event_type
│   ├── monitoring.alert.json
│   └── transaction.authorized.json
├── internal/
│   ├── envelope/envelope.go    # EventEnvelope + ToRedisFields()
│   ├── registry/registry.go    # loads schemas, maps event_type -> compiled validator
│   ├── validator/validator.go  # two-stage validation (envelope + payload)
│   └── persister/persister.go  # Redis XADD + idempotency guard + DLQ
└── cmd/
    ├── processor/main.go       # entrypoint: Kafka consume loop wiring receive→validate→persist
    └── sample_producer/main.go # simulates multiple producers, mixes in invalid events
```

## Running it

1. Start the stack:
   ```
   docker compose up --build
   ```
   The `processor` container waits for Kafka and Redis healthchecks, and for
   a one-shot `kafka-init` service to finish creating the `events` topic,
   before starting. (Topic creation happens as its own step, rather than
   relying on Kafka's auto-create-on-first-use, to avoid a startup race
   where the consumer group's very first partition assignment can otherwise
   run before the topic's partitions are visible, leaving it with an empty,
   never-retried assignment.)

2. In a separate terminal, send some sample events:
   ```
   go run ./cmd/sample_producer
   ```

3. Watch the processor logs — valid events are persisted to `events:ready`, invalid
   ones go to `events:dlq`. Inspect either from `redis-cli`:
   ```
   docker exec -it event-processor-redis redis-cli XRANGE events:ready - +
   docker exec -it event-processor-redis redis-cli XRANGE events:dlq - +
   ```

## Environment variables (processor)

| Variable | Default | Purpose |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `events` | Topic to consume |
| `KAFKA_GROUP_ID` | `event-processor-group` | Consumer group (enables horizontal scaling) |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection string |
| `SCHEMAS_DIR` | `schemas` | Directory of declarative event contracts |

## Notes / next steps

- Scaling out: run multiple `processor` containers with the same `KAFKA_GROUP_ID` —
  Kafka rebalances partitions across them automatically. Partition the topic by
  `tenant_id` if strict per-tenant ordering matters.
- This is a Go port of the Python/PySpark project at
  `../../streaming-event-processor` — same event contracts, same Redis
  stream semantics, different language and (necessarily, since Go has no
  Spark bindings) a plain consumer instead of a Spark cluster.
