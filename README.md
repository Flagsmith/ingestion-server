# ingestion-server

Ingestion server for Flagsmith SDK analytics events. Authenticates by environment key, validates each batch, and produces events to Kafka (Amazon MSK), from where ClickHouse ingests them.

Rust + axum, deployed as an ARM64 container on ECS Fargate.

## Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Liveness probe → `{"status":"healthy"}`. Touches no downstream; returns `503` once the Kafka producer has hit a fatal error, so ECS replaces the task. |
| POST | `/v1/events` | Ingest an event batch. Requires the `X-Environment-Key` header. |

`POST /v1/events` validates each event independently — invalid events are rejected individually and never block valid ones in the same batch.

| Status | Meaning |
|---|---|
| `202` | At least one event accepted and forwarded. |
| `400` | No event accepted: every event invalid, or `events` empty (JSON body below). Syntactically invalid JSON also returns `400`, with the framework's plain-text message. |
| `401` / `403` | Missing / unknown environment key. |
| `415` | Missing `Content-Type: application/json`. |
| `422` | Body is valid JSON but doesn't match the request schema (e.g. wrong field type) — plain-text body. |
| `503` | Downstream delivery failed — retry the whole batch. Delivery is at-least-once: a partially delivered batch may produce duplicates on retry. |

`202` and validation `400` responses carry the same body shape:

```json
{ "accepted": 2, "rejected": [ { "index": 1, "error": "events[1].timestamp must be an epoch-millis value after 2020-01-01" } ] }
```

An empty `events` array adds a top-level `error` field: `{ "accepted": 0, "rejected": [], "error": "events must be non-empty" }`.

`rejected` entries mean the event was invalid as sent — log and drop them, don't resubmit (this applies even when they arrive in a `202`). `index` points into the submitted `events` array.

### Request body

```json
{
  "events": [
    { "event": "$flag_exposure", "feature_name": "checkout_button", "identifier": "user_1", "value": "control", "timestamp": 1782110962955 }
  ]
}
```

`event` and `timestamp` (epoch millis) are required; `timestamp` must be after 2020-01-01, no more than 24h in the future and no more than 10 days in the past (otherwise that event is rejected). `$flag_exposure` is a reserved system event and requires `feature_name` (omitting it rejects that event); `identifier`, `value`, `traits`, and `metadata` are optional.

Field limits — an over-limit field rejects that event: `event` ≤ 256 chars; `feature_name` and `identifier` ≤ 2000 chars, trait keys ≤ 200 chars, trait string values ≤ 2000 chars (all matching Flagsmith's model limits); serialized `value` ≤ 128 KiB (fits Flagsmith's default 20,000-char flag value limit), `traits` ≤ 32 KiB, `metadata` ≤ 8 KiB. `traits` must be a JSON object.

On the wire to Kafka, `traits` and `metadata` are produced as JSON-encoded strings rather than nested objects: the destination `events` table stores them in String columns, and ClickPipes field mappings only handle flat scalar fields.

## Configuration

| Env var | Required | Default |
|---|---|---|
| `SINK` | no | `kafka` (or `clickhouse`) |
| `KAFKA_BOOTSTRAP_SERVERS` | with `kafka` | — |
| `KAFKA_TOPIC` | with `kafka` | — |
| `KAFKA_EXTERNAL_TOPIC` | with `kafka` | — |
| `KAFKA_AUTH` | no | `scram` (`none` for plaintext local brokers) |
| `KAFKA_USERNAME` | with `scram` | — |
| `KAFKA_PASSWORD` | with `scram` | — |
| `CLICKHOUSE_URL` | with `clickhouse` | — |
| `CLICKHOUSE_USER` | no | `default` |
| `CLICKHOUSE_PASSWORD` | no | empty |
| `CLICKHOUSE_DATABASE` | no | `flagsmith_exp` |
| `CLICKHOUSE_TABLE` | no | `events` |
| `SINK_FLUSH_INTERVAL_MS` | no | `1000` |
| `SINK_FLUSH_MAX_EVENTS` | no | `5000` |
| `DATABASE_URL` | yes | — |
| `LISTEN_ADDR` | no | `0.0.0.0:8080` |

With `SINK=kafka`, events from environments that use an external warehouse go to `KAFKA_EXTERNAL_TOPIC`; all others go to `KAFKA_TOPIC`. With `SINK=clickhouse`, every event goes to the one ClickHouse table. The server looks presented keys up in the `experimentation_environment_keys` view in Flagsmith's Postgres, at `DATABASE_URL`.

### Database role

The server connects as its own role, which can only read the environment keys view. Create it once per database, in `psql` as the database owner:

```sql
CREATE ROLE ingestion_server WITH LOGIN PASSWORD '<password>';
GRANT CONNECT ON DATABASE <database> TO ingestion_server;
GRANT USAGE ON SCHEMA public TO ingestion_server;
GRANT SELECT ON experimentation_environment_keys TO ingestion_server;
```

The view reads its tables with its owner's privileges, so the role needs no access to them. If Flagsmith adds an `experimentation_environment_keys_v2` view, grant `SELECT` on it before deploying a server that reads it.

With `KAFKA_AUTH=scram`, the producer authenticates with SASL/SCRAM-SHA-512 over TLS; MSK serves it on port 9096. The task definition injects both credentials from the `AmazonMSK_*` secret in Secrets Manager.

### ClickHouse sink

With `SINK=clickhouse`, `202` means the events are buffered in memory, not yet stored. A background task takes the whole buffer every `SINK_FLUSH_INTERVAL_MS`, or sooner once `SINK_FLUSH_MAX_EVENTS` events are waiting, and writes it as one `INSERT`. A failed batch is retried 5 times over about 17 seconds, enough to ride out a ClickHouse restart, and then dropped with an error log. The buffer is unbounded: requests never wait or get `503`, so while a write is slow or failing (up to about 3 minutes with retries) it grows by the ingest rate times the write time, and the next flush takes it all. A normal shutdown (SIGTERM) flushes the buffer, but events still buffered are lost if the process crashes.

## Run locally

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 KAFKA_TOPIC=events KAFKA_EXTERNAL_TOPIC=external_events KAFKA_AUTH=none DATABASE_URL=postgres://postgres:password@localhost:5432/flagsmith cargo run --release

# In another terminal:
curl localhost:8080/health
```

The Kafka producer and Postgres pool connect lazily, so `/health` responds even without either available.

`cargo test` needs a Postgres it can create databases in, at `DATABASE_URL`, and a ClickHouse it can create databases in, at `CLICKHOUSE_URL` (with `CLICKHOUSE_USER` and `CLICKHOUSE_PASSWORD` if needed): each database test gets a fresh one.

## CI / CD

| Workflow | Trigger | What it does |
|---|---|---|
| `ci.yml` | Pull request to `main` | `cargo fmt`, `clippy`, `test`, then build the image and smoke-test `/health` against the container |
| `deploy-staging.yml` | Push to `main`, or manual dispatch | Build → push to **staging** ECR → deploy to the staging ECS service |
| `deploy-production.yml` | Tag push matching `v*`, or manual dispatch | Build → push to **production** ECR → deploy to the production ECS service |

Both deploy workflows call `.reusable-build-push-ecr.yml`: authenticate to AWS via GitHub OIDC, push the image, render the per-env task-def template under `infrastructure/aws/<env>/` with the new image digest, deploy it to the `events-ingestion-api` service in the `flagsmith-experimentation` cluster, and wait for stability.

### Deploy configuration

Each GitHub Environment (`staging`, `production`) sets these variables — no secrets, OIDC only:

| Variable | Value |
|---|---|
| `AWS_ROLE_ARN` | `arn:aws:iam::<account>:role/gha-deploy-events-ingestion` |
| `AWS_REGION` | `eu-west-2` |
| `ECR_REPOSITORY` | `experimentation/events-ingestion-api` |

The deploy role needs a trust policy for this repo's GitHub OIDC `environment:<env>` subject, plus ECR push and ECS deploy permissions. Per account: create the GitHub OIDC provider and the role, then put the role ARN into the matching GitHub Environment.
