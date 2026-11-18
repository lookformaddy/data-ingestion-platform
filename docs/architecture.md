# Architecture — Data Ingestion Platform

## Goals

1. **One governed path for every source.** Files, HTTP APIs and CDC
   feeds all land through the same validation, dedup, watermark and
   commit machinery — no per-source loaders.
2. **Exactly-once effect, not exactly-once delivery.** Sources may
   replay; the platform makes the replay harmless.
3. **Nothing bad lands silently.** Every rejected row carries a reason
   and lives in the DLQ; every schema surprise is a policy decision,
   not a 3 AM page.
4. **Runs are resumable and auditable.** A crash mid-batch is a
   checkpoint, not a reconciliation project; every lifecycle event is
   hash-chained.

## Components

### Connectors (`connectors.py`)

`SourceConnector.iter_batches(checkpoint, batch_size)` yields `Batch`
objects; each batch carries the checkpoint that resumes *after* it.
Checkpoints are opaque dicts owned by the connector:

| Connector | Checkpoint | Notes |
|---|---|---|
| `FileConnector` | `{"line": N}` | CSV / JSONL / NDJSON, line-offset resume |
| `HttpApiConnector` | `{"page": P, "offset": O}` | paginated fetch, 429 retry with exponential backoff |
| `CdcConnector` | `{"lsn": N}` | LSN-ordered insert/update/delete; deletes land as tombstones (`_deleted: true`) |

Capability flags (`PAGINATED`, `RATE_LIMITED`, `RESUMABLE`, `ORDERED`,
`CDC`) let the engine and the API describe a source without branching
on its kind. The HTTP API and CDC feed are deterministic in-process
simulations (`SimulatedApi`, seeded change events) — they model the
behaviour that matters for ingestion (pagination, rate limits, transient
429s, ordered change events) with no network dependency. A real adapter
(a `requests` HTTP client, a Debezium/Kafka consumer) implements the
same two methods.

### Flow control (`flow.py`)

- **TokenBucket** — per-source rate limiting with an injectable clock
  (deterministic in tests). `try_acquire` never blocks;
  `wait_seconds` tells the caller how long to sleep.
- **BoundedBuffer** — a bounded queue of batches between the producer
  thread (connector) and the consumer (validator/writer). `put` times
  out instead of blocking forever; a timeout is a counted backpressure
  event, not unbounded memory growth. The buffer supports
  pause/resume, and `close()` drains then signals end-of-stream.
- **BatchSizer** — AIMD batch sizing: halve the batch when a batch is
  slower than the target latency, grow it 25% when fast. Available to
  connectors that re-read it per batch.

### Schema governance (`schema.py`)

- **Inference** — sample up to 500 records; infer each field by
  majority vote (a field that is a float in 98% of records and a
  string in 2% is a float field with 2% bad rows, not an untyped
  field; integer+float majorities widen to float; no majority infers
  `ANY`, which drift detection treats as "no signal"). A field is
  `required` when present in at least 95% of the sample, so a handful
  of corrupted rows cannot flip a required field to optional. Nested
  objects recurse; arrays record their item type.
- **Validation** — type checks with safe coercion only (numeric strings
  to numbers, ISO strings to timestamps, `"true"`/`"false"` to
  booleans). Failures carry field paths (`shipping.city`,
  `items[2].price`) and codes (`missing_field`, `type_mismatch`,
  `enum_violation`, `below_minimum`, `above_maximum`). Unknown fields
  are dropped from the landed row — the landing table always conforms
  to the registered schema.
- **Drift detection** — diff an inferred batch schema against the
  registered version: `added` / `removed` / `changed` fields.
  Integer→float widening is a safe evolution, not drift; an `ANY`
  inference (a corrupted row in the sample) is not drift either.
  Policy: `QUARANTINE` quarantines only the rows carrying the drift;
  `EVOLVE` registers a new schema version for additive-only drift and
  lets the rows land.

### Exactly-once commit protocol (`store.py`, `engine.py`)

State layout under the state directory:

```
landing/<table>.jsonl        committed rows (append-only)
staging/<batch_id>.jsonl     rows of a batch not yet committed
dedup/<table>.json           {dedup_key: batch_id} for committed rows
outbox.jsonl                 one commit marker per committed batch
checkpoints/<run_id>.json    per-run resume offsets
runs/<run_id>.json           run record + metrics
dlq.jsonl                    quarantined records + failure history
audit.jsonl                  hash-chained audit log
schemas/<name>-vN.json      registered schema versions
```

A batch commits in four steps: (1) stage rows to
`staging/<batch_id>.jsonl`, (2) append them to the landing file,
(3) append the outbox marker (batch id, row count, SHA-256 checksum
of the payload), (4) update the dedup index and delete the staging
file. A batch is *committed* only when its marker exists. On open,
the store replays the outbox and drops any landing rows whose batch
has no marker, and deletes unmarked staging files — so a crash between
steps (2) and (3) leaves nothing behind. Committing an already-marked
batch is a no-op (idempotent `commit_batch`).

Resume works because the checkpoint is persisted after every committed
batch: the connector replays from the saved offset, and any batch
already marked is skipped by `marker_for`. A full re-run under a new
run id replays everything, but every row's dedup key is already in the
index, so all of it drops as duplicates and zero new rows land.

### Watermarks and late data (`watermark.py`)

`WatermarkTracker` holds the highest event time seen; the watermark is
`max_event_time − allowed_lateness`. A record at or below the watermark
is late. Policy per source: `accept_with_flag` (land it with
`_late: true` and count it) or `quarantine` (DLQ, reason `late`).
`lag_seconds(now)` reports freshness — how far the watermark trails
wall-clock time — for the health rollup.

### Observability (`metrics.py`, `dlq.py`, `audit.py`)

- **Run metrics** — records in / valid / quarantined / duplicates
  dropped / late (accepted + quarantined) / bytes / batches committed /
  backpressure events / retries, plus p50/p95 per-record latency from a
  bounded reservoir (nearest-rank percentiles, stdlib only).
- **Health rollup** — aggregates runs into SLIs: success rate,
  quarantine rate, duplicate count, p95 latency; status is `HEALTHY`
  / `DEGRADED` (quarantine rate > 5%) / `UNHEALTHY` (any failed run).
- **DLQ** — quarantined records keep the raw payload, every error with
  field path and code, source/run/table, and a history of redrive
  attempts. Redrive re-validates against the *current* schema version,
  so a fixed schema or a fixed record can rescue a row.
- **Audit log** — each entry commits to the previous entry's SHA-256
  hash (`run_started`, `batch_committed` via markers, `schema_drift`,
  `schema_evolved`, `run_completed` / `run_failed` / `run_interrupted`).
  `verify()` recomputes the chain and reports the first break.

### Engine (`engine.py`)

One producer thread pulls batches from the connector into the bounded
buffer; the consumer thread validates, drift-checks, dedups, applies
the watermark policy, stages and commits. The checkpoint and run
record are persisted after every committed batch. A fault-injection
hook (`fault_after_batches`) simulates a mid-run crash for the demo
and tests. Pause/resume operate on the buffer; the consumer waits
while paused.

### Service and CLI (`service.py`, `cli.py`)

FastAPI exposes sources, runs, DLQ, metrics, audit and landing-table
peeks; the CLI mirrors the same operations plus `demo` and `serve`.
Both share the same `Engine` and state directory, so a run started
from the CLI is visible in the API and vice versa.

## Sequence: one batch through the platform

```
connector.iter_batches(checkpoint)
        │  Batch{records, checkpoint}
        ▼
BoundedBuffer.put (backpressure if full)
        │  Batch
        ▼
Engine._process_batch
  ├─ outbox marker? → skip (already committed)
  ├─ drift? → quarantine drifted rows / evolve schema
  ├─ validate each record → quarantine with field-path errors
  ├─ dedup key in index? → drop as duplicate
  ├─ event time ≤ watermark? → accept-with-flag / quarantine
  ├─ stage → commit (land + marker + dedup index)
  └─ persist checkpoint + run record + metrics
```

## Trade-offs

- **File-backed state, not a database.** Inspectable, zero
  dependencies, single-writer. The store is one class behind one
  interface; swapping in a warehouse/object store does not touch the
  engine. Not suitable for concurrent writers or multi-GB tables
  without that swap.
- **At-least-once + idempotent writes.** Cheaper and more robust than
  exactly-once delivery; the cost is a dedup index lookup per record
  and an outbox append per batch.
- **Synchronous API runs.** `POST /runs` runs to completion in the
  request so the demo and tests stay deterministic. A production
  deployment would hand the run to a background worker and poll
  `GET /runs/{id}` — the run record already supports that shape.
- **In-memory watermark per run.** The watermark restarts from the
  persisted max event time on resume, but a brand-new run starts
  fresh. Persisting watermarks per table is a natural next step.
- **Simulated sources.** The HTTP API and CDC feed model the
  ingestion-relevant behaviour without network access. Real adapters
  are a connector implementation away.
