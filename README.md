# Data Ingestion Platform

An enterprise data ingestion platform: pluggable source connectors bring
data into governed landing tables with exactly-once semantics, schema
validation and drift handling, event-time watermarks, backpressure, a
dead-letter queue with redrive, and a hash-chained audit log.

All code is original and all data is synthetic — no employer data, no
network calls, no credentials. The HTTP API and CDC feed are simulated
in-process so the whole platform runs offline and deterministically.

## The enterprise problem

Every data platform starts the same way: files land in a bucket, an API
paginates, a database emits change events — and each source gets its own
ad-hoc loader. That produces three expensive failures:

1. **Silent corruption** — a bad row poisons a downstream table, and
   nobody can say which row, which field, or which run.
2. **Duplicates after a crash** — a run interrupted mid-batch replays
   rows that already landed, and reconciliation becomes a project.
3. **Schema drift** — a source adds a field and the pipeline either
   breaks at 3 AM or silently drops data.

This platform answers all three with one governed path: every source
goes through the same connector interface, the same validation, the same
outbox commit protocol, and the same audit log.

## Architecture

```
 sources                       ingestion core                    governed state
┌──────────────┐   ┌──────────────────────┐   ┌──────────────────────────┐
│ File (CSV /  │   │ BoundedBuffer        │   │ landing/<table>.jsonl   │
│  JSONL)      │──▶│  (backpressure,      │──▶│ dedup index (per table) │
├──────────────┤   │  pause/resume)       │   │ outbox.jsonl (markers)  │
│ HTTP API     │   │        │             │   │ checkpoints/<run>.json  │
│  (paginated, │   │ Engine               │   │ runs/<run>.json         │
│  rate-limited│   │  ├─ schema infer +  │   │ dlq.jsonl               │
│  + retries)  │   │  │  validate + drift │   │ audit.jsonl (SHA-256    │
├──────────────┤   │  ├─ dedup + outbox  │   │  hash-chained)          │
│ CDC feed     │   │  │  commit           │   │ schemas/<name>-vN.json  │
│  (LSN-ordered│   │  ├─ watermark +     │   └──────────────────────────┘
│  insert/upd/ │   │  │  late policy      │
│  del)        │   │  └─ metrics + DLQ   │
└──────────────┘   └──────────────────────┘
```

See [`docs/architecture.md`](docs/architecture.md) for the full design,
including the commit protocol, the checkpoint model, and the trade-offs.

## What it demonstrates

- **Pluggable connectors** — file, paginated HTTP API, CDC change feed
  behind one interface with capability flags (`PAGINATED`,
  `RATE_LIMITED`, `RESUMABLE`, `ORDERED`, `CDC`).
- **Schema governance** — inference from samples, validation with
  field-path errors (`shipping.city`, `items[2].price`), drift detection
  (added / removed / type-changed) with quarantine-or-evolve policy.
- **Exactly-once** — dedup keys + idempotent landing writes, an
  outbox-style commit marker per batch, checkpointed resumable runs.
  A run interrupted mid-batch resumes without duplicates or loss, and a
  full re-run lands zero new rows.
- **Backpressure** — bounded in-memory buffers, per-source token-bucket
  rate limits, adaptive batch sizing, pause/resume, slow-consumer
  handling that counts backpressure events instead of growing memory.
- **Watermarks + late data** — event-time watermarks with a configurable
  allowed lateness; late rows are accepted-with-flag or quarantined per
  policy, with per-run freshness/latency metrics.
- **Observability** — per-source/per-run metrics (records in, valid,
  quarantined, duplicates dropped, bytes, p50/p95 latency), run history,
  DLQ with full failure history and redrive, health/SLI rollup.
- **Audit** — SHA-256 hash-chained audit log for run lifecycle events,
  verifiable with `ingest audit verify`.
- **Deterministic synthetic data** — seeded generators with controlled
  corruption (bad types, missing fields, duplicates, late rows, drift)
  so the demo proves quarantine and dedup actually work.

## Quick start

```bash
python3 -m venv /tmp/ingest-venv
/tmp/ingest-venv/bin/pip install -r requirements.txt
PYTHONPATH=src /tmp/ingest-venv/bin/python -m pytest -q   # full suite
PYTHONPATH=src /tmp/ingest-venv/bin/python -m ingest demo # end-to-end demo
```

Or with Make (uses `/tmp/ingest-venv` by default):

```bash
make install && make test && make demo
```

### Run one source from a config file

```bash
PYTHONPATH=src python -m ingest run --config examples/demo_sources.json
PYTHONPATH=src python -m ingest runs            # run history
PYTHONPATH=src python -m ingest dlq list        # quarantined records
PYTHONPATH=src python -m ingest metrics         # health / SLI rollup
PYTHONPATH=src python -m ingest audit verify    # verify the audit chain
```

### Start the API

```bash
PYTHONPATH=src python -m ingest serve --port 8000
# GET  /health            service + audit-chain health
# GET  /sources           connectors + configured sources
# POST /sources           register a source config
# POST /runs              start a run (synchronous, returns the result)
# GET  /runs /runs/{id} /runs/{id}/metrics
# GET  /dlq               POST /dlq/{id}/redrive
# GET  /metrics/health    SLI rollup across runs
# GET  /audit             hash-chain verification + entries
# GET  /landing/{table}   peek at landed rows
```

### Docker

```bash
docker build -t data-ingestion-platform .
docker run -p 8000:8000 data-ingestion-platform
```

## Demo

`ingest demo` ingests **5,000 records across three sources** and prints
exact counts, then proves the guarantees:

- **Phase A — full ingestion:** file orders, paginated HTTP customers
  (page 2 returns 429 twice, retried), CDC payments.
- **Phase B — resume after interruption:** the orders run is crashed
  after 2 committed batches, resumed under the same run id — lands the
  full expected row count, zero duplicates, zero loss.
- **Phase C — idempotent re-run:** the orders source is ingested again
  under a new run id — every record drops as a duplicate, zero new rows.
- **Phase D — schema drift:** a file with a brand-new field is run
  under QUARANTINE (drifted rows quarantined) and under EVOLVE
  (schema v2 registered, rows land).

Example output (exact numbers from the verified run):

```
Phase A — full ingestion: 5091 records in, 4847 valid, 168 quarantined,
  76 duplicates dropped, 33 late, 2 API retries (11 calls)
Phase B — resume after crash: crashed after 2 batches, resumed to
  completed, landed 1934 rows (expected 1934), no duplicates/loss: True
Phase C — idempotent re-run: 2039 records replayed, 1961 dropped as
  duplicates, 0 new rows, zero new rows: True
Phase D — drift: quarantine run quarantined 4 drifted rows; evolve run
  registered schema v2, 50 valid
Health: HEALTHY (quarantine rate 0.0354, success rate 1.0)
Audit: chain verified: 19 entries
```

## Design decisions and trade-offs

- **File-backed state, not a database.** Landing tables, checkpoints,
  outbox and DLQ are JSON/JSONL files under one state directory. That
  keeps the platform dependency-free and inspectable, at the cost of
  single-writer semantics — the right trade for a portfolio platform;
  the store is one class behind one interface, so a real warehouse or
  object store can replace it without touching the engine.
- **At-least-once delivery + idempotent writes = exactly-once effect.**
  Connectors may replay; the dedup index and outbox markers make the
  replay a no-op. This is cheaper and more robust than trying to make
  delivery itself exactly-once.
- **Outbox markers, not two-phase commit.** A batch is committed only
  when its marker exists. A crash between landing and marking leaves an
  unmarked batch that recovery drops. One file append, no coordinator.
- **Fail-closed validation.** Unknown fields are dropped from the
  landed row (the landing table always conforms to the registered
  schema); type errors, missing required fields, enum/range violations
  quarantine the row with a reason. Nothing bad ever lands silently.
- **Drift is a policy, not a crash.** Additive drift can auto-evolve
  the schema (v+1); anything else quarantines only the rows that carry
  the drift. The rest of the batch still lands.
- **Simulated sources.** The HTTP API and CDC feed are in-process
  simulations of the behaviour that matters (pagination, 429s, LSN
  ordering). Real adapters (requests-based HTTP, Debezium-style CDC)
  plug into the same interface.

## Project layout

```
src/ingest/
  schema.py      inference, validation, drift detection
  connectors.py  connector interface + file / HTTP API / CDC
  flow.py        token bucket, bounded buffer, batch sizing
  store.py       landing tables, dedup index, outbox, checkpoints
  watermark.py   event-time watermarks, late-data policy
  audit.py       SHA-256 hash-chained audit log
  metrics.py     run metrics, percentiles, health rollup
  dlq.py         dead-letter queue + redrive
  generators.py  deterministic synthetic data + corruption
  engine.py      run orchestration (producer/consumer, exactly-once)
  service.py     FastAPI service
  cli.py         CLI (demo / run / runs / dlq / metrics / audit / serve)
  demo.py        end-to-end demo with the exactly-once proofs
tests/           pytest suite (schema, connectors, flow, watermark,
                 audit/DLQ, engine, API, CLI/demo)
docs/architecture.md
examples/demo_sources.json   example source config for the CLI
scripts/demo.py              demo entry point
```

## Tests

```bash
PYTHONPATH=src python -m pytest -q
```

The suite covers schema inference/validation/drift, connector
pagination/retry/checkpoint resume, token-bucket and bounded-buffer
behaviour, watermark/late handling, audit tamper detection, DLQ
quarantine/redrive, full engine runs (counts, resume, idempotent
re-run, drift policies), the FastAPI surface, and the demo phases.
