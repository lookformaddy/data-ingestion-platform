"""Command-line interface.

Subcommands:
    ingest demo                     run the 5,000-record end-to-end demo
    ingest run --config FILE        run one source from a JSON config file
    ingest runs                     list runs in the state directory
    ingest dlq list                 list quarantined records
    ingest dlq redrive ID           redrive one DLQ entry
    ingest metrics                  print the health / SLI rollup
    ingest audit verify             verify the hash-chained audit log
    ingest serve                    start the FastAPI service
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from .audit import AuditLog
from .connectors import ConnectorRegistry
from .dlq import DlqStore
from .engine import Engine, SourceConfig
from .metrics import RunMetrics, rollup
from .schema import SchemaVersion
from .store import IngestStore


def _state_dir() -> Path:
    return Path(os.environ.get("INGEST_STATE_DIR", ".ingest-state"))


def _engine(registry: ConnectorRegistry | None = None) -> tuple[Engine, IngestStore]:
    store = IngestStore(_state_dir())
    return Engine(store, registry or ConnectorRegistry()), store


def cmd_demo(_args: argparse.Namespace) -> int:
    from .demo import run_demo

    summary = run_demo()
    print(json.dumps(summary, indent=2, default=str))
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    from .demo import build_registry_for_config

    config_data = json.loads(Path(args.config).read_text(encoding="utf-8"))
    registry = build_registry_for_config(config_data)
    config = SourceConfig.from_dict(config_data["source"])
    engine, _store = _engine(registry)
    result = engine.run(config, run_id=args.run_id)
    print(json.dumps(result.to_dict(), indent=2, default=str))
    return 0 if result.status.value in ("completed", "interrupted") else 1


def cmd_runs(_args: argparse.Namespace) -> int:
    _engine_obj, store = _engine()
    for run in store.list_runs():
        m = run.get("metrics", {})
        print(
            f"{run['run_id']:>16}  {run['source']:<20}  {run['status']:<12}  "
            f"in={m.get('records_in', 0)} valid={m.get('valid', 0)} "
            f"quar={m.get('quarantined', 0)} dup={m.get('duplicates_dropped', 0)}"
        )
    return 0


def cmd_dlq_list(args: argparse.Namespace) -> int:
    dlq = DlqStore(_state_dir() / "dlq.jsonl")
    entries = dlq.list(status=args.status, source=args.source)
    print(f"{len(entries)} DLQ entries")
    for entry in entries[: args.limit]:
        print(
            f"  {entry.id}  {entry.source:<18}  {entry.reason:<10}  "
            f"{entry.errors[0]['path'] if entry.errors else '?'}: "
            f"{entry.errors[0]['message'] if entry.errors else ''}"
        )
    return 0


def cmd_dlq_redrive(args: argparse.Namespace) -> int:
    store = IngestStore(_state_dir())
    dlq = DlqStore(_state_dir() / "dlq.jsonl")
    entry = dlq.get(args.entry_id)
    stored = store.load_schema(entry.table)
    if stored is None:
        print(f"no schema registered for table {entry.table}", file=sys.stderr)
        return 1
    outcome, coerced, errors = dlq.redrive(args.entry_id, SchemaVersion.from_dict(stored))
    print(json.dumps({"outcome": outcome, "errors": [e.to_dict() for e in errors]}, indent=2))
    return 0 if outcome == "redriven" else 1


def cmd_metrics(_args: argparse.Namespace) -> int:
    _engine_obj, store = _engine()
    runs = store.list_runs()
    metrics_list = [RunMetrics.from_dict(r.get("metrics", {})) for r in runs]
    failed = sum(1 for r in runs if r.get("status") == "failed")
    print(json.dumps(rollup(metrics_list, failed_runs=failed).to_dict(), indent=2))
    return 0


def cmd_audit_verify(_args: argparse.Namespace) -> int:
    audit = AuditLog(_state_dir() / "audit.jsonl")
    ok, detail = audit.verify()
    print(detail)
    return 0 if ok else 1


def cmd_serve(args: argparse.Namespace) -> int:
    import uvicorn

    from .service import build_app

    uvicorn.run(build_app(), host=args.host, port=args.port)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ingest", description="Data Ingestion Platform")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("demo", help="run the 5,000-record end-to-end demo")
    p.set_defaults(func=cmd_demo)

    p = sub.add_parser("run", help="run one source from a JSON config file")
    p.add_argument("--config", required=True)
    p.add_argument("--run-id", default=None)
    p.set_defaults(func=cmd_run)

    p = sub.add_parser("runs", help="list runs")
    p.set_defaults(func=cmd_runs)

    p = sub.add_parser("dlq", help="DLQ operations")
    dlq_sub = p.add_subparsers(dest="dlq_command", required=True)
    p2 = dlq_sub.add_parser("list", help="list quarantined records")
    p2.add_argument("--status", default=None)
    p2.add_argument("--source", default=None)
    p2.add_argument("--limit", type=int, default=20)
    p2.set_defaults(func=cmd_dlq_list)
    p2 = dlq_sub.add_parser("redrive", help="redrive one DLQ entry")
    p2.add_argument("entry_id")
    p2.set_defaults(func=cmd_dlq_redrive)

    p = sub.add_parser("metrics", help="print the health / SLI rollup")
    p.set_defaults(func=cmd_metrics)

    p = sub.add_parser("audit", help="audit log operations")
    audit_sub = p.add_subparsers(dest="audit_command", required=True)
    p2 = audit_sub.add_parser("verify", help="verify the hash-chained audit log")
    p2.set_defaults(func=cmd_audit_verify)

    p = sub.add_parser("serve", help="start the FastAPI service")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=8000)
    p.set_defaults(func=cmd_serve)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
