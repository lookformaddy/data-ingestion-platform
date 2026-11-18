"""Demo end-to-end + CLI surface."""

import json
import os

from ingest.demo import run_demo


def test_demo_phases(tmp_path):
    summary = run_demo(state_dir=tmp_path / "demo-state")
    phase_a = summary["phases"]["A_full_ingestion"]["totals"]
    # 2,000 orders + 39 dups, 2,000 customers + 36 dups, 1,000 CDC + 16 dups
    assert phase_a["records_in"] == 2039 + 2036 + 1016
    assert phase_a["api_retries"] == 2  # page 2 fails twice with 429
    m = phase_a
    assert m["valid"] + m["quarantined"] + m["duplicates_dropped"] == m["records_in"]
    assert m["quarantined"] > 0
    assert m["duplicates_dropped"] > 0
    assert m["late"] > 0

    phase_b = summary["phases"]["B_resume_after_interruption"]
    assert phase_b["crashed_status"] == "interrupted"
    assert phase_b["resumed_status"] == "completed"
    assert phase_b["no_duplicates_or_loss"] is True

    phase_c = summary["phases"]["C_idempotent_rerun"]
    assert phase_c["zero_new_rows"] is True
    assert phase_c["new_valid"] == 0

    phase_d = summary["phases"]["D_schema_drift"]
    assert phase_d["quarantine_run"]["quarantined"] > 0
    assert phase_d["evolve_run"]["schema_version_after"] == 2
    assert phase_d["evolve_run"]["valid"] > 0

    assert summary["health"]["success_rate"] == 1.0
    assert "chain verified" in summary["audit"]


def test_cli_runs_metrics_audit(tmp_path, capsys, monkeypatch):
    from ingest.cli import main

    summary = run_demo(state_dir=tmp_path / "cli-state")
    monkeypatch.setenv("INGEST_STATE_DIR", str(tmp_path / "cli-state"))
    assert main(["runs"]) == 0
    out = capsys.readouterr().out
    assert "completed" in out
    assert main(["metrics"]) == 0
    out = capsys.readouterr().out
    health = json.loads(out)
    assert health["runs"] >= 3
    assert main(["audit", "verify"]) == 0
    out = capsys.readouterr().out
    assert "chain verified" in out
    assert main(["dlq", "list", "--limit", "2"]) == 0
