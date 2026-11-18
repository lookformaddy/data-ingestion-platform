#!/usr/bin/env python3
"""Run the end-to-end demo: 5,000 records across three sources, the
resume-after-interruption proof, the idempotent re-run proof, and the
schema-drift scenarios. See src/ingest/demo.py for details."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ingest.demo import main

if __name__ == "__main__":
    main()
