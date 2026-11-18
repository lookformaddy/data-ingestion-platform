"""Data Ingestion Platform — pluggable source connectors, governed
landing tables, exactly-once semantics, watermarks, DLQ and audit."""

from .engine import Engine, RunStatus, SourceConfig

__all__ = ["Engine", "RunStatus", "SourceConfig"]
__version__ = "1.0.0"
