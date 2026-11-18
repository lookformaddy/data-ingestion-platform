"""Shared error types for the ingestion platform.

Every failure raised inside a run carries a machine-readable ``code`` so the
DLQ, the audit log and the API can classify it without string matching.
"""

from __future__ import annotations


class IngestionError(Exception):
    """Base class for all ingestion-platform errors."""

    code = "ingestion_error"

    def __init__(self, message: str, *, code: str | None = None) -> None:
        super().__init__(message)
        if code is not None:
            self.code = code


class SchemaError(IngestionError):
    """Raised when a record fails schema validation."""

    code = "schema_error"


class DriftError(IngestionError):
    """Raised when a batch drifts from the registered schema."""

    code = "schema_drift"


class ConnectorError(IngestionError):
    """Raised when a source connector cannot fetch data."""

    code = "connector_error"


class RateLimitError(ConnectorError):
    """Raised when a source enforces a rate limit that cannot be retried."""

    code = "rate_limited"


class BackpressureError(IngestionError):
    """Raised when the bounded buffer cannot accept a batch in time."""

    code = "backpressure"


class CheckpointError(IngestionError):
    """Raised when a checkpoint is missing or corrupt."""

    code = "checkpoint_error"


class DlqError(IngestionError):
    """Raised by DLQ operations (unknown id, invalid redrive)."""

    code = "dlq_error"
