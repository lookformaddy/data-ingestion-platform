"""Versioned schemas: inference, validation, and drift detection.

Schemas are plain data (JSON-serialisable) so they can be stored in the
state directory, versioned, and diffed. Validation errors carry field
paths (``items[2].price``, ``shipping.city``) so a quarantined record can
be diagnosed without re-running the source.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from .errors import SchemaError

ISO_TS_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:?\d{2})?$"
)


class FieldType(str, Enum):
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    TIMESTAMP = "timestamp"
    OBJECT = "object"
    ARRAY = "array"
    ANY = "any"


@dataclass
class FieldError:
    """One validation failure, addressed by field path."""

    path: str
    code: str
    message: str

    def to_dict(self) -> dict[str, str]:
        return {"path": self.path, "code": self.code, "message": self.message}


@dataclass
class FieldSpec:
    name: str
    type: FieldType
    required: bool = True
    nullable: bool = False
    enum: list[Any] | None = None
    minimum: float | None = None
    maximum: float | None = None
    children: list["FieldSpec"] = field(default_factory=list)  # for OBJECT
    item_type: FieldType | None = None  # for ARRAY

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "name": self.name,
            "type": self.type.value,
            "required": self.required,
            "nullable": self.nullable,
        }
        if self.enum is not None:
            d["enum"] = self.enum
        if self.minimum is not None:
            d["minimum"] = self.minimum
        if self.maximum is not None:
            d["maximum"] = self.maximum
        if self.children:
            d["children"] = [c.to_dict() for c in self.children]
        if self.item_type is not None:
            d["item_type"] = self.item_type.value
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "FieldSpec":
        return cls(
            name=d["name"],
            type=FieldType(d["type"]),
            required=d.get("required", True),
            nullable=d.get("nullable", False),
            enum=d.get("enum"),
            minimum=d.get("minimum"),
            maximum=d.get("maximum"),
            children=[cls.from_dict(c) for c in d.get("children", [])],
            item_type=FieldType(d["item_type"]) if d.get("item_type") else None,
        )


@dataclass
class SchemaVersion:
    """A versioned schema for one landing table."""

    name: str
    version: int
    fields: list[FieldSpec]
    dedup_key: list[str] = field(default_factory=list)
    created_at: str = ""

    def field_map(self) -> dict[str, FieldSpec]:
        return {f.name: f for f in self.fields}

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "dedup_key": self.dedup_key,
            "created_at": self.created_at,
            "fields": [f.to_dict() for f in self.fields],
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "SchemaVersion":
        return cls(
            name=d["name"],
            version=d["version"],
            fields=[FieldSpec.from_dict(f) for f in d["fields"]],
            dedup_key=d.get("dedup_key", []),
            created_at=d.get("created_at", ""),
        )


def infer_type(value: Any) -> FieldType:
    """Infer the most specific type for a single observed value."""
    if isinstance(value, bool):
        return FieldType.BOOLEAN
    if isinstance(value, int):
        return FieldType.INTEGER
    if isinstance(value, float):
        return FieldType.FLOAT
    if isinstance(value, dict):
        return FieldType.OBJECT
    if isinstance(value, list):
        return FieldType.ARRAY
    if isinstance(value, str):
        if ISO_TS_RE.match(value):
            try:
                datetime.fromisoformat(value.replace("Z", "+00:00"))
                return FieldType.TIMESTAMP
            except ValueError:
                return FieldType.STRING
        return FieldType.STRING
    return FieldType.ANY


def _majority_type(counts: dict[FieldType, int], total: int) -> FieldType:
    """Majority-vote type for one field across a sample.

    The winning type needs a strict majority of the sample. Integer and
    float votes combine (numeric widening): all-int samples stay
    INTEGER, any-float majorities become FLOAT. A sample with no
    majority — e.g. a field that is genuinely polymorphic, or one
    corrupted row in a two-row sample — infers ANY (inconclusive), and
    drift detection treats ANY as "no signal". Majority voting matters
    because the sample can contain corrupted rows: a field that is a
    float in 98% of records and a string in 2% is a float field with 2%
    bad rows, not an untyped field.
    """
    from collections import Counter

    counted = Counter({t: c for t, c in counts.items() if t is not FieldType.ANY})
    if not counted:
        return FieldType.ANY
    numeric = counted.get(FieldType.INTEGER, 0) + counted.get(FieldType.FLOAT, 0)
    if numeric > total / 2:
        return FieldType.FLOAT if counted.get(FieldType.FLOAT, 0) else FieldType.INTEGER
    top_type, top_count = counted.most_common(1)[0]
    if top_count > total / 2:
        return top_type
    return FieldType.ANY


def infer_schema(
    records: list[dict[str, Any]],
    name: str,
    *,
    version: int = 1,
    sample_size: int = 500,
    dedup_key: list[str] | None = None,
    required_threshold: float = 0.95,
) -> SchemaVersion:
    """Infer a schema from a sample of records.

    A field is required when it appears in at least ``required_threshold``
    (default 95%) of sampled records — a strict 100% rule would let a
    handful of corrupted rows (missing fields) flip a required field to
    optional. Types are majority-vote (see :func:`_majority_type`).
    Nested objects are inferred recursively; arrays record their item type.
    """
    sample = records[:sample_size]
    if not sample:
        raise SchemaError("cannot infer a schema from zero records")
    type_counts: dict[str, dict[FieldType, int]] = {}
    present: dict[str, int] = {}
    nulls: dict[str, int] = {}
    children: dict[str, list[dict[str, Any]]] = {}
    item_types: dict[str, dict[FieldType, int]] = {}
    for rec in sample:
        for key, value in rec.items():
            present[key] = present.get(key, 0) + 1
            bucket = type_counts.setdefault(key, {})
            ftype = infer_type(value)
            bucket[ftype] = bucket.get(ftype, 0) + 1
            if value is None:
                nulls[key] = nulls.get(key, 0) + 1
            if isinstance(value, dict):
                children.setdefault(key, []).append(value)
            if isinstance(value, list) and value:
                ibucket = item_types.setdefault(key, {})
                itype = infer_type(value[0])
                ibucket[itype] = ibucket.get(itype, 0) + 1
    fields: list[FieldSpec] = []
    for key in sorted(type_counts):
        ftype = _majority_type(type_counts[key], len(sample))
        spec = FieldSpec(
            name=key,
            type=ftype,
            required=present[key] / len(sample) >= required_threshold,
            nullable=nulls.get(key, 0) > 0,
        )
        if ftype == FieldType.OBJECT and key in children:
            sub = infer_schema(children[key], key, version=1)
            spec.children = sub.fields
        if ftype == FieldType.ARRAY and key in item_types:
            spec.item_type = _majority_type(item_types[key], sum(item_types[key].values()))
        fields.append(spec)
    return SchemaVersion(
        name=name,
        version=version,
        fields=fields,
        dedup_key=dedup_key or [],
        created_at=datetime.now(timezone.utc).isoformat(timespec="seconds") + "Z",
    )


def _coerce(value: Any, ftype: FieldType) -> tuple[Any, bool]:
    """Best-effort safe coercion. Returns (value, ok).

    Only lossless, unambiguous coercions are allowed: numeric strings to
    numbers, ISO strings to timestamps, "true"/"false" to booleans. Anything
    else is a validation error, not a silent fix.
    """
    if ftype == FieldType.INTEGER and isinstance(value, str):
        try:
            return int(value), True
        except ValueError:
            return value, False
    if ftype == FieldType.FLOAT and isinstance(value, (int, str)):
        try:
            return float(value), True
        except ValueError:
            return value, False
    if ftype == FieldType.TIMESTAMP and isinstance(value, str):
        if ISO_TS_RE.match(value):
            try:
                datetime.fromisoformat(value.replace("Z", "+00:00"))
                return value, True
            except ValueError:
                return value, False
        return value, False
    if ftype == FieldType.BOOLEAN and isinstance(value, str):
        if value.lower() in ("true", "false"):
            return value.lower() == "true", True
        return value, False
    # No safe coercion for this (type, value) pair: report a mismatch
    # rather than silently accepting the wrong type.
    return value, False


def _check_type(value: Any, ftype: FieldType, path: str, errors: list[FieldError]) -> Any:
    """Type-check (and coerce) one value. Returns the (possibly coerced) value."""
    if ftype == FieldType.ANY:
        return value
    if isinstance(value, bool) and ftype != FieldType.BOOLEAN:
        errors.append(FieldError(path, "type_mismatch",
                                 f"expected {ftype.value}, got boolean"))
        return value
    ok_map = {
        FieldType.STRING: isinstance(value, str),
        FieldType.INTEGER: isinstance(value, int),
        FieldType.FLOAT: isinstance(value, (int, float)),
        FieldType.BOOLEAN: isinstance(value, bool),
        FieldType.OBJECT: isinstance(value, dict),
        FieldType.ARRAY: isinstance(value, list),
        FieldType.TIMESTAMP: isinstance(value, str) and ISO_TS_RE.match(value) is not None,
    }
    if not ok_map.get(ftype, True):
        coerced, ok = _coerce(value, ftype)
        if ok:
            return coerced
        errors.append(FieldError(
            path, "type_mismatch",
            f"expected {ftype.value}, got {type(value).__name__} ({value!r})"))
        return value
    return value


def validate_record(
    record: dict[str, Any], schema: SchemaVersion
) -> tuple[dict[str, Any], list[FieldError]]:
    """Validate one record against a schema version.

    Returns (coerced_record, errors). An empty error list means the record
    is valid. Unknown fields are not errors here — drift detection owns
    that — but they are dropped from the coerced record so the landing
    table always conforms to the registered schema.
    """
    errors: list[FieldError] = []
    out: dict[str, Any] = {}
    for spec in schema.fields:
        present = spec.name in record and record[spec.name] is not None
        if not present:
            if record.get(spec.name) is None and spec.nullable and spec.name in record:
                out[spec.name] = None
                continue
            if spec.required:
                errors.append(FieldError(
                    spec.name, "missing_field",
                    f"required field '{spec.name}' is missing"))
            continue
        value = record[spec.name]
        value = _check_type(value, spec.type, spec.name, errors)
        if spec.type == FieldType.OBJECT and isinstance(value, dict) and spec.children:
            sub = SchemaVersion(name=spec.name, version=1, fields=spec.children)
            coerced_sub, sub_errors = validate_record(value, sub)
            for err in sub_errors:
                errors.append(FieldError(f"{spec.name}.{err.path}", err.code, err.message))
            value = coerced_sub
        if spec.type == FieldType.ARRAY and isinstance(value, list) and spec.item_type:
            for i, item in enumerate(value):
                _check_type(item, spec.item_type, f"{spec.name}[{i}]", errors)
        if spec.enum is not None and value not in spec.enum:
            errors.append(FieldError(
                spec.name, "enum_violation",
                f"value {value!r} not in {spec.enum}"))
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if spec.minimum is not None and value < spec.minimum:
                errors.append(FieldError(
                    spec.name, "below_minimum",
                    f"value {value} below minimum {spec.minimum}"))
            if spec.maximum is not None and value > spec.maximum:
                errors.append(FieldError(
                    spec.name, "above_maximum",
                    f"value {value} above maximum {spec.maximum}"))
        out[spec.name] = value
    return out, errors


@dataclass
class DriftReport:
    """Result of comparing an inferred schema against a registered one."""

    added: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    changed: list[tuple[str, str, str]] = field(default_factory=list)  # (field, old, new)

    @property
    def has_drift(self) -> bool:
        return bool(self.added or self.removed or self.changed)

    @property
    def is_additive_only(self) -> bool:
        """True when the drift only adds new fields — safe to evolve."""
        return bool(self.added) and not self.removed and not self.changed


def detect_drift(inferred: SchemaVersion, registered: SchemaVersion) -> DriftReport:
    """Diff an inferred schema against the registered version."""
    inf = inferred.field_map()
    reg = registered.field_map()
    report = DriftReport()
    for name in sorted(set(inf) - set(reg)):
        report.added.append(name)
    for name in sorted(set(reg) - set(inf)):
        report.removed.append(name)
    for name in sorted(set(inf) & set(reg)):
        old, new = reg[name].type.value, inf[name].type.value
        if old != new:
            # Inferred ANY means the sample was mixed/inconclusive (e.g. a
            # corrupted row) — that is a validation problem, not drift.
            if new == "any":
                continue
            # integer <-> float is compatible in either direction: a
            # sample of all-int values for a float field (or vice versa)
            # is not drift, and validation accepts ints for floats.
            if {old, new} == {"integer", "float"}:
                continue
            report.changed.append((name, old, new))
    return report
