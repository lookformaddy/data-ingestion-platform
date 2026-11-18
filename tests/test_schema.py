"""Schema inference, validation, and drift detection."""

from ingest.schema import (
    FieldType,
    detect_drift,
    infer_schema,
    validate_record,
)


def test_infer_schema_types_and_required(orders_schema):
    records = [
        {"order_id": "ORD-1", "amount": 10.5, "count": 3,
         "paid": True, "event_time": "2026-11-18T09:00:00Z"},
        {"order_id": "ORD-2", "amount": 20, "count": 4,
         "paid": False, "event_time": "2026-11-18T09:01:00Z"},
    ]
    schema = infer_schema(records, "t")
    by_name = schema.field_map()
    assert by_name["order_id"].type is FieldType.STRING
    assert by_name["amount"].type is FieldType.FLOAT  # int + float merge
    assert by_name["count"].type is FieldType.INTEGER
    assert by_name["paid"].type is FieldType.BOOLEAN
    assert by_name["event_time"].type is FieldType.TIMESTAMP
    assert all(f.required for f in schema.fields)


def test_infer_schema_optional_field():
    records = [{"a": 1, "b": 2}, {"a": 1}]
    schema = infer_schema(records, "t")
    by_name = schema.field_map()
    assert by_name["a"].required is True
    assert by_name["b"].required is False


def test_infer_schema_majority_vote_survives_corruption():
    """A sample with a few corrupted rows must not flip the schema:
    the majority type wins and a 98%-present field stays required."""
    records = [{"order_id": f"ORD-{i}", "amount": 10.0} for i in range(49)]
    records.append({"order_id": "ORD-bad", "amount": "oops"})
    records.append({"amount": 5.0})  # missing order_id
    schema = infer_schema(records, "t")
    by_name = schema.field_map()
    assert by_name["order_id"].required is True
    assert by_name["amount"].type is FieldType.FLOAT
    # ...and the corrupted rows then fail validation against it.
    _coerced, errors = validate_record(records[49], schema)
    assert any(e.path == "amount" and e.code == "type_mismatch" for e in errors)
    _coerced, errors = validate_record(records[50], schema)
    assert any(e.path == "order_id" and e.code == "missing_field" for e in errors)


def test_validate_valid_record(orders_schema, valid_order):
    coerced, errors = validate_record(valid_order, orders_schema)
    assert errors == []
    assert coerced["order_id"] == "ORD-100001"
    assert coerced["shipping"]["city"] == "Springfield"


def test_validate_missing_required_field(orders_schema, valid_order):
    del valid_order["customer_id"]
    _coerced, errors = validate_record(valid_order, orders_schema)
    assert len(errors) == 1
    assert errors[0].path == "customer_id"
    assert errors[0].code == "missing_field"


def test_validate_type_mismatch_carries_reason(orders_schema, valid_order):
    valid_order["amount"] = "not-a-number"
    _coerced, errors = validate_record(valid_order, orders_schema)
    assert any(e.path == "amount" and e.code == "type_mismatch" for e in errors)


def test_validate_nested_field_path(orders_schema, valid_order):
    valid_order["shipping"]["city"] = 123
    _coerced, errors = validate_record(valid_order, orders_schema)
    assert any(e.path == "shipping.city" for e in errors)


def test_validate_enum_and_range(orders_schema, valid_order):
    valid_order["currency"] = "JPY"
    valid_order["amount"] = -5
    _coerced, errors = validate_record(valid_order, orders_schema)
    codes = {e.code for e in errors}
    assert "enum_violation" in codes
    assert "below_minimum" in codes


def test_validate_coerces_numeric_string(orders_schema, valid_order):
    valid_order["amount"] = "42.5"
    coerced, errors = validate_record(valid_order, orders_schema)
    assert errors == []
    assert coerced["amount"] == 42.5


def test_validate_drops_unknown_fields(orders_schema, valid_order):
    valid_order["surprise"] = "new"
    coerced, errors = validate_record(valid_order, orders_schema)
    assert errors == []
    assert "surprise" not in coerced


def test_drift_added_removed_changed(orders_schema):
    inferred = infer_schema([
        {"order_id": "x", "customer_id": "c", "amount": 1.0,
         "currency": "USD", "status": "paid",
         "event_time": "2026-11-18T09:00:00Z",
         "shipping": {"city": "S", "zip": "1"}, "items": [], "loyalty_tier": "gold"},
    ], "orders")
    drift = detect_drift(inferred, orders_schema)
    assert drift.has_drift
    assert drift.added == ["loyalty_tier"]
    assert drift.is_additive_only


def test_drift_type_change_detected(orders_schema):
    inferred = infer_schema([
        {"order_id": "x", "customer_id": "c", "amount": "expensive",
         "currency": "USD", "status": "paid",
         "event_time": "2026-11-18T09:00:00Z",
         "shipping": {"city": "S", "zip": "1"}, "items": []},
    ], "orders")
    drift = detect_drift(inferred, orders_schema)
    assert ("amount", "float", "string") in drift.changed
    assert not drift.is_additive_only


def test_drift_int_to_float_is_not_drift(orders_schema):
    inferred = infer_schema([
        {"order_id": "x", "customer_id": "c", "amount": 5,
         "currency": "USD", "status": "paid",
         "event_time": "2026-11-18T09:00:00Z",
         "shipping": {"city": "S", "zip": "1"}, "items": []},
    ], "orders")
    drift = detect_drift(inferred, orders_schema)
    assert not drift.has_drift


def test_drift_mixed_type_sample_is_not_drift(orders_schema):
    # A corrupted row (amount as string) mixed with good rows infers ANY;
    # that is a validation problem, not schema drift.
    inferred = infer_schema([
        {"order_id": "x", "customer_id": "c", "amount": 5.0,
         "currency": "USD", "status": "paid",
         "event_time": "2026-11-18T09:00:00Z",
         "shipping": {"city": "S", "zip": "1"}},
        {"order_id": "y", "customer_id": "c", "amount": "oops",
         "currency": "USD", "status": "paid",
         "event_time": "2026-11-18T09:00:00Z",
         "shipping": {"city": "S", "zip": "1"}, "items": []},
    ], "orders")
    drift = detect_drift(inferred, orders_schema)
    assert not drift.has_drift
