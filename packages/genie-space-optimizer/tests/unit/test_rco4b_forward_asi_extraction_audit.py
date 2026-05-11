"""RCO-4b Phase D Task 3 — forward_asi_extraction_audit tests.

The helper encapsulates the inline body at
``harness._run_gate_checks:~13731``. Pure function — no
``_audit_emit`` call, no Spark, no prints.
"""
from __future__ import annotations

import json

from genie_space_optimizer.optimization.stages.gate_types import (
    AsiExtractionInput,
)


def _make_input(**overrides) -> AsiExtractionInput:
    base = dict(ag_id="ag-001", iteration=1, raw_audit=None)
    base.update(overrides)
    return AsiExtractionInput(**base)


def test_none_audit_yields_no_emission() -> None:
    from genie_space_optimizer.optimization.stages.eval_gates import (
        forward_asi_extraction_audit,
    )
    out = forward_asi_extraction_audit(_make_input(raw_audit=None))
    assert out.should_emit is False


def test_non_dict_audit_yields_no_emission() -> None:
    """Legacy isinstance(_asi_audit_1, dict) guard — strings, lists,
    ints, and other non-dict values must not trigger emission."""
    from genie_space_optimizer.optimization.stages.eval_gates import (
        forward_asi_extraction_audit,
    )
    for bad in ["not a dict", [], 42, 3.14, True]:
        out = forward_asi_extraction_audit(_make_input(raw_audit=bad))
        assert out.should_emit is False, f"bad input {bad!r} should not emit"


def test_empty_dict_audit_emits_with_defaults() -> None:
    """Legacy code: ``isinstance({}, dict)`` is True; the audit row fires
    with default field values."""
    from genie_space_optimizer.optimization.stages.eval_gates import (
        forward_asi_extraction_audit,
    )
    out = forward_asi_extraction_audit(_make_input(raw_audit={}))
    assert out.should_emit is True
    assert out.stage_letter == "C"
    assert out.gate_name == "asi_extraction"
    assert out.decision == "ok"
    assert out.reason_code is None
    assert out.metrics is None


def test_dict_audit_with_string_metrics_parses_json() -> None:
    from genie_space_optimizer.optimization.stages.eval_gates import (
        forward_asi_extraction_audit,
    )
    audit = {
        "stage_letter": "X",
        "gate_name": "asi_extraction_custom",
        "decision": "warn",
        "reason_code": "some_reason",
        "metrics_json": json.dumps({"a": 1.0, "b": 2.0}),
    }
    out = forward_asi_extraction_audit(_make_input(raw_audit=audit))
    assert out.should_emit is True
    assert out.stage_letter == "X"
    assert out.gate_name == "asi_extraction_custom"
    assert out.decision == "warn"
    assert out.reason_code == "some_reason"
    assert out.metrics == {"a": 1.0, "b": 2.0}


def test_dict_audit_with_malformed_string_metrics_yields_none_metrics() -> None:
    """Legacy code: try/except around json.loads sets metrics=None
    on parse failure; the audit row still fires."""
    from genie_space_optimizer.optimization.stages.eval_gates import (
        forward_asi_extraction_audit,
    )
    audit = {
        "stage_letter": "C",
        "decision": "ok",
        "metrics_json": "{not valid json",
    }
    out = forward_asi_extraction_audit(_make_input(raw_audit=audit))
    assert out.should_emit is True
    assert out.metrics is None


def test_dict_audit_with_dict_metrics_passes_through() -> None:
    from genie_space_optimizer.optimization.stages.eval_gates import (
        forward_asi_extraction_audit,
    )
    audit = {"metrics_json": {"already_a_dict": 1.0}}
    out = forward_asi_extraction_audit(_make_input(raw_audit=audit))
    assert out.should_emit is True
    assert out.metrics == {"already_a_dict": 1.0}


def test_dict_audit_with_non_dict_non_string_metrics_yields_none() -> None:
    """Legacy: ``metrics=_asi_metrics if isinstance(_asi_metrics, dict)
    else None``. A list, int, or other non-dict metrics_json value must
    not pass through."""
    from genie_space_optimizer.optimization.stages.eval_gates import (
        forward_asi_extraction_audit,
    )
    for bad_metrics in [[1, 2, 3], 42, True, None]:
        audit = {"metrics_json": bad_metrics}
        out = forward_asi_extraction_audit(_make_input(raw_audit=audit))
        assert out.should_emit is True
        assert out.metrics is None, f"bad metrics {bad_metrics!r}"


def test_dict_audit_with_no_metrics_key_yields_none_metrics() -> None:
    from genie_space_optimizer.optimization.stages.eval_gates import (
        forward_asi_extraction_audit,
    )
    audit = {"stage_letter": "C", "decision": "ok"}
    out = forward_asi_extraction_audit(_make_input(raw_audit=audit))
    assert out.should_emit is True
    assert out.metrics is None


def test_dict_audit_default_letters_when_keys_falsy() -> None:
    """Legacy code uses ``or`` for defaults: empty string, None, or
    0 all fall through to the defaults."""
    from genie_space_optimizer.optimization.stages.eval_gates import (
        forward_asi_extraction_audit,
    )
    audit = {"stage_letter": "", "gate_name": None, "decision": ""}
    out = forward_asi_extraction_audit(_make_input(raw_audit=audit))
    assert out.stage_letter == "C"
    assert out.gate_name == "asi_extraction"
    assert out.decision == "ok"
