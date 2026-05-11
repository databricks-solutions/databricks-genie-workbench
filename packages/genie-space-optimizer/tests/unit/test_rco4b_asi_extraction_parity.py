"""RCO-4b Phase D Task 8 — parity between forward_asi_extraction_audit
and a faithful reimplementation of the legacy inline forwarding logic
from harness._run_gate_checks (the ``_asi_audit_1`` block).
"""
from __future__ import annotations

import json

import pytest

from genie_space_optimizer.optimization.stages.eval_gates import (
    forward_asi_extraction_audit,
)
from genie_space_optimizer.optimization.stages.gate_types import (
    AsiExtractionInput,
)


def _legacy_forward(raw_audit):
    """Reimplements the legacy inline ASI forwarder.

    Returns either None (no emit) or a dict matching the
    ``_audit_emit(...)`` kwargs the legacy code would pass.
    """
    if not isinstance(raw_audit, dict):
        return None
    _asi_metrics = raw_audit.get("metrics_json")
    if isinstance(_asi_metrics, str):
        try:
            _asi_metrics = json.loads(_asi_metrics)
        except (TypeError, ValueError):
            _asi_metrics = None
    return {
        "stage_letter": raw_audit.get("stage_letter") or "C",
        "gate_name": raw_audit.get("gate_name") or "asi_extraction",
        "decision": raw_audit.get("decision") or "ok",
        "reason_code": raw_audit.get("reason_code"),
        "metrics": _asi_metrics if isinstance(_asi_metrics, dict) else None,
    }


PARITY_MATRIX = [
    ("none",                None),
    ("non_dict_string",     "not a dict"),
    ("non_dict_list",       [1, 2, 3]),
    ("empty_dict",          {}),
    ("dict_with_dict_metrics",
        {"stage_letter": "C", "metrics_json": {"a": 1.0}}),
    ("dict_with_str_metrics",
        {"decision": "warn", "metrics_json": '{"b": 2.0}'}),
    ("dict_with_malformed_str_metrics",
        {"metrics_json": "{not valid"}),
    ("dict_with_int_metrics",
        {"metrics_json": 42}),
    ("dict_with_custom_letters",
        {"stage_letter": "X", "gate_name": "asi_extraction_xx", "decision": "skip"}),
]


@pytest.mark.parametrize(
    "desc,raw",
    PARITY_MATRIX,
    ids=[m[0] for m in PARITY_MATRIX],
)
def test_pure_helper_matches_legacy_reference(desc: str, raw) -> None:
    pure = forward_asi_extraction_audit(
        AsiExtractionInput(ag_id="ag-parity", iteration=1, raw_audit=raw)
    )
    legacy = _legacy_forward(raw)
    if legacy is None:
        assert pure.should_emit is False, f"{desc}: helper must skip"
    else:
        assert pure.should_emit is True, f"{desc}: helper must emit"
        assert pure.stage_letter == legacy["stage_letter"], f"{desc} stage_letter"
        assert pure.gate_name == legacy["gate_name"], f"{desc} gate_name"
        assert pure.decision == legacy["decision"], f"{desc} decision"
        assert pure.reason_code == legacy["reason_code"], f"{desc} reason_code"
        actual_metrics = dict(pure.metrics) if pure.metrics else pure.metrics
        assert actual_metrics == legacy["metrics"], f"{desc} metrics"
