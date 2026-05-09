"""Cycle 14-T2 — full_eval_marker emission helper.

Wraps a format_full_eval_marker_payload() output in the canonical
GSO_FULL_EVAL_V1 marker envelope. The marker name regex in
run_analysis_contract.marker_line accepts _V<N> after Cycle 12-T1's
relaxation, so the V1 suffix is valid.
"""

from __future__ import annotations

import json


def _accepted_payload() -> dict:
    return {
        "iteration": 1,
        "ag_id": "AG1",
        "accepted": True,
        "reason_code": "accepted",
        "accepted_label": "PASS -- ACCEPTED",
        "baseline_accuracy": 83.3,
        "candidate_accuracy": 100.0,
        "delta_pp": 16.7,
        "target_qids": ["gs_024"],
        "target_fixed_qids": ["gs_024"],
        "target_still_hard_qids": [],
        "target_delta_states": [["gs_024", "fixed"]],
        "out_of_target_regressed_qids": [],
        "regression_debt_qids": [],
        "soft_to_hard_regressed_qids": [],
        "passing_to_hard_regressed_qids": [],
        "unknown_to_hard_regressed_qids": [],
        "reason_detail": "reason=accepted; ...",
    }


def test_full_eval_marker_returns_v1_marker_line() -> None:
    from genie_space_optimizer.optimization.run_analysis_contract import (
        full_eval_marker,
    )

    payload = _accepted_payload()
    line = full_eval_marker(
        optimization_run_id="run-1",
        payload=payload,
    )
    assert line.startswith("GSO_FULL_EVAL_V1 ")
    body = line[len("GSO_FULL_EVAL_V1 "):]
    parsed = json.loads(body)
    assert parsed["optimization_run_id"] == "run-1"
    assert parsed["payload"] == payload


def test_full_eval_marker_is_deterministic_across_calls() -> None:
    """JSON serialisation uses sort_keys=True; same inputs produce
    byte-identical output. C14-T3's I9 byte-equality invariant
    relies on this."""
    from genie_space_optimizer.optimization.run_analysis_contract import (
        full_eval_marker,
    )

    payload = {
        "iteration": 1,
        "ag_id": "AG1",
        "accepted": False,
        "reason_code": "target_resolution_failed",
        "accepted_label": "FAIL (REGRESSION)",
        "baseline_accuracy": 78.3,
        "candidate_accuracy": 78.3,
        "delta_pp": 0.0,
        "target_qids": ["gs_026"],
        "target_fixed_qids": [],
        "target_still_hard_qids": [],
        "target_delta_states": [["gs_026", "lookup_failed"]],
        "out_of_target_regressed_qids": [],
        "regression_debt_qids": [],
        "soft_to_hard_regressed_qids": [],
        "passing_to_hard_regressed_qids": [],
        "unknown_to_hard_regressed_qids": [],
        "reason_detail": "reason=target_resolution_failed; ...",
    }
    a = full_eval_marker(optimization_run_id="run-1", payload=payload)
    b = full_eval_marker(optimization_run_id="run-1", payload=payload)
    assert a == b


def test_full_eval_marker_name_passes_marker_line_regex() -> None:
    """Cycle 12-T1's marker_line regex requires GSO_<NAME>_V<N>; V1
    is the canonical first version."""
    from genie_space_optimizer.optimization.run_analysis_contract import (
        full_eval_marker,
    )

    line = full_eval_marker(
        optimization_run_id="run-1",
        payload=_accepted_payload(),
    )
    assert line.startswith("GSO_FULL_EVAL_V1 ")
