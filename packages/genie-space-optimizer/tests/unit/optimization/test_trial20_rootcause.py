"""Trial 20 — A1 root-cause replay marker.

Pins:

* Marker shape stable; postmortem joins read these field names.
* The airline-shaped fixture (post-arbiter +4.2pp, pre-arbiter -12.5pp,
  rescued QID outside target_qids) classifies as
  ``target_attribution_drift``.
* The post-arbiter-gain-only fixture (rescued QID inside target_qids,
  but target_fixed empty due to baseline mismatch) classifies as
  one of the structural fix-surface labels.
* Missing arbiter field on all rows classifies as
  ``arbiter_field_stripped``.
"""
from __future__ import annotations

import json

from genie_space_optimizer.optimization.trial20_rootcause import (
    build_full_eval_root_cause_marker,
    format_marker_line,
)


def _row(qid: str, *, rc: str, av: str | None = "neither_correct") -> dict:
    """Synthetic eval row with the fields ``row_is_hard_failure`` reads."""
    out: dict = {
        "question_id": qid,
        "feedback/result_correctness/value": rc,
    }
    if av is not None:
        out["feedback/arbiter/value"] = av
    return out


def test_airline_shape_target_attribution_drift():
    """Airline iter-2: arbiter rescued gs_X but target was gs_009."""
    pre_rows = [
        _row("gs_009", rc="no"),
        _row("gs_X", rc="no"),
        _row("gs_007", rc="yes"),
    ]
    post_rows = [
        _row("gs_009", rc="no"),
        _row("gs_X", rc="no", av="both_correct"),
        _row("gs_007", rc="yes"),
    ]
    payload = build_full_eval_root_cause_marker(
        run_id="airline_519131527536322",
        ag_id="AG_DECOMPOSED_H002",
        iteration=2,
        pre_rows=pre_rows,
        post_rows=post_rows,
        target_qids=["gs_009"],
        baseline_accuracy=87.5,
        candidate_accuracy=91.7,
        baseline_pre_arbiter_accuracy=87.5,
        candidate_pre_arbiter_accuracy=75.0,
    )
    assert payload["marker"] == "GSO_TRIAL20_FULL_EVAL_ROOT_CAUSE_V1"
    assert payload["target_fixed_qids"] == []
    assert "gs_X" in payload["rescued_qids"]
    assert payload["identified_fix_surface"] == "target_attribution_drift"
    assert payload["post_arbiter_delta_pp"] == 4.2
    assert payload["pre_arbiter_delta_pp"] == -12.5
    assert payload["decision_today_accepted"] is False


def test_arbiter_field_stripped():
    """Every row missing arbiter field — row-projection bug surface."""
    pre_rows = [_row("gs_009", rc="no", av=None)]
    post_rows = [_row("gs_009", rc="no", av=None)]
    payload = build_full_eval_root_cause_marker(
        run_id="r",
        ag_id="AG",
        iteration=1,
        pre_rows=pre_rows,
        post_rows=post_rows,
        target_qids=["gs_009"],
        baseline_accuracy=50.0,
        candidate_accuracy=60.0,
        baseline_pre_arbiter_accuracy=50.0,
        candidate_pre_arbiter_accuracy=40.0,
    )
    assert payload["arbiter_field_present_count"] == 0
    assert payload["arbiter_field_missing_count"] == 2
    assert payload["identified_fix_surface"] == "arbiter_field_stripped"


def test_marker_line_format_round_trips():
    payload = build_full_eval_root_cause_marker(
        run_id="r",
        ag_id="AG",
        iteration=1,
        pre_rows=[_row("gs_009", rc="no")],
        post_rows=[_row("gs_009", rc="yes")],
        target_qids=["gs_009"],
        baseline_accuracy=50.0,
        candidate_accuracy=60.0,
        baseline_pre_arbiter_accuracy=50.0,
        candidate_pre_arbiter_accuracy=60.0,
    )
    line = format_marker_line(payload)
    assert line.startswith("GSO_TRIAL20_FULL_EVAL_ROOT_CAUSE_V1 ")
    parsed = json.loads(line.split(" ", 1)[1])
    assert parsed["target_fixed_qids"] == ["gs_009"]
    assert parsed["identified_fix_surface"] == "not_pre_arbiter_blocked"


def test_post_arbiter_gain_absorbs_pre_arbiter_regression_label():
    """No rescued QID outside targets; no arbiter strip; just pre-arbiter drop."""
    pre_rows = [
        _row("gs_009", rc="no"),
        _row("gs_007", rc="yes"),
    ]
    post_rows = [
        _row("gs_009", rc="yes"),
        _row("gs_007", rc="no"),
    ]
    payload = build_full_eval_root_cause_marker(
        run_id="r",
        ag_id="AG",
        iteration=1,
        pre_rows=pre_rows,
        post_rows=post_rows,
        target_qids=["gs_009"],
        baseline_accuracy=50.0,
        candidate_accuracy=55.0,
        baseline_pre_arbiter_accuracy=80.0,
        candidate_pre_arbiter_accuracy=60.0,
    )
    assert payload["target_fixed_qids"] == ["gs_009"]
    assert payload["identified_fix_surface"] == "not_pre_arbiter_blocked"
