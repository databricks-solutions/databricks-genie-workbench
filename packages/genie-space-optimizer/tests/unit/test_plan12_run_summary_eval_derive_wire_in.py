"""Plan 12 PR 7 Task 7.5 deferred — harness wire-in for
build_run_summary's eval_result threading.

The wire-in projects ``_latest_eval_result`` (the harness's per-iter
carrier shape) into the ``{rows: [...]}`` shape ``build_run_summary``
consumes, then passes it through. Flag OFF preserves byte-stability;
flag ON makes the run_summary's hard_failures_count reflect reality
instead of always-0.
"""
import os
from unittest.mock import patch


# ── Flag tests ────────────────────────────────────────────────────────


def test_flag_off_by_default():
    from genie_space_optimizer.common.config import (
        plan12_live_run_summary_eval_derive_enabled,
    )
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("GSO_PLAN12_LIVE_RUN_SUMMARY_EVAL_DERIVE", None)
        assert plan12_live_run_summary_eval_derive_enabled() is False


def test_flag_on_with_truthy_values():
    from genie_space_optimizer.common.config import (
        plan12_live_run_summary_eval_derive_enabled,
    )
    for val in ("true", "True", "TRUE", "1", "yes", "on"):
        with patch.dict(
            os.environ,
            {"GSO_PLAN12_LIVE_RUN_SUMMARY_EVAL_DERIVE": val},
        ):
            assert plan12_live_run_summary_eval_derive_enabled() is True, (
                f"Expected True for {val!r}"
            )


# ── Projector tests ───────────────────────────────────────────────────


def test_projector_handles_empty_carrier():
    from genie_space_optimizer.optimization.harness import (
        _latest_eval_result_to_rows,
    )
    assert _latest_eval_result_to_rows(None) == {"rows": []}
    assert _latest_eval_result_to_rows({}) == {"rows": []}


def test_projector_maps_yes_no_to_pass_hard():
    """yes → 1.0 (pass, ignored by build_run_summary's hard/soft split),
    no → 0.0 (hard failure)."""
    from genie_space_optimizer.optimization.harness import (
        _latest_eval_result_to_rows,
    )
    carrier = {
        "question_ids": ["q1", "q2", "q3"],
        "scores": {"q1": "yes", "q2": "no", "q3": "no"},
    }
    out = _latest_eval_result_to_rows(carrier)
    rows = out["rows"]
    assert len(rows) == 3
    by_qid = {r["question_id"]: r["score"] for r in rows}
    assert by_qid["q1"] == 1.0
    assert by_qid["q2"] == 0.0
    assert by_qid["q3"] == 0.0


def test_projector_normalizes_verdict_aliases():
    """yes/true/1/pass all map to pass; anything else maps to hard."""
    from genie_space_optimizer.optimization.harness import (
        _latest_eval_result_to_rows,
    )
    carrier = {
        "scores": {
            "q1": "yes",
            "q2": "Yes",
            "q3": "true",
            "q4": "1",
            "q5": "pass",
            "q6": "no",
            "q7": "",
        },
    }
    rows = _latest_eval_result_to_rows(carrier)["rows"]
    by_qid = {r["question_id"]: r["score"] for r in rows}
    assert by_qid["q1"] == 1.0
    assert by_qid["q2"] == 1.0
    assert by_qid["q3"] == 1.0
    assert by_qid["q4"] == 1.0
    assert by_qid["q5"] == 1.0
    assert by_qid["q6"] == 0.0
    assert by_qid["q7"] == 0.0


def test_projector_chains_into_build_run_summary():
    """End-to-end: projector + build_run_summary produces correct
    hard_failures_count. Closes the wire from the harness's carrier
    shape to the run_summary.json output."""
    from genie_space_optimizer.optimization.harness import (
        _latest_eval_result_to_rows,
    )
    from genie_space_optimizer.optimization.run_output_bundle import (
        build_run_summary,
    )

    carrier = {
        "scores": {"q1": "yes", "q2": "no", "q3": "no", "q4": "yes"},
    }
    summary = build_run_summary(
        baseline={"overall_accuracy": 50.0},
        terminal_state={"status": "max_iterations"},
        iteration_count=3,
        accuracy_delta_pp=10.0,
        eval_result=_latest_eval_result_to_rows(carrier),
    )
    # 2 "no" rows → hard=2; "yes" rows score 1.0 → pass (neither hard
    # nor soft). soft=0 because the yes/no carrier has no middle ground.
    assert summary["hard_failures_count"] == 2
    assert summary["soft_failures_count"] == 0


def test_projector_handles_malformed_scores_field():
    """If ``scores`` is missing or not a dict, the projector returns
    empty rows — build_run_summary then defaults hard/soft to 0,
    matching the legacy behavior."""
    from genie_space_optimizer.optimization.harness import (
        _latest_eval_result_to_rows,
    )
    assert _latest_eval_result_to_rows({"question_ids": []}) == {"rows": []}
    assert _latest_eval_result_to_rows({"scores": None}) == {"rows": []}
    assert _latest_eval_result_to_rows({"scores": "garbage"}) == {"rows": []}
