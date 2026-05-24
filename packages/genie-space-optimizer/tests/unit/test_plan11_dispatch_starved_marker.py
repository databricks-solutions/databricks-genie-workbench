"""Phase 4 / Track 3 — Plan 11 dispatch starvation marker.

Today the only signal of dispatch starvation is
``GSO_INPUT_PROJECTION_CONTRACT_VIOLATION_V1``, which fires only when
BOTH the SM and Plan 11 dispatch see zero hard QIDs while the harness
sees more. The trial-11 / dc89d1a9 partial-drift case (harness=N,
SM=N, Plan 11 dispatch=0) is currently silent — it shows up only as
``GSO_PLAN11_DISPATCH_DECISION_V1`` with skip_reason
``build_failing_qids_empty``, which is also the legitimate reason for
"no hard QIDs found this iteration."

This test pins a new typed observability-only marker:
``GSO_PLAN11_DISPATCH_STARVED_V1`` fires when Plan 11 dispatch sees
zero failing QIDs AND the SM (from ``_sm_hard_qid_count``) saw > 0.
This is NOT fail-closed — it's an attribution signal for postmortems.
"""
from __future__ import annotations

import io
import json
from contextlib import redirect_stdout

from genie_space_optimizer.optimization.state_machine.markers import (
    plan11_dispatch_starved_marker,
)


def test_plan11_dispatch_starved_marker_round_trips() -> None:
    line = plan11_dispatch_starved_marker(
        run_id="run_x",
        iteration=2,
        plan11_failing_qids_count=0,
        sm_hard_qid_count=3,
        harness_hard_qid_count=3,
    )
    assert line.startswith("GSO_PLAN11_DISPATCH_STARVED_V1 ")
    payload = json.loads(line.split(" ", 1)[1])
    assert payload["run_id"] == "run_x"
    assert payload["iteration"] == 2
    assert payload["plan11_failing_qids_count"] == 0
    assert payload["sm_hard_qid_count"] == 3
    assert payload["harness_hard_qid_count"] == 3


def _run_dispatch(metadata_snapshot: dict, failing_qids: list[str]) -> str:
    """Invoke ``_decide_and_run_plan11_dispatch`` and capture stdout."""
    from genie_space_optimizer.optimization.optimizer import (
        _decide_and_run_plan11_dispatch,
    )

    buf = io.StringIO()
    with redirect_stdout(buf):
        _decide_and_run_plan11_dispatch(
            failing_qids=failing_qids,
            rca_evidence_typed={},
            metadata_snapshot=metadata_snapshot,
            namespace="ns",
            signal_type="ns",
            run_id="run_x",
            w=None,
        )
    return buf.getvalue()


def test_case_a_dispatch_starved_when_plan11_zero_but_sm_nonzero() -> None:
    """harness=3, SM=3, Plan 11 dispatch=0 → starved marker fires."""
    metadata = {
        "iteration": 2,
        "optimization_run_id": "run_x",
        "_eval_rows_failing": [],  # produces failing_qids_input=0
        "_sm_hard_qid_count": 3,
        "_harness_hard_qid_count": 3,
    }
    out = _run_dispatch(metadata, failing_qids=["q1", "q2", "q3"])
    assert "GSO_PLAN11_DISPATCH_STARVED_V1" in out


def test_case_b_no_starved_marker_when_sm_also_zero() -> None:
    """harness=3, SM=0, Plan 11=0 → existing contract-violation path,
    NOT the new starved marker (which is observation, not severity).
    """
    metadata = {
        "iteration": 2,
        "optimization_run_id": "run_x",
        "_eval_rows_failing": [],
        "_sm_hard_qid_count": 0,
        "_harness_hard_qid_count": 3,
    }
    out = _run_dispatch(metadata, failing_qids=[])
    assert "GSO_PLAN11_DISPATCH_STARVED_V1" not in out


def test_case_c_no_starved_marker_when_dispatch_nonempty() -> None:
    """harness=3, SM=3, Plan 11=3 → nothing starved fires."""
    metadata = {
        "iteration": 2,
        "optimization_run_id": "run_x",
        "_eval_rows_failing": [
            {
                "question_id": "q1",
                "question": "How many?",
                "ground_truth_sql": "SELECT 1",
                "generated_sql": "SELECT 2",
                "judge_rationale": "wrong",
                "result_correctness": "no",
                "arbiter": "wrong",
                "feedback/asi/metadata": {
                    "failure_type": "missing_filter",
                    "blame_set": ["t.col"],
                },
            },
        ],
        "_sm_hard_qid_count": 3,
        "_harness_hard_qid_count": 3,
    }
    out = _run_dispatch(metadata, failing_qids=["q1"])
    assert "GSO_PLAN11_DISPATCH_STARVED_V1" not in out
