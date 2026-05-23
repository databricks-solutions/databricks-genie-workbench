"""SM Cutover Phase 1.B — shared eval-row admission helper.

The legacy harness pre-filter and the v4 state machine admission must
produce the same hard qid set. The 2026-05-23 trial surfaced an
``INPUT_PROJECTION_PARITY_PARTIAL_DRIFT`` because each path applied a
different subset of filters. This test pins the shared helper.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from genie_space_optimizer.optimization.eval_row_admission import (
    AdmissionResult,
    admit_eval_rows,
)

FIXTURE_PATH = (
    Path(__file__).parent / "fixtures" / "production_eval_rows.json"
)


@pytest.fixture(scope="module")
def production_rows() -> list[dict]:
    with FIXTURE_PATH.open() as f:
        data = json.load(f)
    return [dict(r) for r in data["eval_rows"]]


def test_admit_partitions_production_rows(production_rows: list[dict]) -> None:
    result = admit_eval_rows(production_rows)

    assert isinstance(result, AdmissionResult)
    assert len(result.hard_rows) >= 2  # at minimum gs_009 and gs_024
    # Each row must reach exactly one partition.
    total = (
        len(result.hard_rows)
        + len(result.gt_correction_rows)
        + len(result.soft_signal_rows)
        + len(result.non_failing_rows)
        + len(result.quarantined_rows)
        + len(result.excluded_rows)
    )
    assert total == len(production_rows)


def test_admit_excludes_quarantined_qids(production_rows: list[dict]) -> None:
    """A quarantined qid must NOT appear in hard_rows even if its row is hard."""
    target = "airline_ticketing_and_fare_analysis_gs_009"
    result = admit_eval_rows(production_rows, quarantined=(target,))

    from genie_space_optimizer.optimization._qid_extraction import (
        extract_question_id,
    )
    hard_qids = {extract_question_id(dict(r))[0] for r in result.hard_rows}
    assert target not in hard_qids
    quarantined_qids = {
        extract_question_id(dict(r))[0] for r in result.quarantined_rows
    }
    assert target in quarantined_qids


def test_admit_matches_versioned_quarantine_suffix() -> None:
    """``gs_009`` quarantine must exclude ``gs_009:v2`` variants too."""
    rows = [
        {"inputs/question_id": "gs_009", "feedback/result_correctness/value": "no",
         "feedback/arbiter/value": "ground_truth_correct"},
        {"inputs/question_id": "gs_009:v2", "feedback/result_correctness/value": "no",
         "feedback/arbiter/value": "ground_truth_correct"},
    ]
    result = admit_eval_rows(rows, quarantined=("gs_009",))
    assert result.hard_rows == ()


def test_admit_diverts_gt_correction_candidates() -> None:
    """``arbiter=genie_correct`` rows must go to gt_correction, not hard."""
    rows = [
        {"inputs/question_id": "gs_010",
         "feedback/result_correctness/value": "no",
         "feedback/arbiter/value": "genie_correct"},
    ]
    result = admit_eval_rows(rows)
    assert result.hard_rows == ()
    assert len(result.gt_correction_rows) == 1


def test_admit_is_idempotent() -> None:
    """Calling admit_eval_rows on its own hard_rows output must return the same."""
    rows = [
        {"inputs/question_id": "gs_009", "feedback/result_correctness/value": "no",
         "feedback/arbiter/value": "ground_truth_correct"},
        {"inputs/question_id": "gs_011", "feedback/result_correctness/value": "yes",
         "feedback/arbiter/value": "both_correct"},
    ]
    first = admit_eval_rows(rows)
    second = admit_eval_rows(list(first.hard_rows))
    assert first.hard_rows == second.hard_rows


def test_sm_admission_uses_same_helper_as_harness(production_rows: list[dict]) -> None:
    """``build_initial_states_from_eval_rows`` must admit exactly the same
    qids that ``admit_eval_rows(...).hard_rows`` produces. This is the
    parity invariant the 2026-05-23 trial violated.
    """
    from genie_space_optimizer.optimization.state_machine.transformers.dispatch_input import (
        build_initial_states_from_eval_rows,
    )
    from genie_space_optimizer.optimization._qid_extraction import (
        extract_question_id,
    )

    helper_hard_qids = {
        extract_question_id(dict(r))[0]
        for r in admit_eval_rows(production_rows).hard_rows
    }
    sm_states = build_initial_states_from_eval_rows(
        production_rows, iteration=1,
    )
    sm_hard_qids = {s.qid for s in sm_states}

    assert sm_hard_qids == helper_hard_qids
