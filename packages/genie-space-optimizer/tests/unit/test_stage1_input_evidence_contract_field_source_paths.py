"""Phase 3 (Trial 13) — ``Stage1InputEvidenceContract.field_sources`` reports
the actual originating path string per field, not just ``"present"`` /
``"absent"``.

The Trial 12 reporter returned a literal ``"present"`` / ``"absent"``
two-valued enum. That was enough to detect *whether* a field hydrated,
but not *where from* — so when the next production row shape drifts
(as it did between Trial 12 and Trial 13: ``request.question`` instead
of ``request.kwargs.question``), the postmortem can only say "question
was empty" without naming the production path that needed to drift.

Trial 13 enriches the reporter so the postmortem can quote the typed
``source_path`` (e.g. ``"request.question"`` vs
``"inputs/question"``). This pins the regression boundary at the row
shape, not at "Stage 1 input was empty".
"""
from __future__ import annotations

import json
import pathlib

import pytest

from genie_space_optimizer.optimization.eval_row_access import (
    build_stage1_evidence_card,
)
from genie_space_optimizer.optimization.stage1_input_evidence_contract import (
    DEFAULT_STAGE1_CONTRACT,
)

FIXTURE_PATH = (
    pathlib.Path(__file__).parent
    / "fixtures"
    / "production_eval_rows_real.json"
)


def _load_real_row(namespaced_qid_suffix: str) -> dict:
    payload = json.loads(FIXTURE_PATH.read_text())
    for bucket in (
        "production_rows_98ec8950",
        "production_rows_98ec8950_iter03",
        "production_rows_dc89d1a9",
    ):
        for entry in payload.get(bucket, []):
            if str(entry["namespaced_qid"]).endswith(namespaced_qid_suffix):
                return dict(entry["row"])
    raise KeyError(namespaced_qid_suffix)


REAL_QID_SUFFIXES = [
    "gs_009",
    "gs_024",
    "gs_016",
    "gs_001",
    "gs_013",
    "gs_021",
    "gs_026",
]


@pytest.mark.parametrize("suffix", REAL_QID_SUFFIXES)
def test_field_sources_emits_request_question_path_for_real_rows(suffix: str) -> None:
    """For real production rows, ``question_text`` MUST report the
    originating path ``"request.question"``.
    """
    row = _load_real_row(suffix)
    card = build_stage1_evidence_card(suffix, row)
    sources = DEFAULT_STAGE1_CONTRACT.field_sources(card)
    assert sources["question_text"] == "request.question", (
        f"expected request.question for {suffix}, got {sources['question_text']!r}; "
        f"card={card}"
    )


def test_field_sources_emits_inputs_question_path_for_synthetic_inputs_row() -> None:
    """Synthetic row whose question lives at ``inputs/question`` MUST
    report ``"inputs/question"`` rather than the generic ``"present"``.
    """
    row = {
        "inputs/question_id": "gs_synthetic",
        "inputs/question": "What is the answer?",
        "inputs/expected_sql": "SELECT 1",
        "outputs/response": "SELECT 1",
        "judge_rationale": "judge",
        "arbiter/metadata": {
            "blame_set": ["t.col"],
            "failure_type": "wrong_aggregation",
            "wrong_clause": "GROUP BY",
            "counterfactual_fix": "fix",
            "patch_family": "add_sql_snippet_expression",
            "rca_kind": "missing_filter",
        },
    }
    card = build_stage1_evidence_card("gs_synthetic", row)
    sources = DEFAULT_STAGE1_CONTRACT.field_sources(card)
    assert sources["question_text"] == "inputs/question", (
        f"expected inputs/question, got {sources['question_text']!r}"
    )


def test_field_sources_emits_absent_for_empty_card() -> None:
    """Empty card MUST report ``"absent"`` for every field."""
    card = build_stage1_evidence_card("gs_empty", {})
    sources = DEFAULT_STAGE1_CONTRACT.field_sources(card)
    assert sources["question_text"] == "absent"
    assert sources["ground_truth_sql"] == "absent"
    assert sources["generated_sql"] == "absent"
    assert sources["judge_rationale"] == "absent"
    assert sources["blame_set_seed"] == "absent"
    assert sources["rca_evidence"] == "absent"


def test_field_sources_backwards_compat_present_is_still_truthy() -> None:
    """Legacy readers checking ``field_sources()[field] == 'present'``
    must remain unbroken when a field hydrates from an *unknown* path
    (no recorded source).

    The contract still emits the literal ``"present"`` for fields the
    builder cannot attribute to a specific row path (e.g. typed-evidence
    overrides). Empty fields still report ``"absent"``.
    """
    card = {
        "qid": "gs_x",
        "question_text": "Hello?",
        "ground_truth_sql": "SELECT 1",
        "generated_sql": "SELECT 2",
        "judge_rationale": "j",
        "blame_set_seed": ["t.c"],
        "rca_evidence": {
            "observed_failure": "of",
            "generated_sql_issue": "",
            "expected_sql_shape": "",
            "suggested_repair_family": "",
        },
        # NOTE: NO _source_paths sidecar — simulate a hand-built card
        # (e.g. a unit test) so the backwards-compat shim kicks in.
    }
    sources = DEFAULT_STAGE1_CONTRACT.field_sources(card)
    # All populated fields fall back to the legacy literal "present".
    assert sources["question_text"] == "present"
    assert sources["ground_truth_sql"] == "present"
    assert sources["generated_sql"] == "present"
    assert sources["judge_rationale"] == "present"
    assert sources["blame_set_seed"] == "present"
    assert sources["rca_evidence"] == "present"
