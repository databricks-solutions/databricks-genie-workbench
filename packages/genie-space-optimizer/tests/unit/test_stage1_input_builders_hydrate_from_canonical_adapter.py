"""Phase 2 / Track 1 — Stage 1 input builders MUST hydrate evidence
from the canonical row-shape adapter for all five documented shapes.

The three builders today use flat ``row.get(...)`` and hardcoded empty
``rca_evidence.*`` subfields, producing the empty cards that the
Trial 11 LLM correctly declined as ``missing_schema_context``. These
tests pin the post-wire-in contract: every fixture row produces a
non-empty card, regardless of row shape.

Today (HEAD) every parametrize case fails. After Phase 2 green
(wire-in to ``eval_row_access`` + ``iter_asi_metadata``), they pass.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from genie_space_optimizer.optimization.optimizer import (
    _build_plan11_failing_qids_from_raw,
    _build_plan11_failing_qids_from_typed_evidence,
)
from genie_space_optimizer.optimization.rca_evidence_typed import (
    PatchType,
    PerQidRcaEvidence,
)
from genie_space_optimizer.optimization.state_machine.funnel import (
    FunnelStage,
)
from genie_space_optimizer.optimization.state_machine.records import (
    HardQidSeenRecord,
)
from genie_space_optimizer.optimization.state_machine.state import (
    QuestionStateInIteration,
)
from genie_space_optimizer.optimization.state_machine.transformers.diagnose_llm import (
    _build_failing_qid_payload,
)

FIXTURE_PATH = (
    Path(__file__).parent / "fixtures" / "production_eval_rows.json"
)
EXPECTED_QIDS = ["gs_009", "gs_021", "gs_024", "gs_026", "gs_004"]


@pytest.fixture(scope="module")
def hydration_rows() -> list[dict]:
    with FIXTURE_PATH.open() as f:
        data = json.load(f)
    return [dict(r) for r in data["hydration_rows"]]


def _fake_state(qid: str) -> QuestionStateInIteration:
    return QuestionStateInIteration(
        qid=qid,
        iteration=1,
        current_stage=FunnelStage.HARD_QID_SEEN,
        deepest_stage_reached=FunnelStage.HARD_QID_SEEN,
        seen=HardQidSeenRecord(
            eval_row_id=f"row_{qid}",
            predicate="row_is_hard_failure",
            score=0.0,
            baseline_sql="",
            expected_shape="",
            iteration_first_seen=1,
        ),
    )


def _row_for(rows: list[dict], qid: str) -> dict:
    return next(r for r in rows if r["_expected_qid"] == qid)


def _assert_card_is_hydrated(card: dict, qid: str) -> None:
    assert card["qid"] == qid
    assert card["question_text"].strip() != "", (
        f"question_text empty for {qid}; card={card}"
    )
    assert card["ground_truth_sql"].strip() != "", (
        f"ground_truth_sql empty for {qid}; card={card}"
    )
    assert card["generated_sql"].strip() != "", (
        f"generated_sql empty for {qid}; card={card}"
    )
    assert card["judge_rationale"].strip() != "", (
        f"judge_rationale empty for {qid}; card={card}"
    )
    assert card["blame_set_seed"], (
        f"blame_set_seed empty for {qid}; card={card}"
    )
    rca = card["rca_evidence"]
    assert isinstance(rca, dict)
    assert any(
        str(rca.get(key) or "").strip()
        for key in (
            "generated_sql_issue",
            "expected_sql_shape",
            "suggested_repair_family",
        )
    ), f"all ASI-derived rca_evidence subfields empty for {qid}; rca={rca}"


@pytest.mark.parametrize("qid", EXPECTED_QIDS)
def test_diagnose_llm_build_failing_qid_payload_hydrates_from_canonical_adapter(
    hydration_rows: list[dict], qid: str
) -> None:
    row = _row_for(hydration_rows, qid)
    card = _build_failing_qid_payload(_fake_state(qid), row)
    _assert_card_is_hydrated(card, qid)


@pytest.mark.parametrize("qid", EXPECTED_QIDS)
def test_optimizer_build_plan11_failing_qids_from_raw_hydrates_from_canonical_adapter(
    hydration_rows: list[dict], qid: str
) -> None:
    out = _build_plan11_failing_qids_from_raw(
        failing_qids=[qid],
        eval_rows=hydration_rows,
    )
    assert len(out) == 1, (
        f"expected single card for {qid}; got {len(out)}; rows had "
        f"{[r['_expected_qid'] for r in hydration_rows]}"
    )
    _assert_card_is_hydrated(out[0], qid)


def _typed_evidence_for(qid: str) -> PerQidRcaEvidence:
    return PerQidRcaEvidence(
        qid=qid,
        observed_failure=f"observed failure for {qid}",
        generated_sql_issue=f"sql issue for {qid}",
        expected_sql_shape=f"expected shape for {qid}",
        blame_set=(f"catalog.schema.tbl_{qid}",),
        suggested_repair_family="add_sql_snippet_expression",
        repair_hint_patch_type=PatchType.ADD_EXAMPLE_SQL,
        confidence="high",
        quoted_evidence=(f"quote about {qid}",),
    )


@pytest.mark.parametrize("qid", EXPECTED_QIDS)
def test_optimizer_build_plan11_failing_qids_from_typed_evidence_overrides_with_row_text(
    hydration_rows: list[dict], qid: str
) -> None:
    """When typed evidence is present AND eval_rows are provided, the
    builder must hydrate ``question_text``/``ground_truth_sql``/
    ``generated_sql`` from the matching row via the canonical adapter
    while letting typed-evidence fields win for ``rca_evidence`` and
    ``blame_set_seed``.
    """
    rca = {qid: _typed_evidence_for(qid)}
    out = _build_plan11_failing_qids_from_typed_evidence(
        rca, eval_rows=hydration_rows,
    )
    assert len(out) == 1
    card = out[0]
    _assert_card_is_hydrated(card, qid)
    assert card["rca_evidence"]["observed_failure"] == f"observed failure for {qid}"
    assert f"catalog.schema.tbl_{qid}" in card["blame_set_seed"]
