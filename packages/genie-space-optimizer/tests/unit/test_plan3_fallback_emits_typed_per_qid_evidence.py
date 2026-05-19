"""Plan 8 Task 6 — when Plan 3's LLM path is disabled or declines,
the deterministic fallback in stages/rca_evidence.collect emits a
typed PerQidRcaEvidence alongside the legacy dict so Plan 4 LLM
clustering and Plan 5 LLM intent synthesis see fallback'd qids."""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.rca_evidence_typed import (
    PerQidRcaEvidence,
)
from genie_space_optimizer.optimization.stages import StageContext
from genie_space_optimizer.optimization.stages.rca_evidence import (
    RcaEvidenceInput, collect,
)


@pytest.fixture(autouse=True)
def _disable_llm_plan3(monkeypatch):
    monkeypatch.setenv("GSO_PLAN3_LLM_RCA_EVIDENCE", "0")


def _ctx() -> StageContext:
    return StageContext(
        run_id="r1", iteration=1, space_id="s", domain="d",
        catalog="c", schema="sc", apply_mode="apply",
        journey_emit=lambda *a, **k: None,
        decision_emit=lambda *a, **k: None,
    )


def test_fallback_populates_typed_evidence_sidecar():
    inp = RcaEvidenceInput(
        eval_rows=({"question_id": "q1", "genie_sql": "SELECT col_b FROM t"},),
        hard_failure_qids=("q1",),
        soft_signal_qids=(),
        per_qid_judge={
            "q1": {"judge_name": "judge_asi", "verdict": "wrong_column"},
        },
        asi_metadata={
            "q1": {
                "failure_type": "wrong_column",
                "expected_objects": ["catalog.s.t.col_a"],
                "actual_objects": ["catalog.s.t.col_b"],
                "blame_set": ["catalog.s.t.col_a"],
            },
        },
    )
    bundle = collect(_ctx(), inp)

    assert "q1" in bundle.per_qid_evidence_typed
    ev = bundle.per_qid_evidence_typed["q1"]
    assert isinstance(ev, PerQidRcaEvidence)
    assert ev.qid == "q1"
    assert "catalog.s.t.col_a" in ev.blame_set
    # Fallback always reports medium confidence by convention.
    assert ev.confidence in {"medium", "low"}
