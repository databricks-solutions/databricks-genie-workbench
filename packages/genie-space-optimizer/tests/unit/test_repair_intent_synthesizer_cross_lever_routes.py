"""Plan 5 Task 14 — Plan-5 declines feed into _L5B_RICH_PATH_DECLINES.

The harness drains this ledger and emits NO_STRUCTURAL_CANDIDATE
decision records. Plan 5 declines (LLM decline, validator rejection,
cross-lever routing failure) must show up there so postmortem
visibility is uniform across paths.

The _build_decline_record helper gains an optional
intent_decline_reason field — additive (existing callers pass nothing).
"""
from __future__ import annotations

from genie_space_optimizer.optimization.l5b_rich_dispatch import (
    _L5B_RICH_PATH_DECLINES,
    _build_decline_record,
    drain_l5b_rich_path_declines,
)


def test_build_decline_record_accepts_intent_decline_reason_kwarg() -> None:
    cluster = {
        "cluster_id": "H001",
        "question_ids": ["gs_001"],
        "root_cause": "missing_top_n",
        "asi_failure_type": "missing_top_n",
    }
    rec = _build_decline_record(
        cluster=cluster,
        attempted_archetypes=("top_n_by_metric",),
        skipped_reason="plan5_validator_rejected",
        intent_decline_reason="blame_set_not_in_identifier_allowlist",
    )
    assert rec["intent_decline_reason"] == "blame_set_not_in_identifier_allowlist"
    assert rec["skipped_reason"] == "plan5_validator_rejected"
    assert rec["cluster_id"] == "H001"
    assert rec["question_ids"] == ("gs_001",)


def test_build_decline_record_back_compat_when_intent_decline_reason_omitted() -> None:
    """Existing callers that don't pass intent_decline_reason still
    produce records WITHOUT the new field (or with it as None)."""
    cluster = {
        "cluster_id": "H001",
        "question_ids": ["gs_001"],
        "root_cause": "x",
        "asi_failure_type": "x",
    }
    rec = _build_decline_record(
        cluster=cluster,
        attempted_archetypes=(),
        skipped_reason="exception",
    )
    assert rec.get("intent_decline_reason") is None


def test_drain_ledger_includes_plan5_decline_records() -> None:
    """When Plan-5 records a decline, drain returns it with the
    intent_decline_reason populated."""
    _L5B_RICH_PATH_DECLINES.clear()

    cluster = {
        "cluster_id": "H001",
        "question_ids": ["gs_001"],
        "root_cause": "x",
        "asi_failure_type": "x",
    }
    _L5B_RICH_PATH_DECLINES.append(
        _build_decline_record(
            cluster=cluster,
            attempted_archetypes=(),
            skipped_reason="plan5_llm_declined",
            intent_decline_reason="ambiguous_failure",
        )
    )

    drained = drain_l5b_rich_path_declines()
    assert len(drained) == 1
    assert drained[0]["intent_decline_reason"] == "ambiguous_failure"
    assert drain_l5b_rich_path_declines() == []
