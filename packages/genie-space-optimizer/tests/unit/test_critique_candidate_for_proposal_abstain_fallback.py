"""Plan 6 Task 7 — critique_candidate_for_proposal abstain + error paths.

Driver returns None on every non-success path. The CALLER (the stage's
execute, Task 9) records the None outcome as a typed iteration
outcome and lets the proposal through (advisory) OR drops it (when
enforcing and the verdict was a discard — but None isn't a discard,
so None means "let through with a typed marker").
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization import optimizer
from genie_space_optimizer.optimization.rca_evidence_typed import (
    PerQidRcaEvidence,
)
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairIntent,
    RepairShape,
)
from genie_space_optimizer.optimization.stages.candidate_critique import (
    critique_candidate_for_proposal,
)


def _stub_with(envelope_json: str) -> MagicMock:
    client = MagicMock()
    choice = MagicMock()
    choice.message.content = envelope_json
    completion = MagicMock()
    completion.choices = [choice]
    completion.usage = MagicMock(
        prompt_tokens=600, completion_tokens=100, total_tokens=700,
    )
    client.chat.completions.create.return_value = completion
    return client


def _evidence(qid: str) -> PerQidRcaEvidence:
    return PerQidRcaEvidence(
        qid=qid, observed_failure="x", generated_sql_issue="x",
        expected_sql_shape="x", blame_set=("sales.fact_sales.revenue",),
        suggested_repair_family="x",
        repair_hint_patch_type=PatchType.ADD_EXAMPLE_SQL,
        confidence="medium", quoted_evidence=(),
    )


def _intent() -> RepairIntent:
    return RepairIntent(
        intent_id="intent_H001_AG3_001",
        intent_name="x", intent_description="x",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="x", confidence="high", source="x",
        cluster_id="H001", target_qids=("gs_009",),
        blame_set=("sales.fact_sales.revenue",),
        rca_card_id="", ag_id="AG3",
    )


def _proposal_with_intent() -> dict:
    return {
        "proposal_id": "prop_H001_AG3_001",
        "example_question": "q", "example_sql": "SELECT 1",
        "repair_intent": _intent().to_json(),
    }


def test_driver_returns_none_on_insufficient_signal_decline() -> None:
    decline = json.dumps({
        "result": None,
        "declined": {
            "reason": "insufficient_signal",
            "explanation": "x",
            "needed_evidence": ["per_qid_evidence"],
            "suggested_next_step": "rerun_after_evidence_extraction",
        },
    })
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_with(decline),
    ):
        verdict = critique_candidate_for_proposal(
            w=None, proposal=_proposal_with_intent(),
            cluster_id="H001", ag_id="AG3", iteration=1,
            cluster_semantic_theme="x",
            per_qid_evidence={"gs_009": _evidence("gs_009")},
            passing_qids_at_risk=(),
        )
    assert verdict is None


def test_driver_returns_none_on_ambiguous_failure_decline() -> None:
    decline = json.dumps({
        "result": None,
        "declined": {
            "reason": "ambiguous_failure",
            "explanation": "every qid's observed_failure is generic",
            "needed_evidence": ["expected_sql_shape"],
            "suggested_next_step": "re_extract_with_better_anchor",
        },
    })
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_with(decline),
    ):
        verdict = critique_candidate_for_proposal(
            w=None, proposal=_proposal_with_intent(),
            cluster_id="H001", ag_id="AG3", iteration=1,
            cluster_semantic_theme="x",
            per_qid_evidence={"gs_009": _evidence("gs_009")},
            passing_qids_at_risk=(),
        )
    assert verdict is None


def test_driver_returns_none_when_envelope_parse_fails() -> None:
    malformed = '{"not": "envelope shape"}'
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_with(malformed),
    ):
        verdict = critique_candidate_for_proposal(
            w=None, proposal=_proposal_with_intent(),
            cluster_id="H001", ag_id="AG3", iteration=1,
            cluster_semantic_theme="x",
            per_qid_evidence={"gs_009": _evidence("gs_009")},
            passing_qids_at_risk=(),
        )
    assert verdict is None


def test_driver_returns_none_when_http_call_fails() -> None:
    def _boom(*args, **kwargs):
        raise RuntimeError("Serving endpoint returned 429 after 3 retries")
    with patch.object(optimizer, "_traced_llm_call", side_effect=_boom):
        verdict = critique_candidate_for_proposal(
            w=None, proposal=_proposal_with_intent(),
            cluster_id="H001", ag_id="AG3", iteration=1,
            cluster_semantic_theme="x",
            per_qid_evidence={"gs_009": _evidence("gs_009")},
            passing_qids_at_risk=(),
        )
    assert verdict is None
