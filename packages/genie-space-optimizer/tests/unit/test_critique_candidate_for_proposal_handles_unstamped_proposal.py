"""Plan 6 Task 14 — short-circuit behaviour for unstamped proposals.

A legacy proposal (no Plan-1 / Plan-5 RepairIntent stamp) cannot be
critiqued — the SKILL.md insufficient_signal rule short-circuits the
driver WITHOUT dispatching the LLM. This saves token budget and
matches the "advisory or gating only through a typed deterministic
contract" reviewer requirement.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization import optimizer
from genie_space_optimizer.optimization.rca_evidence_typed import (
    PerQidRcaEvidence,
)
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
)
from genie_space_optimizer.optimization.stages.candidate_critique import (
    critique_candidate_for_proposal,
)


def _evidence(qid: str) -> PerQidRcaEvidence:
    return PerQidRcaEvidence(
        qid=qid, observed_failure="x", generated_sql_issue="x",
        expected_sql_shape="x", blame_set=(),
        suggested_repair_family="x",
        repair_hint_patch_type=PatchType.ADD_EXAMPLE_SQL,
        confidence="high", quoted_evidence=(),
    )


def test_proposal_without_repair_intent_short_circuits_without_llm_call() -> None:
    proposal = {
        "proposal_id": "prop_legacy",
        "example_question": "x",
        "example_sql": "SELECT 1",
    }
    client = MagicMock(name="OpenAIClientShouldNotBeCalled")
    with patch.object(optimizer, "_get_openai_client", return_value=client):
        verdict = critique_candidate_for_proposal(
            w=None, proposal=proposal,
            cluster_id="H001", ag_id="AG3", iteration=2,
            cluster_semantic_theme="x",
            per_qid_evidence={"gs_009": _evidence("gs_009")},
            passing_qids_at_risk=(),
        )
    assert verdict is None
    assert client.chat.completions.create.call_count == 0


def test_proposal_with_malformed_repair_intent_dict_short_circuits() -> None:
    """If repair_intent is present but unparseable, treat it as missing
    (same short-circuit). extract_repair_intent_from_proposal returns
    None for malformed payloads."""
    proposal = {
        "proposal_id": "prop_malformed",
        "example_question": "x",
        "example_sql": "SELECT 1",
        "repair_intent": "this is a string not a dict",
    }
    client = MagicMock(name="OpenAIClientShouldNotBeCalled")
    with patch.object(optimizer, "_get_openai_client", return_value=client):
        verdict = critique_candidate_for_proposal(
            w=None, proposal=proposal,
            cluster_id="H001", ag_id="AG3", iteration=2,
            cluster_semantic_theme="x",
            per_qid_evidence={"gs_009": _evidence("gs_009")},
            passing_qids_at_risk=(),
        )
    assert verdict is None
    assert client.chat.completions.create.call_count == 0


def test_proposal_with_empty_repair_intent_dict_short_circuits() -> None:
    """Empty dict for repair_intent → extract returns None → short-circuit."""
    proposal = {
        "proposal_id": "prop_empty_intent",
        "example_question": "x",
        "example_sql": "SELECT 1",
        "repair_intent": {},
    }
    client = MagicMock(name="OpenAIClientShouldNotBeCalled")
    with patch.object(optimizer, "_get_openai_client", return_value=client):
        verdict = critique_candidate_for_proposal(
            w=None, proposal=proposal,
            cluster_id="H001", ag_id="AG3", iteration=2,
            cluster_semantic_theme="x",
            per_qid_evidence={"gs_009": _evidence("gs_009")},
            passing_qids_at_risk=(),
        )
    assert verdict is None
    assert client.chat.completions.create.call_count == 0
