"""Plan 7 Task 8 — hypothesize_rollback_for_cluster abstain + error paths.

Driver returns None on every non-success path. The CALLER (the
iteration entry, Task 9) records the None outcome as a typed iteration
outcome.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization import optimizer
from genie_space_optimizer.optimization.repair_intent import (
    IntentOutcome,
    PatchType,
    RepairIntent,
    RepairShape,
)
from genie_space_optimizer.optimization.rollback_learning import (
    hypothesize_rollback_for_cluster,
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


def _intent() -> RepairIntent:
    return RepairIntent(
        intent_id="intent_001",
        intent_name="x", intent_description="x",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="x", confidence="high", source="x",
        cluster_id="H001", target_qids=(), blame_set=(),
        rca_card_id="", ag_id="AG3",
    )


def _intent_outcome() -> IntentOutcome:
    return IntentOutcome(
        intent_id="intent_001", ag_id="AG3",
        outcome="rolled_back",
        applied_signature="sig_x",
        applied_at_iter=1,
        rollback_reason="out_of_target_regression",
    )


def test_driver_returns_none_on_insufficient_signal_decline() -> None:
    decline = json.dumps({
        "result": None,
        "declined": {
            "reason": "insufficient_signal",
            "explanation": "rollback_reason and eval_diffs empty",
            "needed_evidence": ["eval_diffs"],
            "suggested_next_step": "rerun_after_acceptance_emits_reason",
        },
    })
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_with(decline),
    ):
        h = hypothesize_rollback_for_cluster(
            w=None, cluster_id="H001", ag_id="AG3", iteration=1,
            rolled_back_repair_intent=_intent(),
            intent_outcome=_intent_outcome(),
            per_qid_evidence={},
            critique_verdict=None,
            eval_diffs_for_cluster=(),
            identifier_allowlist=set(),
            applied_patch_fingerprints=set(),
        )
    assert h is None


def test_driver_returns_none_on_ambiguous_failure_decline() -> None:
    decline = json.dumps({
        "result": None,
        "declined": {
            "reason": "ambiguous_failure",
            "explanation": "every qid identical",
            "needed_evidence": ["differentiated_per_qid_evidence"],
            "suggested_next_step": "re_run_after_rca_extraction",
        },
    })
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_with(decline),
    ):
        h = hypothesize_rollback_for_cluster(
            w=None, cluster_id="H001", ag_id="AG3", iteration=1,
            rolled_back_repair_intent=_intent(),
            intent_outcome=_intent_outcome(),
            per_qid_evidence={},
            critique_verdict=None,
            eval_diffs_for_cluster=(),
            identifier_allowlist=set(),
            applied_patch_fingerprints=set(),
        )
    assert h is None


def test_driver_returns_none_when_envelope_parse_fails() -> None:
    malformed = '{"not": "envelope shape"}'
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_with(malformed),
    ):
        h = hypothesize_rollback_for_cluster(
            w=None, cluster_id="H001", ag_id="AG3", iteration=1,
            rolled_back_repair_intent=_intent(),
            intent_outcome=_intent_outcome(),
            per_qid_evidence={},
            critique_verdict=None,
            eval_diffs_for_cluster=(),
            identifier_allowlist=set(),
            applied_patch_fingerprints=set(),
        )
    assert h is None


def test_driver_returns_none_when_http_call_fails() -> None:
    def _boom(*args, **kwargs):
        raise RuntimeError("Serving endpoint returned 429 after 3 retries")
    with patch.object(optimizer, "_traced_llm_call", side_effect=_boom):
        h = hypothesize_rollback_for_cluster(
            w=None, cluster_id="H001", ag_id="AG3", iteration=1,
            rolled_back_repair_intent=_intent(),
            intent_outcome=_intent_outcome(),
            per_qid_evidence={},
            critique_verdict=None,
            eval_diffs_for_cluster=(),
            identifier_allowlist=set(),
            applied_patch_fingerprints=set(),
        )
    assert h is None
