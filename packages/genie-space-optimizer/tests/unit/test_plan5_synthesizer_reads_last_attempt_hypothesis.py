"""Plan 7 Task 12 — Plan 5's _render_user_prompt reads
metadata_snapshot["_last_attempt_hypothesis_by_cluster"][cluster_id]
and surfaces it as the new optional ``last_attempt_hypothesis``
context field in the user prompt JSON.

Additive — when no hypothesis is stamped (existing tests), the field
is ``null`` and the prompt is byte-stable.
"""
from __future__ import annotations

import json

from genie_space_optimizer.optimization.cluster_typed import LlmCluster
from genie_space_optimizer.optimization.repair_intent import (
    RepairShape,
)
from genie_space_optimizer.optimization.repair_intent_synthesizer import (
    _render_user_prompt,
)


def _llm_cluster() -> LlmCluster:
    return LlmCluster(
        cluster_id="H001",
        semantic_theme="top-N revenue ranking missing",
        member_qids=("gs_009",),
        unifying_evidence="x",
        suggested_repair_shape=RepairShape.TOP_N_BY_METRIC,
        primary_blame_set=("sales.fact_sales.revenue",),
        confidence="high",
    )


def _fixture_kwargs() -> dict:
    return {
        "cluster": _llm_cluster(),
        "rca_evidence_typed": {},
        "identifier_allowlist": {"sales.fact_sales.revenue"},
        "ag_id": "AG3",
        "iteration": 3,
        "existing_examples_preview": "",
    }


def test_render_user_prompt_emits_last_attempt_hypothesis_null_when_absent() -> None:
    """No metadata_snapshot OR no hypothesis stamped → field is null."""
    out = _render_user_prompt(
        metadata_snapshot=None,
        **_fixture_kwargs(),
    )
    payload = json.loads(out)
    assert "last_attempt_hypothesis" in payload
    assert payload["last_attempt_hypothesis"] is None


def test_render_user_prompt_emits_last_attempt_hypothesis_null_when_key_missing() -> None:
    """metadata_snapshot present but no key → field is null."""
    out = _render_user_prompt(
        metadata_snapshot={"_failure_clusters": []},
        **_fixture_kwargs(),
    )
    payload = json.loads(out)
    assert payload["last_attempt_hypothesis"] is None


def test_render_user_prompt_emits_last_attempt_hypothesis_null_when_cluster_absent() -> None:
    """Other clusters have stamped hypotheses but the current cluster
    doesn't → field is null."""
    out = _render_user_prompt(
        metadata_snapshot={
            "_last_attempt_hypothesis_by_cluster": {
                "H002": {"cluster_id": "H002", "confidence": "low"},
            },
        },
        **_fixture_kwargs(),
    )
    payload = json.loads(out)
    assert payload["last_attempt_hypothesis"] is None


def test_render_user_prompt_surfaces_hypothesis_when_present() -> None:
    hypothesis_payload = {
        "rolled_back_intent_id": "intent_H001_AG3_001",
        "cluster_id": "H001",
        "ag_id": "AG3",
        "iteration": 2,
        "why_failed": "patch was too broad",
        "failure_mode": "overgeneralized_filter",
        "revised_repair_shape": None,
        "revised_patch_type": "add_sql_snippet_filter",
        "revised_blame_set": None,
        "additional_evidence_needed": [],
        "forbidden_signatures": ["sig_top_n_abc123"],
        "confidence": "high",
    }
    out = _render_user_prompt(
        metadata_snapshot={
            "_last_attempt_hypothesis_by_cluster": {"H001": hypothesis_payload},
        },
        **_fixture_kwargs(),
    )
    payload = json.loads(out)
    assert payload["last_attempt_hypothesis"] == hypothesis_payload
