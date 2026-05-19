"""Plan 5 Task 8 — synthesize_repair_intent abstain + error fallback.

The driver returns None on every non-success path. The CALLER (the
flag-gated short-circuit in Task 12) is responsible for falling back
to intent_from_archetype.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization import optimizer
from genie_space_optimizer.optimization.cluster_typed import LlmCluster
from genie_space_optimizer.optimization.rca_evidence_typed import (
    PerQidRcaEvidence,
)
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)
from genie_space_optimizer.optimization.repair_intent_synthesizer import (
    synthesize_repair_intent_for_cluster,
)


def _stub_with(envelope_json: str) -> MagicMock:
    client = MagicMock()
    choice = MagicMock()
    choice.message.content = envelope_json
    completion = MagicMock()
    completion.choices = [choice]
    completion.usage = MagicMock(
        prompt_tokens=1000, completion_tokens=200, total_tokens=1200,
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


def _llm_cluster() -> LlmCluster:
    return LlmCluster(
        cluster_id="H001", semantic_theme="x",
        member_qids=("gs_001", "gs_002"), unifying_evidence="x",
        suggested_repair_shape=RepairShape.TOP_N_BY_METRIC,
        primary_blame_set=("sales.fact_sales.revenue",),
        confidence="medium",
    )


def test_driver_returns_none_on_blame_set_too_sparse_decline() -> None:
    decline = json.dumps({
        "result": None,
        "declined": {
            "reason": "blame_set_too_sparse",
            "explanation": "every qid has empty blame_set",
            "needed_evidence": ["blame_set"],
            "suggested_next_step": "re_run_after_per_qid_extraction",
        },
    })
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_with(decline),
    ):
        rp = synthesize_repair_intent_for_cluster(
            w=None, cluster=_llm_cluster(),
            rca_evidence_typed={"gs_001": _evidence("gs_001"),
                                 "gs_002": _evidence("gs_002")},
            identifier_allowlist={"sales.fact_sales.revenue"},
            ag_id="AG3", iteration=1, seq=1,
            existing_examples_preview="", benchmarks=None,
        )
    assert rp is None


def test_driver_returns_none_on_schema_does_not_support_shape_decline() -> None:
    decline = json.dumps({
        "result": None,
        "declined": {
            "reason": "schema_does_not_support_shape",
            "explanation": "qid expects join but only one table allowlisted",
            "needed_evidence": ["additional_table_in_allowlist"],
            "suggested_next_step": "re_run_after_ag_expansion",
        },
    })
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_with(decline),
    ):
        rp = synthesize_repair_intent_for_cluster(
            w=None, cluster=_llm_cluster(),
            rca_evidence_typed={"gs_001": _evidence("gs_001"),
                                 "gs_002": _evidence("gs_002")},
            identifier_allowlist={"sales.fact_sales.revenue"},
            ag_id="AG3", iteration=1, seq=1,
            existing_examples_preview="", benchmarks=None,
        )
    assert rp is None


def test_driver_returns_none_on_no_applicable_patch_type_decline() -> None:
    decline = json.dumps({
        "result": None,
        "declined": {
            "reason": "no_applicable_patch_type",
            "explanation": "fix requires add_tvf which is not in available_patch_types",
            "needed_evidence": ["add_tvf_in_supported_overrides"],
            "suggested_next_step": "expand_cross_lever_router",
        },
    })
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_with(decline),
    ):
        rp = synthesize_repair_intent_for_cluster(
            w=None, cluster=_llm_cluster(),
            rca_evidence_typed={"gs_001": _evidence("gs_001"),
                                 "gs_002": _evidence("gs_002")},
            identifier_allowlist={"sales.fact_sales.revenue"},
            ag_id="AG3", iteration=1, seq=1,
            existing_examples_preview="", benchmarks=None,
        )
    assert rp is None


def test_driver_returns_none_when_envelope_parse_fails() -> None:
    malformed = '{"not": "envelope shape"}'
    with patch.object(
        optimizer, "_get_openai_client",
        return_value=_stub_with(malformed),
    ):
        rp = synthesize_repair_intent_for_cluster(
            w=None, cluster=_llm_cluster(),
            rca_evidence_typed={"gs_001": _evidence("gs_001"),
                                 "gs_002": _evidence("gs_002")},
            identifier_allowlist={"sales.fact_sales.revenue"},
            ag_id="AG3", iteration=1, seq=1,
            existing_examples_preview="", benchmarks=None,
        )
    assert rp is None


def test_driver_returns_none_when_http_call_fails() -> None:
    def _boom(*args, **kwargs):
        raise RuntimeError("Serving endpoint returned 429 after 3 retries")
    with patch.object(optimizer, "_traced_llm_call", side_effect=_boom):
        rp = synthesize_repair_intent_for_cluster(
            w=None, cluster=_llm_cluster(),
            rca_evidence_typed={"gs_001": _evidence("gs_001"),
                                 "gs_002": _evidence("gs_002")},
            identifier_allowlist={"sales.fact_sales.revenue"},
            ag_id="AG3", iteration=1, seq=1,
            existing_examples_preview="", benchmarks=None,
        )
    assert rp is None
