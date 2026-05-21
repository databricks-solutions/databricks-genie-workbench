"""Plan 11 — Stage 1/2 wiring in optimizer.cluster_failures.

PR 2 wired only the Stage 3 callsites (lines 17240 + 17407). This
file covers the Tasks 10.3/10.4 wiring that landed in the follow-up:

  * cluster_failures(rca_evidence_typed=...) now takes a top-priority
    Plan 11 branch under plan11_llm_first_enabled() that runs
    diagnose_failing_qids → cluster_diagnoses → legacy-dict adapter.
  * Two helpers project between the legacy PerQidRcaEvidence carrier
    and the Plan 11 FailureCluster carrier:
      _build_plan11_failing_qids_from_typed_evidence
      _plan11_failure_cluster_to_legacy_dict

The tests mock both stages at their import surface so no real LLM is
needed. Flag-OFF behavior (legacy heuristic / Plan 4) is exercised
elsewhere — the regression sweep in PR 3 confirmed it stays
byte-identical.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from genie_space_optimizer.optimization import optimizer
from genie_space_optimizer.optimization.llm_reasoning_io import (
    LlmReasoningResponse,
)
from genie_space_optimizer.optimization.optimizer import (
    _build_plan11_failing_qids_from_typed_evidence,
    _plan11_failure_cluster_to_legacy_dict,
    cluster_failures,
)
from genie_space_optimizer.optimization.rca_evidence_typed import (
    PerQidRcaEvidence,
)
from genie_space_optimizer.optimization.repair_intent import PatchType
from genie_space_optimizer.optimization.stages.plan11_types import (
    FailureCluster,
)


def _mk_evidence(qid: str) -> PerQidRcaEvidence:
    return PerQidRcaEvidence(
        qid=qid,
        observed_failure=f"observed failure for {qid}",
        generated_sql_issue=f"sql issue for {qid}",
        expected_sql_shape="ORDER BY x DESC LIMIT 10",
        blame_set=("catalog.schema.orders.amount",),
        suggested_repair_family="top_n_with_ordering",
        repair_hint_patch_type=PatchType.ADD_EXAMPLE_SQL,
        confidence="high",
        quoted_evidence=(),
    )


# ─────────────────────────────────────────────────────────────────────
# Adapter unit tests — pure projections, no LLM involvement.
# ─────────────────────────────────────────────────────────────────────


def test_build_failing_qids_from_typed_evidence_carries_blame_set():
    rca = {"gs_009": _mk_evidence("gs_009")}
    failing = _build_plan11_failing_qids_from_typed_evidence(rca)
    assert len(failing) == 1
    item = failing[0]
    assert item["qid"] == "gs_009"
    assert item["judge_rationale"] == "observed failure for gs_009"
    assert item["blame_set_seed"] == ["catalog.schema.orders.amount"]
    # question_text / ground_truth_sql / generated_sql intentionally
    # empty in this adapter — see docstring.
    assert item["question_text"] == ""
    assert item["ground_truth_sql"] == ""
    assert item["generated_sql"] == ""
    # rca_evidence sub-dict captures the richer narrative.
    assert (
        item["rca_evidence"]["expected_sql_shape"]
        == "ORDER BY x DESC LIMIT 10"
    )
    assert (
        item["rca_evidence"]["suggested_repair_family"]
        == "top_n_with_ordering"
    )


def test_failure_cluster_to_legacy_dict_matches_legacy_keys():
    fc = FailureCluster(
        cluster_id="H001",
        semantic_theme="top-N collapse",
        member_qids=("gs_009",),
        unifying_evidence="missing LIMIT clause",
        repair_hypothesis="Use ROW_NUMBER() + LIMIT 10",
        primary_blame_set=("catalog.schema.orders.amount",),
        confidence="high",
    )
    legacy = _plan11_failure_cluster_to_legacy_dict(fc, signal_type="hard")
    # Field-for-field compat with LlmCluster.to_legacy_dict.
    assert legacy["cluster_id"] == "H001"
    assert legacy["question_ids"] == ["gs_009"]
    assert legacy["asi_blame_set"] == ["catalog.schema.orders.amount"]
    assert legacy["root_cause"] == "top-N collapse"
    assert legacy["semantic_theme"] == "top-N collapse"
    assert legacy["llm_confidence"] == "high"
    assert legacy["llm_rationale"] == "missing LIMIT clause"
    assert legacy["signal_type"] == "hard"
    # Plan 11 specifics.
    assert legacy["source"] == "llm_plan11"
    assert legacy["suggested_repair_shape"] == "other"
    assert legacy["repair_hypothesis"] == "Use ROW_NUMBER() + LIMIT 10"


# ─────────────────────────────────────────────────────────────────────
# Integration — full Stage 1+2 chain via cluster_failures.
# ─────────────────────────────────────────────────────────────────────


def _stage_response(skill_id: str, parsed: dict) -> LlmReasoningResponse:
    return LlmReasoningResponse(
        call_id=f"test_{skill_id}",
        skill_id=skill_id,
        succeeded=True,
        parsed_output=parsed,
        declined=None,
        raw_text=json.dumps({"result": parsed, "declined": None}),
        tokens_input=100,
        tokens_output=50,
        duration_ms=1,
        error=None,
    )


def _diagnose_payload(qid: str) -> dict:
    return {
        "diagnoses": [
            {
                "qid": qid,
                "rca_kind_label": "top-N collapsed to single row",
                "observed_failure": "Query returned 1 row instead of 10",
                "generated_sql_issue": "RANK() unbounded by LIMIT",
                "expected_sql_shape": "ROW_NUMBER() with LIMIT 10",
                "blame_set": ["catalog.schema.orders.amount"],
                "evidence_summary": "RANK() returns all rows tied at 1",
                "confidence": "high",
            },
        ],
    }


def _cluster_payload(qid: str) -> dict:
    return {
        "clusters": [
            {
                "semantic_theme": "top-N row limit failures",
                "member_qids": [qid],
                "unifying_evidence": "Single-qid cluster of top-N",
                "repair_hypothesis": (
                    "Replace RANK() with ROW_NUMBER() and add LIMIT 10"
                ),
                "primary_blame_set": ["catalog.schema.orders.amount"],
                "confidence": "high",
            },
        ],
    }


def test_cluster_failures_plan11_branch_returns_legacy_dicts(monkeypatch):
    """Flag-ON: cluster_failures runs diagnose → cluster → legacy-dict
    adapter and returns dicts shaped like LlmCluster.to_legacy_dict.

    Plan 11 default is ON; this test mocks the two LLM stages so no
    real call is needed.
    """
    monkeypatch.setenv("GSO_PLAN11_LLM_FIRST", "1")

    rca = {"gs_009": _mk_evidence("gs_009")}

    from genie_space_optimizer.optimization.stages import (
        cluster_plan11 as _stage2_mod,
        diagnose as _stage1_mod,
    )

    with patch.object(_stage1_mod, "LlmReasoningCall") as MockS1, \
         patch.object(_stage2_mod, "LlmReasoningCall") as MockS2:
        MockS1.return_value.invoke = MagicMock(
            return_value=_stage_response(
                "plan11_diagnose", _diagnose_payload("gs_009"),
            ),
        )
        MockS2.return_value.invoke = MagicMock(
            return_value=_stage_response(
                "plan11_cluster", _cluster_payload("gs_009"),
            ),
        )

        clusters = cluster_failures(
            eval_results={"eval_result": None},
            metadata_snapshot={
                "schema_columns": ["catalog.schema.orders.amount"],
                "iteration": 1,
            },
            rca_evidence_typed=rca,
            signal_type="hard",
            namespace="hard",
            w=MagicMock(),
        )

    assert isinstance(clusters, list)
    assert len(clusters) == 1
    c = clusters[0]
    # Shape compat with LlmCluster.to_legacy_dict.
    assert c["cluster_id"].startswith("H")
    assert c["question_ids"] == ["gs_009"]
    assert c["source"] == "llm_plan11"
    assert c["signal_type"] == "hard"
    # Plan 11 free-text fields flow through.
    assert (
        c["repair_hypothesis"]
        == "Replace RANK() with ROW_NUMBER() and add LIMIT 10"
    )
    assert c["root_cause"] == "top-N row limit failures"


def test_cluster_failures_plan11_decline_falls_through(monkeypatch):
    """Flag-ON with no Plan 11 input (rca_evidence_typed=None):
    the Plan 11 branch is skipped and the legacy path runs.
    """
    monkeypatch.setenv("GSO_PLAN11_LLM_FIRST", "1")
    # plan4_llm_clustering_enabled defaults ON; disable it too so we
    # fall through to the deterministic heuristic and get an empty
    # result list (no failing qids in eval_results).
    monkeypatch.setenv("GSO_PLAN4_LLM_CLUSTERING", "0")

    clusters = cluster_failures(
        eval_results={"eval_result": None},
        metadata_snapshot={"iteration": 1},
        rca_evidence_typed=None,
        signal_type="hard",
        namespace="hard",
        w=MagicMock(),
    )

    # No rca_evidence_typed → Plan 11 branch is skipped. The
    # heuristic returns [] for empty input.
    assert clusters == []


def test_cluster_failures_flag_off_uses_legacy_path(monkeypatch):
    """Flag-OFF: Plan 11 branch is not taken regardless of input.

    This is the byte-identical-under-flag-OFF assertion PR 2/3
    targeted; with rca_evidence_typed supplied and the flag off, the
    Plan 4 branch is responsible (or the heuristic if Plan 4 also
    off). We disable Plan 4 too so the call returns from the
    heuristic body cleanly.
    """
    monkeypatch.setenv("GSO_PLAN11_LLM_FIRST", "0")
    monkeypatch.setenv("GSO_PLAN4_LLM_CLUSTERING", "0")

    rca = {"gs_009": _mk_evidence("gs_009")}
    # Patch the Plan 11 stage modules — if the legacy path
    # accidentally falls into them, this raises and the test
    # fails loudly.
    from genie_space_optimizer.optimization.stages import (
        cluster_plan11 as _stage2_mod,
        diagnose as _stage1_mod,
    )

    def _explode(*args, **kwargs):  # noqa: ANN001
        raise AssertionError(
            "Plan 11 stage invoked despite GSO_PLAN11_LLM_FIRST=0"
        )

    with patch.object(_stage1_mod, "diagnose_failing_qids", _explode), \
         patch.object(_stage2_mod, "cluster_diagnoses", _explode):
        clusters = cluster_failures(
            eval_results={"eval_result": None},
            metadata_snapshot={"iteration": 1},
            rca_evidence_typed=rca,
            signal_type="hard",
            namespace="hard",
            w=MagicMock(),
        )

    # Empty eval_results + heuristic path → empty list. Crucial: no
    # AssertionError from the _explode stubs above.
    assert clusters == []
