"""Plan 10 Phase C — anchor replay deploy gate.

Four production failures from the two zero-delta deployments
(``59a173d3`` airline + ``ab65fefe`` 7now) are captured as fixtures
under ``tests/replay/fixtures/plan10/``. Each fixture replays the
exact cluster the Plan 9 LLM-direct dispatch had to handle — and that
the production pipeline silently dropped through Leaks 1 / 2 / 3.

This test is a **deploy gate**. It must:

  * Run RED on the current main / cycle-12 tree (proves the leak
    chain).
  * Run GREEN after Plan 10 Phase A1+A2 land (proves the ratification
    closed the leak chain at least to the materialization handoff).

What the test isolates
----------------------
For each fixture, ``_dispatch_lever_5b_for_cluster`` is invoked with
the production cluster + ``rca_evidence_typed = {}`` (production
reality) + a populated ``LlmCluster`` (Plan 4 reaches the dispatch with
this in the new world) + a mocked ``LlmReasoningCall`` that returns a
**Plan-9-shaped** proposal envelope (target_objects and
required_constructs populated).

The legacy fallback paths (``synthesize_example_sqls`` and the rich
synthesizer) are patched to return ``None`` / ``[]`` so the test
specifically probes the Plan 9 dispatch lane.

Assertions
----------
A. **Leak 1** — dispatch returns at least one proposal_dict.
   Fails today because gate at ``optimizer.py:10443`` requires
   non-empty ``rca_evidence_typed`` (which production proves empty
   per ``02_rca_evidence/output.json``).

B. **Leak 2** — stamped ``repair_intent.target_objects`` is non-empty.
   Fails today because ``repair_intent_synthesizer.py:346-356`` manually
   constructs ``RepairProposal(...)`` without ``target_objects=…``,
   stripping the Plan 9 fields the LLM emitted.

Both assertions remain coupled to the same dispatch call — the first
that goes red short-circuits the test, mirroring how the leaks
compound in production.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from genie_space_optimizer.optimization import optimizer
from genie_space_optimizer.optimization.cluster_typed import LlmCluster
from genie_space_optimizer.optimization.llm_reasoning_io import (
    LlmReasoningResponse,
)
from genie_space_optimizer.optimization.optimizer import (
    _dispatch_lever_5b_for_cluster,
)
from genie_space_optimizer.optimization.repair_intent import RepairShape

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "plan10"

_FIXTURE_FILES = (
    "airline_gs_009_plural_top_n_collapse.json",
    "airline_gs_024_missing_filter.json",
    "7now_gs_013_wrong_filter_condition.json",
    "7now_gs_026_plural_top_n_collapse.json",
)


# ─────────────────────────────────────────────────────────────────────
# Fixture helpers
# ─────────────────────────────────────────────────────────────────────


def _load_fixture(name: str) -> dict:
    with (FIXTURES_DIR / name).open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _build_llm_cluster(seed: dict) -> LlmCluster:
    """Construct an ``LlmCluster`` mirroring what Plan 4 hands off."""
    return LlmCluster(
        cluster_id=str(seed["cluster_id"]),
        semantic_theme=str(seed["semantic_theme"]),
        member_qids=tuple(str(q) for q in seed.get("member_qids", [])),
        unifying_evidence=str(seed.get("unifying_evidence", "") or ""),
        suggested_repair_shape=RepairShape(seed["suggested_repair_shape"]),
        primary_blame_set=tuple(
            str(b) for b in seed.get("primary_blame_set", [])
        ),
        confidence=seed.get("confidence", "medium"),  # type: ignore[arg-type]
    )


def _plan9_envelope_for(fixture: dict) -> dict:
    """A Plan-9-shaped LLM proposal envelope.

    Mirrors ``LlmRepairProposalOutput.model_dump()`` with **populated**
    ``target_objects`` and ``required_constructs`` — the Plan 9 fields
    Phase A1 must preserve through ``RepairProposal.from_llm_output``.

    Patch type is ``add_example_sql`` so ``to_proposal_dict`` runs
    through a stable per-patch-type projection. The example SQL is
    intentionally a small, generic shape — this test does not gate on
    SQL quality, only on the wire-through of Plan 9 fields.

    ``blame_set`` is left empty (vacuously valid per the synthesizer's
    allowlist validator) because the minimal replay ``metadata_snapshot``
    carries no ``schema_columns`` and the production ``rca_evidence_typed``
    is empty — there is no allowlist for blame_set entries to be inside.
    """
    qid = fixture["failing_qid"]
    return {
        "intent_name": f"plan10_replay_{fixture['fixture_id']}",
        "intent_description": (
            f"Plan 10 replay fixture proposal for {qid}. "
            f"Failure shape: {fixture['failure_shape']}."
        ),
        "repair_shape": "top_n_by_metric",
        "patch_type": "add_example_sql",
        "rationale": (
            "Replay envelope — exercise Plan 9 field preservation."
        ),
        "confidence": "medium",
        "patch_body": {
            "example_question": (
                f"Replay question for {fixture['failure_shape']}"
            ),
            "example_sql": (
                "SELECT region, SUM(revenue) r "
                "FROM sales.fact_sales "
                "GROUP BY region ORDER BY r DESC LIMIT 10"
            ),
            "usage_guidance": "replay-fixture canonical example",
            "parameters": [],
        },
        "blame_set": [],
        "target_objects": [
            {
                "asset_kind": "table",
                "identifier": "sales.fact_sales",
                "columns": ["revenue", "region"],
            },
        ],
        "required_constructs": ["SELECT", "GROUP_BY", "ORDER_BY", "LIMIT"],
    }


def _fake_reasoning_response(envelope: dict) -> LlmReasoningResponse:
    return LlmReasoningResponse(
        call_id="plan10_replay_call",
        skill_id="repair_intent_synthesis",
        succeeded=True,
        parsed_output=envelope,
        declined=None,
        raw_text=json.dumps({"result": envelope, "declined": None}),
        tokens_input=1000,
        tokens_output=500,
        duration_ms=1,
        error=None,
    )


# ─────────────────────────────────────────────────────────────────────
# Deploy gate test — parametrized across all 4 production fixtures
# ─────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("fixture_name", _FIXTURE_FILES)
def test_plan10_anchor_replay_produces_typed_proposal(
    fixture_name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each fixture's production cluster MUST produce a non-empty
    proposal_dict whose stamped ``repair_intent.target_objects`` is
    populated. Today both assertions fail (Leaks 1 and 2)."""
    fixture = _load_fixture(fixture_name)

    monkeypatch.setenv("GSO_PLAN5_LEVER_5B_LLM_INTENT", "true")

    cluster: dict[str, Any] = dict(fixture["cluster"])
    metadata: dict[str, Any] = dict(fixture["minimal_metadata_snapshot"])
    # Production reality — empty typed RCA evidence triggers Leak 1.
    rca_evidence_typed: dict = {}
    llm_cluster = _build_llm_cluster(fixture["llm_cluster_seed"])
    envelope = _plan9_envelope_for(fixture)
    response = _fake_reasoning_response(envelope)

    # ── 1. Force the Plan 9 LLM lane to be the only viable producer ──
    # Patch the synthesizer's bound LlmReasoningCall.invoke to return
    # a typed, Plan-9-shaped response.
    from genie_space_optimizer.optimization import repair_intent_synthesizer

    with patch.object(
        repair_intent_synthesizer, "LlmReasoningCall"
    ) as MockCall:
        instance = MockCall.return_value
        instance.invoke.return_value = response

        # ── 2. Force the legacy fallback paths to RETURN EMPTY ───────
        # so the only way the test can go green is via the Plan 9 lane.
        from genie_space_optimizer.optimization import synthesis as _synth
        from genie_space_optimizer.optimization import (
            l5b_rich_dispatch as _rich,
        )

        with patch.object(
            _synth, "synthesize_example_sqls", return_value=None
        ), patch.object(
            _rich,
            "_dispatch_rich_synthesis_for_l5b",
            return_value=[],
        ), patch.object(
            _rich,
            "should_route_l5b_to_rich_synthesizer",
            return_value=False,
        ):
            result = _dispatch_lever_5b_for_cluster(
                cluster=cluster,
                metadata_snapshot=metadata,
                w=None,
                benchmark_corpus=None,
                benchmarks=None,
                rca_evidence_typed=rca_evidence_typed,
                llm_cluster=llm_cluster,
                ag_id=fixture["source_ag_id_synthetic"],
                iteration=int(fixture["source_iteration"]),
                run_id=f"plan10_replay_{fixture['fixture_id']}",
            )

    # Assertion A — Leak 1 (silent gate closure on empty rca_evidence_typed).
    assert result, (
        f"{fixture['fixture_id']}: dispatch returned an empty list. "
        "Plan 10 Leak 1 still present — the gate at "
        "_dispatch_lever_5b_for_cluster (optimizer.py:~10443) still "
        "requires non-empty rca_evidence_typed, which production "
        "evidence proves is empty for every failing anchor in the "
        "ab65fefe (7now) and 59a173d3 (airline) zero-delta runs."
    )

    proposal_dict = result[0]
    repair_intent = proposal_dict.get("repair_intent") or {}
    target_objects = repair_intent.get("target_objects") or []

    # Assertion B — Leak 2 (synthesizer strips Plan 9 fields on
    # manual RepairProposal construction).
    assert target_objects, (
        f"{fixture['fixture_id']}: stamped repair_intent.target_objects "
        "is empty even though the LLM envelope emitted a populated "
        "target_objects list. Plan 10 Leak 2 still present — "
        "repair_intent_synthesizer.py:~346-356 constructs the "
        "RepairProposal manually (no target_objects= kwarg), so "
        "Plan 9 fields the LLM produced are dropped before "
        "to_repair_intent() reaches stamp_repair_intent_on_proposal()."
    )

    # Light sanity assertions — these only run after A and B pass.
    assert proposal_dict.get("intent_id"), (
        f"{fixture['fixture_id']}: proposal_dict missing intent_id stamp"
    )
    assert proposal_dict.get("example_sql"), (
        f"{fixture['fixture_id']}: proposal_dict missing example_sql "
        "from to_proposal_dict projection"
    )


# ─────────────────────────────────────────────────────────────────────
# Plan 11 — flag-ON anchor replay
#
# Same 4 anchors, but driven through the new diagnose → cluster →
# synthesize stages. The legacy fallback / archetype path is bypassed
# entirely; every LLM call is mocked at the per-stage import surface
# so no real model is hit.
#
# Acceptance for each anchor: Stage 3's :class:`ClusterSynthesisResult`
# carries a non-None ``proposal`` AND its ``skipped_reason`` is empty
# (success path, not a closed-vocabulary decline).
# ─────────────────────────────────────────────────────────────────────


_PLAN11_ANCHOR_CASES = (
    (
        "airline_gs_009_plural_top_n_collapse.json",
        "plural_top_n_collapse",
        "Replace RANK() with ROW_NUMBER() OVER (...) and add LIMIT 10",
        "add_example_sql",
    ),
    (
        "airline_gs_024_missing_filter.json",
        "missing_filter",
        "Remove the filter on PAYMENT_CURRENCY_CD = USD from the WHERE clause",
        "update_instruction_section",
    ),
    (
        "7now_gs_013_wrong_filter_condition.json",
        "wrong_filter_condition",
        "Fix the date filter to use the correct column reference",
        "add_example_sql",
    ),
    (
        "7now_gs_026_plural_top_n_collapse.json",
        "plural_top_n_collapse",
        "Use ROW_NUMBER() with LIMIT 10 instead of RANK()",
        "add_example_sql",
    ),
)


def _plan11_patch_body(patch_type: str) -> dict:
    """Build a minimal patch_body matching what each PatchType expects.

    The Plan 11 dispatcher's Stage 3 doesn't validate patch_body shape —
    that's deferred to validate_patch. These bodies are non-empty so
    a downstream call to validate_patch could plausibly succeed.
    """
    if patch_type == "add_example_sql":
        return {
            "example_question": "Replay anchor question?",
            "example_sql": "SELECT * FROM t ORDER BY x DESC LIMIT 10",
        }
    if patch_type == "update_instruction_section":
        return {
            "instruction_text": (
                "## Filter Conventions\n"
                "Do not filter on PAYMENT_CURRENCY_CD by default.\n"
            ),
        }
    return {}


def _plan11_stage_response(skill_id: str, payload: dict) -> LlmReasoningResponse:
    return LlmReasoningResponse(
        call_id=f"replay_call.{skill_id}",
        skill_id=skill_id,
        succeeded=True,
        parsed_output=payload,
        declined=None,
        raw_text=json.dumps({"result": payload, "declined": None}),
        tokens_input=200,
        tokens_output=100,
        duration_ms=1,
        error=None,
    )


@pytest.mark.parametrize(
    "fixture_name,expected_label,expected_hypothesis,expected_patch_type",
    _PLAN11_ANCHOR_CASES,
    ids=[name.replace(".json", "") for name, *_ in _PLAN11_ANCHOR_CASES],
)
def test_plan10_anchor_replay_with_plan11_flag_on(
    fixture_name: str,
    expected_label: str,
    expected_hypothesis: str,
    expected_patch_type: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Plan 11 flag-ON: each Phase C anchor produces a non-None
    :class:`ClusterSynthesisResult.proposal` via the new
    diagnose → cluster → synthesize stages.

    Mocks LlmReasoningCall at each stage's import surface so no real
    LLM is needed. The downstream pipeline (validation, application)
    is exercised separately in
    ``test_plan11_validation_pipeline.py``.
    """
    monkeypatch.setenv("GSO_PLAN11_LLM_FIRST", "true")

    fixture = _load_fixture(fixture_name)
    qid = str(fixture["failing_qid"])
    fixture_id = str(fixture["fixture_id"])

    # ── Stage 1 mock — diagnose_failing_qids
    diagnose_payload = {
        "diagnoses": [
            {
                "qid": qid,
                "rca_kind_label": expected_label,
                "observed_failure": "Generated SQL does not match ground truth",
                "generated_sql_issue": "Wrong aggregation / filter pattern",
                "expected_sql_shape": expected_hypothesis[:80],
                "blame_set": [],
                "evidence_summary": (
                    f"Judge said: {expected_hypothesis[:200]}"
                ),
                "confidence": "high",
            },
        ],
    }

    # ── Stage 2 mock — cluster_diagnoses
    cluster_payload = {
        "clusters": [
            {
                "semantic_theme": expected_label,
                "member_qids": [qid],
                "unifying_evidence": "single-qid replay cluster",
                "repair_hypothesis": expected_hypothesis,
                "primary_blame_set": [],
                "confidence": "high",
            },
        ],
    }

    # ── Stage 3 mock — synthesize one proposal
    # Plan 12 survival contract requires every proposal exiting Stage 3
    # to carry non-empty target_objects + blame_set + target_qids (see
    # repair_proposal_typed.validate_survival_contract). A real LLM
    # emits these; mirror the populated shape used by
    # ``_plan9_proposal_envelope`` so the anchor exercises survival
    # rather than tripping the missing-required-fields contract.
    synth_payload = {
        "proposals": [
            {
                "intent_name": f"Fix {expected_label}"[:80],
                "intent_description": expected_hypothesis[:100],
                "repair_hypothesis": expected_hypothesis,
                "patch_type": expected_patch_type,
                "rationale": "Addresses the diagnosed root cause",
                "confidence": "high",
                "patch_body": _plan11_patch_body(expected_patch_type),
                "blame_set": ["sales.fact_sales.revenue"],
                "target_objects": [
                    {
                        "asset_kind": "table",
                        "identifier": "sales.fact_sales",
                        "columns": ["revenue"],
                    },
                ],
                "target_qids": [qid],
            },
        ],
    }

    from genie_space_optimizer.optimization.stages import (
        cluster_plan11 as _stage2_mod,
        diagnose as _stage1_mod,
        synthesize as _stage3_mod,
    )

    def _stage_invoke(skill_id: str):
        return {
            "plan11_diagnose": lambda w, request: _plan11_stage_response(
                skill_id, diagnose_payload,
            ),
            "plan11_cluster": lambda w, request: _plan11_stage_response(
                skill_id, cluster_payload,
            ),
            "plan11_synthesize": lambda w, request: _plan11_stage_response(
                skill_id, synth_payload,
            ),
        }[skill_id]

    from unittest.mock import MagicMock as _MagicMock

    with patch.object(_stage1_mod, "LlmReasoningCall") as MockS1, \
         patch.object(_stage2_mod, "LlmReasoningCall") as MockS2, \
         patch.object(_stage3_mod, "LlmReasoningCall") as MockS3:
        MockS1.return_value.invoke = _stage_invoke("plan11_diagnose")
        MockS2.return_value.invoke = _stage_invoke("plan11_cluster")
        MockS3.return_value.invoke = _stage_invoke("plan11_synthesize")

        diagnoses = _stage1_mod.diagnose_failing_qids(
            failing_qids=[
                {
                    "qid": qid,
                    "question_text": "replay question",
                    "ground_truth_sql": "SELECT 1",
                    "generated_sql": "SELECT 2",
                    "judge_rationale": "wrong shape",
                    "blame_set_seed": [],
                },
            ],
            schema_columns=[],
            optimization_run_id=f"replay_{fixture_id}",
            iteration=1,
            w=_MagicMock(),
        )
        assert len(diagnoses) == 1, (
            f"{fixture_id}: Stage 1 dropped the diagnosis"
        )
        assert diagnoses[0].rca_kind_label == expected_label, (
            f"{fixture_id}: Stage 1 returned wrong rca_kind_label"
        )

        clusters = _stage2_mod.cluster_diagnoses(
            diagnoses=diagnoses,
            schema_columns=[],
            optimization_run_id=f"replay_{fixture_id}",
            iteration=1,
            namespace="hard",
            w=_MagicMock(),
        )
        assert len(clusters) == 1, (
            f"{fixture_id}: Stage 2 produced no clusters"
        )
        assert clusters[0].repair_hypothesis == expected_hypothesis, (
            f"{fixture_id}: Stage 2 returned wrong repair_hypothesis"
        )

        result = _stage3_mod.run_plan11_synthesis_for_single_cluster(
            cluster=clusters[0],
            schema_slice={},
            history=[],
            optimization_run_id=f"replay_{fixture_id}",
            iteration=1,
            ag_id="AG_H001",
            w=_MagicMock(),
        )

    assert result.skipped_reason == "", (
        f"{fixture_id}: Stage 3 declined unexpectedly — "
        f"skipped_reason={result.skipped_reason!r}"
    )
    assert result.proposal is not None, (
        f"{fixture_id}: Stage 3 produced no proposal"
    )
    assert result.proposal.get("patch_type") == expected_patch_type, (
        f"{fixture_id}: Stage 3 returned wrong patch_type "
        f"{result.proposal.get('patch_type')!r} (expected "
        f"{expected_patch_type!r})"
    )
