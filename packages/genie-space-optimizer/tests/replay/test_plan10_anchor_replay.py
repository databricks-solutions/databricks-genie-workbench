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
