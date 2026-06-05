"""Trial 17.1 — pin the iteration-1 lever-selection bias in the Stage 3
prompt.

Production gap (live-LLM workbench sweep 2026-05-25T20:24:37Z, run
``live-llm-20260525T202437Z``): gs_009 reached the LLM with diagnosis
``RANK() instead of LIMIT 10 plus unrequested defensive NULL filters``
but Stage 3 returned ``patch_type=add_instruction`` (lever-5a, prose).
On iteration 1 there are no ``forbidden_signatures``, so the existing
"prefer structural lever after target_unchanged" guidance never fires.
The LLM defaulted to prose because the lever menu carried only the
allowed-patch-types allow-list — no semantic description and no
RCA-shape → lever preference signal.

Trial 17.1 fixes that by surfacing, in the prompt:
  * a ``description`` and ``prefer_when`` on EACH lever_menu entry,
    independent of iteration history;
  * an explicit "grammar-pivot diagnoses prefer lever-6 / lever-5b
    over lever-5a" bullet inside ``lever_contract_instructions``.

This test exercises ``synthesize._build_request`` directly so it does
NOT require a live LLM call.
"""
from __future__ import annotations

import json

from genie_space_optimizer.optimization.stages import synthesize as syn
from genie_space_optimizer.optimization.stages.plan11_types import (
    FailureCluster,
)


def _gs009_cluster() -> FailureCluster:
    """Mimic the cluster gs_009 lands in (top-N cardinality collapse)."""
    return FailureCluster(
        cluster_id="H001",
        semantic_theme="top_n_cardinality_collapse",
        member_qids=("gs_009",),
        unifying_evidence="rca: RANK() instead of LIMIT 10",
        repair_hypothesis="anchor top-N grammar via snippet or example_sql",
        primary_blame_set=("main.demo.orders.amount",),
        confidence="high",
    )


def _build_iteration1_request():
    return syn._build_request(
        cluster=_gs009_cluster(),
        schema_slice={},
        member_qid_evidence=[
            {"qid": "gs_009", "blame_set": ["main.demo.orders.amount"]},
        ],
        history=[],
        iteration=1,
        forbidden_signatures=(),  # iteration 1 has no signatures yet
    )


def _lever_menu_from_request(req) -> list[dict]:
    """Phase 0 P0.5 — ``lever_menu`` is now sent as a
    ``cacheable_user_blocks`` entry instead of inside the dynamic
    user_prompt payload. Extract it from there for assertions."""
    menu_block = next(
        b for b in req.cacheable_user_blocks if "lever_menu" in b
    )
    return json.loads(menu_block)["lever_menu"]


def _lever_contract_from_request(req) -> str:
    """Phase 0 P0.5 — ``lever_contract_instructions`` is now sent as a
    cacheable plaintext block. Strip the leading
    ``lever_contract_instructions:\\n`` header before returning."""
    block = next(
        b for b in req.cacheable_user_blocks
        if "lever_contract_instructions" in b
    )
    return block.split(":", 1)[1].lstrip("\n")


def test_lever_menu_entries_carry_description_and_prefer_when_on_iteration1():
    """Every lever entry must surface ``description`` and
    ``prefer_when`` so the iteration-1 LLM has semantic context for
    selection — not just the patch-type allow-list."""
    req = _build_iteration1_request()
    lever_menu = _lever_menu_from_request(req)
    assert isinstance(lever_menu, list)
    assert len(lever_menu) == 6
    for entry in lever_menu:
        assert isinstance(entry.get("description"), str)
        assert entry["description"], (
            f"{entry.get('id', '?')}: description must be non-empty so "
            "the iteration-1 LLM has a 'what this lever does' signal."
        )
        assert isinstance(entry.get("prefer_when"), list)
        assert entry["prefer_when"], (
            f"{entry.get('id', '?')}: prefer_when list must be non-empty "
            "so the LLM can match the diagnosis to a lever family."
        )


def test_lever_6_prefer_when_includes_rank_to_limit_token_on_iteration1():
    """gs_009's diagnosis names 'RANK() instead of LIMIT 10' — lever-6
    must surface the matching closed-vocab token so the LLM can route
    to it without needing a forbidden-signature pivot first."""
    req = _build_iteration1_request()
    lever_menu = _lever_menu_from_request(req)
    by_id = {e["id"]: e for e in lever_menu}
    lever6_prefers = set(by_id["lever-6"]["prefer_when"])
    assert "grammar_pivot:rank_to_limit" in lever6_prefers, (
        "lever-6 must carry the grammar_pivot:rank_to_limit hint so "
        "iteration-1 gs_009-style top-N pivots route here instead of "
        "defaulting to lever-5a prose."
    )


def test_lever_contract_instructions_call_out_grammar_pivot_bias():
    """The instructions block must explicitly warn against using
    lever-5a prose for grammar-shape diagnoses, regardless of whether
    forbidden_signatures already exist."""
    req = _build_iteration1_request()
    instructions = _lever_contract_from_request(req)
    assert "Grammar-pivot" in instructions or "grammar-pivot" in instructions, (
        "lever_contract_instructions must mention 'Grammar-pivot' "
        "diagnoses so the LLM can identify gs_009-style failures."
    )
    # The text should name BOTH the high-friction lever (5a / prose /
    # add_instruction) and the recommended alternatives (lever-6 OR
    # 5b / example_sql).
    assert "lever-6" in instructions
    assert "lever-5b" in instructions or "add_example_sql" in instructions
    assert "lever-5a" in instructions or "add_instruction" in instructions


def test_iteration1_prompt_does_not_require_forbidden_signatures():
    """Regression guard: the iteration-1 bias must NOT depend on the
    presence of forbidden_signatures. If the implementation ever gates
    the new instructions on a non-empty signature list, gs_009 will
    revert to picking prose on iteration 1."""
    req = _build_iteration1_request()
    payload = json.loads(req.user_prompt)
    assert payload["forbidden_signatures"] == [], (
        "Test setup invariant: iteration-1 path must run with empty "
        "forbidden_signatures so we exercise the iteration-1 bias."
    )
    # And yet the grammar-pivot bias is still in the prompt.
    instructions = _lever_contract_from_request(req)
    assert "grammar" in instructions.lower()
