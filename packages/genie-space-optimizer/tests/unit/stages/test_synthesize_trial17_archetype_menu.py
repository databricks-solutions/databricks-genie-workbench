"""Trial 17 Step 7 — pin that the Stage 3 user prompt carries both the
closed lever menu AND the archetype catalog as menu context.

Step 7 deprioritises ``pick_archetype`` as a control-flow gate; the
archetype catalog now travels as menu context inside the Stage 3 LLM
prompt so the LLM can reason about it without code hard-rejecting
clusters that don't match a shipped archetype.

This test exercises ``synthesize._build_request`` directly so it does
NOT require a live LLM call.
"""
from __future__ import annotations

import json

from genie_space_optimizer.optimization.stages import synthesize as syn
from genie_space_optimizer.optimization.stages.plan11_types import (
    FailureCluster,
)


def _cluster() -> FailureCluster:
    return FailureCluster(
        cluster_id="H_X",
        semantic_theme="top_n_cardinality_collapse",
        member_qids=("gs_009",),
        unifying_evidence="x",
        repair_hypothesis="x",
        primary_blame_set=("main.demo.t.col",),
        confidence="high",
    )


def test_build_request_includes_archetype_catalog_menu():
    """The Stage 3 user_prompt JSON must carry ``archetype_catalog_menu``
    alongside ``lever_menu``. Each archetype entry must be a JSON dict
    with the documented keys.
    """
    req = syn._build_request(
        cluster=_cluster(),
        schema_slice={},
        member_qid_evidence=[
            {"qid": "gs_009", "blame_set": ["main.demo.t.col"]},
        ],
        history=[],
        iteration=2,
        forbidden_signatures=(),
    )
    # Phase 0 P0.5 — ``lever_menu`` and ``archetype_catalog_menu`` are
    # now sent as separate ``cacheable_user_blocks`` so the Anthropic
    # prompt cache can serve them at 0.1x cost after the first call.
    # The blocks remain visible to the LLM as part of the user message
    # — we just need to look in the cacheable blocks tuple instead of
    # in the dynamic user_prompt JSON payload.
    cacheable_blob = "\n".join(req.cacheable_user_blocks)
    assert "lever_menu" in cacheable_blob
    assert "archetype_catalog_menu" in cacheable_blob, (
        "Step 7 requires archetype catalog be passed as menu context"
    )
    # Extract the archetype menu JSON block and validate its contract.
    archetype_block_json = next(
        b for b in req.cacheable_user_blocks
        if "archetype_catalog_menu" in b
    )
    archetype_payload = json.loads(archetype_block_json)
    menu = archetype_payload["archetype_catalog_menu"]
    assert isinstance(menu, list)
    assert menu, "archetype catalog menu must not be empty"
    names = {e.get("name") for e in menu}
    assert "simple_enumerate" in names
    for entry in menu:
        assert {
            "name",
            "applicable_root_causes",
            "required_constructs",
            "patch_type",
        } <= set(entry.keys())
