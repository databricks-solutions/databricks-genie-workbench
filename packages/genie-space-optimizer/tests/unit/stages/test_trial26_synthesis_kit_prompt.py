"""Trial 26 W26.2 — the Stage 3 synthesis prompt's kit-at-source
mandate must be DERIVED from the active KIT_FOR_RCA map, not a
hard-coded list of RCA kinds.

Root cause this pins: W26.2 expanded the validator's kit map
(``action_groups._TRIAL26_KIT_FOR_RCA`` → ``wrong_aggregation`` /
``wrong_column``) so a single-lever proposal for those kinds is
hard-rejected as ``kit_for_rca_violation:rca=...:singleton``. But the
producer prompt only ever named the two original Trial 24 kinds, so the
LLM was never told to emit the companion kit for the W26.2 kinds —
every ``wrong_aggregation`` proposal died as a singleton and
``stage3_returned_none``, stranding the kit-at-source mechanism the
whole of Trial 26 exists to reach. The fix makes the prompt read the
same ``active_kit_for_rca_map()`` the validator reads, so producer and
gate can never drift again.

These tests are deterministic and exercise a NON-anchor synthetic
cluster (``H001`` / ``gs_013``), proving the fix generalises across the
entire kit-contract RCA family rather than any single anchor QID.
"""
from __future__ import annotations

import json

import pytest

from genie_space_optimizer.optimization.stages.action_groups import (
    KIT_FOR_RCA,
    active_kit_for_rca_map,
)
from genie_space_optimizer.optimization.stages.plan11_types import (
    FailureCluster,
)
from genie_space_optimizer.optimization.stages.synthesize import (
    _build_request,
)


def _make_cluster() -> FailureCluster:
    return FailureCluster(
        cluster_id="H001",
        semantic_theme="wrong_aggregation_or_filter",
        member_qids=("gs_013",),
        unifying_evidence="",
        repair_hypothesis="",
        primary_blame_set=(),
        confidence="high",
    )


def _kit_instructions() -> str:
    """Return the full Stage 3 prompt text the LLM sees.

    Phase 0 P0.5 splits the static lever menu + lever_contract_instructions
    into ``cacheable_user_blocks`` (byte-identical across iterations so the
    Anthropic prompt cache serves them at 0.1x). The kit-at-source mandate
    lives in those blocks, not the dynamic ``user_prompt`` JSON, so we
    join the raw block strings to inspect the mandate verbatim.
    """
    request = _build_request(
        cluster=_make_cluster(),
        schema_slice={},
        member_qid_evidence=[],
        history=[],
        iteration=1,
    )
    blocks = request.cacheable_user_blocks or ()
    parts: list[str] = []
    for block in blocks:
        if isinstance(block, str):
            parts.append(block)
        elif isinstance(block, dict):
            parts.append(str(block.get("text", "")) or json.dumps(block, default=str))
        else:
            parts.append(str(block))
    return "\n".join(parts)


@pytest.fixture(autouse=True)
def _flags_default_on(monkeypatch):
    """Trial 24 kit-at-source + Trial 26 W26.2 both default ON — the
    production anchor config. No env overrides unless a test sets them.
    """
    for var in (
        "GSO_TRIAL24_KIT_AT_SOURCE",
        "GSO_TRIAL26_KIT_GATE_REACHABLE",
        "GSO_TRIAL26_KIT_MAP_EXPANDED",
        "GSO_TRIAL26_RCA_KIND_CANONICAL_NORMALISE",
    ):
        monkeypatch.delenv(var, raising=False)
    yield


def test_kit_prompt_enumerates_w26_2_extension_kinds_when_flags_on():
    """With Trial 26 W26.2 ON, the kit-at-source mandate must name the
    newly-mapped RCA kinds AND their companion levers, so the LLM emits
    the kit on the first try instead of a doomed singleton.
    """
    text = _kit_instructions()
    # The W26.2 kinds must each appear as a kit-mandate BULLET (not just
    # incidentally in the archetype catalog's applicable_root_causes).
    assert "* wrong_aggregation —" in text, (
        "Stage 3 kit-at-source prompt omits the 'wrong_aggregation' kit "
        "mandate even though W26.2 added it to the validator's "
        "KIT_FOR_RCA map — the producer/validator desync that strands "
        "the kit at source."
    )
    assert "* wrong_column —" in text, (
        "Stage 3 kit-at-source prompt omits the 'wrong_column' kit "
        "mandate even though W26.2 added it to the validator's "
        "KIT_FOR_RCA map."
    )
    # Companion levers for wrong_aggregation == {lever-1, lever-6}.
    assert "lever-1" in text and "lever-6" in text, (
        "kit mandate must name the companion lever families so the LLM "
        f"knows which kit to emit; got:\n{text}"
    )
    # The original Trial 24 kind is still present (no regression).
    assert "* top_n_cardinality_collapse —" in text


def test_kit_prompt_omits_w26_2_kinds_when_map_expansion_off():
    """Surgical rollback: ``GSO_TRIAL26_KIT_MAP_EXPANDED=0`` shrinks the
    kit map back to Trial-24 coverage. The prompt must then drop the
    W26.2 kinds (byte-stable Trial-24 behaviour) while keeping the
    Trial-24 mandate — proving the enumeration tracks the active map.
    """
    import os

    os.environ["GSO_TRIAL26_KIT_MAP_EXPANDED"] = "0"
    try:
        text = _kit_instructions()
    finally:
        os.environ.pop("GSO_TRIAL26_KIT_MAP_EXPANDED", None)
    assert "* wrong_aggregation —" not in text, (
        "With W26.2 off the prompt must not mandate a kit for "
        "'wrong_aggregation' (the validator no longer demands one)."
    )
    assert "* wrong_column —" not in text
    # Trial 24 baseline kind still present.
    assert "* top_n_cardinality_collapse —" in text


def test_kit_prompt_enumeration_is_map_driven_not_hardcoded():
    """Generality guard: every RCA kind in the kit mandate must be an
    EXTENSION-map key (active map minus the base KIT_FOR_RCA), and every
    extension key must appear. This is the structural proof the prompt
    is derived from ``active_kit_for_rca_map()`` — any future map
    expansion is reflected with no further prompt edit, and no per-kind
    branch can creep in.
    """
    text = _kit_instructions()
    expected_extension_kinds = {
        rca for rca in active_kit_for_rca_map() if rca not in KIT_FOR_RCA
    }
    assert expected_extension_kinds, (
        "test precondition: at least one extension kit kind must be "
        "active under the default flags"
    )
    for rca in expected_extension_kinds:
        assert f"* {rca} —" in text, (
            f"extension kit kind {rca!r} is in active_kit_for_rca_map() "
            f"but absent from the producer prompt — map/prompt desync"
        )
