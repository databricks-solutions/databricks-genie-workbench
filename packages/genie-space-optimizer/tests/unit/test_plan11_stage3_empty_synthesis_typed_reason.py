"""Phase 7 (Trial 13) — Plan 11 Stage 3 ``empty_synthesis`` typed reason.

The dc89d1a9 run emitted 6 ``empty_synthesis`` markers with
``proposals_count=0``, ``proposal_ids=[]``, ``patch_types=[]``,
``target_qids_union=[]``. ~112k input tokens were spent across these
calls with no diagnostic signal — postmortems could not distinguish
"LLM produced 0 proposals because no archetype matched" from "all
candidates failed safety" from "prompt-constraint collision". Trial
13 makes the reason a required marker field and populates
``target_qids_union`` from the input cluster even when proposals is
empty.
"""
from __future__ import annotations

import json

import pytest


def test_marker_requires_synthesis_empty_reason_on_empty() -> None:
    """``outcome="empty_synthesis"`` without a typed
    ``synthesis_empty_reason`` MUST raise rather than silently emit."""
    from genie_space_optimizer.optimization.run_analysis_contract import (
        plan11_stage3_synthesis_marker,
    )

    with pytest.raises(ValueError, match="synthesis_empty_reason"):
        plan11_stage3_synthesis_marker(
            optimization_run_id="run_t13",
            iteration=1,
            ag_id="ag_001",
            cluster_id="cluster_x",
            outcome="empty_synthesis",
            proposals_count=0,
        )


def test_marker_accepts_typed_synthesis_empty_reason() -> None:
    """Typed reasons round-trip into the payload."""
    from genie_space_optimizer.optimization.run_analysis_contract import (
        plan11_stage3_synthesis_marker,
    )

    line = plan11_stage3_synthesis_marker(
        optimization_run_id="run_t13",
        iteration=1,
        ag_id="ag_001",
        cluster_id="cluster_x",
        outcome="empty_synthesis",
        proposals_count=0,
        target_qids_union=["gs_009", "gs_021"],
        synthesis_empty_reason="parse_returned_zero",
    )
    payload = json.loads(line.split(" ", 1)[1])
    assert payload["outcome"] == "empty_synthesis"
    assert payload["synthesis_empty_reason"] == "parse_returned_zero"
    assert payload["target_qids_union"] == ["gs_009", "gs_021"]


def test_marker_rejects_unknown_synthesis_empty_reason() -> None:
    """Closed vocabulary — caller must use one of the typed reasons."""
    from genie_space_optimizer.optimization.run_analysis_contract import (
        plan11_stage3_synthesis_marker,
    )

    with pytest.raises(ValueError, match="unknown synthesis_empty_reason"):
        plan11_stage3_synthesis_marker(
            optimization_run_id="run_t13",
            iteration=1,
            ag_id="ag_001",
            cluster_id="cluster_x",
            outcome="empty_synthesis",
            proposals_count=0,
            synthesis_empty_reason="made_up_reason",
        )


def test_marker_omits_synthesis_empty_reason_when_outcome_synthesized() -> None:
    """On the happy path, synthesis_empty_reason is empty (omitted or '')."""
    from genie_space_optimizer.optimization.run_analysis_contract import (
        plan11_stage3_synthesis_marker,
    )

    line = plan11_stage3_synthesis_marker(
        optimization_run_id="run_t13",
        iteration=1,
        ag_id="ag_001",
        cluster_id="cluster_x",
        outcome="synthesized",
        proposals_count=1,
        proposal_ids=["cluster_x_000"],
        patch_types=["add_sql_snippet_filter"],
        target_qids_union=["gs_009"],
    )
    payload = json.loads(line.split(" ", 1)[1])
    assert payload["outcome"] == "synthesized"
    assert payload.get("synthesis_empty_reason", "") == ""


def test_marker_synthesis_empty_reason_allowed_set() -> None:
    """All four documented reasons must be accepted.

    Trial 13e — ``all_candidates_unsafe`` now also requires the typed
    ``synthesis_rejected_patch_types`` map; we supply a minimal one
    here and reserve the full guardrail exercise for the dedicated
    test below.
    """
    from genie_space_optimizer.optimization.run_analysis_contract import (
        plan11_stage3_synthesis_marker,
        SYNTHESIS_EMPTY_REASONS,
    )

    assert SYNTHESIS_EMPTY_REASONS == frozenset({
        "no_applicable_archetype",
        "all_candidates_unsafe",
        "prompt_constraint_collision",
        "parse_returned_zero",
    })
    for reason in SYNTHESIS_EMPTY_REASONS:
        kwargs: dict = {
            "optimization_run_id": "r",
            "iteration": 0,
            "ag_id": "a",
            "cluster_id": "c",
            "outcome": "empty_synthesis",
            "proposals_count": 0,
            "synthesis_empty_reason": reason,
        }
        if reason == "all_candidates_unsafe":
            kwargs["synthesis_rejected_patch_types"] = {"add_nothing": 1}
        line = plan11_stage3_synthesis_marker(**kwargs)
        payload = json.loads(line.split(" ", 1)[1])
        assert payload["synthesis_empty_reason"] == reason


def test_marker_requires_rejected_patch_types_on_all_candidates_unsafe() -> None:
    """Trial 13e — ``all_candidates_unsafe`` MUST carry a non-empty
    ``synthesis_rejected_patch_types`` map. This is the permanent
    canary: a Stage 3 emission that says "every proposal was unsafe"
    without naming a single rejected raw is itself a contract defect.
    """
    from genie_space_optimizer.optimization.run_analysis_contract import (
        plan11_stage3_synthesis_marker,
    )

    with pytest.raises(ValueError, match="synthesis_rejected_patch_types"):
        plan11_stage3_synthesis_marker(
            optimization_run_id="run_t13e",
            iteration=1,
            ag_id="ag_001",
            cluster_id="cluster_x",
            outcome="empty_synthesis",
            proposals_count=0,
            synthesis_empty_reason="all_candidates_unsafe",
        )


def test_marker_round_trips_rejected_patch_types() -> None:
    """The map is preserved verbatim (string keys, int counts)."""
    from genie_space_optimizer.optimization.run_analysis_contract import (
        plan11_stage3_synthesis_marker,
    )

    line = plan11_stage3_synthesis_marker(
        optimization_run_id="run_t13e",
        iteration=1,
        ag_id="ag_001",
        cluster_id="cluster_x",
        outcome="empty_synthesis",
        proposals_count=0,
        synthesis_empty_reason="all_candidates_unsafe",
        synthesis_rejected_patch_types={
            "ADD_INSTRUCTION": 3,
            "ADD_EXAMPLE_SQL": 2,
            "ADD_COLUMN_DESCRIPTION": 1,
        },
    )
    payload = json.loads(line.split(" ", 1)[1])
    assert payload["synthesis_rejected_patch_types"] == {
        "ADD_INSTRUCTION": 3,
        "ADD_EXAMPLE_SQL": 2,
        "ADD_COLUMN_DESCRIPTION": 1,
    }


def test_marker_rejected_patch_types_defaults_to_empty_when_omitted() -> None:
    """On the happy path the field is an empty dict (additive, no
    breakage for old consumers)."""
    from genie_space_optimizer.optimization.run_analysis_contract import (
        plan11_stage3_synthesis_marker,
    )

    line = plan11_stage3_synthesis_marker(
        optimization_run_id="run_t13e",
        iteration=1,
        ag_id="ag_001",
        cluster_id="cluster_x",
        outcome="synthesized",
        proposals_count=1,
        proposal_ids=["cluster_x_000"],
        patch_types=["add_instruction"],
        target_qids_union=["gs_009"],
    )
    payload = json.loads(line.split(" ", 1)[1])
    assert payload["synthesis_rejected_patch_types"] == {}
