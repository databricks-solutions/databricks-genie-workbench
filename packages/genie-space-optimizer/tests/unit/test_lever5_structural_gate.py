"""Regression tests for the Phase A3 Lever 5 structural gate.

A3a covers the strategist path ``generate_proposals_from_strategy`` —
when an AG's cluster has a SQL-shape root cause but no example_sql is
attached, the Lever 5 block drops the instruction-only proposal and
bumps the ``lever5_text_only_blocked`` counter.

A3b covers the resolver path ``_resolve_lever5_llm_result`` — when the
LLM returns ``text_instruction`` for a SQL-shape cluster, the resolver
returns the ``skipped_no_example_sql`` sentinel instead of a weak
text instruction. ``test_leakage_firewall.py`` carries the Bug #4
parity check and is deliberately updated to reflect the new contract.
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.optimizer import (
    _BUG4_COUNTERS,
    _SQL_SHAPE_ROOT_CAUSES,
    _resolve_lever5_llm_result,
    generate_proposals_from_strategy,
    reset_bug4_counters,
)


# P1 pattern labels promoted into _SQL_SHAPE_ROOT_CAUSES. The structural
# gate (A3a/A3b) must treat each one as SQL-shape, blocking text-only
# proposals and demanding example_sql instead.
P1_PATTERN_LABELS = [
    "plural_top_n_collapse",
    "time_window_pivot",
    "value_format_mismatch",
    "column_disambiguation",
    "granularity_drop",
]


# ---------------------------------------------------------------------------
# A3b: resolver path
# ---------------------------------------------------------------------------


def test_resolver_returns_skipped_for_sql_shape_text_instruction() -> None:
    reset_bug4_counters()
    cluster = {"root_cause": "missing_filter", "sql_contexts": []}
    llm_result = {
        "instruction_type": "text_instruction",
        "instruction_text": "Always filter by active rows",
    }
    patch_type, extra = _resolve_lever5_llm_result(
        llm_result, original_patch_type="add_instruction", cluster=cluster,
    )
    assert patch_type == "skipped_no_example_sql"
    assert extra["root_cause"] == "missing_filter"
    assert extra["root_cause"] in _SQL_SHAPE_ROOT_CAUSES


def test_resolver_still_allows_example_sql_for_sql_shape() -> None:
    """The sentinel only fires for text_instruction. An example_sql response
    is the acceptable structural signal and should pass through normally.
    """
    reset_bug4_counters()
    cluster = {"root_cause": "missing_filter", "sql_contexts": []}
    llm_result = {
        "instruction_type": "example_sql",
        "example_question": "Active users last 7 days",
        "example_sql": "SELECT * FROM users WHERE is_active = true AND ts > NOW() - INTERVAL 7 DAY",
    }
    patch_type, extra = _resolve_lever5_llm_result(
        llm_result, original_patch_type="add_example_sql", cluster=cluster,
    )
    assert patch_type == "add_example_sql"
    assert "example_sql" in extra


def test_resolver_does_not_skip_for_non_sql_shape_root_cause() -> None:
    """Routing / instruction root causes (which aren't structural) must
    still accept text_instruction — Lever 5 is the right lever for those.
    """
    reset_bug4_counters()
    cluster = {"root_cause": "asset_routing_error", "sql_contexts": []}
    llm_result = {
        "instruction_type": "text_instruction",
        "instruction_text": "Route questions about orders to the metric view",
    }
    patch_type, extra = _resolve_lever5_llm_result(
        llm_result, original_patch_type="add_instruction", cluster=cluster,
    )
    assert patch_type == "add_instruction"
    assert "new_text" in extra


# ---------------------------------------------------------------------------
# A3a: strategist path
# ---------------------------------------------------------------------------


def _structural_ag() -> dict:
    return {
        "id": "AG_TEST",
        "root_cause_summary": "missing filter diagnosis",
        "affected_questions": ["q1", "q2"],
        "source_cluster_ids": ["C001"],
        "lever_directives": {
            "5": {
                "instruction_sections": {
                    "QUERY RULES": "Always include a WHERE is_active = true filter",
                },
            },
        },
    }


def _metadata_snapshot_with_cluster(root_cause: str) -> dict:
    return {
        "_failure_clusters": [
            {
                "cluster_id": "C001",
                "root_cause": root_cause,
                "asi_failure_type": root_cause,
                "asi_blame_set": [],
                "question_ids": ["q1", "q2"],
            },
        ],
        "data_sources": {"tables": [], "metric_views": []},
        "instructions": {},
        "tables": [],
        "metric_views": [],
    }


def test_strategist_blocks_instruction_only_for_sql_shape() -> None:
    """When the AG's cluster has a SQL-shape root cause and no example_sql,
    the Lever 5 strategist path must not emit a rewrite_instruction."""
    reset_bug4_counters()
    proposals = generate_proposals_from_strategy(
        strategy={},
        action_group=_structural_ag(),
        metadata_snapshot=_metadata_snapshot_with_cluster("missing_filter"),
        target_lever=5,
        apply_mode="genie_config",
    )
    rewrite = [p for p in proposals if p.get("patch_type") == "rewrite_instruction"]
    assert not rewrite, rewrite
    assert _BUG4_COUNTERS["lever5_text_only_blocked"] >= 1


def test_strategist_allows_instruction_for_non_sql_shape() -> None:
    """Routing AGs are free to use text instructions."""
    reset_bug4_counters()
    proposals = generate_proposals_from_strategy(
        strategy={},
        action_group=_structural_ag(),
        metadata_snapshot=_metadata_snapshot_with_cluster("asset_routing_error"),
        target_lever=5,
        apply_mode="genie_config",
    )
    # At least one proposal should be a rewrite_instruction (the AG has
    # instruction_sections non-empty).
    rewrite = [p for p in proposals if p.get("patch_type") == "rewrite_instruction"]
    assert rewrite, proposals
    assert _BUG4_COUNTERS["lever5_text_only_blocked"] == 0


def test_strategist_allows_instruction_when_example_sql_attached() -> None:
    """Adding an example_sql satisfies the structural-cause gate — the
    instruction proposal may proceed alongside the example_sql path."""
    reset_bug4_counters()
    ag = _structural_ag()
    ag["lever_directives"]["5"]["example_sqls"] = [
        {
            "question": "Active users last quarter",
            "sql": "SELECT COUNT(*) FROM users WHERE is_active = true",
        }
    ]
    proposals = generate_proposals_from_strategy(
        strategy={},
        action_group=ag,
        metadata_snapshot=_metadata_snapshot_with_cluster("missing_filter"),
        target_lever=5,
        apply_mode="genie_config",
    )
    # The instruction block should not be blocked when an example_sql is
    # present (even if downstream synthesis might reject it for other
    # reasons, the gate itself must not fire).
    assert _BUG4_COUNTERS["lever5_text_only_blocked"] == 0


# ---------------------------------------------------------------------------
# Lever 5 instruction_guidance branch — regression for UnboundLocalError
# ---------------------------------------------------------------------------


def _instruction_guidance_only_ag() -> dict:
    """AG with free-form ``instruction_guidance`` but no ``instruction_sections``.

    Reproduces the strategist output shape that crashed iter 4 of the
    airline-ticketing run (AG4 levers 2/5 against soft clusters): the
    strategist returned free-form text guidance and the Lever 5 directive
    therefore had no structured ``instruction_sections`` dict.
    """
    return {
        "id": "AG_GUIDANCE_ONLY",
        "root_cause_summary": "Genie generates non-deterministic SQL",
        "affected_questions": ["q1"],
        "source_cluster_ids": ["S001"],
        "lever_directives": {
            "5": {
                "instruction_guidance": (
                    "Always include a deterministic ORDER BY when emitting "
                    "results so repeated runs return identical row order."
                ),
            },
        },
    }


def test_strategist_lever5_instruction_guidance_only_does_not_crash() -> None:
    """Pin against UnboundLocalError on the instruction_guidance-only path.

    ``invoked_levers`` was computed only inside the
    ``if isinstance(instruction_sections, dict) and instruction_sections:``
    branch. Python treats the name as local for the entire function, so
    when the strategist emits only free-form ``instruction_guidance``
    (no structured sections), control falls into the sibling
    ``elif instruction_guidance:`` branch — which references
    ``invoked_levers`` and raises ``UnboundLocalError``.

    Use a non-SQL-shape root cause (``asset_routing_error``) so the A3a
    structural gate does not null out ``instruction_guidance`` before the
    elif fires; the regression must reproduce the bug, not the gate.
    """
    reset_bug4_counters()

    proposals = generate_proposals_from_strategy(
        strategy={},
        action_group=_instruction_guidance_only_ag(),
        metadata_snapshot=_metadata_snapshot_with_cluster("asset_routing_error"),
        target_lever=5,
        apply_mode="genie_config",
    )

    rewrite = [p for p in proposals if p.get("patch_type") == "rewrite_instruction"]
    assert rewrite, (
        "instruction_guidance-only AG must produce at least one "
        f"rewrite_instruction proposal; got {proposals!r}"
    )
    assert rewrite[0]["invoked_levers"] == [5], (
        f"invoked_levers must reflect AG lever_directives keys; "
        f"got {rewrite[0].get('invoked_levers')!r}"
    )


def test_strategist_lever5_invoked_levers_includes_co_invoked_levers() -> None:
    """When the AG declares directives for multiple levers, the Lever 5
    proposal payload must record every numeric lever id, not just lever 5.

    Pins the contract that section-ownership re-routing depends on: if
    Lever 4 co-owns a section that landed under Lever 5, the re-routing
    logic at optimizer.py needs ``invoked_levers`` to contain Lever 4's
    id so the section can be handed off.
    """
    reset_bug4_counters()

    ag = _instruction_guidance_only_ag()
    ag["lever_directives"]["4"] = {"kind": "join_specification"}

    proposals = generate_proposals_from_strategy(
        strategy={},
        action_group=ag,
        metadata_snapshot=_metadata_snapshot_with_cluster("asset_routing_error"),
        target_lever=5,
        apply_mode="genie_config",
    )

    rewrite = [p for p in proposals if p.get("patch_type") == "rewrite_instruction"]
    assert rewrite, proposals
    assert rewrite[0]["invoked_levers"] == [4, 5], (
        f"invoked_levers must include every numeric lever id from the AG "
        f"directives; got {rewrite[0].get('invoked_levers')!r}"
    )


# ---------------------------------------------------------------------------
# P1 pattern labels: gate must treat each as SQL-shape
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("root_cause", P1_PATTERN_LABELS)
def test_p1_pattern_labels_in_sql_shape_set(root_cause: str) -> None:
    """The five P1 pattern labels must all be in _SQL_SHAPE_ROOT_CAUSES so
    both gate paths (resolver A3b and strategist A3a) recognise them.
    """
    assert root_cause in _SQL_SHAPE_ROOT_CAUSES


@pytest.mark.parametrize("root_cause", P1_PATTERN_LABELS)
def test_resolver_skips_text_instruction_for_p1_pattern(root_cause: str) -> None:
    """A3b: when the LLM returns text_instruction for any P1 pattern
    cluster, the resolver must return the skipped_no_example_sql sentinel.
    """
    reset_bug4_counters()
    cluster = {"root_cause": root_cause, "sql_contexts": []}
    llm_result = {
        "instruction_type": "text_instruction",
        "instruction_text": "Always include the right shape",
    }
    patch_type, extra = _resolve_lever5_llm_result(
        llm_result, original_patch_type="add_instruction", cluster=cluster,
    )
    assert patch_type == "skipped_no_example_sql"
    assert extra["root_cause"] == root_cause
    assert extra["root_cause"] in _SQL_SHAPE_ROOT_CAUSES


@pytest.mark.parametrize("root_cause", P1_PATTERN_LABELS)
def test_strategist_blocks_instruction_only_for_p1_pattern(
    root_cause: str,
) -> None:
    """A3a: when the AG's cluster has a P1 pattern root cause and only an
    instruction directive (no example_sqls), the strategist path must
    drop the rewrite_instruction and bump the lever5_text_only_blocked
    counter.
    """
    reset_bug4_counters()
    proposals = generate_proposals_from_strategy(
        strategy={},
        action_group=_structural_ag(),
        metadata_snapshot=_metadata_snapshot_with_cluster(root_cause),
        target_lever=5,
        apply_mode="genie_config",
    )
    rewrite = [p for p in proposals if p.get("patch_type") == "rewrite_instruction"]
    assert not rewrite, rewrite
    assert _BUG4_COUNTERS["lever5_text_only_blocked"] >= 1
