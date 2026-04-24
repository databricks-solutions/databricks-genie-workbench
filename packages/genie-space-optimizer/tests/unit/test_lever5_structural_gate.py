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

from genie_space_optimizer.optimization.optimizer import (
    _BUG4_COUNTERS,
    _SQL_SHAPE_ROOT_CAUSES,
    _resolve_lever5_llm_result,
    generate_proposals_from_strategy,
    reset_bug4_counters,
)


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
