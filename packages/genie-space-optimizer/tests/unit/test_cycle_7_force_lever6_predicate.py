"""Cycle 7 T1 — predicate that decides when to force a Lever-6 proposal.

Closes the run-to-run variance on `gs_009 missing_filter` between
attempt 596465849524605 (no L6 emitted, terminal 95.8%) and attempt
993610879088298 (L6 emitted, terminal 100%).
"""
from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _isolate_flag(monkeypatch):
    monkeypatch.delenv("GSO_REQUIRE_LEVER6_FOR_SQL_SHAPE_RCA", raising=False)


def test_flag_default_off() -> None:
    from genie_space_optimizer.common.config import (
        require_lever6_for_sql_shape_rca_enabled,
    )
    assert require_lever6_for_sql_shape_rca_enabled() is False


def test_flag_on_when_env_true(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GSO_REQUIRE_LEVER6_FOR_SQL_SHAPE_RCA", "1")
    from genie_space_optimizer.common.config import (
        require_lever6_for_sql_shape_rca_enabled,
    )
    assert require_lever6_for_sql_shape_rca_enabled() is True


def test_predicate_false_when_flag_off(monkeypatch: pytest.MonkeyPatch) -> None:
    """All five conditions met but the flag is off => no force-emit.
    This is the byte-stability guarantee."""
    monkeypatch.delenv("GSO_REQUIRE_LEVER6_FOR_SQL_SHAPE_RCA", raising=False)
    from genie_space_optimizer.optimization.harness import (
        _should_force_lever6_proposal,
    )
    assert _should_force_lever6_proposal(
        cluster_root_cause="missing_filter",
        cluster_recommended_levers=(3, 5, 6),
        ag_target_qids=("airline_ticketing_and_fare_analysis_gs_009",),
        ag_proposals_so_far=[{"patch_type": "rewrite_instruction"}],
    ) is False


def test_predicate_false_when_root_cause_not_sql_shape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GSO_REQUIRE_LEVER6_FOR_SQL_SHAPE_RCA", "1")
    from genie_space_optimizer.optimization.harness import (
        _should_force_lever6_proposal,
    )
    assert _should_force_lever6_proposal(
        cluster_root_cause="evidence_gap",
        cluster_recommended_levers=(3, 5, 6),
        ag_target_qids=("gs_001",),
        ag_proposals_so_far=[{"patch_type": "rewrite_instruction"}],
    ) is False


def test_predicate_false_when_recommended_levers_lacks_6(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Per-question shape clusters (recommended_levers=(3,5)) MUST NOT
    be forced to L6 — that is Cycle 2 Task 4's whole point."""
    monkeypatch.setenv("GSO_REQUIRE_LEVER6_FOR_SQL_SHAPE_RCA", "1")
    from genie_space_optimizer.optimization.harness import (
        _should_force_lever6_proposal,
    )
    assert _should_force_lever6_proposal(
        cluster_root_cause="missing_filter",
        cluster_recommended_levers=(3, 5),
        ag_target_qids=("gs_009",),
        ag_proposals_so_far=[{"patch_type": "rewrite_instruction"}],
    ) is False


def test_predicate_false_when_lever6_already_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The retry exemplar 993610879088298 already had L6; we must not
    duplicate."""
    monkeypatch.setenv("GSO_REQUIRE_LEVER6_FOR_SQL_SHAPE_RCA", "1")
    from genie_space_optimizer.optimization.harness import (
        _should_force_lever6_proposal,
    )
    assert _should_force_lever6_proposal(
        cluster_root_cause="missing_filter",
        cluster_recommended_levers=(3, 5, 6),
        ag_target_qids=("gs_009",),
        ag_proposals_so_far=[
            {"patch_type": "add_join_spec"},
            {"patch_type": "add_sql_snippet_filter"},
        ],
    ) is False


def test_predicate_false_when_no_target_qids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An AG with no hard target qids (e.g. soft-pile diagnostic AG)
    is not the variance lane — leave it alone."""
    monkeypatch.setenv("GSO_REQUIRE_LEVER6_FOR_SQL_SHAPE_RCA", "1")
    from genie_space_optimizer.optimization.harness import (
        _should_force_lever6_proposal,
    )
    assert _should_force_lever6_proposal(
        cluster_root_cause="missing_filter",
        cluster_recommended_levers=(3, 5, 6),
        ag_target_qids=(),
        ag_proposals_so_far=[{"patch_type": "rewrite_instruction"}],
    ) is False


def test_predicate_true_when_all_conditions_met(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The N3 variance case: gs_009 missing_filter, AG2 has only a
    rewrite_instruction patch, no L6 yet — fire."""
    monkeypatch.setenv("GSO_REQUIRE_LEVER6_FOR_SQL_SHAPE_RCA", "1")
    from genie_space_optimizer.optimization.harness import (
        _should_force_lever6_proposal,
    )
    assert _should_force_lever6_proposal(
        cluster_root_cause="missing_filter",
        cluster_recommended_levers=(3, 5, 6),
        ag_target_qids=("airline_ticketing_and_fare_analysis_gs_009",),
        ag_proposals_so_far=[{"patch_type": "rewrite_instruction"}],
    ) is True


def test_predicate_true_when_proposals_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Empty proposal slate (e.g. the LLM emitted nothing) on a SQL-
    shape AG should still force an L6 attempt."""
    monkeypatch.setenv("GSO_REQUIRE_LEVER6_FOR_SQL_SHAPE_RCA", "1")
    from genie_space_optimizer.optimization.harness import (
        _should_force_lever6_proposal,
    )
    assert _should_force_lever6_proposal(
        cluster_root_cause="wrong_aggregation",
        cluster_recommended_levers=(3, 5, 6),
        ag_target_qids=("gs_024",),
        ag_proposals_so_far=[],
    ) is True


def test_predicate_recognizes_all_sql_snippet_variants(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`add_sql_snippet_measure` / `add_sql_snippet_filter` /
    `add_sql_snippet_expression` all count as L6-already-present."""
    monkeypatch.setenv("GSO_REQUIRE_LEVER6_FOR_SQL_SHAPE_RCA", "1")
    from genie_space_optimizer.optimization.harness import (
        _should_force_lever6_proposal,
    )
    for ptype in (
        "add_sql_snippet_measure",
        "add_sql_snippet_filter",
        "add_sql_snippet_expression",
    ):
        assert _should_force_lever6_proposal(
            cluster_root_cause="missing_filter",
            cluster_recommended_levers=(3, 5, 6),
            ag_target_qids=("gs_009",),
            ag_proposals_so_far=[{"patch_type": ptype}],
        ) is False, f"L6 patch_type {ptype} must short-circuit predicate"
