"""Cycle 7 T3 — wire the predicate at the post-AG-proposal-aggregation
site. Flag-off path is byte-identical to HEAD; flag-on path appends one
forced L6 candidate per qualifying AG and emits one record + one marker.
"""
from __future__ import annotations

import io
from contextlib import redirect_stdout

import pytest


def _make_proposal(patch_type: str, proposal_id: str = "P001") -> dict:
    return {
        "patch_type": patch_type,
        "proposal_id": proposal_id,
        "scope": "genie_config",
        "change_description": f"stub for {patch_type}",
    }


def _stub_l6_proposal() -> dict:
    return {
        "patch_type": "add_sql_snippet_filter",
        "snippet_type": "filter",
        "display_name": "fare_paid_status",
        "sql": "PAID = TRUE",
        "questions_fixed": 1,
    }


def test_force_lever6_helper_returns_none_when_predicate_false(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GSO_REQUIRE_LEVER6_FOR_SQL_SHAPE_RCA", raising=False)
    from genie_space_optimizer.optimization import harness as _harness

    out = _harness._force_lever6_proposal_for_ag(
        run_id="run-abc",
        iteration=2,
        ag_id="AG2",
        cluster={
            "cluster_id": "H001",
            "root_cause": "missing_filter",
            "recommended_levers": [3, 5, 6],
        },
        ag_target_qids=("gs_009",),
        ag_proposals_so_far=[_make_proposal("rewrite_instruction")],
        metadata_snapshot={},
        decision_emit=lambda rec: None,
        generate_lever6=lambda *_a, **_k: _stub_l6_proposal(),
    )
    assert out is None


def test_force_lever6_helper_emits_when_predicate_true(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GSO_REQUIRE_LEVER6_FOR_SQL_SHAPE_RCA", "1")
    from genie_space_optimizer.optimization import harness as _harness
    from genie_space_optimizer.optimization.rca_decision_trace import (
        ReasonCode,
    )

    emitted: list = []
    captured = io.StringIO()
    proposals: list[dict] = [_make_proposal("rewrite_instruction")]

    with redirect_stdout(captured):
        out = _harness._force_lever6_proposal_for_ag(
            run_id="run-abc",
            iteration=2,
            ag_id="AG2",
            cluster={
                "cluster_id": "H001",
                "root_cause": "missing_filter",
                "recommended_levers": [3, 5, 6],
                "rca_id": "RCA-1",
            },
            ag_target_qids=("airline_ticketing_and_fare_analysis_gs_009",),
            ag_proposals_so_far=proposals,
            metadata_snapshot={},
            decision_emit=emitted.append,
            generate_lever6=lambda *_a, **_k: _stub_l6_proposal(),
        )

    assert out is not None
    assert out["patch_type"] == "add_sql_snippet_filter"
    assert out.get("provenance", {}).get(
        "lever6_force_reason"
    ) == "lever6_forced_for_sql_shape_rca"
    assert len(emitted) == 1
    assert (
        emitted[0].reason_code
        is ReasonCode.LEVER6_FORCED_FOR_SQL_SHAPE_RCA
    )
    assert "GSO_LEVER6_FORCED_V1 " in captured.getvalue()


def test_force_lever6_helper_returns_none_when_generator_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If `_generate_lever6_proposal` returns None (LLM/validation fail),
    no record + no marker emit."""
    monkeypatch.setenv("GSO_REQUIRE_LEVER6_FOR_SQL_SHAPE_RCA", "1")
    from genie_space_optimizer.optimization import harness as _harness

    emitted: list = []
    captured = io.StringIO()

    with redirect_stdout(captured):
        out = _harness._force_lever6_proposal_for_ag(
            run_id="run-abc",
            iteration=2,
            ag_id="AG2",
            cluster={
                "cluster_id": "H001",
                "root_cause": "missing_filter",
                "recommended_levers": [3, 5, 6],
            },
            ag_target_qids=("gs_009",),
            ag_proposals_so_far=[_make_proposal("rewrite_instruction")],
            metadata_snapshot={},
            decision_emit=emitted.append,
            generate_lever6=lambda *_a, **_k: None,
        )

    assert out is None
    assert emitted == []
    assert "GSO_LEVER6_FORCED_V1" not in captured.getvalue()


def test_force_lever6_helper_skips_when_l6_already_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GSO_REQUIRE_LEVER6_FOR_SQL_SHAPE_RCA", "1")
    from genie_space_optimizer.optimization import harness as _harness

    emitted: list = []
    captured = io.StringIO()

    with redirect_stdout(captured):
        out = _harness._force_lever6_proposal_for_ag(
            run_id="run-abc",
            iteration=2,
            ag_id="AG2",
            cluster={
                "cluster_id": "H001",
                "root_cause": "missing_filter",
                "recommended_levers": [3, 5, 6],
            },
            ag_target_qids=("gs_009",),
            ag_proposals_so_far=[
                _make_proposal("add_join_spec"),
                _make_proposal("add_sql_snippet_filter", proposal_id="P002"),
            ],
            metadata_snapshot={},
            decision_emit=emitted.append,
            generate_lever6=lambda *_a, **_k: _stub_l6_proposal(),
        )

    assert out is None
    assert emitted == []
    assert "GSO_LEVER6_FORCED_V1" not in captured.getvalue()
