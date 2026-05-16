"""Plan C2 — replay-pinned validation that the shape-overlap gate
eliminates false-positive collateral-drop flags.

Two assertions per fixture:
  - Flag OFF (today's behaviour): high_collateral_risk fires even
    though no benchmark references the snippet's column.
  - Flag ON (the fix): high_collateral_risk does NOT fire.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from genie_space_optimizer.optimization.harness import (
    _t24_counterfactual_scan,
)

_FIXTURE = (
    Path(__file__).parent / "fixtures" / "plan_c"
    / "c2_sql_shape_overlap_false_positive.json"
)


def _load() -> dict[str, Any]:
    with _FIXTURE.open() as fp:
        return json.load(fp)


def _run_scan(monkeypatch: Any, flag: str) -> dict[str, Any]:
    monkeypatch.setenv("GSO_SQL_SHAPE_OVERLAP_GATE", flag)
    fx = _load()
    proposal = dict(fx["proposals"][0])
    _t24_counterfactual_scan(
        all_proposals=[proposal],
        benchmarks=fx["benchmarks"],
        ag=fx["ag"],
        prev_failure_qids=set(fx["prev_failure_qids"]),
    )
    return proposal


def test_c2_replay_flag_off_emits_false_positive(monkeypatch: Any) -> None:
    """Pin today's buggy behaviour so a future deletion of the flag-off
    branch can't silently change it."""
    proposal = _run_scan(monkeypatch, "0")
    assert proposal.get("high_collateral_risk") is True, (
        "Pre-Plan-C2 baseline: table-only overlap fires "
        "high_collateral_risk for the FALSE POSITIVE case."
    )
    # All three benchmarks share the ``orders`` table.
    assert sorted(proposal.get("passing_dependents") or []) == [
        "gs_010", "gs_020", "gs_030",
    ]


def test_c2_replay_flag_on_eliminates_false_positive(monkeypatch: Any) -> None:
    """PLAN C2 GATE — the shape-overlap conjunction prevents the
    false-positive high-risk stamp."""
    proposal = _run_scan(monkeypatch, "1")
    assert proposal.get("high_collateral_risk", False) is False, (
        "Plan C2 gate ON must not flag high_collateral_risk when "
        "benchmarks share the target table but do NOT use the "
        "snippet's column ``is_refunded``."
    )
    assert proposal.get("passing_dependents") == [], (
        "All three benchmarks lack any shape-token overlap with the "
        "snippet; the dependent list must be empty."
    )


def test_c2_replay_flag_on_word_boundary_excludes_substring_match(
    monkeypatch: Any,
) -> None:
    """The third fixture benchmark contains the SUBSTRING ``status``
    inside ``order_status_history``. Word-boundary matching means a
    snippet's shape token ``status`` must not flag this benchmark.
    """
    monkeypatch.setenv("GSO_SQL_SHAPE_OVERLAP_GATE", "1")
    snippet = {
        "type": "add_sql_snippet_filter",
        "proposal_id": "P_STATUS",
        "target": "catalog.schema.orders",
        "column": "status",
        "sql": "status = 'shipped'",
    }
    fx = _load()
    # Use only the third benchmark — the one with the substring trap.
    third_benchmark = fx["benchmarks"][2]
    _t24_counterfactual_scan(
        all_proposals=[snippet],
        benchmarks=[third_benchmark],
        ag=fx["ag"],
        prev_failure_qids=set(fx["prev_failure_qids"]),
    )
    assert snippet.get("high_collateral_risk", False) is False
    assert snippet.get("passing_dependents") == []
