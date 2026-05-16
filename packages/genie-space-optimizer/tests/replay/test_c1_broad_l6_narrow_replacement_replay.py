"""Plan C1 — replay-pinned validation that broad-L6 collateral drops
actually invoke the narrow-replacement synthesizer.

Pre-fix: ``_BROAD_L6_TYPES`` contains placeholder names that never
match production patch types. ``try_narrow_replacement`` falls through
the type-dispatch branches at ``auto_narrow_replacement.py:66-83`` and
returns ``attempted=False``. The fixture pins this with a failing
assertion (``attempted is True`` AFTER the fix).

Post-fix: ``_BROAD_L6_TYPES`` includes the three real production
patch type names. The L6 narrow-replacement synthesizer is invoked
with ``protected_dependents`` threaded through from
``outside_target_qids``.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from genie_space_optimizer.optimization.auto_narrow_replacement import (
    try_narrow_replacement,
)

_FIXTURE = (
    Path(__file__).parent / "fixtures" / "plan_c"
    / "c1_broad_l6_collateral_drop.json"
)


def _load() -> dict[str, Any]:
    with _FIXTURE.open() as fp:
        return json.load(fp)


def test_c1_replay_broad_l6_collateral_drop_invokes_narrow_replacement() -> None:
    """PLAN C1 GATE — narrow-replacement IS attempted when an
    ``add_sql_snippet_filter`` patch is collateral-dropped.
    """
    fx = _load()

    l6_calls: list[dict[str, Any]] = []
    l5_calls: list[dict[str, Any]] = []

    def fake_l6(**kwargs: Any) -> dict[str, Any]:
        l6_calls.append(kwargs)
        return {
            "patch_id": "narrow_l6_repl",
            "patch_type": "add_sql_snippet_filter",
            "target": kwargs.get("original_dropped_patch", {}).get("target"),
            "protected_dependents": kwargs.get("protected_dependents"),
        }

    def fake_l5(**kwargs: Any) -> None:
        l5_calls.append(kwargs)
        return None

    result = try_narrow_replacement(
        dropped_patches=fx["dropped_patches"],
        outside_target_qids=tuple(fx["outside_target_qids"]),
        cluster=fx["cluster"],
        rca_card=fx["rca_card"],
        synthesis_callable_l6=fake_l6,
        synthesis_callable_l5=fake_l5,
    )

    assert result.attempted is True, (
        "C1 backstop must fire: broad-L6 collateral drop should "
        "attempt narrow replacement. Pre-fix this assertion fails "
        "because _BROAD_L6_TYPES has placeholder names that never "
        "match production patch types."
    )
    assert result.replacement_patch is not None
    assert result.replacement_patch["patch_type"] == "add_sql_snippet_filter"
    assert len(l6_calls) == 1
    assert l6_calls[0]["protected_dependents"] == (
        "gs_005", "gs_007", "gs_012",
    )
    assert l5_calls == []


def test_c1_replay_all_three_real_l6_patch_types_route_to_l6_synthesis() -> None:
    """Each of the three production L6 patch types must dispatch to
    the L6 synthesizer, not L5 or the fall-through branch.
    """
    real_types = (
        "add_sql_snippet_expression",
        "add_sql_snippet_measure",
        "add_sql_snippet_filter",
    )

    for ptype in real_types:
        l6_calls: list[dict[str, Any]] = []

        def fake_l6(**kwargs: Any) -> dict[str, Any]:
            l6_calls.append(kwargs)
            return {"patch_id": "r", "patch_type": ptype}

        result = try_narrow_replacement(
            dropped_patches=[{
                "patch_id": "p_1",
                "patch_type": ptype,
                "drop_reason": "high_collateral_risk_flagged",
            }],
            outside_target_qids=("gs_001",),
            cluster={"cluster_id": "c"},
            rca_card={"root_cause": "r"},
            synthesis_callable_l6=fake_l6,
            synthesis_callable_l5=lambda **_: None,
        )
        assert result.attempted is True, (
            f"C1 backstop must fire for production patch_type={ptype}"
        )
        assert len(l6_calls) == 1, (
            f"L6 synthesizer must be invoked once for patch_type={ptype}"
        )
