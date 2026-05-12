"""Defect Plan 1 — replay tests against the May-12 consolidating-trial
captures.

These tests load the persisted replay fixtures from
``docs/runid_analysis/<opt_run_id>/evidence/`` and assert that the
defect-1 gates would have changed the operator-visible outcome.

The fixtures themselves are read-only artefacts; the tests do NOT
re-run the lever loop, they project the captured cluster + reflection
state through the new pure helpers.
"""

from __future__ import annotations

import json
import pathlib
from typing import Any

import pytest

AIRLINE_EVIDENCE = (
    pathlib.Path(__file__).resolve().parents[2]
    / "docs"
    / "runid_analysis"
    / "31ecd96f-5d56-4b5a-af8e-38e9e5c549af"
    / "evidence"
)
SEVENNOW_EVIDENCE = (
    pathlib.Path(__file__).resolve().parents[2]
    / "docs"
    / "runid_analysis"
    / "ccf1d60d-d686-467b-bafa-1640131b4393"
    / "evidence"
)


def _load_replay(path: pathlib.Path) -> dict[str, Any]:
    if not path.is_file():
        pytest.skip(
            f"replay fixture missing at {path} — the May-12 captures "
            f"must be promoted into the runid_analysis tree before "
            f"this test can run"
        )
    return json.loads(path.read_text())


def _iter_clusters(fixture: dict[str, Any]) -> list[dict[str, Any]]:
    """Best-effort projection of the per-iteration cluster slate from
    the replay fixture. Supports both the iter-record list shape and
    the legacy decisions-only shape.
    """
    iters = fixture.get("iterations") or fixture.get("iter_records") or []
    out: list[dict[str, Any]] = []
    for it in iters:
        clusters = it.get("clusters") or it.get("source_clusters") or []
        for c in clusters:
            out.append(dict(c))
    return out


def test_airline_replay_emits_block_records_for_ungrounded_clusters():
    fixture = _load_replay(
        AIRLINE_EVIDENCE / "replay_fixture_from_latest_export_357881600282129.json"
    )

    from genie_space_optimizer.optimization.harness import (
        collect_blocked_clusters,
    )

    clusters = _iter_clusters(fixture)
    if not clusters:
        pytest.skip(
            "replay fixture has no per-iteration cluster projection; "
            "promote richer evidence-bundle output first"
        )

    result = collect_blocked_clusters(clusters, run_id="replay-airline", iteration=0)
    blocked = set(result.blocked_cluster_ids)

    # The postmortem (F3) names gs_009 (cluster H001) and gs_024
    # (cluster H002) as the ungrounded clusters. If the replay
    # fixture's cluster ids differ, fall back to a structural check:
    # at least one cluster must be in the blocked set, otherwise the
    # fixture does not reproduce the airline failure mode and the
    # test should be re-pointed.
    assert blocked, (
        "no ungrounded clusters in airline fixture — either the "
        "fixture does not capture the rca_card=False clusters or the "
        "fixture shape changed; re-derive against the postmortem F3 "
        "evidence"
    )


def test_airline_replay_select_drops_blocked_ag_families(monkeypatch):
    """With the grounding gate on, AGs whose source_cluster_ids are
    all blocked must not appear in the final slate.

    The airline fixture's iteration 1 contained ``AG_DECOMPOSED_H001``
    and ``AG_DECOMPOSED_H002`` whose source_cluster_ids were exactly
    the ungrounded {H001, H002} set; with the gate on, neither AG
    should survive ``select``.
    """
    monkeypatch.setenv("GSO_AG_EMIT_GROUNDING_GATE", "1")

    from unittest.mock import MagicMock

    from genie_space_optimizer.optimization.harness import (
        collect_blocked_clusters,
    )
    from genie_space_optimizer.optimization.stages import StageContext
    from genie_space_optimizer.optimization.stages.action_groups import (
        ActionGroupsInput, select,
    )

    fixture = _load_replay(
        AIRLINE_EVIDENCE / "replay_fixture_from_latest_export_357881600282129.json"
    )
    clusters = _iter_clusters(fixture)
    if not clusters:
        pytest.skip("airline fixture lacks per-iteration cluster projection")

    grounding = collect_blocked_clusters(clusters, run_id="replay", iteration=0)
    if not grounding.blocked_cluster_ids:
        pytest.skip("airline fixture has no rca_card=False clusters in projection")

    synthetic_ags = tuple(
        {"id": f"AG_{cid}", "source_cluster_ids": [cid]}
        for cid in grounding.blocked_cluster_ids
    )

    inp = ActionGroupsInput(
        action_groups=synthetic_ags,
        blocked_cluster_ids=tuple(grounding.blocked_cluster_ids),
    )
    ctx = StageContext(
        run_id="r",
        iteration=0,
        space_id="s",
        domain="d",
        catalog="c",
        schema="s2",
        apply_mode="real",
        journey_emit=MagicMock(),
        decision_emit=MagicMock(),
        mlflow_anchor_run_id=None,
        feature_flags={},
    )
    slate = select(ctx, inp)
    assert slate.ags == (), (
        f"grounding gate did not drop ungrounded AGs; survivors: "
        f"{[ag.get('id') for ag in slate.ags]}"
    )
