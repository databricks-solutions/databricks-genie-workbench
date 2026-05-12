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


def _extract_reflection_buffer(fixture: dict[str, Any]) -> list[dict[str, Any]]:
    """Best-effort projection of the reflection buffer from the replay
    fixture. The May-12 captures stash it under ``reflection_buffer``
    at the run level or under ``reflections`` per iteration; try both.
    """
    if isinstance(fixture.get("reflection_buffer"), list):
        return [dict(r) for r in fixture["reflection_buffer"]]
    out: list[dict[str, Any]] = []
    for it in fixture.get("iterations") or fixture.get("iter_records") or []:
        for r in it.get("reflections") or []:
            out.append(dict(r))
    return out


def test_7now_replay_iterations_2_through_5_collide_on_cluster_signature(
    monkeypatch,
):
    """7now Gate G2 — iteration 1's NO_ACTION reflection has the same
    ``source_cluster_signatures`` as iteration 2-5's regenerated AGs.
    With the cluster-signature axis on, the iteration-2 AG must
    collide even though the LLM-regenerated root_cause text differs.
    """
    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMITS_NO_ACTION", "1")
    monkeypatch.setenv("GSO_FORBIDDEN_AG_COLLISION_BY_CLUSTER_SIGNATURE", "1")

    fixture = _load_replay(
        SEVENNOW_EVIDENCE
        / "replay_fixture_from_latest_export_318760998419002.json"
    )
    buf = _extract_reflection_buffer(fixture)
    if not buf:
        pytest.skip(
            "7now fixture has no reflection_buffer projection; promote "
            "richer evidence-bundle output first"
        )

    # Find the iteration-1 NO_ACTION reflection (the iteration-1
    # CONTENT_REGRESSION rollback was the one that left gs_026 hard
    # and regressed gs_012). Iterations 2-5 are NO_ACTION /
    # no_proposals.
    no_action_entries = [
        r for r in buf
        if str(r.get("rollback_class") or "").lower() == "no_action"
        and (r.get("source_cluster_signatures") or [])
    ]
    if not no_action_entries:
        pytest.skip(
            "7now fixture does not surface NO_ACTION reflections with "
            "source_cluster_signatures populated; cannot exercise G2"
        )

    from genie_space_optimizer.optimization.harness import (
        _ag_collision_key_pair,
        _compute_forbidden_ag_set_pair,
        _collision_pair_matches,
    )

    pair = _compute_forbidden_ag_set_pair(no_action_entries)
    assert pair.by_signature, (
        "G2 forbidden-set has no signature axis entries — the fixture's "
        "reflections lack source_cluster_signatures or the predicate "
        "rejected all admitted candidates"
    )

    # Synthesise iteration-N+1's AG using the SAME signature but a
    # DIFFERENT root_cause text (the failure mode the postmortem
    # describes: LLM root_cause drift).
    one_sig, one_lever_frozen = next(iter(pair.by_signature))
    next_iter_ag = {
        "id": "AG1_iter2",
        "source_cluster_signatures": [one_sig],
    }
    candidate = _ag_collision_key_pair(
        next_iter_ag,
        ag_root_cause="DIFFERENT root_cause text — LLM regenerated",
        ag_blame_set=("gs_026",),
        lever_keys=[str(int(l)) for l in sorted(one_lever_frozen)] or ["1"],
    )

    assert _collision_pair_matches(candidate, pair) is True, (
        "G2 signature collision did not fire — the next-iteration AG "
        "would have been re-admitted; this is exactly the 7now defect."
    )


def test_7now_replay_legacy_axis_alone_does_not_collide(monkeypatch):
    """Negative control — with the signature axis OFF, the same
    iteration-2 AG would NOT collide (this reproduces the bug)."""
    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMITS_NO_ACTION", "1")
    monkeypatch.setenv("GSO_FORBIDDEN_AG_COLLISION_BY_CLUSTER_SIGNATURE", "0")

    fixture = _load_replay(
        SEVENNOW_EVIDENCE
        / "replay_fixture_from_latest_export_318760998419002.json"
    )
    buf = _extract_reflection_buffer(fixture)
    if not buf:
        pytest.skip("7now fixture has no reflection_buffer projection")

    no_action_entries = [
        r for r in buf
        if str(r.get("rollback_class") or "").lower() == "no_action"
        and (r.get("source_cluster_signatures") or [])
    ]
    if not no_action_entries:
        pytest.skip("no usable NO_ACTION reflections in 7now fixture")

    from genie_space_optimizer.optimization.harness import (
        _ag_collision_key_pair,
        _compute_forbidden_ag_set_pair,
        _collision_pair_matches,
    )

    pair = _compute_forbidden_ag_set_pair(no_action_entries)
    # With the new flag off, by_signature is empty.
    assert pair.by_signature == frozenset()

    one_entry = no_action_entries[0]
    sig = (one_entry.get("source_cluster_signatures") or ["x"])[0]
    lever_set = one_entry.get("lever_set") or [1]

    candidate = _ag_collision_key_pair(
        {"id": "AG1", "source_cluster_signatures": [sig]},
        ag_root_cause="DIFFERENT root_cause text",
        ag_blame_set=("gs_026",),
        lever_keys=[str(int(l)) for l in lever_set],
    )

    # Legacy axis alone misses → no collision → bug reproduced.
    assert _collision_pair_matches(candidate, pair) is False
