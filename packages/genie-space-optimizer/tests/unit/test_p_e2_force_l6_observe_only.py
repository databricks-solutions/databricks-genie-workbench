"""P-E2 — wire-up test for ``_maybe_force_lever6_with_cache``.

The wrapper landed by P-E1 already threads ``cluster_signature``
through to the decline record's evidence_refs. P-E2 adds the
observe-only check at the top of the wrapper (before any cache
probe or LLM call): if the AG's collision pair matches the
forbidden set, one observe-only record + marker is emitted, and
the LLM / cache path continues unchanged.
"""
from __future__ import annotations


def test_force_l6_emits_observe_only_when_collision_matches(monkeypatch, capsys):
    monkeypatch.setenv("GSO_PROPOSAL_STAGE_FORBIDDEN_AG_OBSERVED", "1")
    monkeypatch.setenv("GSO_L6_DECLINE_CACHE", "1")
    monkeypatch.setenv("GSO_LEVER6_FORCE_TYPED_OUTCOMES", "1")
    from genie_space_optimizer.optimization.harness import (
        _ag_collision_key_pair,
        _ForbiddenSetPair,
        _maybe_force_lever6_with_cache,
    )
    ag = {"id": "AG_X", "source_cluster_signatures": ["sig_A"]}
    pair = _ag_collision_key_pair(
        ag=ag, ag_root_cause="missing_filter",
        ag_blame_set=["t.col"], lever_keys=["6"],
    )
    forbidden = _ForbiddenSetPair(
        by_root_cause=frozenset(),
        by_signature=frozenset({("sig_A", frozenset([6]))}),
    )
    llm_calls = {"n": 0}

    def fake_force_lever6() -> dict | None:
        llm_calls["n"] += 1
        return None  # simulate decline

    iter_inputs = {"decision_records": [], "markers": []}
    result = _maybe_force_lever6_with_cache(
        run_id="r1", iteration=2, ag_id="AG_X",
        collision_pair=pair, snippet_type=None,
        decline_cache={},
        iter_inputs=iter_inputs,
        force_l6_call=fake_force_lever6,
        cluster={"cluster_id": "H004", "root_cause": "missing_filter"},
        target_qids=(),
        cluster_signature="sig_A",
        forbidden_pair=forbidden,
    )
    assert result is None
    # Observe-only check fires, AND the LLM call still happens.
    assert llm_calls["n"] == 1
    observe = [
        r for r in iter_inputs["decision_records"]
        if r["reason_code"] == "proposal_stage_forbidden_ag_observed"
    ]
    declined = [
        r for r in iter_inputs["decision_records"]
        if r["reason_code"] == "lever6_force_llm_declined"
    ]
    assert len(observe) == 1
    assert observe[0]["metrics"]["call_site"] == "force_lever6"
    assert observe[0]["metrics"]["match_axis"] == "cluster_signature"
    assert len(declined) == 1  # P-E1's live-decline emission still fires
    out = capsys.readouterr().out
    assert "GSO_PROPOSAL_STAGE_FORBIDDEN_AG_OBSERVED_V1" in out


def test_force_l6_silent_when_no_forbidden_pair_passed(monkeypatch):
    """Backwards-compatible: callers that omit ``forbidden_pair``
    (or pass ``None``) see no observe-only record. P-E1's behaviour
    is untouched."""
    monkeypatch.setenv("GSO_PROPOSAL_STAGE_FORBIDDEN_AG_OBSERVED", "1")
    monkeypatch.setenv("GSO_L6_DECLINE_CACHE", "1")
    monkeypatch.setenv("GSO_LEVER6_FORCE_TYPED_OUTCOMES", "1")
    from genie_space_optimizer.optimization.harness import (
        _ag_collision_key_pair,
        _maybe_force_lever6_with_cache,
    )
    ag = {"id": "AG_X", "source_cluster_signatures": ["sig_A"]}
    pair = _ag_collision_key_pair(
        ag=ag, ag_root_cause="missing_filter",
        ag_blame_set=["t.col"], lever_keys=["6"],
    )

    def fake_force_lever6() -> dict | None:
        return None

    iter_inputs = {"decision_records": [], "markers": []}
    _maybe_force_lever6_with_cache(
        run_id="r1", iteration=2, ag_id="AG_X",
        collision_pair=pair, snippet_type=None,
        decline_cache={},
        iter_inputs=iter_inputs,
        force_l6_call=fake_force_lever6,
        cluster={"cluster_id": "H004", "root_cause": "missing_filter"},
        target_qids=(),
        cluster_signature="sig_A",
        # forbidden_pair intentionally omitted
    )
    observe = [
        r for r in iter_inputs["decision_records"]
        if r["reason_code"] == "proposal_stage_forbidden_ag_observed"
    ]
    assert observe == []


def test_force_l6_silent_when_collision_does_not_match(monkeypatch):
    monkeypatch.setenv("GSO_PROPOSAL_STAGE_FORBIDDEN_AG_OBSERVED", "1")
    monkeypatch.setenv("GSO_L6_DECLINE_CACHE", "1")
    monkeypatch.setenv("GSO_LEVER6_FORCE_TYPED_OUTCOMES", "1")
    from genie_space_optimizer.optimization.harness import (
        _ag_collision_key_pair,
        _ForbiddenSetPair,
        _maybe_force_lever6_with_cache,
    )
    ag = {"id": "AG_X", "source_cluster_signatures": ["sig_A"]}
    pair = _ag_collision_key_pair(
        ag=ag, ag_root_cause="missing_filter",
        ag_blame_set=["t.col"], lever_keys=["6"],
    )
    # Forbidden set targets a different signature → no match.
    forbidden = _ForbiddenSetPair(
        by_root_cause=frozenset(),
        by_signature=frozenset({("sig_DIFFERENT", frozenset([6]))}),
    )

    def fake_force_lever6() -> dict | None:
        return None

    iter_inputs = {"decision_records": [], "markers": []}
    _maybe_force_lever6_with_cache(
        run_id="r1", iteration=2, ag_id="AG_X",
        collision_pair=pair, snippet_type=None,
        decline_cache={},
        iter_inputs=iter_inputs,
        force_l6_call=fake_force_lever6,
        cluster={"cluster_id": "H004", "root_cause": "missing_filter"},
        target_qids=(),
        cluster_signature="sig_A",
        forbidden_pair=forbidden,
    )
    observe = [
        r for r in iter_inputs["decision_records"]
        if r["reason_code"] == "proposal_stage_forbidden_ag_observed"
    ]
    assert observe == []
