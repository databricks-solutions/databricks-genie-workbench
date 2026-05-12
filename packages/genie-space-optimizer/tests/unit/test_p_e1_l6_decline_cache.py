"""P-E1 — iteration-scoped Lever-6 decline cache."""
from __future__ import annotations


def test_lever6_force_llm_declined_record_defaults_to_live():
    """Backwards-compatible: callers that omit the new fields see
    cached=False and original_decline_iteration=None.
    """
    from genie_space_optimizer.optimization.decision_emitters import (
        lever6_force_llm_declined_record,
    )
    rec = lever6_force_llm_declined_record(
        run_id="r1",
        iteration=2,
        ag_id="AG_X",
        cluster_id="H004",
        root_cause="missing_filter",
        target_qids=("gs_024",),
    )
    d = rec.to_dict()
    assert d.get("metrics", {}).get("cached") is False
    assert d.get("metrics", {}).get("original_decline_iteration") is None


def test_lever6_force_llm_declined_record_carries_cache_provenance():
    """When cached=True, the record records the iteration of the
    original live decline.
    """
    from genie_space_optimizer.optimization.decision_emitters import (
        lever6_force_llm_declined_record,
    )
    rec = lever6_force_llm_declined_record(
        run_id="r1",
        iteration=2,
        ag_id="AG_X",
        cluster_id="H004",
        root_cause="missing_filter",
        target_qids=("gs_024",),
        cached=True,
        original_decline_iteration=2,
    )
    d = rec.to_dict()
    assert d["metrics"]["cached"] is True
    assert d["metrics"]["original_decline_iteration"] == 2


def test_lever6_force_llm_declined_record_emits_cluster_signature_evidence_ref():
    """P-E1 — when ``cluster_signature`` is supplied, the record's
    ``evidence_refs`` carries a ``signature:<cluster_signature>`` token so
    the I14 dedup invariant can group records by cluster signature without
    re-deriving it from the AG payload."""
    from genie_space_optimizer.optimization.decision_emitters import (
        lever6_force_llm_declined_record,
    )
    rec = lever6_force_llm_declined_record(
        run_id="r1",
        iteration=2,
        ag_id="AG_X",
        cluster_id="H004",
        root_cause="missing_filter",
        target_qids=("gs_024",),
        cluster_signature="sig_abc123",
    )
    refs = rec.to_dict().get("evidence_refs") or ()
    assert "signature:sig_abc123" in refs
    assert "ag:AG_X" in refs
    assert "cluster:H004" in refs


def test_lever6_force_llm_declined_record_omits_empty_signature_evidence_ref():
    """Empty ``cluster_signature`` (the default) must not emit a stray
    ``signature:`` token — evidence_refs stay byte-stable for legacy
    callers."""
    from genie_space_optimizer.optimization.decision_emitters import (
        lever6_force_llm_declined_record,
    )
    rec = lever6_force_llm_declined_record(
        run_id="r1",
        iteration=2,
        ag_id="AG_X",
        cluster_id="H004",
        root_cause="missing_filter",
        target_qids=("gs_024",),
    )
    refs = rec.to_dict().get("evidence_refs") or ()
    assert not any(str(r).startswith("signature:") for r in refs)


def test_cache_key_shape_uses_collision_pair_plus_snippet_type():
    """The cache key must be (CollisionPair, snippet_type) so that
    AG-selection forbidden-set matches and L6-cache hits use the
    SAME identity. Drift between the two would re-introduce the
    redundant-LLM-call symptom from 31ecd96f.
    """
    from genie_space_optimizer.optimization.harness import (
        _ag_collision_key_pair,
        _l6_decline_cache_key,
    )
    ag = {
        "id": "AG_X",
        "source_cluster_signatures": ["sig_A"],
    }
    pair = _ag_collision_key_pair(
        ag=ag,
        ag_root_cause="missing_filter",
        ag_blame_set=["t.col"],
        lever_keys=["6"],
    )
    key_filter = _l6_decline_cache_key(pair, snippet_type="filter")
    key_filter_dup = _l6_decline_cache_key(pair, snippet_type="filter")
    key_measure = _l6_decline_cache_key(pair, snippet_type="measure")
    key_none = _l6_decline_cache_key(pair, snippet_type=None)

    assert key_filter == key_filter_dup
    assert key_filter != key_measure
    assert key_none != key_filter
    # The key must be hashable so it can index the cache dict.
    {key_filter, key_measure, key_none}


def test_emit_force_l6_outcome_propagates_cached_to_record(monkeypatch):
    monkeypatch.setenv("GSO_LEVER6_FORCE_TYPED_OUTCOMES", "1")
    from genie_space_optimizer.optimization.harness import (
        _emit_force_l6_outcome,
    )
    iter_inputs = {"decision_records": [], "markers": []}
    _emit_force_l6_outcome(
        outcome="declined",
        run_id="r1", iteration=2, ag_id="AG_X",
        cluster_id="H004", root_cause="missing_filter",
        target_qids=("gs_024",),
        exception_repr="",
        iter_inputs=iter_inputs,
        cached=True,
        original_decline_iteration=2,
        cluster_signature="sig_abc123",
    )
    declined = [
        r for r in iter_inputs["decision_records"]
        if r["reason_code"] == "lever6_force_llm_declined"
    ]
    assert len(declined) == 1
    assert declined[0]["metrics"]["cached"] is True
    assert declined[0]["metrics"]["original_decline_iteration"] == 2
    assert "signature:sig_abc123" in (declined[0].get("evidence_refs") or ())


def test_emit_force_l6_outcome_default_cached_false(monkeypatch):
    monkeypatch.setenv("GSO_LEVER6_FORCE_TYPED_OUTCOMES", "1")
    from genie_space_optimizer.optimization.harness import (
        _emit_force_l6_outcome,
    )
    iter_inputs = {"decision_records": [], "markers": []}
    _emit_force_l6_outcome(
        outcome="declined",
        run_id="r1", iteration=2, ag_id="AG_X",
        cluster_id="H004", root_cause="missing_filter",
        target_qids=(), exception_repr="",
        iter_inputs=iter_inputs,
    )
    declined = [
        r for r in iter_inputs["decision_records"]
        if r["reason_code"] == "lever6_force_llm_declined"
    ]
    assert len(declined) == 1
    assert declined[0]["metrics"]["cached"] is False
    refs = declined[0].get("evidence_refs") or ()
    assert not any(str(r).startswith("signature:") for r in refs)


def test_force_l6_cache_hit_short_circuits_llm(monkeypatch):
    """Two consecutive same-iteration force-L6 attempts for the same
    (collision_pair, snippet_type) result in exactly ONE LLM call and
    the second attempt emits lever6_force_llm_declined{cached=True}.
    """
    monkeypatch.setenv("GSO_L6_DECLINE_CACHE", "1")
    monkeypatch.setenv("GSO_LEVER6_FORCE_TYPED_OUTCOMES", "1")
    from genie_space_optimizer.optimization.harness import (
        _ag_collision_key_pair,
        _l6_decline_cache_key,
        _maybe_force_lever6_with_cache,
    )

    iter_inputs = {"decision_records": [], "markers": []}
    decline_cache: dict[tuple, int] = {}
    llm_calls = {"n": 0}

    def fake_force_lever6(*args, **kwargs) -> dict | None:
        llm_calls["n"] += 1
        return None  # simulate LLM decline

    ag = {"id": "AG_X", "source_cluster_signatures": ["sig_A"]}
    pair = _ag_collision_key_pair(
        ag=ag, ag_root_cause="missing_filter",
        ag_blame_set=["t.col"], lever_keys=["6"],
    )

    # First attempt: LLM is called, decline is cached.
    result1 = _maybe_force_lever6_with_cache(
        run_id="r1", iteration=2, ag_id="AG_X",
        collision_pair=pair, snippet_type=None,
        decline_cache=decline_cache,
        iter_inputs=iter_inputs,
        force_l6_call=fake_force_lever6,
        cluster={"cluster_id": "H004", "root_cause": "missing_filter"},
        target_qids=("gs_024",),
        cluster_signature="sig_A",
    )
    assert result1 is None
    assert llm_calls["n"] == 1
    assert _l6_decline_cache_key(pair, snippet_type=None) in decline_cache

    # Second attempt in the same iteration: cache hit, no LLM call.
    result2 = _maybe_force_lever6_with_cache(
        run_id="r1", iteration=2, ag_id="AG_X",
        collision_pair=pair, snippet_type=None,
        decline_cache=decline_cache,
        iter_inputs=iter_inputs,
        force_l6_call=fake_force_lever6,
        cluster={"cluster_id": "H004", "root_cause": "missing_filter"},
        target_qids=("gs_024",),
        cluster_signature="sig_A",
    )
    assert result2 is None
    assert llm_calls["n"] == 1  # unchanged — cache hit
    declined = [
        r for r in iter_inputs["decision_records"]
        if r["reason_code"] == "lever6_force_llm_declined"
    ]
    # One live decline + one cached decline.
    assert len(declined) == 2
    assert any(r["metrics"]["cached"] is True for r in declined)
    assert any(r["metrics"]["cached"] is False for r in declined)
    # Both records must carry the cluster signature in evidence_refs so I14
    # can group them.
    assert all(
        "signature:sig_A" in (r.get("evidence_refs") or ())
        for r in declined
    )


def test_force_l6_cache_paranoia_guard_treats_stale_entry_as_miss(monkeypatch, caplog):
    """P-E1 paranoia guard — if a cache entry's recorded iteration does
    not match the current iteration, the helper logs a one-line warning
    and treats the entry as a miss (i.e. it calls the LLM and overwrites
    the entry).
    """
    monkeypatch.setenv("GSO_L6_DECLINE_CACHE", "1")
    monkeypatch.setenv("GSO_LEVER6_FORCE_TYPED_OUTCOMES", "1")
    import logging

    from genie_space_optimizer.optimization.harness import (
        _ag_collision_key_pair,
        _l6_decline_cache_key,
        _maybe_force_lever6_with_cache,
    )

    iter_inputs = {"decision_records": [], "markers": []}
    llm_calls = {"n": 0}

    def fake_force_lever6(*args, **kwargs) -> dict | None:
        llm_calls["n"] += 1
        return None

    ag = {"id": "AG_X", "source_cluster_signatures": ["sig_A"]}
    pair = _ag_collision_key_pair(
        ag=ag, ag_root_cause="missing_filter",
        ag_blame_set=["t.col"], lever_keys=["6"],
    )
    key = _l6_decline_cache_key(pair, snippet_type=None)
    # Stale entry from a "prior" iteration — the harness's iteration
    # reset failed to clear this. The guard must trip.
    decline_cache: dict[tuple, int] = {key: 1}

    caplog.set_level(logging.WARNING)
    result = _maybe_force_lever6_with_cache(
        run_id="r1", iteration=2, ag_id="AG_X",
        collision_pair=pair, snippet_type=None,
        decline_cache=decline_cache,
        iter_inputs=iter_inputs,
        force_l6_call=fake_force_lever6,
        cluster={"cluster_id": "H004", "root_cause": "missing_filter"},
        target_qids=(),
        cluster_signature="sig_A",
    )
    assert result is None
    assert llm_calls["n"] == 1, "stale entry must not short-circuit the LLM call"
    # The stale entry is overwritten with the current iteration.
    assert decline_cache[key] == 2
    # Exactly one decline record (live) — no cached emission.
    declined = [
        r for r in iter_inputs["decision_records"]
        if r["reason_code"] == "lever6_force_llm_declined"
    ]
    assert len(declined) == 1
    assert declined[0]["metrics"]["cached"] is False
    # The guard logged a single warning naming the stale iteration.
    assert any(
        "l6_decline_cache_stale_entry" in r.message for r in caplog.records
    )


def test_force_l6_cache_disabled_when_flag_off(monkeypatch):
    monkeypatch.setenv("GSO_L6_DECLINE_CACHE", "0")
    monkeypatch.setenv("GSO_LEVER6_FORCE_TYPED_OUTCOMES", "1")
    from genie_space_optimizer.optimization.harness import (
        _ag_collision_key_pair,
        _maybe_force_lever6_with_cache,
    )

    iter_inputs = {"decision_records": [], "markers": []}
    decline_cache: dict[tuple, int] = {}
    llm_calls = {"n": 0}

    def fake_force_lever6(*args, **kwargs) -> dict | None:
        llm_calls["n"] += 1
        return None

    ag = {"id": "AG_X", "source_cluster_signatures": ["sig_A"]}
    pair = _ag_collision_key_pair(
        ag=ag, ag_root_cause="missing_filter",
        ag_blame_set=["t.col"], lever_keys=["6"],
    )

    _maybe_force_lever6_with_cache(
        run_id="r1", iteration=2, ag_id="AG_X",
        collision_pair=pair, snippet_type=None,
        decline_cache=decline_cache,
        iter_inputs=iter_inputs,
        force_l6_call=fake_force_lever6,
        cluster={"cluster_id": "H004", "root_cause": "missing_filter"},
        target_qids=(),
        cluster_signature="sig_A",
    )
    _maybe_force_lever6_with_cache(
        run_id="r1", iteration=2, ag_id="AG_X",
        collision_pair=pair, snippet_type=None,
        decline_cache=decline_cache,
        iter_inputs=iter_inputs,
        force_l6_call=fake_force_lever6,
        cluster={"cluster_id": "H004", "root_cause": "missing_filter"},
        target_qids=(),
        cluster_signature="sig_A",
    )
    # With flag off, both calls hit the LLM.
    assert llm_calls["n"] == 2
    assert decline_cache == {}


def test_decision_trace_registers_new_reason_code():
    """P-E1 adds NARROW_SKIPPED_NO_ORIGINAL_PATCH_TYPE to the
    ReasonCode enum. Decision-trace consumers iterate the enum
    explicitly; if any consumer relies on a frozen enum size, this
    pin catches the drift.
    """
    from genie_space_optimizer.optimization.rca_decision_trace import (
        ReasonCode,
    )
    members = {rc.value for rc in ReasonCode}
    assert "narrow_skipped_no_original_patch_type" in members
    # Spot-check we did not accidentally remove neighbours.
    assert "narrow_not_applicable" in members
    assert "unrecognized_patch_type" not in members  # never was an enum
    assert "lever6_force_llm_declined" in members


def test_cache_key_is_stable_across_blame_set_orderings():
    """``_normalise_blame`` already canonicalises the blame_set inside
    ``_ag_collision_key_pair``. Pin that two equivalent AGs (same
    blame set, different list order) yield the SAME cache key.
    """
    from genie_space_optimizer.optimization.harness import (
        _ag_collision_key_pair,
        _l6_decline_cache_key,
    )
    ag = {"id": "AG_X", "source_cluster_signatures": ["sig_A"]}
    pair_a = _ag_collision_key_pair(
        ag=ag, ag_root_cause="missing_filter",
        ag_blame_set=["t.col_a", "t.col_b"], lever_keys=["6"],
    )
    pair_b = _ag_collision_key_pair(
        ag=ag, ag_root_cause="missing_filter",
        ag_blame_set=["t.col_b", "t.col_a"], lever_keys=["6"],
    )
    assert (
        _l6_decline_cache_key(pair_a, snippet_type=None)
        == _l6_decline_cache_key(pair_b, snippet_type=None)
    )
