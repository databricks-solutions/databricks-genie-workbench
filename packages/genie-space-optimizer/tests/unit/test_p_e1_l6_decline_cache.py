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
