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
