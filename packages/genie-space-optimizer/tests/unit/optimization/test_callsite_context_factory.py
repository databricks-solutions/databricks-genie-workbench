"""Smoke tests for the harness-level factory that builds a
ProposalFailureCallSiteContext from iteration state."""

from __future__ import annotations

from genie_space_optimizer.optimization.proposal_failure_callsite_context import (
    ProposalFailureCallSiteContext,
)


def test_factory_returns_noop_when_cluster_id_missing():
    from genie_space_optimizer.optimization.harness import (
        _build_proposal_failure_callsite_context,
    )

    ctx = _build_proposal_failure_callsite_context(
        cluster_id="",
        source_clusters_by_id={"C1": {"cluster_id": "C1"}},
        rca_recovery_holder={},
        signatures_counter={},
        findings=[],
        metadata_snapshot={},
        spark=None,
    )
    assert isinstance(ctx, ProposalFailureCallSiteContext)
    assert ctx.cluster == {}
    assert ctx.cache is None
    assert ctx.policy is None


def test_factory_resolves_cluster_from_iter_lookup():
    from genie_space_optimizer.optimization.harness import (
        _build_proposal_failure_callsite_context,
    )
    cluster_dict = {"cluster_id": "C1", "question_ids": ["Q1"]}

    ctx = _build_proposal_failure_callsite_context(
        cluster_id="C1",
        source_clusters_by_id={"C1": cluster_dict},
        rca_recovery_holder={},
        signatures_counter={},
        findings=[],
        metadata_snapshot={"meta": 1},
        spark=None,
    )
    assert ctx.cluster is cluster_dict
    assert ctx.metadata_snapshot == {"meta": 1}


def test_factory_lazily_allocates_cache_and_policy_when_recovery_flag_on(monkeypatch):
    from genie_space_optimizer.optimization.harness import (
        _build_proposal_failure_callsite_context,
    )
    from genie_space_optimizer.optimization.rca_execution import (
        RcaRegenerationCache, RcaRegenerationPolicy,
    )

    monkeypatch.setenv("GSO_RCA_REGEN_RECOVERY_POLICY", "1")
    holder: dict = {}
    ctx = _build_proposal_failure_callsite_context(
        cluster_id="C1",
        source_clusters_by_id={"C1": {"cluster_id": "C1"}},
        rca_recovery_holder=holder,
        signatures_counter={},
        findings=[],
        metadata_snapshot={},
        spark=None,
    )
    assert isinstance(ctx.cache, RcaRegenerationCache)
    assert isinstance(ctx.policy, RcaRegenerationPolicy)
    # The holder is mutated so subsequent factory calls reuse the
    # same instances.
    assert holder.get("rca_regen_cache") is ctx.cache
    assert holder.get("rca_regen_policy") is ctx.policy


def test_factory_returns_none_cache_when_recovery_flag_off(monkeypatch):
    from genie_space_optimizer.optimization.harness import (
        _build_proposal_failure_callsite_context,
    )

    # The recovery-policy flag is default-on (Plan P-D, 2026-05-12);
    # set it to "0" to exercise the explicit-rollback branch.
    monkeypatch.setenv("GSO_RCA_REGEN_RECOVERY_POLICY", "0")
    ctx = _build_proposal_failure_callsite_context(
        cluster_id="C1",
        source_clusters_by_id={"C1": {"cluster_id": "C1"}},
        rca_recovery_holder={},
        signatures_counter={},
        findings=[],
        metadata_snapshot={},
        spark=None,
    )
    assert ctx.cache is None
    assert ctx.policy is None
