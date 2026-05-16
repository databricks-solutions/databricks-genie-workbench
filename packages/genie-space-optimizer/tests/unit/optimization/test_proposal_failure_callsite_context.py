"""Unit tests for ProposalFailureCallSiteContext — the typed bundle the
harness passes from each of the 5 proposal-failure emit sites into
``_emit_proposal_failure_decided`` so it can invoke
``_handle_proposal_failure_next_action`` without growing 8 new kwargs."""

from __future__ import annotations

from unittest.mock import MagicMock

from genie_space_optimizer.optimization.proposal_failure_callsite_context import (
    ProposalFailureCallSiteContext,
    noop_context,
)


def test_dataclass_is_frozen():
    ctx = noop_context()
    try:
        ctx.cluster = {"cluster_id": "other"}  # type: ignore[misc]
    except Exception as exc:
        assert "frozen" in str(exc).lower() or "cannot assign" in str(exc).lower()
        return
    raise AssertionError("ProposalFailureCallSiteContext must be frozen")


def test_noop_context_has_empty_cluster_and_no_cache():
    ctx = noop_context()
    assert ctx.cluster == {}
    assert ctx.findings == []
    assert ctx.evidence_snapshot == {}
    assert ctx.cache is None
    assert ctx.policy is None
    assert ctx.signatures_counter == {}
    assert ctx.metadata_snapshot == {}
    assert ctx.spark is None


def test_fields_round_trip_constructor():
    cluster = {"cluster_id": "C1"}
    cache = MagicMock()
    policy = MagicMock()
    signatures: dict[str, int] = {}
    ctx = ProposalFailureCallSiteContext(
        cluster=cluster,
        findings=[{"rca_id": "r-1"}],
        evidence_snapshot={"k": "v"},
        cache=cache,
        policy=policy,
        signatures_counter=signatures,
        metadata_snapshot={"meta": 1},
        spark=None,
    )
    assert ctx.cluster is cluster
    assert ctx.cache is cache
    assert ctx.policy is policy
    assert ctx.signatures_counter is signatures
    assert ctx.findings[0]["rca_id"] == "r-1"
