"""Phase 1.4 — compute prior_failure_count from reflection_buffer."""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.terminal_reason import TerminalReason
from genie_space_optimizer.optimization.terminal_signature import (
    EmittedPatchShape,
    build_terminal_signature,
)
from genie_space_optimizer.optimization.reflection_buffer_schema import (
    build_reflection_entry,
)
from genie_space_optimizer.optimization.prior_failure_count import (
    compute_prior_failure_count,
)


def _csig(cluster_id: str, qids: list[str]) -> tuple:
    """Canonical cluster_signature (computed independently of
    TerminalSignature, which no longer carries cluster_id)."""
    return ((str(cluster_id), tuple(sorted(qids))),)


def _entry(iteration: int, cluster_id: str, qid: str, accepted: bool,
           terminal_reason: "TerminalReason | None") -> dict:
    sig = None if terminal_reason is None else build_terminal_signature(
        root_cause="r",
        blame_set=(),
        lever_set=[5],
        target_qids=[qid],
        terminal_reason=terminal_reason,
    )
    return build_reflection_entry(
        iteration=iteration, ag_id=f"ag-{iteration}",
        rollback_class="content_regression" if not accepted else "accepted",
        accepted=accepted,
        terminal_signature=sig,
        cluster_signature=_csig(cluster_id, [qid]),
        emitted_patch_shape=EmittedPatchShape.STRUCTURAL,
        legacy_fields={"target_qids": [qid]},
    )


def test_empty_buffer_zero():
    assert compute_prior_failure_count(
        cluster_signature=_csig("c1", ["gs_001"]),
        reflection_buffer=[],
    ) == 0


def test_counts_only_matching_cluster_non_accepted():
    buffer = [
        _entry(1, "c1", "gs_001", False, TerminalReason.NO_APPLIED_PATCHES),
        _entry(2, "c1", "gs_001", False, TerminalReason.PROPOSAL_GENERATION_EMPTY),
        _entry(3, "c2", "gs_002", False, TerminalReason.NO_APPLIED_PATCHES),  # diff cluster
        _entry(4, "c1", "gs_001", True, None),  # accepted iter — terminal_reason=None
    ]
    assert compute_prior_failure_count(
        cluster_signature=_csig("c1", ["gs_001"]),
        reflection_buffer=buffer,
    ) == 2  # iter 1 + iter 2


def test_accepted_entries_excluded():
    # Accepted iterations have terminal_reason=None per spec Section 5
    # (AcceptanceTier — Plan B — owns the accepted vocabulary).
    buffer = [
        _entry(1, "c1", "gs_001", True, None),
        _entry(2, "c1", "gs_001", True, None),
    ]
    assert compute_prior_failure_count(
        cluster_signature=_csig("c1", ["gs_001"]),
        reflection_buffer=buffer,
    ) == 0


def test_cluster_signature_normalized_target_qid_order_insensitive():
    # _csig sorts QIDs deterministically.
    sig_canonical = _csig("c1", ["gs_002", "gs_001"])
    assert sig_canonical == (("c1", ("gs_001", "gs_002")),)
    buffer = [
        _entry(1, "c1", "gs_001", False, TerminalReason.NO_APPLIED_PATCHES),
        _entry(2, "c1", "gs_002", False, TerminalReason.PROPOSAL_GENERATION_EMPTY),
    ]
    # Buffer entries from our _entry() helper use only one qid each, so
    # they don't match (("c1", ("gs_001", "gs_002")),). This test
    # validates the helper compares cluster_signatures byte-stably:
    assert compute_prior_failure_count(
        cluster_signature=sig_canonical,
        reflection_buffer=buffer,
    ) == 0
    # But a buffer entry with the multi-qid signature MUST match:
    multi_sig = build_terminal_signature(
        root_cause="r", blame_set=(), lever_set=[5],
        target_qids=["gs_001", "gs_002"],
        terminal_reason=TerminalReason.NO_APPLIED_PATCHES,
    )
    multi_entry = build_reflection_entry(
        iteration=3, ag_id="ag-3", rollback_class="no_action",
        accepted=False, terminal_signature=multi_sig,
        cluster_signature=_csig("c1", ["gs_001", "gs_002"]),
        emitted_patch_shape=EmittedPatchShape.STRUCTURAL,
        legacy_fields={},
    )
    assert compute_prior_failure_count(
        cluster_signature=_csig("c1", ["gs_001", "gs_002"]),
        reflection_buffer=[multi_entry],
    ) == 1
