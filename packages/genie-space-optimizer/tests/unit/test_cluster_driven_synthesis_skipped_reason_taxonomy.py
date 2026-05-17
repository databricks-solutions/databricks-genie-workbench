"""Phase 8.1 — every early-exit return in
``run_cluster_driven_synthesis_for_single_cluster`` must emit a
``ClusterSynthesisResult.skipped_reason`` whose prefix (up to the
first ``:``) is a member of the closed ``SkippedReason`` enum.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.cluster_driven_synthesis import (
    ClusterSynthesisResult,
    SkippedReason,
)


def test_enum_values_are_stable():
    """The values must not change without a postmortem-tooling impact
    review — they appear in GSO_NO_STRUCTURAL_CANDIDATE_V1 markers."""
    assert SkippedReason.SAFETY_CAP_REACHED.value == "safety_cap_reached"
    assert SkippedReason.BUDGET_EXHAUSTED.value == "budget_exhausted"
    assert SkippedReason.FORMAT_AFS_FAILED.value == "format_afs_failed"
    assert SkippedReason.VALIDATE_AFS_REJECTED.value == "validate_afs_rejected"
    assert SkippedReason.NO_ARCHETYPE_OR_SLICE.value == "no_archetype_or_slice"
    assert SkippedReason.NO_TOP_N_ARCHETYPE.value == "no_top_n_archetype"


def test_result_with_skipped_reason_accepts_enum_value():
    result = ClusterSynthesisResult(
        proposal=None,
        attempted_archetypes=("top_n_collapse_archetype",),
        skipped_reason=SkippedReason.NO_TOP_N_ARCHETYPE.value,
    )
    assert result.skipped_reason == "no_top_n_archetype"


def test_result_with_skipped_reason_rejects_unknown_string():
    """The post-init invariant enforces that any non-empty
    ``skipped_reason`` MUST be — or begin with — a member of
    :class:`SkippedReason`."""
    with pytest.raises(ValueError, match="invalid skipped_reason"):
        ClusterSynthesisResult(
            proposal=None,
            attempted_archetypes=(),
            skipped_reason="something_unknown",
        )


def test_result_with_empty_skipped_reason_is_allowed():
    """A successful result has ``proposal=<dict>`` and
    ``skipped_reason=''``."""
    result = ClusterSynthesisResult(
        proposal={"foo": "bar"},
        attempted_archetypes=("top_n_collapse_archetype",),
        skipped_reason="",
    )
    assert result.skipped_reason == ""


def test_dynamic_prefixed_values_pass_invariant():
    """The codebase carries detail past the colon: ``safety_cap:5>=5``,
    ``gate:foo:bar``, ``genie_agreement:reason``. The invariant
    accepts these because the prefix (split on ``:``) matches an
    enum member."""
    for raw in (
        "safety_cap:5>=5",
        "budget:3>=2",
        "gate:rowcount:mismatch",
        "genie_agreement:disagrees",
    ):
        # Should not raise.
        ClusterSynthesisResult(
            proposal=None,
            attempted_archetypes=(),
            skipped_reason=raw,
        )
