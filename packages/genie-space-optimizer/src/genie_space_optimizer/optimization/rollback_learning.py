"""Plan 7 — LLM-driven rollback-learning module.

Public surface (filled across Tasks 6-11):
  * ``hypothesize_next_attempts_for_iteration(ctx, ...)`` — iteration
    entry; one LLM call per rolled-back cluster. (Task 9)
  * ``hypothesize_rollback_for_cluster(...)`` — single-cluster driver.
    (Task 7)
  * ``stamp_hypotheses_on_metadata_snapshot(...)`` — writes
    ``_last_attempt_hypothesis_by_cluster`` side-channel. (Task 10)
  * ``apply_forbidden_signatures_to_rollback_fingerprints(...)`` —
    union helper that feeds the existing deterministic
    content-fingerprint dedup gate. (Task 11)

Private helpers (this task, Task 6):
  * ``_validate_revised_blame_set_subset_of_allowlist``
  * ``_validate_forbidden_signatures_subset_of_applied``
  * ``_validate_revised_patch_type_in_closed_enum``

Imports are kept minimal; the public entry's heavyweight imports
(LlmReasoningCall, skill loader) live in the per-cluster driver
which is exercised only when the GSO_PLAN7_ROLLBACK_LEARNING flag is
on.
"""
from __future__ import annotations

import logging

from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
)
from genie_space_optimizer.optimization.rollback_hypothesis_typed import (
    NextAttemptHypothesis,
)

logger = logging.getLogger(__name__)


def _validate_revised_blame_set_subset_of_allowlist(
    hypothesis: NextAttemptHypothesis,
    *,
    identifier_allowlist: set[str],
) -> None:
    """Reject when any revised_blame_set entry is not in the AG's
    identifier allowlist.

    None blame_set is vacuously valid — the LLM chose not to revise.
    Case-sensitive — UC identifiers in Genie Spaces are case-sensitive."""
    if hypothesis.revised_blame_set is None:
        return
    unknown = [
        b for b in hypothesis.revised_blame_set
        if b not in identifier_allowlist
    ]
    if unknown:
        raise ValueError(
            f"hypothesis cluster={hypothesis.cluster_id!r}: "
            f"revised_blame_set entries outside identifier_allowlist: "
            f"{sorted(unknown)}"
        )


def _validate_forbidden_signatures_subset_of_applied(
    hypothesis: NextAttemptHypothesis,
    *,
    applied_patch_fingerprints: set[str],
) -> None:
    """Reject when any forbidden_signature is not in the AG's
    applied-patch fingerprint set.

    The LLM NOMINATES from existing fingerprints; it cannot invent.
    Empty forbidden_signatures is vacuously valid."""
    unknown = [
        s for s in hypothesis.forbidden_signatures
        if s not in applied_patch_fingerprints
    ]
    if unknown:
        raise ValueError(
            f"hypothesis cluster={hypothesis.cluster_id!r}: "
            f"forbidden_signatures entries outside "
            f"applied_patch_fingerprints: {sorted(unknown)}"
        )


def _validate_revised_patch_type_in_closed_enum(
    hypothesis: NextAttemptHypothesis,
) -> None:
    """Runtime double-check that revised_patch_type is a closed-enum
    member. Pydantic already enforces this at parse time; the runtime
    check pins the invariant for callers that build a hypothesis
    dataclass directly (test fixtures, replay).

    None is vacuously valid."""
    if hypothesis.revised_patch_type is None:
        return
    if not isinstance(hypothesis.revised_patch_type, PatchType):
        raise ValueError(
            f"hypothesis cluster={hypothesis.cluster_id!r}: "
            f"revised_patch_type {hypothesis.revised_patch_type!r} "
            f"is not a PatchType enum member"
        )
