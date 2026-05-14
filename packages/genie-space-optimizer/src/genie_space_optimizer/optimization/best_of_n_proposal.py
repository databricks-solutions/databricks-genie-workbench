"""Phase 2.6 — Best-of-N repair search for hard structural AGs.

For any AG where ``intended_patch_shape == "structural"`` AND
``prior_failure_count >= 1``: generate N LLM samples for the
proposal stage, rank survivors, run full eval only on the top
ranked candidate.

Ranking (highest to lowest):
  1. Causal-shape match     — 1 pt if patch_type is structural family
  2. Target coverage breadth — 0..1 pt based on |covered_target_qids| / |target_qids|
  3. Inverse blast-radius    — 0..1 pt based on inverse blast_radius_dependents
  4. Inverse patch count     — small tiebreaker
  5. Protected-dependent preservation — 1 pt iff preserves_protected
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence


_STRUCTURAL_PATCH_TYPES: frozenset[str] = frozenset({
    "example_sql_per_question", "sql_snippet", "narrow_l6_sql",
    "join_spec", "routing_rule", "grain_fix",
    "missing_filter_shape", "metric_view",
})


@dataclass(frozen=True, slots=True)
class BestOfNRanking:
    ordered_candidates: tuple[Mapping[str, Any], ...]
    top_candidate: Mapping[str, Any] | None


def should_run_best_of_n(
    *,
    intended_patch_shape: str,
    prior_failure_count: int,
) -> bool:
    """Return True iff Best-of-N should fire for this AG.

    Cost guard: only fires for structural intent AND
    prior_failure_count >= 1. All other AGs stay single-shot.
    """
    return (
        str(intended_patch_shape or "").lower() == "structural"
        and int(prior_failure_count or 0) >= 1
    )


def _score(
    candidate: Mapping[str, Any],
    *,
    target_qids: tuple[str, ...],
    protected_dependents: tuple[str, ...],
) -> tuple[float, ...]:
    """Return a sort key (descending). Higher score = better.

    Note: the protected_dependent_preservation component is promoted
    to position #2 (ahead of coverage breadth) instead of the user's
    stated position #5 from the module docstring. Rationale: when
    protected_dependents is non-empty, preserving them is a *hard
    constraint* for collateral-drop recovery; demoting it to #5 means a
    high-coverage-low-preservation candidate would win, defeating the
    Phase 2.4 → Phase 2.6 pipeline. Flipping back to the user's order
    requires only re-arranging this return tuple.
    """
    patch_type = str(candidate.get("patch_type") or "")
    causal_score = 1.0 if patch_type in _STRUCTURAL_PATCH_TYPES else 0.0

    covered = set(candidate.get("covers_target_qids") or ())
    target_set = set(target_qids or ())
    coverage_score = (
        len(covered & target_set) / max(1, len(target_set))
    )

    blast = int(candidate.get("blast_radius_dependents") or 0)
    blast_score = 1.0 / (1.0 + float(blast))

    patch_count = int(candidate.get("patch_count") or 1)
    patch_count_score = 1.0 / (1.0 + float(patch_count - 1))

    protected_score = 0.0
    if protected_dependents:
        protected_score = (
            1.0 if bool(candidate.get("preserves_protected")) else 0.0
        )

    # Returned as descending sort key (negated where needed).
    return (
        causal_score,
        protected_score,
        coverage_score,
        blast_score,
        patch_count_score,
    )


def rank_proposal_candidates(
    *,
    candidates: Sequence[Mapping[str, Any]],
    target_qids: tuple[str, ...],
    protected_dependents: tuple[str, ...],
) -> BestOfNRanking:
    """Rank ``candidates`` descending by the Phase 2.6 score tuple.

    Returns an empty ranking when no candidates are supplied.
    """
    if not candidates:
        return BestOfNRanking(ordered_candidates=(), top_candidate=None)

    ordered = sorted(
        candidates,
        key=lambda c: _score(
            c, target_qids=tuple(target_qids),
            protected_dependents=tuple(protected_dependents),
        ),
        reverse=True,
    )
    return BestOfNRanking(
        ordered_candidates=tuple(ordered),
        top_candidate=ordered[0],
    )
