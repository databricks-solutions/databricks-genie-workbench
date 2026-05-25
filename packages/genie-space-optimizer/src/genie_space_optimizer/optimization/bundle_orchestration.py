"""Trial 17 Step 5 — multi-lever proposal bundles.

The Stage 3 LLM may emit multiple ``RepairProposal`` instances that
share a ``bundle_id``. The bundle contract is:

1. Apply each proposal in the bundle sequentially via the existing
   applier → evaluated → acceptance cycle.
2. Sliced eval runs **between** each apply.
3. If a mid-bundle patch returns ``target_unchanged``, terminate the
   bundle early — prior accepted patches stay applied.
4. The bundle is accepted iff the **last applied** patch's
   ``post_apply_score`` improved over the bundle's
   ``initial_pre_apply_score`` (the score before ANY bundle patch was
   applied).

Proposals with ``bundle_id == ""`` are single-proposal as today (no
behavior change for the legacy path).

This module contains the pure orchestration / grouping primitives.
The state-machine transformers (``applier_gate``, ``evaluated_gate``,
``acceptance_gate``) consume the typed ``BundleStep`` carrier inside
the harness loop. The harness wiring lives behind the
``GSO_TRIAL17_BUNDLES`` flag (default ON; opt out via
``GSO_TRIAL17_BUNDLES=0`` for emergency rollback).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional, Sequence

from genie_space_optimizer.optimization.repair_proposal_typed import (
    RepairProposal,
)


# ── Flag ─────────────────────────────────────────────────────────────


# Canonical impl lives in ``trial17_flags``; this re-export preserves
# the public ``bundle_orchestration.trial17_bundles_enabled`` symbol so
# existing imports (workbench tests, harness) keep working.
from genie_space_optimizer.optimization.trial17_flags import (
    trial17_bundles_enabled,
)


# ── Typed bundle carrier ─────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class BundleStep:
    """One step in a bundle's apply → eval → accept cycle.

    ``order`` is the 0-based position in the bundle (the LLM-declared
    order is preserved; bundles emitted in a particular order reflect
    the LLM's deliberate sequencing of patches that build on each
    other).
    """

    bundle_id: str
    order: int
    proposal: RepairProposal


@dataclass(frozen=True, slots=True)
class Bundle:
    """A group of proposals sharing the same non-empty ``bundle_id``.

    ``steps`` is sorted by emission order. ``initial_pre_apply_score``
    is captured by the orchestrator before the FIRST step is applied
    and is the comparator used for the bundle's final acceptance
    verdict.
    """

    bundle_id: str
    steps: tuple[BundleStep, ...]

    @property
    def size(self) -> int:
        return len(self.steps)

    def last_step(self) -> BundleStep:
        if not self.steps:
            raise ValueError(
                f"bundle {self.bundle_id!r} has no steps"
            )
        return self.steps[-1]


# ── Grouping ─────────────────────────────────────────────────────────


def group_proposals_by_bundle(
    proposals: Sequence[RepairProposal],
) -> tuple[list[Bundle], list[RepairProposal]]:
    """Partition proposals into bundles and singletons.

    Proposals carrying a non-empty ``bundle_id`` are grouped into
    :class:`Bundle` instances preserving emission order. Proposals
    with empty ``bundle_id`` are returned as-is in the singletons list
    (the legacy single-proposal path).

    The grouping is stable: the first occurrence of a ``bundle_id``
    defines the bundle's position in the returned list.
    """
    bundles_by_id: dict[str, list[BundleStep]] = {}
    bundle_order: list[str] = []
    singletons: list[RepairProposal] = []

    for proposal in proposals:
        bundle_id = (proposal.bundle_id or "").strip()
        if not bundle_id:
            singletons.append(proposal)
            continue
        if bundle_id not in bundles_by_id:
            bundles_by_id[bundle_id] = []
            bundle_order.append(bundle_id)
        steps = bundles_by_id[bundle_id]
        steps.append(
            BundleStep(
                bundle_id=bundle_id,
                order=len(steps),
                proposal=proposal,
            )
        )

    bundles = [
        Bundle(bundle_id=bid, steps=tuple(bundles_by_id[bid]))
        for bid in bundle_order
    ]
    return bundles, singletons


# ── Acceptance contract ──────────────────────────────────────────────


def bundle_accepted(
    *,
    initial_pre_apply_score: float,
    last_post_apply_score: float,
    epsilon: float = 0.0,
) -> bool:
    """Trial 17 Step 5 bundle acceptance rule.

    A bundle is accepted iff the **last applied** patch's
    ``post_apply_score`` is strictly greater than the bundle's
    ``initial_pre_apply_score`` by at least ``epsilon``. Equal scores
    are rejected (consistent with the single-proposal contract that
    ``target_unchanged = post <= pre``).
    """
    return last_post_apply_score > initial_pre_apply_score + epsilon


# ── Early-termination contract ───────────────────────────────────────


def should_terminate_bundle_early(
    *,
    step_target_unchanged: bool,
    step_apply_failed: bool,
) -> bool:
    """Trial 17 Step 5 — mid-bundle abort rule.

    The bundle terminates early when the current step either:

    - apply itself failed (``applier_gate`` rejected before sliced
      eval), or
    - sliced eval shows ``target_unchanged``.

    Prior accepted steps stay applied (already mutated state); the
    bundle's final acceptance verdict is computed against the last
    *successfully applied* step's score.
    """
    return bool(step_target_unchanged) or bool(step_apply_failed)


# ── Iteration helper ─────────────────────────────────────────────────


def iter_bundle_steps(
    bundle: Bundle,
) -> Iterable[BundleStep]:
    """Yield steps in declared order. Wrapper for clarity at call sites."""
    yield from bundle.steps


__all__ = [
    "Bundle",
    "BundleStep",
    "bundle_accepted",
    "group_proposals_by_bundle",
    "iter_bundle_steps",
    "should_terminate_bundle_early",
    "trial17_bundles_enabled",
]
