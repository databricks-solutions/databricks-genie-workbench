"""Phase 3 Action 3.2 — typed near-miss reflections + AG-shape gate.

This module hosts:

  * ``AGShapeSignature`` — frozen dataclass identifying the (archetype,
    scope) shape of an AG attempt on a given target.
  * ``shapes_are_equal`` — pure equality on ``(repair_archetype,
    target_scope)`` only; ``primary_cluster_id`` and ``target_qids``
    are carried for postmortem grouping but ignored by the gate.
  * ``compute_ag_shape_signature`` — pulls ``repair_archetype`` from
    the per-cluster repair-kit lookup and ``target_scope`` from the
    deterministic deriver in ``optimization.target_scope``.
  * ``NearMissReflection`` — typed prompt payload for ``DIAGNOSTIC_HOLD``
    and ``NET_WIN_WITH_DEBT`` outcomes.
  * ``build_net_win_with_debt_reflection`` /
    ``build_diagnostic_hold_reflection`` — typed builders.
  * ``AGShapeAssertionResult`` + ``assert_ag_shape_differs_from_priors``
    — pure check for the harness pre-strategy gate.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, Optional

from genie_space_optimizer.optimization.target_scope import (
    TargetScope,
    derive_target_scope,
)


@dataclass(frozen=True)
class AGShapeSignature:
    """Phase 3 Action 3.2 — (repair_archetype, target_scope) tuple.

    ``primary_cluster_id`` and ``target_qids`` are carried for
    postmortem grouping but DO NOT participate in shape equality. Two
    AGs on different clusters with the same archetype + scope are
    shape-equal — that is the gate's contract: "the strategist must
    change repair_archetype OR target_scope on the next attempt
    against this target."
    """

    repair_archetype: str
    target_scope: TargetScope
    primary_cluster_id: str
    target_qids: tuple[str, ...]


def shapes_are_equal(a: AGShapeSignature, b: AGShapeSignature) -> bool:
    """Phase 3 Action 3.2 — gate equality.

    Two signatures are shape-equal iff they share both
    ``repair_archetype`` AND ``target_scope``. Cluster id and target
    qids are deliberately ignored — those identify the target, not
    the shape of the attempt against it.
    """
    return (
        a.repair_archetype == b.repair_archetype
        and a.target_scope == b.target_scope
    )


def compute_ag_shape_signature(
    ag: dict,
    clusters: Iterable[dict],
    repair_kit_lookup: Mapping[str, dict],
) -> AGShapeSignature:
    """Phase 3 Action 3.2 — pure-function shape computation.

    ``repair_archetype`` is read from the per-cluster repair kit:
    ``repair_kit_lookup[ag["primary_cluster_id"]]["repair_archetype"]``.
    Falls back to ``"unknown"`` when no kit is stamped.

    ``target_scope`` is delegated to ``derive_target_scope``.
    """
    primary = str(ag.get("primary_cluster_id") or "")
    kit = repair_kit_lookup.get(primary) or {}
    archetype = str(kit.get("repair_archetype") or "unknown")
    scope = derive_target_scope(ag, clusters)
    target_qids_raw = ag.get("target_qids") or ag.get("affected_questions") or []
    target_qids = tuple(sorted(str(q) for q in target_qids_raw if q))
    return AGShapeSignature(
        repair_archetype=archetype,
        target_scope=scope,
        primary_cluster_id=primary,
        target_qids=target_qids,
    )


@dataclass(frozen=True)
class NearMissReflection:
    """Phase 3 Action 3.2 — typed reflection payload for the strategist.

    Emitted for ``DIAGNOSTIC_HOLD`` and ``NET_WIN_WITH_DEBT`` outcomes.
    The strategist sees ``reflection_text`` rendered into the prompt;
    the harness pre-strategy gate uses ``required_next_iter_change``
    together with ``prior_ag_shape`` to refuse identical-shape repeats.
    """

    kind: str  # "net_win_with_debt" | "diagnostic_hold"
    iteration: int
    target_qids: tuple[str, ...]
    prior_ag_shape: AGShapeSignature
    prior_lever_directives: tuple[dict, ...]
    regression_debt: dict[str, list[str]]
    reflection_text: str
    required_next_iter_change: str  # "different_repair_archetype" | "different_target_scope" | "either"


def _format_lever_directives(directives: tuple[dict, ...]) -> str:
    if not directives:
        return "(none)"
    rendered = ", ".join(
        str(d.get("lever") or "unknown")
        for d in directives if isinstance(d, dict)
    )
    return rendered or "(none)"


def _format_debt(debt: dict[str, list[str]]) -> str:
    if not debt:
        return "(none)"
    parts = []
    for bucket in sorted(debt.keys()):
        qids = debt[bucket]
        parts.append(f"{bucket}=[{', '.join(qids)}]")
    return "; ".join(parts)


def build_net_win_with_debt_reflection(
    *,
    iteration: int,
    target_qids: tuple[str, ...],
    prior_ag_shape: AGShapeSignature,
    prior_lever_directives: tuple[dict, ...],
    regression_debt: dict[str, list[str]],
) -> NearMissReflection:
    """Phase 3 Action 3.2 — NET_WIN_WITH_DEBT reflection builder.

    "You accepted +Xpp but didn't fix target Y. Next AG targeting Y
    MUST use a different repair_archetype." Archetype is the higher-
    level discriminator (patch_family is derived from archetype +
    intended_patch_shape), so the required change is
    ``different_repair_archetype``.
    """
    targets_str = ", ".join(target_qids) if target_qids else "(no declared targets)"
    text = (
        f"NET-WIN-WITH-DEBT (iteration {iteration}): the prior attempt on "
        f"target(s) {targets_str} accepted but did not fix the declared "
        f"target. Prior archetype was '{prior_ag_shape.repair_archetype}' "
        f"at scope '{prior_ag_shape.target_scope.value}'; lever directives "
        f"were [{_format_lever_directives(prior_lever_directives)}]; "
        f"regression debt was [{_format_debt(regression_debt)}]. The next "
        f"AG against this target MUST use a different repair_archetype."
    )
    return NearMissReflection(
        kind="net_win_with_debt",
        iteration=iteration,
        target_qids=tuple(target_qids),
        prior_ag_shape=prior_ag_shape,
        prior_lever_directives=tuple(prior_lever_directives),
        regression_debt=dict(regression_debt),
        reflection_text=text,
        required_next_iter_change="different_repair_archetype",
    )


def build_diagnostic_hold_reflection(
    *,
    iteration: int,
    target_qids: tuple[str, ...],
    prior_ag_shape: AGShapeSignature,
    prior_lever_directives: tuple[dict, ...],
    regression_debt: dict[str, list[str]],
) -> NearMissReflection:
    """Phase 3 Action 3.2 — DIAGNOSTIC_HOLD reflection builder.

    "You produced +Xpp but with too much debt to accept. Next AG must
    EITHER fix more qids before introducing the same regressions, OR
    scope the patches narrower to avoid the regression." Both escape
    paths are valid → ``required_next_iter_change="either"`` (the
    harness gate treats this as a logical OR over both shape
    dimensions: change repair_archetype OR target_scope).
    """
    targets_str = ", ".join(target_qids) if target_qids else "(no declared targets)"
    text = (
        f"DIAGNOSTIC-HOLD (iteration {iteration}): the prior attempt on "
        f"target(s) {targets_str} produced an aggregate gain but with "
        f"unacceptable regression debt and so was held. Prior archetype "
        f"was '{prior_ag_shape.repair_archetype}' at scope "
        f"'{prior_ag_shape.target_scope.value}'; lever directives were "
        f"[{_format_lever_directives(prior_lever_directives)}]; "
        f"regression debt was [{_format_debt(regression_debt)}]. The "
        f"next AG against this target MUST EITHER use a different "
        f"repair_archetype to fix more qids before introducing the "
        f"same regressions, OR scope the patches narrower to avoid the "
        f"regression."
    )
    return NearMissReflection(
        kind="diagnostic_hold",
        iteration=iteration,
        target_qids=tuple(target_qids),
        prior_ag_shape=prior_ag_shape,
        prior_lever_directives=tuple(prior_lever_directives),
        regression_debt=dict(regression_debt),
        reflection_text=text,
        required_next_iter_change="either",
    )


@dataclass(frozen=True)
class AGShapeAssertionResult:
    """Phase 3 Action 3.2 — pure-function result of the AG-shape gate.

    * ``differs=True`` — the candidate shape is not equal to any prior;
      the AG is allowed through.
    * ``differs=False`` — the candidate matches ``matched_prior_shape``;
      the harness emits ``NEAR_MISS_AG_SHAPE_REPEATED`` (and, when
      ``near_miss_reflection_strict_drop_enabled()``, drops the AG).
    """

    differs: bool
    matched_prior_shape: Optional[AGShapeSignature]


def assert_ag_shape_differs_from_priors(
    *,
    candidate_shape: AGShapeSignature,
    prior_shapes: tuple[AGShapeSignature, ...],
    required_next_iter_change: str,
) -> AGShapeAssertionResult:
    """Phase 3 Action 3.2 — pure check used by the harness pre-strategy
    gate.

    Returns ``differs=True`` when:
      * No priors exist.
      * The candidate's ``(repair_archetype, target_scope)`` pair does
        not match any prior under ``shapes_are_equal``.

    Returns ``differs=False`` (with ``matched_prior_shape`` set) when
    the candidate is shape-equal to any prior. The
    ``required_next_iter_change`` parameter is informational here —
    the gate's equality is unconditional. Callers consult it to log
    *which* dimension the strategist failed to change.
    """
    for prior in prior_shapes:
        if shapes_are_equal(candidate_shape, prior):
            return AGShapeAssertionResult(differs=False, matched_prior_shape=prior)
    return AGShapeAssertionResult(differs=True, matched_prior_shape=None)
