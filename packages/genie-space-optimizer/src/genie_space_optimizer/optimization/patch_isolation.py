"""Cycle 14B-T3 — patch-subset isolation pure helpers.

Re-evaluates a candidate after peeling off a single patch identified
as the sole attributable cause of an out-of-target regression. The
orchestration that calls these helpers (and performs the live
re-eval) lives in ``harness.py``; this module holds only the pure
attribution + subset + verdict logic so it is unit-testable without
Databricks dependencies.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping


@dataclass(frozen=True)
class SinglePatchAttribution:
    """Result of attempting to attribute a regression to one patch.

    ``confidence`` semantics:
      1.0 — only one applied patch overlaps the regressed qid via
            ``affected_qids``.
      0.5 — only one applied patch's cluster lineage covers the
            regressed qid (qid-overlap data not available).
      (returns None when neither condition holds, or when more than
      one patch matches.)
    """

    patch_id: str
    expanded_patch_id: str
    cluster_id: str
    confidence: float


def _patch_qids(patch: Mapping) -> set[str]:
    return {str(q) for q in (patch.get("affected_qids") or []) if str(q)}


def attribute_regression_to_single_patch(
    *,
    regressed_qid: str,
    applied_patches: Iterable[Mapping],
    cluster_qids: Mapping[str, Iterable[str]] | None = None,
) -> SinglePatchAttribution | None:
    """Identify a single applied patch attributable to ``regressed_qid``.

    Returns ``None`` when zero or multiple patches match. Pure: no I/O.
    """
    target = str(regressed_qid)
    if not target:
        return None

    patches = list(applied_patches or [])
    if not patches:
        return None

    # First pass: direct affected_qids overlap.
    direct = [
        p for p in patches if isinstance(p, Mapping) and target in _patch_qids(p)
    ]
    if len(direct) == 1:
        p = direct[0]
        return SinglePatchAttribution(
            patch_id=str(p.get("patch_id") or ""),
            expanded_patch_id=str(p.get("expanded_patch_id") or ""),
            cluster_id=str(p.get("cluster_id") or ""),
            confidence=1.0,
        )
    if len(direct) > 1:
        return None

    # Second pass: cluster lineage fallback. Only fires when no
    # direct overlap matched.
    if cluster_qids:
        cluster_match = [
            p
            for p in patches
            if isinstance(p, Mapping)
            and target
            in {
                str(q)
                for q in (cluster_qids.get(str(p.get("cluster_id") or "")) or ())
            }
        ]
        if len(cluster_match) == 1:
            p = cluster_match[0]
            return SinglePatchAttribution(
                patch_id=str(p.get("patch_id") or ""),
                expanded_patch_id=str(p.get("expanded_patch_id") or ""),
                cluster_id=str(p.get("cluster_id") or ""),
                confidence=0.5,
            )

    return None


def build_isolation_subset(
    *,
    applied_patches: Iterable[Mapping],
    patch_to_remove: str,
) -> tuple[Mapping, ...]:
    """Return ``applied_patches`` minus the named patch.

    Identity is matched by ``expanded_patch_id`` first, then
    ``patch_id``. Order is preserved. Pure: no I/O.
    """
    target = str(patch_to_remove)
    if not target:
        return tuple(applied_patches or ())

    out: list[Mapping] = []
    for p in applied_patches or ():
        if not isinstance(p, Mapping):
            out.append(p)
            continue
        expanded = str(p.get("expanded_patch_id") or "")
        pid = str(p.get("patch_id") or "")
        if expanded and expanded == target:
            continue
        if pid and pid == target and not expanded:
            continue
        out.append(p)
    return tuple(out)


@dataclass(frozen=True)
class IsolationVerdict:
    """Outcome of comparing a subset decision to the original.

    ``outcome`` values:
      subset_accepts_clean       — subset has zero debt and clears
                                   the aggregate floor; accept
                                   directly with reason 'accepted'.
      subset_accepts_with_debt   — subset still has bounded debt
                                   under policy; route through
                                   accepted_with_partial_harvest_debt.
      subset_still_over_policy   — subset's debt still violates
                                   policy; halt with multi-patch
                                   reason.
      subset_regresses_aggregate — subset's aggregate is worse than
                                   baseline; the removed patch was
                                   load-bearing; halt with
                                   subset_load_bearing reason.
    """

    outcome: str
    subset_aggregate_gain_pp: float
    subset_debt_qids: tuple[str, ...]


def evaluate_isolation_verdict(
    *,
    original_decision,
    subset_decision,
    policy,
) -> IsolationVerdict:
    """Pure router: given an original full-AG decision and a
    hypothetical subset decision, return the appropriate verdict.

    No imports at module load time on RegressionDebtPolicy or
    ControlPlaneAcceptance (forward references) — the helper is
    duck-typed on the fields it reads, so unit tests can use real
    or stub objects.
    """
    from genie_space_optimizer.optimization.control_plane import (
        evaluate_regression_debt,
    )

    subset_gain = float(subset_decision.delta_pp)
    subset_debt = tuple(subset_decision.out_of_target_regressed_qids or ())

    if subset_gain < 0:
        return IsolationVerdict(
            outcome="subset_regresses_aggregate",
            subset_aggregate_gain_pp=subset_gain,
            subset_debt_qids=subset_debt,
        )

    if not subset_debt:
        # Aggregate-floor check: subset must still beat the policy's
        # min_aggregate_improvement_pp for an accept-clean. Below
        # that floor, the subset is honest but not accepted under
        # partial-harvest semantics — halt the isolation arm.
        if subset_gain < float(policy.min_aggregate_improvement_pp):
            return IsolationVerdict(
                outcome="subset_still_over_policy",
                subset_aggregate_gain_pp=subset_gain,
                subset_debt_qids=(),
            )
        return IsolationVerdict(
            outcome="subset_accepts_clean",
            subset_aggregate_gain_pp=subset_gain,
            subset_debt_qids=(),
        )

    verdict = evaluate_regression_debt(
        decision=subset_decision,
        policy=policy,
        cumulative_debt=0,
        threshold_pass_rate=1.0,
    )
    if verdict.under_policy and verdict.debt_qids:
        return IsolationVerdict(
            outcome="subset_accepts_with_debt",
            subset_aggregate_gain_pp=subset_gain,
            subset_debt_qids=verdict.debt_qids,
        )
    return IsolationVerdict(
        outcome="subset_still_over_policy",
        subset_aggregate_gain_pp=subset_gain,
        subset_debt_qids=subset_debt,
    )
