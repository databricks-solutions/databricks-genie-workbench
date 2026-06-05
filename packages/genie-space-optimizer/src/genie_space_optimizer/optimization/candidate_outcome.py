"""Canonical run-level ``CandidateOutcome`` record.

Single source of truth for the optimizer's accepted candidate. Every
downstream projection surface — ``GSO_OPTIMIZER_OUTCOME_V1``, the
iteration candidate ledger, the ``run_summary.json`` / ``scoreboard.json``
accuracy deltas, and deploy eligibility — should project from this one
record instead of re-deriving the run outcome from disjoint per-QID
trajectory signals.

Motivation (e94376a3 / d13938e7 postmortems): a clean full-eval
acceptance (``accepted=true``, positive ``delta_pp``, target fixed, no
debt) was authoritative for *nothing* in the control plane. The run
outcome was classified from per-QID trajectories that only reached
``kept_insufficient``; the candidate ledger row carried default
``0.0 / reject_loss / 0 / ""`` values; the accuracy deltas were
computed three different ways. This record makes the full-eval result
the canonical fact that those surfaces read.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Iterable


# Explicit terminal reason for a no-work iteration that runs AFTER a
# clean full-eval win already landed. Replaces the misleading default
# "unknown" that the budget-boundary rule collapsed into a
# GSO_RUN_ABORTED_V1 in the e94376a3 postmortem.
POST_WIN_NO_WORK_TERMINAL_REASON = "run_already_won_pending_gt_review"


def relabel_post_win_terminal_reason(
    candidate: "CandidateOutcome | None",
    current_terminal_reason: str,
) -> str:
    """Honest terminal_reason for a possibly post-win no-work iteration.

    When a clean full-eval win already landed (``candidate.accepted``)
    and the current iteration produced no candidate (terminal_reason
    is still the default ``"unknown"``), return the explicit post-win
    reason so the run does not emit a misleading abort. Otherwise the
    current reason passes through unchanged.
    """
    reason = str(current_terminal_reason or "unknown")
    if candidate is not None and candidate.accepted and reason == "unknown":
        return POST_WIN_NO_WORK_TERMINAL_REASON
    return reason


def _as_str_tuple(value: Any) -> tuple[str, ...]:
    if not value:
        return ()
    if isinstance(value, (str, bytes)):
        return (str(value),)
    try:
        return tuple(str(v) for v in value if str(v).strip())
    except TypeError:
        return ()


def _as_int_tuple(value: Any) -> tuple[int, ...]:
    if not value:
        return ()
    out: list[int] = []
    try:
        for v in value:
            try:
                out.append(int(v))
            except (TypeError, ValueError):
                continue
    except TypeError:
        return ()
    return tuple(out)


@dataclass(frozen=True)
class CandidateOutcome:
    """The run-level authoritative accepted-candidate fact.

    Built from the full-eval acceptance payload
    (``control_plane.format_full_eval_marker_payload``) plus the
    iteration-local apply context (selected proposal, levers, patch
    count). Field names are a CONTRACT for the projection surfaces.
    """

    accepted: bool
    iteration: int
    ag_id: str
    delta_pp: float
    baseline_accuracy: float
    candidate_accuracy: float
    target_qids: tuple[str, ...] = ()
    target_fixed_qids: tuple[str, ...] = ()
    target_still_hard_qids: tuple[str, ...] = ()
    regression_debt_qids: tuple[str, ...] = ()
    selected_proposal_id: str = ""
    acceptance_tier: str = ""
    patches_applied: int = 0
    levers: tuple[int, ...] = ()
    reason_code: str = ""
    decision: str = "accepted"

    @property
    def has_target_debt(self) -> bool:
        """True when an accepted gain left some target QID unfixed.

        Mirrors the ``OPTIMIZER_AGGREGATE_GAIN_TARGET_DEBT`` predicate:
        the target set is non-empty and is not fully covered by the
        fixed set, or some target QID is still hard.
        """
        target_set = frozenset(self.target_qids)
        fixed_set = frozenset(self.target_fixed_qids)
        still_hard = frozenset(self.target_still_hard_qids)
        return bool(target_set and (not target_set.issubset(fixed_set) or still_hard))

    @property
    def is_clean_win(self) -> bool:
        """A clean, deployable win: accepted, positive delta, no debt."""
        return bool(
            self.accepted
            and self.delta_pp > 0.0
            and not self.target_still_hard_qids
            and not self.regression_debt_qids
            and not self.has_target_debt
        )

    def to_aggregate_kwargs(self) -> dict[str, Any]:
        """Scalars for ``classify_run_outcome_from_aggregates``."""
        return {
            "any_iteration_accepted": bool(self.accepted),
            "any_iteration_post_gt_pre": bool(self.delta_pp > 0.0),
            "last_accepted_decision": str(self.decision or "accepted"),
            "target_qids": tuple(self.target_qids),
            "target_fixed_qids": tuple(self.target_fixed_qids),
            "target_still_hard_qids": tuple(self.target_still_hard_qids),
        }

    def ledger_overrides_for_iteration(
        self,
        iteration: int,
        *,
        current_tier: str = "reject_loss",
        current_patches: int = 0,
    ) -> dict[str, Any]:
        """Ledger-field overrides if this record owns ``iteration``.

        Returns ``{}`` unless this is the accepted iteration. When it is,
        returns the authoritative ``accuracy_delta_pp`` / ``acceptance_tier``
        / ``patches_applied`` (and ``selected_proposal_id`` when known) so
        the ledger row stops carrying the unassigned defaults
        (``0.0 / reject_loss / 0 / ""``) that contradicted
        ``terminal_reason="accepted"`` in the e94376a3 postmortem.
        """
        if not (self.accepted and int(self.iteration) == int(iteration)):
            return {}
        if self.acceptance_tier:
            tier = self.acceptance_tier
        elif current_tier == "reject_loss":
            tier = "accepted"
        else:
            tier = current_tier
        out: dict[str, Any] = {
            "accuracy_delta_pp": float(self.delta_pp),
            "acceptance_tier": str(tier),
            "patches_applied": int(
                self.patches_applied or current_patches or 0
            ),
        }
        if self.selected_proposal_id:
            out["selected_proposal_id"] = str(self.selected_proposal_id)
        return out

    @staticmethod
    def project_run_outcome(
        trajectory_outcome: str,
        candidate: "CandidateOutcome | None",
    ) -> str:
        """Project the canonical run outcome.

        An accepted ``CandidateOutcome`` dominates the trajectory-derived
        outcome (which only sees the per-QID acceptance lane and would
        misreport a cumulative win as ``OPTIMIZER_TRIED_INSUFFICIENT_GAIN``)
        — EXCEPT when the trajectory classifier reports a genuine
        ``OPTIMIZER_INVARIANT_VIOLATION``, which is kept so SM1 breakage
        stays visible.
        """
        if (
            candidate is not None
            and candidate.accepted
            and trajectory_outcome != "OPTIMIZER_INVARIANT_VIOLATION"
        ):
            from genie_space_optimizer.optimization.state_machine.outcome import (
                classify_run_outcome_from_aggregates,
            )
            return classify_run_outcome_from_aggregates(
                **candidate.to_aggregate_kwargs()
            )
        return str(trajectory_outcome)

    @classmethod
    def from_full_eval_payload(
        cls,
        payload: dict[str, Any],
        *,
        selected_proposal_id: str = "",
        acceptance_tier: str = "",
        patches_applied: int = 0,
        levers: Any = (),
    ) -> "CandidateOutcome":
        """Build the canonical record from a full-eval marker payload.

        ``payload`` is the dict produced by
        ``control_plane.format_full_eval_marker_payload`` (the body of
        ``GSO_FULL_EVAL_V1``). Apply-context fields that the full-eval
        payload does not carry (selected proposal, acceptance tier,
        patch count, levers) are supplied by the caller from the
        iteration-local apply state.
        """
        return cls(
            accepted=bool(payload.get("accepted", False)),
            iteration=int(payload.get("iteration", 0) or 0),
            ag_id=str(payload.get("ag_id", "") or ""),
            delta_pp=float(payload.get("delta_pp", 0.0) or 0.0),
            baseline_accuracy=float(payload.get("baseline_accuracy", 0.0) or 0.0),
            candidate_accuracy=float(payload.get("candidate_accuracy", 0.0) or 0.0),
            target_qids=_as_str_tuple(payload.get("target_qids")),
            target_fixed_qids=_as_str_tuple(payload.get("target_fixed_qids")),
            target_still_hard_qids=_as_str_tuple(
                payload.get("target_still_hard_qids")
            ),
            regression_debt_qids=_as_str_tuple(payload.get("regression_debt_qids")),
            selected_proposal_id=str(selected_proposal_id or ""),
            acceptance_tier=str(acceptance_tier or ""),
            patches_applied=int(patches_applied or 0),
            levers=_as_int_tuple(levers),
            reason_code=str(payload.get("reason_code", "") or ""),
        )


def _applied_patches(per_ag_snapshots: Iterable[Any]) -> list[dict]:
    """Flatten the ``applied`` patch dicts across per-AG snapshots.

    Accepts any object exposing an ``applied`` list-of-dicts attribute
    (``PatchSurvivalSnapshot``). Non-dict entries are ignored so a
    malformed snapshot can never crash the ledger path.
    """
    out: list[dict] = []
    for snap in per_ag_snapshots or ():
        applied = getattr(snap, "applied", None) or ()
        for patch in applied:
            if isinstance(patch, dict):
                out.append(patch)
    return out


@dataclass(frozen=True)
class CandidateLifecycle:
    """Per-iteration single-writer for the applied-patch funnel facts.

    :class:`CandidateOutcome` models only the *accepted-win endpoint* —
    its :meth:`CandidateOutcome.ledger_overrides_for_iteration` returns
    ``{}`` for every iteration that did not full-eval ACCEPT. That left
    the candidate ledger reporting ``patches_applied=0`` /
    ``selected_proposal_id=""`` on the (common) non-accepted iteration
    even when ``patch_survival`` recorded an applied patch — the
    e94376a3 iteration-3 contradiction.

    ``CandidateLifecycle`` reads the iteration's ``patch_survival``
    snapshots (the authoritative "actually applied" record) so the
    ledger reports the SAME applied count and selected proposal on
    EVERY iteration. It is a *floor*, never an override: it raises the
    ledger's applied facts to what patch_survival recorded but never
    lowers a higher value an accept already set (e.g. ``len(patches)``).
    """

    iteration: int
    patches_applied: int = 0
    selected_proposal_id: str = ""
    proposal_attempts: int = 0
    # Track B / B2 — set when an applied lone-instruction patch left the
    # generated SQL shape unchanged for a SQL-shape RCA (a phantom
    # accept). Drives selection down-ranking and lets the reconciler
    # refuse to treat the iteration as a genuine win.
    applied_but_inert: bool = False

    @classmethod
    def from_patch_survival(
        cls,
        *,
        iteration: int,
        per_ag_snapshots: Iterable[Any],
        proposal_attempts: int = 0,
    ) -> "CandidateLifecycle":
        """Build the lifecycle record from the iteration's patch_survival.

        ``patches_applied`` is the count of applied patches across all
        per-AG snapshots; ``selected_proposal_id`` is the first applied
        patch's ``proposal_id`` (falling back to ``intent_id``).
        ``proposal_attempts`` is threaded through from the caller's
        deriver (the patch-outcome emitter) so the ledger has one source
        for the funnel head as well as its tail.
        """
        applied = _applied_patches(per_ag_snapshots)
        selected = ""
        for patch in applied:
            pid = str(patch.get("proposal_id") or patch.get("intent_id") or "")
            if pid:
                selected = pid
                break
        return cls(
            iteration=int(iteration),
            patches_applied=len(applied),
            selected_proposal_id=selected,
            proposal_attempts=int(proposal_attempts or 0),
        )

    def with_applied_but_inert(
        self, applied_but_inert: bool
    ) -> "CandidateLifecycle":
        """Return a copy with the Track B / B2 inertness flag set.

        The detector (``sql_shape_inertness.detect_applied_but_inert``)
        runs post-apply with the generated SQL before/after; the harness
        threads its verdict onto the (frozen) lifecycle record here.
        """
        return replace(self, applied_but_inert=bool(applied_but_inert))

    def ledger_floor_for_iteration(
        self,
        iteration: int,
        *,
        current_patches: int = 0,
        current_selected: str = "",
    ) -> dict[str, Any]:
        """Applied-funnel floor for the ledger row on ANY iteration.

        Returns ``{}`` unless this record owns ``iteration``. Otherwise
        returns ``patches_applied`` raised to ``max(survival, current)``
        and ``selected_proposal_id`` (preferring an already-set
        ``current_selected`` from an accept, else the survival value).
        Keys are omitted when their value is empty so an iteration with
        genuinely no applied patches stays byte-stable (no spurious
        ``patches_applied=0`` write churn).
        """
        if int(self.iteration) != int(iteration):
            return {}
        out: dict[str, Any] = {}
        floor_patches = max(
            int(self.patches_applied or 0), int(current_patches or 0)
        )
        if floor_patches:
            out["patches_applied"] = floor_patches
        selected = str(current_selected or self.selected_proposal_id or "")
        if selected:
            out["selected_proposal_id"] = selected
        return out
