"""Plan P-F — Proposal Failure Taxonomy and Recovery Policy.

Pure module: maps a ``ProposalFailureContext`` to a closed-vocabulary
``ProposalFailureNextAction`` label. No I/O. No side effects. No imports
from ``harness`` or ``decision_emitters``.

The policy is intentionally deterministic and small: the caller (harness)
fills the context dataclass from observable state (AG, cluster, rca, prior
reflection buffer counts) and the policy returns exactly one decision.

Closed vocabulary on the trigger side (``failure_mode``):

* ``proposal_generation_empty`` — proposer returned zero proposals.
* ``lever6_force_llm_declined`` — force-L6 LLM returned no candidate.
* ``no_causal_applyable_patch`` — every RCA-matched proposal dropped
  upstream of patch_cap.
* ``all_selected_patches_dropped_by_applier`` — every patch_cap survivor
  rejected by the applier.
* ``no_applied_patches`` — applier produced zero applied entries
  (post-apply skip).

Closed vocabulary on the output side (``ProposalFailureNextAction``):

* ``rotate_lever_family`` — untried lever family remains.
* ``narrow_ag_scope`` — multi-cluster AG should split.
* ``mark_evidence_gap`` — synthesis cannot operate on the supplied evidence.
* ``block_ag_retry_by_cluster_signature`` — repeated failure; admit to
  forbidden set.
* ``escalate_unsupported_repair_shape`` — every lever exhausted.
* ``request_evidence_gathering`` — RCA itself is the bottleneck.

Evidence anchor: runid_analysis/{ccf1d60d,31ecd96f}/postmortem.md.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class ProposalFailureNextAction(str, Enum):
    """Closed vocabulary of orchestration decisions."""

    ROTATE_LEVER_FAMILY = "rotate_lever_family"
    NARROW_AG_SCOPE = "narrow_ag_scope"
    MARK_EVIDENCE_GAP = "mark_evidence_gap"
    BLOCK_AG_RETRY_BY_CLUSTER_SIGNATURE = (
        "block_ag_retry_by_cluster_signature"
    )
    ESCALATE_UNSUPPORTED_REPAIR_SHAPE = (
        "escalate_unsupported_repair_shape"
    )
    REQUEST_EVIDENCE_GATHERING = "request_evidence_gathering"
    ESCALATE_STALEMATE = "escalate_stalemate"


@dataclass(frozen=True)
class ProposalFailureContext:
    """Typed input to ``decide_next_action``.

    Every field is observable at the harness emit site. The dataclass is
    frozen so the policy cannot mutate state by accident.
    """

    failure_mode: str
    ag_id: str
    cluster_id: str
    cluster_signature: str
    rca_id: str
    root_cause: str
    lever_set: tuple[int, ...]
    tried_lever_families: tuple[int, ...]
    ag_source_cluster_count: int
    rca_card_grounded: bool
    prior_failure_count: int
    prior_identical_failure_count: int = 0
    """C4 (2026-05-16) — how many times this AG's exact
    iteration-failure signature has fired *before* this call. The
    stalemate branch in ``decide_next_action`` escalates when this
    is >= 1 so the harness terminates instead of looping."""


@dataclass(frozen=True)
class ProposalFailureDecision:
    """Typed output of ``decide_next_action``."""

    next_action: ProposalFailureNextAction
    rationale: str


_BLOCK_AFTER_FAILURE_COUNT: int = 2
"""Block-by-signature kicks in once prior_failure_count meets this."""


def decide_next_action(ctx: ProposalFailureContext) -> ProposalFailureDecision:
    """Map a ``ProposalFailureContext`` to a ``ProposalFailureDecision``.

    Branch order (each branch is mutually exclusive with the next):

    1. Block-by-signature when the same context has failed ``prior_failure_count
       >= 2`` times.
    2. Escalate stalemate when ``prior_identical_failure_count >= 1`` —
       the iteration-failure signature has already fired in this AG, so
       further iterations are guaranteed loops.
    3. Escalate when every lever in ``lever_set`` is in ``tried_lever_families``
       AND the failure is ``no_causal_applyable_patch``.
    4. Narrow when the AG covers ``ag_source_cluster_count >= 2`` AND the
       failure is an applier/gate rejection.
    5. Mark evidence gap when the LLM declined (force-L6) and the RCA is
       grounded (i.e. evidence reached the prompt but synthesis still failed).
    6. Request evidence when ``rca_card_grounded`` is False on
       ``proposal_generation_empty``.
    7. Rotate lever family otherwise (the default path when an untried family
       remains).

    Fallback (unknown failure mode): request evidence gathering. The loop
    never returns "no decision" — the contract is one record per trigger.
    """
    if ctx.prior_failure_count >= _BLOCK_AFTER_FAILURE_COUNT:
        return ProposalFailureDecision(
            next_action=(
                ProposalFailureNextAction.BLOCK_AG_RETRY_BY_CLUSTER_SIGNATURE
            ),
            rationale=(
                f"prior_failure_count={ctx.prior_failure_count} for "
                f"signature={ctx.cluster_signature}"
            ),
        )

    if ctx.prior_identical_failure_count >= 1:
        return ProposalFailureDecision(
            next_action=ProposalFailureNextAction.ESCALATE_STALEMATE,
            rationale=(
                f"stalemate: prior_identical_failure_count="
                f"{ctx.prior_identical_failure_count} for signature="
                f"{ctx.cluster_signature}"
            ),
        )

    untried = set(ctx.lever_set) - set(ctx.tried_lever_families)
    if not untried and ctx.failure_mode == "no_causal_applyable_patch":
        return ProposalFailureDecision(
            next_action=(
                ProposalFailureNextAction.ESCALATE_UNSUPPORTED_REPAIR_SHAPE
            ),
            rationale="every lever family exhausted",
        )

    _APPLIER_LIKE_MODES = {
        "all_selected_patches_dropped_by_applier",
        "no_applied_patches",
        "no_causal_applyable_patch",
    }
    if (
        ctx.failure_mode in _APPLIER_LIKE_MODES
        and ctx.ag_source_cluster_count >= 2
    ):
        return ProposalFailureDecision(
            next_action=ProposalFailureNextAction.NARROW_AG_SCOPE,
            rationale=(
                f"ag_source_cluster_count={ctx.ag_source_cluster_count} "
                f"on applier-rejection mode"
            ),
        )

    if ctx.failure_mode == "lever6_force_llm_declined" and ctx.rca_card_grounded:
        return ProposalFailureDecision(
            next_action=ProposalFailureNextAction.MARK_EVIDENCE_GAP,
            rationale="LLM declined despite grounded RCA",
        )

    if (
        ctx.failure_mode == "proposal_generation_empty"
        and not ctx.rca_card_grounded
    ):
        return ProposalFailureDecision(
            next_action=ProposalFailureNextAction.REQUEST_EVIDENCE_GATHERING,
            rationale="rca_card_grounded=False",
        )

    _KNOWN_FAILURE_MODES = {
        "proposal_generation_empty",
        "lever6_force_llm_declined",
        "no_causal_applyable_patch",
        "all_selected_patches_dropped_by_applier",
        "no_applied_patches",
    }
    if untried and ctx.failure_mode in _KNOWN_FAILURE_MODES:
        return ProposalFailureDecision(
            next_action=ProposalFailureNextAction.ROTATE_LEVER_FAMILY,
            rationale=f"untried_lever_families={sorted(untried)}",
        )

    return ProposalFailureDecision(
        next_action=ProposalFailureNextAction.REQUEST_EVIDENCE_GATHERING,
        rationale="fallback: no other branch matched",
    )
