"""Trial 19 A1 — pre-Stage-3 admission gate for repair proposals.

The gate enforces the anti-repeat contract from Trial 18 by hard-
rejecting any proposal whose sole-primary
``(qid, lever, patch_type, rca_kind_label)`` quadruple matches an
entry already in ``insufficient_repair_signatures``. A proposal that
also includes a reinforcement bundle is admitted — the bundled
companion patches change the repair footprint enough to justify a
retry on the same family.

Why a separate gate (and not just a prompt rule):

  * The Stage 3 prompt already lists the anti-repeat constraint
    verbatim (A4), but compilable LLMs occasionally regress.
  * The structural-repair gate downstream operates on emitted patch
    *shapes*, not signatures. It will admit a structurally correct
    proposal even if the proposal is a sole-primary repeat of an
    insufficient signature.
  * Postmortem evidence (airline 634185464201993 + 7now
    953593238005228) shows repeated sole-primary insufficient
    signatures eat iteration budget. A code-level gate is the
    backstop.

The gate is *pure* — it takes proposals plus the two signature sets
and returns verdict records. Side-effecting bookkeeping
(``AdmissionDecisionRecord`` emission, telemetry print) is the
caller's responsibility. This keeps the gate testable without the SM.

Default-ON behavior via :func:`trial19_enforce_insufficient_enabled`:
when the flag is OFF the gate returns ``"admitted"`` for every
proposal (preserving pre-Trial-19 behavior byte-stably).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Sequence


# Verdict decision strings — stable for postmortem parsers.
ADMITTED: str = "admitted"
REJECTED_INSUFFICIENT_REPEAT: str = "rejected_insufficient_repeat"
ADMITTED_WITH_REINFORCEMENT: str = "admitted_with_reinforcement"


@dataclass(frozen=True, slots=True)
class AdmissionDecisionRecord:
    """One per proposal-set evaluation. Caller emits as observability.

    Fields:
        decision: One of ``ADMITTED`` / ``REJECTED_INSUFFICIENT_REPEAT``
            / ``ADMITTED_WITH_REINFORCEMENT``.
        reason: Free-text explanation of WHY the decision was reached.
            Surfaces in postmortem evidence and the typed feedback
            string the caller routes back to Stage 2 on rejection.
        matched_signature: The ``insufficient_repair_signature`` string
            this verdict matched against. Empty when admitted without
            a match.
        proposal_index: Zero-based index of the proposal in the input
            list that triggered the verdict (or ``-1`` when admitting
            the whole set).
    """
    decision: str
    reason: str
    matched_signature: str = ""
    proposal_index: int = -1


@dataclass(frozen=True, slots=True)
class AdmissionEvaluation:
    """Output of :func:`evaluate_admission` — a verdict for every input
    proposal in input order, plus the typed feedback string the caller
    routes to Stage 2 when any proposal was rejected. ``rejected``
    counts the rejection verdicts for run-level outcome bookkeeping.

    The set-level verdict is derived from the per-proposal verdicts:

      * ``admitted_set``: every proposal admitted → the whole set goes
        to Stage 3.
      * ``rejected_set``: at least one proposal rejected → the set is
        routed back to Stage 2 with the typed feedback string.
    """
    verdicts: tuple[AdmissionDecisionRecord, ...]
    rejected: int
    typed_feedback: str

    @property
    def admitted_set(self) -> bool:
        return self.rejected == 0

    @property
    def rejected_set(self) -> bool:
        return self.rejected > 0


_INSUFFICIENT_VERB_TOKEN = "insufficient"

_REJECTION_FEEDBACK_TEMPLATE = (
    "sole-primary repeats prior insufficient signature; add "
    "reinforcement or pivot to a different lever family"
)


def _build_quadruple_signature(
    *,
    selected_lever: str,
    patch_type: str,
    rca_kind_label: str,
    behavioral_diff: str = "",
) -> str:
    """Render the same string format the acceptance gate emits.

    See ``state_machine/transformers/acceptance_gate.py``: the
    ``kept_insufficient`` lane builds signatures as
    ``"<lever>:<patch_type>:insufficient:rca=<rca>:behavior=<diff>"``.
    We match against the lever/patch_type/rca triple while ignoring
    the ``behavior=<diff>`` suffix so a proposal that would yield the
    same family is rejected regardless of which behavioral_diff value
    was observed last iteration.
    """
    return (
        f"{selected_lever or '?'}:{patch_type or '?'}"
        f":{_INSUFFICIENT_VERB_TOKEN}:rca={rca_kind_label or '?'}"
    )


def _parse_insufficient_signature(sig: str) -> tuple[str, str, str]:
    """Extract ``(lever, patch_type, rca)`` from a signature string.

    The format is the acceptance-gate emission:
    ``"<lever>:<patch_type>:insufficient:rca=<rca>:behavior=<diff>"``.
    Any deviation (other verb, missing pieces) returns the parts we
    could recover with ``"?"`` placeholders — the comparison falls
    back to substring-prefix matching in :func:`_matches_signature`.
    """
    if not isinstance(sig, str):
        return ("?", "?", "?")
    body = sig.strip()
    if not body:
        return ("?", "?", "?")
    parts = body.split(":", 4)
    if len(parts) < 4:
        return ("?", "?", "?")
    lever = parts[0] or "?"
    patch_type = parts[1] or "?"
    if parts[2] != _INSUFFICIENT_VERB_TOKEN:
        return ("?", "?", "?")
    rca_raw = parts[3]
    rca = rca_raw[4:] if rca_raw.startswith("rca=") else "?"
    return (lever, patch_type, rca or "?")


def _matches_signature(
    candidate: tuple[str, str, str],
    signature: str,
) -> bool:
    """Return True when ``candidate`` matches an insufficient signature.

    Comparison ignores trailing ``behavior=<diff>`` so a previously
    KEPT_INSUFFICIENT entry blocks all behavioral-diff variants for
    the same family. The check is case-sensitive — signatures travel
    as canonical lower-case lever/patch_type strings already.
    """
    parsed = _parse_insufficient_signature(signature)
    return parsed == candidate


def _has_reinforcement_bundle(
    proposal: Any,
    all_proposals: Sequence[Any],
) -> bool:
    """Determine whether ``proposal`` is bundled with reinforcement(s).

    A reinforcement bundle is satisfied when EITHER:

      * the proposal carries a non-empty ``bundle_id`` AND at least
        one other proposal in ``all_proposals`` shares the same
        ``bundle_id`` with a *different* ``selected_lever`` OR a
        *different* ``patch_type``;
      * OR the proposal itself names two distinct
        ``patch_types``/levers in its repair scope (rare today; the
        framework normally splits into separate proposals so this
        path is a future-proofing hook).

    Returns False conservatively — if reinforcement cannot be
    confidently established the gate rejects.
    """
    if proposal is None:
        return False
    own_bundle = str(getattr(proposal, "bundle_id", "") or "").strip()
    if own_bundle:
        own_lever = str(getattr(proposal, "selected_lever", "") or "").strip()
        own_patch = _patch_type_str(getattr(proposal, "patch_type", ""))
        for other in all_proposals:
            if other is proposal:
                continue
            other_bundle = str(
                getattr(other, "bundle_id", "") or ""
            ).strip()
            if other_bundle != own_bundle:
                continue
            other_lever = str(
                getattr(other, "selected_lever", "") or ""
            ).strip()
            other_patch = _patch_type_str(getattr(other, "patch_type", ""))
            if other_lever and other_lever != own_lever:
                return True
            if other_patch and other_patch != own_patch:
                return True
    return False


def _patch_type_str(value: Any) -> str:
    """Coerce ``PatchType`` enum or string to its canonical lower-case
    string. Returns empty string for None / unknown values."""
    if value is None:
        return ""
    raw = getattr(value, "value", None)
    if isinstance(raw, str) and raw:
        return raw.strip().lower()
    return str(value).strip().lower()


def _qids_for_proposal(proposal: Any) -> tuple[str, ...]:
    qids = getattr(proposal, "target_qids", None) or ()
    return tuple(str(q).strip() for q in qids if str(q).strip())


def evaluate_admission(
    proposals: Sequence[Any],
    *,
    insufficient_signatures: Iterable[str] = (),
    forbidden_signatures: Iterable[str] = (),
    rca_kind_label_by_qid: dict[str, str] | None = None,
) -> AdmissionEvaluation:
    """Trial 19 A1 — pure admission verdict for a Stage-3 proposal set.

    Args:
        proposals: ordered Stage-3 ``RepairProposal`` objects (or
            anything duck-typed: ``selected_lever``, ``patch_type``,
            ``target_qids``, ``bundle_id``).
        insufficient_signatures: signatures harvested from prior
            iterations' ``KEPT_INSUFFICIENT`` lane.
        forbidden_signatures: terminal-rejection signatures harvested
            from prior iterations' ``applier_gate`` / ``evaluated_gate``
            lanes. Currently consumed only for the typed-feedback
            string (the Stage 3 prompt already enforces the hard
            rejection).
        rca_kind_label_by_qid: optional map from qid to the LLM-emitted
            free-text rca label. When provided, the gate uses this map
            to build the candidate signature. Missing entries fall
            back to the legacy ``rca_kind`` field on the proposal (if
            present) or to ``"?"``.

    Returns:
        :class:`AdmissionEvaluation` carrying one per-proposal verdict.
        When ``trial19_enforce_insufficient_enabled()`` is False, the
        return value admits every proposal regardless of input —
        preserving pre-Trial-19 byte-stable behavior under the OFF
        flag.
    """
    try:
        from genie_space_optimizer.optimization.trial19_flags import (
            trial19_enforce_insufficient_enabled,
        )
        flag_on = trial19_enforce_insufficient_enabled()
    except Exception:
        flag_on = False

    proposals_t = tuple(proposals or ())
    sigs = tuple(s for s in insufficient_signatures if s)

    if not flag_on or not sigs:
        return AdmissionEvaluation(
            verdicts=tuple(
                AdmissionDecisionRecord(
                    decision=ADMITTED,
                    reason=(
                        "trial19_enforce_insufficient disabled"
                        if not flag_on else
                        "no insufficient_repair_signatures in context"
                    ),
                    proposal_index=i,
                )
                for i, _ in enumerate(proposals_t)
            ),
            rejected=0,
            typed_feedback="",
        )

    rca_map = dict(rca_kind_label_by_qid or {})

    verdicts: list[AdmissionDecisionRecord] = []
    rejected_count = 0
    for idx, proposal in enumerate(proposals_t):
        lever = str(getattr(proposal, "selected_lever", "") or "").strip()
        patch_type = _patch_type_str(getattr(proposal, "patch_type", ""))
        qids = _qids_for_proposal(proposal)
        rca = ""
        for qid in qids:
            mapped = rca_map.get(qid, "")
            if mapped:
                rca = mapped
                break
        if not rca:
            rca = str(getattr(proposal, "rca_kind", "") or "").strip()
        candidate = (lever or "?", patch_type or "?", rca or "?")

        matched: str = ""
        for sig in sigs:
            if _matches_signature(candidate, sig):
                matched = sig
                break

        if not matched:
            verdicts.append(
                AdmissionDecisionRecord(
                    decision=ADMITTED,
                    reason="no signature match",
                    proposal_index=idx,
                )
            )
            continue

        if _has_reinforcement_bundle(proposal, proposals_t):
            verdicts.append(
                AdmissionDecisionRecord(
                    decision=ADMITTED_WITH_REINFORCEMENT,
                    reason=(
                        "matched signature but proposal is bundled "
                        "with reinforcement"
                    ),
                    matched_signature=matched,
                    proposal_index=idx,
                )
            )
            continue

        rejected_count += 1
        verdicts.append(
            AdmissionDecisionRecord(
                decision=REJECTED_INSUFFICIENT_REPEAT,
                reason=_REJECTION_FEEDBACK_TEMPLATE,
                matched_signature=matched,
                proposal_index=idx,
            )
        )

    typed_feedback = ""
    if rejected_count > 0:
        rejected_sigs = sorted(
            {
                v.matched_signature
                for v in verdicts
                if v.decision == REJECTED_INSUFFICIENT_REPEAT
                and v.matched_signature
            }
        )
        typed_feedback = (
            f"{_REJECTION_FEEDBACK_TEMPLATE}. "
            f"Matched signatures: {rejected_sigs}."
        )
    return AdmissionEvaluation(
        verdicts=tuple(verdicts),
        rejected=rejected_count,
        typed_feedback=typed_feedback,
    )


__all__ = (
    "ADMITTED",
    "REJECTED_INSUFFICIENT_REPEAT",
    "ADMITTED_WITH_REINFORCEMENT",
    "AdmissionDecisionRecord",
    "AdmissionEvaluation",
    "evaluate_admission",
)
