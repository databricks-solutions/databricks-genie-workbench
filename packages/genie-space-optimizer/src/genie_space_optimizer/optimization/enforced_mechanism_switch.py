"""Trial 30 W30.1b — deterministic enforced inert-mechanism switch.

Post-LLM guard: if a synthesised proposal re-emits a mechanism the
acceptance gate already proved behaviorally inert for its
``(qid, rca_kind)`` (recorded in :class:`InertMechanismHistory`), and a
structurally-distinct fallback mechanism is still present in the same
QID's surviving slate, hard-drop the re-emit. If NO fallback survives,
keep the re-emit and flag ``no_fallback`` — the guard never zeroes out a
QID.

Comparison is on :class:`PatchMechanism` (the behavioral unit), not
lever-id strings, so lever-5 / 5a / 5b aliasing cannot let a re-emit
slip through. See :func:`mechanisms_for_rejected_levers`.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Sequence

from genie_space_optimizer.optimization.patch_mechanism import (
    PatchMechanism,
    mechanism_for_patch_type,
)
from genie_space_optimizer.optimization.rca_mechanism_routing import (
    _structural_fix_mechanisms,
    mechanisms_for_rejected_levers,
)


@dataclass
class EnforcedSwitchOutcome:
    survivors: list[Any]
    dropped: list[Any] = field(default_factory=list)
    dropped_reasons: dict[str, str] = field(default_factory=dict)
    no_fallback_qids: list[str] = field(default_factory=list)


def _proposal_mechanism(prop: Any) -> PatchMechanism | None:
    """Resolve a proposal-like object to its PatchMechanism.

    Accepts either an explicit ``mechanism`` (PatchMechanism, used by the
    pure unit tests) or a ``patch_type`` (wire string or enum, used by
    the real RepairProposal adapter).
    """
    mech = getattr(prop, "mechanism", None)
    if isinstance(mech, PatchMechanism):
        return mech
    patch_type = getattr(prop, "patch_type", None)
    pt_value = getattr(patch_type, "value", patch_type)
    if pt_value:
        return mechanism_for_patch_type(str(pt_value))
    return None


def enforced_switch_survivors(
    proposals: Sequence[Any],
    history: Iterable[Any],
) -> EnforcedSwitchOutcome:
    """Filter ``proposals`` against the inert-mechanism ``history``.

    Pure. ``proposals`` need only expose ``intent_id``, ``qid``,
    ``rca_kind``, and either ``mechanism`` (PatchMechanism) or
    ``patch_type``. Order of survivors is preserved.
    """
    by_pair: dict[tuple[str, str], frozenset[PatchMechanism]] = {}
    for entry in history or ():
        key = (str(entry.qid), str(entry.rca_kind))
        rejected = mechanisms_for_rejected_levers(entry.rejected_mechanisms)
        by_pair[key] = by_pair.get(key, frozenset()) | rejected

    if not by_pair:
        return EnforcedSwitchOutcome(survivors=list(proposals))

    # Per (qid, rca_kind): which mechanisms are present in the slate.
    slate_mechs: dict[tuple[str, str], set[PatchMechanism]] = {}
    for p in proposals:
        key = (str(getattr(p, "qid", "")), str(getattr(p, "rca_kind", "")))
        m = _proposal_mechanism(p)
        if m is not None:
            slate_mechs.setdefault(key, set()).add(m)

    survivors: list[Any] = []
    dropped: list[Any] = []
    dropped_reasons: dict[str, str] = {}
    no_fallback_qids: list[str] = []

    for p in proposals:
        key = (str(getattr(p, "qid", "")), str(getattr(p, "rca_kind", "")))
        rejected = by_pair.get(key)
        mech = _proposal_mechanism(p)
        if not rejected or mech is None or mech not in rejected:
            survivors.append(p)
            continue
        # This proposal re-emits a rejected mechanism. Is there a
        # structurally-distinct fallback present in the surviving slate?
        structural = _structural_fix_mechanisms(key[1])
        available_fallbacks = structural - rejected
        slate_fallbacks = slate_mechs.get(key, set()) & available_fallbacks
        if slate_fallbacks:
            dropped.append(p)
            chosen = sorted(m.value for m in slate_fallbacks)
            dropped_reasons[str(getattr(p, "intent_id", ""))] = (
                "GSO_TRIAL30_ENFORCED_SWITCH_V1:"
                f"rca={key[1]}:rejected={mech.value}:"
                f"fallback={','.join(chosen)}"
            )
        else:
            survivors.append(p)
            if key[0] not in no_fallback_qids:
                no_fallback_qids.append(key[0])

    return EnforcedSwitchOutcome(
        survivors=survivors,
        dropped=dropped,
        dropped_reasons=dropped_reasons,
        no_fallback_qids=no_fallback_qids,
    )


__all__ = ["EnforcedSwitchOutcome", "enforced_switch_survivors"]
