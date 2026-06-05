"""Trial 23 W8 — pivot with a destination (repair, not drop).

Trial 20 D3 correctly refuses to repeat a sole-lever proposal whose
lever family already produced a ``kept_insufficient`` (behaviour-
unchanged) candidate — but the refusal has no destination: when the
only proposal is the rejected sole lever, the slate empties to
``stage3_returned_none`` and the iteration dies with nothing to apply.

W8 gives the refusal a destination. When the D3 drop would empty the
slate, the synthesizer issues ONE replacement re-prompt that demands a
*multi-lever bundle* — pairing the rejected family with a DIFFERENT
companion lever (or a different mechanism entirely) — and routes the
result back through the full normalization + gate pipeline via
``llm_response_override``. Only if that replacement also fails to
survive does the slate empty.

This module is the *pure* directive/predicate/marker brain. The
synthesizer owns the flag gate, the LLM call, and the re-entry.
"""
from __future__ import annotations

import json
from collections.abc import Sequence


def slate_emptied_by_sole_lever(
    *,
    proposals_before: int,
    proposals_after: int,
    rejected_families: Sequence[str],
) -> bool:
    """True when D3 dropped EVERY proposal as sole-lever-in-rejected.

    The pivot only fires when the drop left nothing (``after == 0``)
    AND there was something to drop (``before > 0``) AND a rejected
    family actually drove the drop. A slate that still has survivors
    needs no pivot — those survivors are the destination.
    """
    return (
        int(proposals_before) > 0
        and int(proposals_after) == 0
        and bool([f for f in (rejected_families or ()) if str(f).strip()])
    )


def build_pivot_directive(
    *,
    rejected_families: Sequence[str],
    cluster_id: str,
    root_cause: str = "",
) -> str:
    """Build the pivot re-prompt directive.

    Names the rejected lever families explicitly and pins the contract:
    emit a multi-lever bundle (>=2 proposals, shared bundle_id, DIFFERENT
    lever families) that recruits a NEW companion lever or a different
    mechanism — never the rejected family alone.
    """
    fams = sorted({str(f).strip() for f in (rejected_families or ()) if str(f).strip()})
    fam_str = ", ".join(fams) if fams else "the prior lever family"
    rca = str(root_cause or "").strip()
    rca_clause = (
        f" The root cause is '{rca}'." if rca else ""
    )
    return (
        "PIVOT REQUIRED. The prior repair used lever family/families "
        f"[{fam_str}] alone and was kept_insufficient: it applied "
        "cleanly but did NOT change behaviour on the target QID, so "
        "repeating it solo is forbidden." + rca_clause + " Emit a "
        "MULTI-LEVER BUNDLE of >=2 proposals that SHARE one bundle_id "
        "and set the SAME selected_levers kit on each member. The kit "
        "MUST pair the rejected family with at least one DIFFERENT "
        "companion lever family (or pivot to a different mechanism "
        "entirely). Do NOT emit any sole-lever proposal drawn only "
        f"from [{fam_str}] — it will be dropped again. cluster_id="
        f"{cluster_id}."
    )


def pivot_destination_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    cluster_id: str,
    rejected_families: Sequence[str],
    outcome: str,
    replacement_proposals: int = 0,
) -> str:
    """Build the ``GSO_TRIAL23_PIVOT_DESTINATION_V1`` marker line.

    ``outcome`` ∈ {``"pivot_attempted"`` (replacement re-prompt issued),
    ``"pivot_landed"`` (replacement bundle survived to a non-empty
    slate), ``"pivot_emptied_slate"`` (replacement also empty → the
    anti-success signature from the plan guardrails)}.
    """
    payload = {
        "optimization_run_id": str(optimization_run_id),
        "iteration": int(iteration),
        "cluster_id": str(cluster_id),
        "rejected_families": sorted(
            {str(f).strip() for f in (rejected_families or ()) if str(f).strip()}
        ),
        "outcome": str(outcome),
        "replacement_proposals": int(replacement_proposals),
        "anti_success": str(outcome) == "pivot_emptied_slate",
    }
    return (
        "GSO_TRIAL23_PIVOT_DESTINATION_V1 "
        + json.dumps(payload, sort_keys=True)
    )


__all__ = [
    "slate_emptied_by_sole_lever",
    "build_pivot_directive",
    "pivot_destination_marker",
]
