"""Stage 6: Safety Gates (Phase F6).

Composable sub-handlers for the five gate kinds:
  - Content-fingerprint dedup (PR-E)
  - Lever-5 structural gate
  - RCA-groundedness gate
  - Blast-radius gate
  - Dead-on-arrival (DOA) gate

The public ``filter(ctx, inp)`` runs them in ``GATE_PIPELINE_ORDER``.
``run_gate(name, ctx, inp)`` is exposed for focused unit tests so the
file stays auditable.

F6 is observability-only: per the plan's Reality Check, the four gate
sites in harness.py are NOT contiguous and don't correspond to single
primitives. Lifting them all under F6's byte-stability gate is
high-risk. F6 stands up the typed surface and decision-record emission
entry; the actual gate logic in harness stays put. The sub-handlers
here implement minimal field-driven gate logic that the unit tests
exercise in isolation; production gates continue to fire from harness
until a follow-up plan does the full extraction.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


STAGE_KEY: str = "safety_gates"


@dataclass(frozen=True)
class DroppedCausalPatch:
    """Cycle 5 T2 — payload captured at every gate drop where the
    dropped patch carried target_qids overlapping the AG's causal
    target set. The harness threads a tuple of these into the next
    iteration's
    ``ActionGroupsInput.prior_iteration_dropped_causal_patches`` so the
    strategist can propose a narrower variant or shift levers instead
    of re-emitting the same dropped pattern.

    Frozen so instances are hashable for set-membership dedup across
    iterations.
    """
    gate: str
    reason: str
    proposal_id: str
    patch_type: str
    target: str
    target_qids: tuple[str, ...]
    dependents_outside_target: tuple[str, ...]
    rca_id: str
    root_cause: str


def capture_dropped_causal_patch(
    *,
    decision: dict,
    ag_target_qids: tuple[str, ...],
    rca_id: str,
    root_cause: str,
) -> "DroppedCausalPatch | None":
    """Cycle 5 T2 — return a ``DroppedCausalPatch`` when ``decision``
    is a drop AND the dropped patch carried target qids overlapping
    ``ag_target_qids``. Returns ``None`` otherwise (the strategist
    only learns from drops that were on its actual causal path; broad
    drops without a target qid intersection are noise).
    """
    if str(decision.get("outcome") or "") != "dropped":
        return None
    target_qids = tuple(
        str(q) for q in (ag_target_qids or ()) if str(q)
    )
    if not target_qids:
        return None
    metrics = decision.get("metrics") or {}
    dependents = tuple(
        str(q)
        for q in (metrics.get("passing_dependents_outside_target") or ())
    )
    return DroppedCausalPatch(
        gate=str(decision.get("gate") or ""),
        reason=str(
            decision.get("reason_detail")
            or decision.get("reason_code") or ""
        ),
        proposal_id=str(decision.get("proposal_id") or ""),
        patch_type=str(metrics.get("patch_type") or ""),
        target=str(metrics.get("target") or ""),
        target_qids=target_qids,
        dependents_outside_target=dependents,
        rca_id=str(rca_id or ""),
        root_cause=str(root_cause or ""),
    )


GATE_PIPELINE_ORDER: tuple[str, ...] = (
    # Cycle 2 Task 1: intra_ag_dedup runs first as a safety pre-pass —
    # collapse proposals with identical body text under different
    # patch_type before any other gate sees them.
    "intra_ag_dedup",
    # Phase H Completion Task 3 (F6 follow-up plan Path C): align F6
    # module's pipeline order with the harness's actual inline gate
    # firing order — lever5_structural, rca_groundedness, blast_radius
    # (matching harness inline emit sites). content_fingerprint_dedup
    # and dead_on_arrival run after as F6-only observability gates.
    "lever5_structural",
    "rca_groundedness",
    "blast_radius",
    "content_fingerprint_dedup",
    "dead_on_arrival",
)


# Default blast-radius cap — proposals touching more than this many
# distinct tables get dropped. Production cap is computed elsewhere;
# this default lets the sub-handler unit-test a realistic threshold.
_DEFAULT_BLAST_RADIUS_TABLE_CAP: int = 5


@dataclass
class GateDrop:
    proposal_id: str
    gate: str
    reason: str
    detail: str = ""


@dataclass
class GatesInput:
    proposals_by_ag: dict[str, tuple[dict[str, Any], ...]]
    ags: tuple[dict[str, Any], ...]
    rca_evidence: dict[str, dict[str, Any]] = field(default_factory=dict)
    applied_history: tuple[dict[str, Any], ...] = ()
    rolled_back_content_fingerprints: set[str] = field(default_factory=set)
    forbidden_signatures: set[str] = field(default_factory=set)
    space_snapshot: dict[str, Any] = field(default_factory=dict)


@dataclass
class GateOutcome:
    survived_by_ag: dict[str, tuple[dict[str, Any], ...]]
    dropped: tuple[GateDrop, ...] = ()
    new_dead_on_arrival_signatures: tuple[str, ...] = ()


def _run_intra_ag_dedup(
    ctx,
    proposals_by_ag: dict[str, tuple[dict[str, Any], ...]],
) -> tuple[dict[str, tuple[dict[str, Any], ...]], list[GateDrop]]:
    """Cycle 2 Task 1 — collapse intra-AG body duplicates.

    Two proposals in the same AG with identical body text but
    different ``patch_type`` produce different ``content_fingerprint``
    values today (since the fingerprint includes ``patch_type``) and
    so survive the existing cross-iteration dedup gate. This pass runs
    before content_fingerprint_dedup, keys on body alone, and keeps
    the first occurrence. Disabling
    ``GSO_INTRA_AG_PROPOSAL_DEDUP`` returns the input untouched.
    """
    from genie_space_optimizer.common.config import (
        intra_ag_proposal_dedup_enabled,
    )

    if not intra_ag_proposal_dedup_enabled():
        return proposals_by_ag, []

    from genie_space_optimizer.optimization.reflection_retry import (
        patch_body_fingerprint,
    )

    survived: dict[str, tuple[dict[str, Any], ...]] = {}
    drops: list[GateDrop] = []
    for ag_id, props in proposals_by_ag.items():
        seen: set[str] = set()
        kept: list[dict[str, Any]] = []
        for p in props:
            fp = patch_body_fingerprint(p)
            if not fp:
                kept.append(p)
                continue
            if fp in seen:
                drops.append(
                    GateDrop(
                        proposal_id=str(p.get("proposal_id") or ""),
                        gate="intra_ag_dedup",
                        reason="duplicate_body_within_ag",
                        detail=(
                            f"body_fp={fp} duplicates earlier "
                            f"proposal in ag={ag_id}"
                        ),
                    )
                )
                continue
            seen.add(fp)
            kept.append(p)
        survived[ag_id] = tuple(kept)
    return survived, drops


def _run_content_fingerprint_dedup(
    ctx,
    proposals_by_ag: dict[str, tuple[dict[str, Any], ...]],
    rolled_back_fingerprints: set[str],
) -> tuple[dict[str, tuple[dict[str, Any], ...]], list[GateDrop]]:
    """PR-E: block byte-identical re-proposals across rollback classes."""
    survived: dict[str, tuple[dict[str, Any], ...]] = {}
    drops: list[GateDrop] = []
    for ag_id, props in proposals_by_ag.items():
        kept: list[dict[str, Any]] = []
        for p in props:
            fp = str(p.get("content_fingerprint") or "")
            if fp and fp in rolled_back_fingerprints:
                drops.append(
                    GateDrop(
                        proposal_id=str(p.get("proposal_id") or ""),
                        gate="content_fingerprint_dedup",
                        reason="rolled_back_fingerprint_repeat",
                        detail=f"fingerprint={fp[:12]}... was rolled back",
                    )
                )
            else:
                kept.append(p)
        survived[ag_id] = tuple(kept)
    return survived, drops


def _run_lever5_structural_gate(
    ctx,
    proposals_by_ag: dict[str, tuple[dict[str, Any], ...]],
) -> tuple[dict[str, tuple[dict[str, Any], ...]], list[GateDrop]]:
    """Lever-5 structural gate: proposals must carry non-empty patch
    content (``patch_text`` / ``value`` / ``new_text`` / ``example_sql``)."""
    survived: dict[str, tuple[dict[str, Any], ...]] = {}
    drops: list[GateDrop] = []
    for ag_id, props in proposals_by_ag.items():
        kept: list[dict[str, Any]] = []
        for p in props:
            content = (
                str(p.get("patch_text") or "")
                or str(p.get("value") or "")
                or str(p.get("new_text") or "")
                or str(p.get("example_sql") or "")
            )
            if content.strip():
                kept.append(p)
            else:
                drops.append(
                    GateDrop(
                        proposal_id=str(p.get("proposal_id") or ""),
                        gate="lever5_structural",
                        reason="empty_patch_content",
                        detail="patch carries no patch_text/value/new_text/example_sql",
                    )
                )
        survived[ag_id] = tuple(kept)
    return survived, drops


def _run_rca_groundedness_gate(
    ctx,
    proposals_by_ag: dict[str, tuple[dict[str, Any], ...]],
) -> tuple[dict[str, tuple[dict[str, Any], ...]], list[GateDrop]]:
    """RCA-groundedness gate: proposals must carry an ``rca_id`` linking
    them back to a clustered RCA finding. Orphan proposals (no rca_id)
    can't be grounded against the cross-checker contract and are dropped."""
    survived: dict[str, tuple[dict[str, Any], ...]] = {}
    drops: list[GateDrop] = []
    for ag_id, props in proposals_by_ag.items():
        kept: list[dict[str, Any]] = []
        for p in props:
            rca_id = str(p.get("rca_id") or "")
            if rca_id.strip():
                kept.append(p)
            else:
                drops.append(
                    GateDrop(
                        proposal_id=str(p.get("proposal_id") or ""),
                        gate="rca_groundedness",
                        reason="orphan_no_rca_id",
                        detail="proposal carries no rca_id; cannot ground against RCA contract",
                    )
                )
        survived[ag_id] = tuple(kept)
    return survived, drops


def _run_blast_radius_gate(
    ctx,
    proposals_by_ag: dict[str, tuple[dict[str, Any], ...]],
    table_cap: int = _DEFAULT_BLAST_RADIUS_TABLE_CAP,
) -> tuple[dict[str, tuple[dict[str, Any], ...]], list[GateDrop]]:
    """Blast-radius gate: a proposal touching more than ``table_cap``
    distinct tables is too wide and gets dropped."""
    survived: dict[str, tuple[dict[str, Any], ...]] = {}
    drops: list[GateDrop] = []
    for ag_id, props in proposals_by_ag.items():
        kept: list[dict[str, Any]] = []
        for p in props:
            affected = p.get("affected_tables") or []
            n_tables = len({str(t) for t in affected if str(t)})
            if n_tables <= int(table_cap):
                kept.append(p)
            else:
                drops.append(
                    GateDrop(
                        proposal_id=str(p.get("proposal_id") or ""),
                        gate="blast_radius",
                        reason="too_many_affected_tables",
                        detail=f"affected_tables={n_tables} > cap={table_cap}",
                    )
                )
        survived[ag_id] = tuple(kept)
    return survived, drops


def _run_doa_gate(
    ctx,
    proposals_by_ag: dict[str, tuple[dict[str, Any], ...]],
) -> tuple[dict[str, tuple[dict[str, Any], ...]], list[GateDrop], list[str]]:
    """Dead-on-arrival gate: proposals flagged as no-ops are dropped, and
    their ``doa_signature`` is recorded so future iterations can dedup
    against it."""
    survived: dict[str, tuple[dict[str, Any], ...]] = {}
    drops: list[GateDrop] = []
    new_signatures: list[str] = []
    for ag_id, props in proposals_by_ag.items():
        kept: list[dict[str, Any]] = []
        for p in props:
            if p.get("noop") is True:
                sig = str(p.get("doa_signature") or "")
                if sig:
                    new_signatures.append(sig)
                drops.append(
                    GateDrop(
                        proposal_id=str(p.get("proposal_id") or ""),
                        gate="dead_on_arrival",
                        reason="patch_application_is_noop",
                        detail="proposal flagged noop=True",
                    )
                )
            else:
                kept.append(p)
        survived[ag_id] = tuple(kept)
    return survived, drops, new_signatures


def run_gate(name: str, ctx, inp: GatesInput) -> GateOutcome:
    """Run a single gate sub-handler. Used by focused unit tests."""
    if name == "intra_ag_dedup":
        survived, drops = _run_intra_ag_dedup(ctx, inp.proposals_by_ag)
        return GateOutcome(survived_by_ag=survived, dropped=tuple(drops))
    if name == "content_fingerprint_dedup":
        survived, drops = _run_content_fingerprint_dedup(
            ctx, inp.proposals_by_ag, inp.rolled_back_content_fingerprints,
        )
        return GateOutcome(survived_by_ag=survived, dropped=tuple(drops))
    if name == "lever5_structural":
        survived, drops = _run_lever5_structural_gate(ctx, inp.proposals_by_ag)
        return GateOutcome(survived_by_ag=survived, dropped=tuple(drops))
    if name == "rca_groundedness":
        survived, drops = _run_rca_groundedness_gate(ctx, inp.proposals_by_ag)
        return GateOutcome(survived_by_ag=survived, dropped=tuple(drops))
    if name == "blast_radius":
        survived, drops = _run_blast_radius_gate(ctx, inp.proposals_by_ag)
        return GateOutcome(survived_by_ag=survived, dropped=tuple(drops))
    if name == "dead_on_arrival":
        survived, drops, sigs = _run_doa_gate(ctx, inp.proposals_by_ag)
        return GateOutcome(
            survived_by_ag=survived,
            dropped=tuple(drops),
            new_dead_on_arrival_signatures=tuple(sigs),
        )
    raise ValueError(f"Unknown gate: {name}")


def filter(ctx, inp: GatesInput) -> GateOutcome:
    """Stage 6 entry. Runs every sub-handler in GATE_PIPELINE_ORDER,
    accumulating drops and DOA signatures."""
    survived = dict(inp.proposals_by_ag)
    all_drops: list[GateDrop] = []
    new_doa_signatures: list[str] = []

    for gate_name in GATE_PIPELINE_ORDER:
        sub_inp = GatesInput(
            proposals_by_ag=survived,
            ags=inp.ags,
            rca_evidence=inp.rca_evidence,
            applied_history=inp.applied_history,
            rolled_back_content_fingerprints=inp.rolled_back_content_fingerprints,
            forbidden_signatures=inp.forbidden_signatures,
            space_snapshot=inp.space_snapshot,
        )
        sub_out = run_gate(gate_name, ctx, sub_inp)
        survived = dict(sub_out.survived_by_ag)
        all_drops.extend(sub_out.dropped)
        new_doa_signatures.extend(sub_out.new_dead_on_arrival_signatures)

    return GateOutcome(
        survived_by_ag=survived,
        dropped=tuple(all_drops),
        new_dead_on_arrival_signatures=tuple(new_doa_signatures),
    )


# ── Phase H: explicit Input/Output class declarations ─────────────────
# Phase H's per-stage I/O capture decorator imports these to serialize
# the stage's typed input and output to MLflow.
INPUT_CLASS = GatesInput
OUTPUT_CLASS = GateOutcome


# ── G-lite: uniform execute() alias ───────────────────────────────────
# The named verb above is preserved for human-readable harness call
# sites. The ``execute`` alias is what the stage registry, conformance
# test, and Phase H capture decorator import.
execute = filter
