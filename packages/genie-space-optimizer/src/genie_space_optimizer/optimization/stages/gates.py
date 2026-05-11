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

from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


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
class GatesInput(JsonRoundTrip):
    """Input to stages.gates.filter.

    C15 Phase 4.2: mixes JsonRoundTrip for boundary-fixture replay.
    Sets (rolled_back_content_fingerprints, forbidden_signatures) are
    serialised as sorted lists and restored as sets in from_json.
    """

    proposals_by_ag: dict[str, tuple[dict[str, Any], ...]]
    ags: tuple[dict[str, Any], ...]
    rca_evidence: dict[str, dict[str, Any]] = field(default_factory=dict)
    applied_history: tuple[dict[str, Any], ...] = ()
    rolled_back_content_fingerprints: set[str] = field(default_factory=set)
    forbidden_signatures: set[str] = field(default_factory=set)
    space_snapshot: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:  # type: ignore[override]
        return {
            "proposals_by_ag": {
                ag_id: [dict(p) for p in props]
                for ag_id, props in (self.proposals_by_ag or {}).items()
            },
            "ags": [dict(ag) for ag in (self.ags or ())],
            "rca_evidence": {k: dict(v) for k, v in (self.rca_evidence or {}).items()},
            "applied_history": [dict(h) for h in (self.applied_history or ())],
            "rolled_back_content_fingerprints": sorted(
                self.rolled_back_content_fingerprints or set()
            ),
            "forbidden_signatures": sorted(
                self.forbidden_signatures or set()
            ),
            "space_snapshot": dict(self.space_snapshot or {}),
        }

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "GatesInput":  # type: ignore[override]
        return cls(
            proposals_by_ag={
                ag_id: tuple(dict(p) for p in props)
                for ag_id, props in (payload.get("proposals_by_ag") or {}).items()
            },
            ags=tuple(dict(ag) for ag in (payload.get("ags") or [])),
            rca_evidence={
                k: dict(v)
                for k, v in (payload.get("rca_evidence") or {}).items()
            },
            applied_history=tuple(
                dict(h) for h in (payload.get("applied_history") or [])
            ),
            rolled_back_content_fingerprints=set(
                payload.get("rolled_back_content_fingerprints") or []
            ),
            forbidden_signatures=set(
                payload.get("forbidden_signatures") or []
            ),
            space_snapshot=dict(payload.get("space_snapshot") or {}),
        )


@dataclass
class GateOutcome(JsonRoundTrip):
    """Output of stages.gates.filter.

    C15 Phase 4.2: mixes JsonRoundTrip for boundary-fixture replay.
    GateDrop instances are serialised as plain dicts and restored via
    GateDrop(**...) in from_json.
    """

    survived_by_ag: dict[str, tuple[dict[str, Any], ...]]
    dropped: tuple[GateDrop, ...] = ()
    new_dead_on_arrival_signatures: tuple[str, ...] = ()

    def to_json(self) -> dict[str, Any]:  # type: ignore[override]
        return {
            "survived_by_ag": {
                ag_id: [dict(p) for p in props]
                for ag_id, props in (self.survived_by_ag or {}).items()
            },
            "dropped": [
                {
                    "proposal_id": d.proposal_id,
                    "gate": d.gate,
                    "reason": d.reason,
                    "detail": d.detail,
                }
                for d in (self.dropped or ())
            ],
            "new_dead_on_arrival_signatures": list(
                self.new_dead_on_arrival_signatures or ()
            ),
        }

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "GateOutcome":  # type: ignore[override]
        return cls(
            survived_by_ag={
                ag_id: tuple(dict(p) for p in props)
                for ag_id, props in (payload.get("survived_by_ag") or {}).items()
            },
            dropped=tuple(
                GateDrop(
                    proposal_id=str(d.get("proposal_id", "")),
                    gate=str(d.get("gate", "")),
                    reason=str(d.get("reason", "")),
                    detail=str(d.get("detail", "")),
                )
                for d in (payload.get("dropped") or [])
            ),
            new_dead_on_arrival_signatures=tuple(
                payload.get("new_dead_on_arrival_signatures") or []
            ),
        )


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


@dataclass(frozen=True)
class StructuralCausalDrop:
    """Cycle 16 T4 — payload captured for every blast-radius drop whose
    dropped patch was a structural-shape causal patch (L6 expression /
    measure bound to the AG's RCA) AND was not replaced by a narrow
    survivor.

    The harness emits one ``STRUCTURAL_CAUSAL_DROPPED`` decision record
    per instance, then halts the AG with one
    ``NO_STRUCTURAL_ALTERNATIVE`` record + one NO_ACTION reflection
    entry so C13's forbidden-set picks up the constraint.
    """
    ag_rca_id: str
    original_proposal_id: str
    original_patch_type: str
    original_target: str
    drop_reason: str
    target_qids: tuple[str, ...]


_STRUCTURAL_CAUSAL_PATCH_TYPES: frozenset[str] = frozenset({
    "add_sql_snippet_expression",
    "add_sql_snippet_measure",
})


def detect_structural_causal_drop(
    *,
    blast_dropped: tuple[dict, ...] | list[dict],
    narrow_survivors: tuple[dict, ...] | list[dict],
    ag_rca_id: str,
    ag_target_qids: tuple[str, ...] | list[str],
) -> tuple[StructuralCausalDrop, ...]:
    """Cycle 16 T4 — return one ``StructuralCausalDrop`` per dropped
    structural-causal patch whose causal RCA was not replaced by any
    narrow survivor in this AG's drop list.

    A drop is *structural-causal* iff:
      * ``original_patch.patch_type ∈ {add_sql_snippet_expression,
        add_sql_snippet_measure}`` (structural shape), AND
      * ``original_patch.rca_id == ag_rca_id`` (causal — bound to the
        AG's RCA).

    A narrow survivor *replaces* a dropped patch iff
    ``survivor.derived_from == dropped.original_patch.proposal_id``.

    When ``ag_rca_id`` is empty (diagnostic AG with no inherited RCA),
    returns ``()`` — diagnostic AGs are not subject to this halt.

    Pure: no I/O.
    """
    rca = str(ag_rca_id or "").strip()
    if not rca:
        return ()
    survivor_derived_from: set[str] = {
        str(s.get("derived_from") or "").strip()
        for s in (narrow_survivors or ())
        if isinstance(s, dict)
    }
    survivor_derived_from.discard("")
    targets = tuple(
        str(q).strip()
        for q in (ag_target_qids or ())
        if str(q).strip()
    )
    out: list[StructuralCausalDrop] = []
    for drop in (blast_dropped or ()):
        if not isinstance(drop, dict):
            continue
        original = drop.get("original_patch") or {}
        if not isinstance(original, dict):
            continue
        ptype = str(original.get("patch_type") or "").strip()
        if ptype not in _STRUCTURAL_CAUSAL_PATCH_TYPES:
            continue
        orig_rca = str(original.get("rca_id") or "").strip()
        if orig_rca != rca:
            continue
        original_pid = str(original.get("proposal_id") or "").strip()
        if original_pid and original_pid in survivor_derived_from:
            continue
        out.append(StructuralCausalDrop(
            ag_rca_id=rca,
            original_proposal_id=original_pid,
            original_patch_type=ptype,
            original_target=str(
                original.get("target")
                or drop.get("target")
                or ""
            ),
            drop_reason=str(drop.get("reason") or ""),
            target_qids=targets,
        ))
    return tuple(out)


# ── RCO-4: production blast-radius gate orchestration ─────────────────
#
# Pure orchestration extracted from harness.py:20860-20940. The
# underlying predicates ``patch_blast_radius_is_safe`` and
# ``instruction_patch_scope_is_safe`` already live in
# ``optimization/proposal_grounding.py`` and are pure. This helper
# wraps the per-candidate iteration + dropped-record-shape construction
# so the harness call site collapses to one delegation under
# ``GSO_STAGE6_BLAST_RADIUS_PURE``.


def run_blast_radius_production_gate(
    inp: "BlastRadiusProductionInput",
) -> "BlastRadiusProductionOutcome":
    """RCO-4 Task 5 — production blast-radius orchestration.

    Iterates the candidate patches, calls the two pure predicates
    (``patch_blast_radius_is_safe`` and ``instruction_patch_scope_is_safe``)
    on each, and accumulates kept / dropped lists with the same field
    shape that the legacy harness inline code at ~harness.py:20860-20940
    produces.

    Returns a frozen ``BlastRadiusProductionOutcome``. Pure: no I/O,
    no journey emission, no DecisionRecord construction (the harness
    still owns those side effects in the legacy code path).
    """
    from genie_space_optimizer.optimization.proposal_grounding import (
        instruction_patch_scope_is_safe,
        patch_blast_radius_is_safe,
    )
    from genie_space_optimizer.optimization.stages.gate_types import (
        BlastRadiusProductionOutcome,
    )

    kept: list[dict[str, Any]] = []
    dropped: list[dict[str, Any]] = []

    for candidate in inp.patches:
        decision = patch_blast_radius_is_safe(
            candidate,
            ag_target_qids=inp.ag_target_qids,
            max_outside_target=int(inp.max_outside_target),
            live_hard_qids=inp.live_hard_qids,
        )
        if not decision["safe"]:
            dropped.append({
                "proposal_id": str(
                    candidate.get("proposal_id")
                    or candidate.get("id")
                    or "?"
                ),
                "patch_type": str(
                    candidate.get("type")
                    or candidate.get("patch_type")
                    or "?"
                ),
                "reason": decision["reason"],
                "passing_dependents_outside_target": decision.get(
                    "passing_dependents_outside_target", []
                ),
                "target": str(
                    candidate.get("target")
                    or candidate.get("table")
                    or ""
                ),
                "original_patch": candidate,
            })
            continue

        scope_decision = instruction_patch_scope_is_safe(
            candidate,
            ag_target_qids=inp.ag_target_qids,
        )
        if not scope_decision["safe"]:
            dropped.append({
                "proposal_id": str(
                    candidate.get("proposal_id")
                    or candidate.get("id")
                    or "?"
                ),
                "patch_type": str(
                    candidate.get("type")
                    or candidate.get("patch_type")
                    or "?"
                ),
                "reason": scope_decision["reason"],
                "passing_dependents_outside_target": [],
                "target": str(
                    candidate.get("target")
                    or candidate.get("table")
                    or ""
                ),
                "original_patch": candidate,
            })
            continue

        kept.append(candidate)

    return BlastRadiusProductionOutcome(
        kept=tuple(kept),
        dropped=tuple(dropped),
    )


def resolve_narrow_replacement(
    inp: "NarrowReplacementInput",
    *,
    narrow_survivors: tuple[dict[str, Any], ...] | list[dict[str, Any]] = (),
) -> "NarrowReplacementOutcome":
    """RCO-4 Task 7 — pure narrow-replacement orchestration.

    Given the AG-level blast-radius drops AND the already-computed
    narrow survivors from ``_run_narrow_l6_replacement_loop`` (which
    the harness still calls; this helper is downstream of that loop),
    returns:

    * ``narrow_survivors``: passthrough of the loop's survivors.
    * ``structural_causal_dropped``: the structural-causal drops that
      have no narrow-survivor replacement (i.e., the halt-causing set).
    * ``halt_no_structural_alternative``: True iff
      ``structural_causal_dropped`` is non-empty AND ``ag_rca_id`` is
      present (diagnostic AGs with no inherited RCA are not subject
      to the halt).

    Reuses the already-pure ``detect_structural_causal_drop`` from
    Cycle 16 T4. Pure: no I/O, no journey emission.
    """
    from genie_space_optimizer.optimization.stages.gate_types import (
        NarrowReplacementOutcome,
    )

    survivors_tuple = tuple(narrow_survivors or ())

    rca = str(inp.ag_rca_id or "").strip()
    if not rca:
        return NarrowReplacementOutcome(
            narrow_survivors=survivors_tuple,
            structural_causal_dropped=(),
            halt_no_structural_alternative=False,
        )

    structural_drops = detect_structural_causal_drop(
        blast_dropped=inp.blast_dropped,
        narrow_survivors=survivors_tuple,
        ag_rca_id=rca,
        ag_target_qids=inp.ag_target_qids,
    )

    # ``StructuralCausalDrop`` is a frozen dataclass; serialize to dicts
    # so the outcome stays JSON-roundtrippable.
    serialized = tuple(
        {
            "ag_rca_id": d.ag_rca_id,
            "original_proposal_id": d.original_proposal_id,
            "original_patch_type": d.original_patch_type,
            "original_target": d.original_target,
            "drop_reason": d.drop_reason,
            "target_qids": list(d.target_qids),
        }
        for d in structural_drops
    )

    return NarrowReplacementOutcome(
        narrow_survivors=survivors_tuple,
        structural_causal_dropped=serialized,
        halt_no_structural_alternative=bool(serialized),
    )


def run_applyability_gate(
    inp: "ApplyabilityGateInput",
) -> "ApplyabilityGateOutcome":
    """RCO-4 Task 8 — stage-uniform wrapper over the pure
    ``optimization/patch_applyability.py`` module.

    Thin adapter. The actual applyability decision lives in
    ``patch_applyability.check_patch_applyability`` and the public
    decision function in that module. This wrapper exists only to
    give Stage-6 callers a uniform ``(typed_input) → typed_outcome``
    surface; no logic moves and no flag changes behavior here. The
    flag-gated harness call site is wired in Task 9.

    Pure: no I/O. The applyability dry-run already deep-copies the
    metadata snapshot — see ``patch_applyability`` module docstring.
    """
    from genie_space_optimizer.optimization.patch_applyability import (
        check_patch_applyability,
    )
    from genie_space_optimizer.optimization.stages.gate_types import (
        ApplyabilityGateOutcome,
    )

    applyable: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for candidate in inp.candidates:
        decision = check_patch_applyability(
            patch=candidate,
            metadata_snapshot=inp.metadata_snapshot,
            space_id="",
        )
        if decision.applyable:
            applyable.append(candidate)
        else:
            rejected.append({
                "proposal_id": decision.proposal_id,
                "expanded_patch_id": decision.expanded_patch_id,
                "patch_type": decision.patch_type,
                "target": decision.target,
                "table": decision.table,
                "column": decision.column,
                "applyable": decision.applyable,
                "reason": decision.reason,
                "error_excerpt": decision.error_excerpt,
            })
    return ApplyabilityGateOutcome(
        applyable=tuple(applyable),
        rejected=tuple(rejected),
    )
