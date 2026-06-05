"""State-machine contract invariants — 12 predicates, pure code.

Every invariant is a function ``(LocalRunArtifacts) -> InvariantResult``
that returns ``ok=True`` if the SM contract held for that run, or
``ok=False`` with a typed violation surface.

Invariants are organised by group (matching the v1.7 plan):

* **A. Lifecycle (A1-A3)** — every admitted QID terminates exactly
  once, stage order is monotone, no funnel-skipping.
* **B. Terminal records (B1-B3)** — every ``TerminalRecord`` carries a
  non-empty ``kind``, validation-gate terminals carry a non-empty
  ``forbidden_signature`` (Trial 16 wiring), and signatures are well-
  formed.
* **C. QID set conservation (C1-C2)** — input set equals terminal set,
  no QID appears in both accepted and terminated.
* **D. Marker contract (D1-D3)** — applied patches are followed by a
  downstream verdict, rejected verdicts carry well-formed
  ``predicate_inputs``, and the Trial 16 ``GSO_POST_APPLY_EVAL_SLICED_V1``
  marker is emitted when a non-trivial slice is requested.
* **E. Forbidden-signatures channel (E1-E2)** — every gate-emitted
  terminal carries a signature, and identical inputs produce identical
  signatures across permutations (stability is asserted by the
  permutation tests in chunk 2, not here).

The invariants are intentionally narrow and observational — they
observe markers and final states that production already emits. No new
policy logic.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping

from genie_space_optimizer.optimization.state_machine.funnel import (
    FunnelStage,
)


# ─── Public dataclasses ─────────────────────────────────────────────


@dataclass(frozen=True)
class InvariantViolation:
    """A single violation detected by one invariant on one run."""

    invariant_id: str
    invariant_name: str
    qid: str
    detail: str
    evidence: Mapping[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        return (
            f"{self.invariant_id} ({self.invariant_name}): "
            f"qid={self.qid or '<run>'}: {self.detail}"
        )


@dataclass(frozen=True)
class InvariantResult:
    """Aggregate result of running every invariant on one run."""

    violations: tuple[InvariantViolation, ...]

    @property
    def ok(self) -> bool:
        return not self.violations

    def by_id(self, invariant_id: str) -> tuple[InvariantViolation, ...]:
        return tuple(
            v for v in self.violations if v.invariant_id == invariant_id
        )


# ─── Stage-order helpers ────────────────────────────────────────────


# The funnel partial order. Acceptance and termination are both
# terminal stages and share the same rank — neither is "deeper" than
# the other. We allow either direction off APPLIED/EVALUATED.
_STAGE_RANK: dict[str, int] = {
    FunnelStage.HARD_QID_SEEN.value: 0,
    FunnelStage.DIAGNOSED.value: 1,
    FunnelStage.CLUSTERED.value: 2,
    FunnelStage.PROPOSED.value: 3,
    FunnelStage.NORMALIZED.value: 4,
    FunnelStage.APPLYABLE.value: 5,
    FunnelStage.APPLIED.value: 6,
    FunnelStage.EVALUATED.value: 7,
    FunnelStage.ACCEPTED.value: 8,
    FunnelStage.TERMINATED.value: 8,
}

_TERMINAL_STAGES: frozenset[str] = frozenset({
    FunnelStage.ACCEPTED.value,
    FunnelStage.TERMINATED.value,
})

# Gates whose rejections must carry a typed forbidden_signature
# (Trial 16 wiring). Outside this set (e.g. blast_radius_batch),
# rejections route to the proposal store rather than emitting a
# TerminalRecord, so B2/E1 do not apply.
_VALIDATION_GATE_NAMES: frozenset[str] = frozenset({
    "applier_gate",
    "evaluated_gate",
    "acceptance_gate",
    "structural_repair_gate",
})


# ─── stdout marker aggregation ──────────────────────────────────────


_MARKER_RE = re.compile(r"^(GSO_[A-Z0-9_]+_V\d+)\s+(\{.*\})$", re.MULTILINE)
_GATE_REASONING_RE = re.compile(
    r"^GSO_GATE_REASONING_V1 gate=(?P<gate>\S+) qid=(?P<qid>\S+) "
    r"verdict=(?P<verdict>\S+) reason=(?P<reason>.+?) "
    r"predicate_inputs=(?P<inputs>\{.*\})$",
    re.MULTILINE,
)


def _aggregate_typed_markers(stdout_text: str) -> dict[str, list[dict]]:
    """Group typed marker payloads by name (JSON-tail markers)."""
    out: dict[str, list[dict]] = {}
    for match in _MARKER_RE.finditer(stdout_text or ""):
        try:
            payload = json.loads(match.group(2))
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            out.setdefault(match.group(1), []).append(payload)
    return out


def _iter_gate_reasoning(stdout_text: str):
    """Yield each ``GSO_GATE_REASONING_V1`` line as a dict.

    This marker has a hybrid format (kv pairs + a JSON tail) so it is
    not caught by the generic ``_MARKER_RE``.
    """
    for match in _GATE_REASONING_RE.finditer(stdout_text or ""):
        yield {
            "gate": match.group("gate"),
            "qid": match.group("qid"),
            "verdict": match.group("verdict"),
            "reason": match.group("reason"),
            "predicate_inputs_raw": match.group("inputs"),
        }


def _stage_value(state: Any) -> str:
    """Read FunnelStage value off a state, robust to enum vs str."""
    deepest = getattr(state, "deepest_stage_reached", None)
    return str(getattr(deepest, "value", deepest) or "")


def _terminal(state: Any):
    return getattr(state, "terminal", None)


# ─── Invariant predicates ───────────────────────────────────────────


def _check_a1_single_terminal(
    artifacts: Any,
    markers: Mapping[str, list[dict]],
) -> list[InvariantViolation]:
    """A1: every admitted QID ends in a terminal state.

    "Terminal" here means one of:
    * ``state.terminal`` is a ``TerminalRecord`` (gate terminated the
      QID — ``current_stage`` is then ``TERMINATED`` and
      ``deepest_stage_reached`` retains the high-water mark the QID
      reached before termination), or
    * ``deepest_stage_reached`` is ``ACCEPTED`` (acceptance_gate took
      the green path).

    Note: we deliberately do *not* require
    ``deepest_stage_reached in {ACCEPTED, TERMINATED}`` because the SM
    preserves the deepest *forward* progress for postmortem visibility
    — a QID terminated from APPLYABLE legitimately keeps
    ``deepest_stage_reached='applyable'``. The terminal record is the
    authoritative signal.
    """
    out: list[InvariantViolation] = []
    for state in artifacts.final_states:
        rec = _terminal(state)
        if rec is not None:
            continue
        stage = _stage_value(state)
        if stage == FunnelStage.ACCEPTED.value:
            continue
        out.append(InvariantViolation(
            invariant_id="A1",
            invariant_name="single_terminal_stage",
            qid=str(getattr(state, "qid", "")),
            detail=(
                f"QID ended without a TerminalRecord and is not at "
                f"ACCEPTED; deepest_stage_reached={stage!r}. Every "
                f"admitted QID must end terminated or accepted."
            ),
            evidence={"deepest_stage_reached": stage},
        ))
    return out


def _check_a2_monotone_stage(
    artifacts: Any,
    markers: Mapping[str, list[dict]],
) -> list[InvariantViolation]:
    """A2: stage transitions are monotone per QID in the marker stream.

    Reads ``GSO_QSTATE_TRANSITION_V1`` markers and asserts each per-QID
    transition either advances or stays at the same rank. Validation
    gates that route back to PROPOSED (legacy retry path) are
    explicitly tolerated because they advance via a different
    invariant — A3 catches a skip, A2 catches a regression.
    """
    out: list[InvariantViolation] = []
    transitions = markers.get("GSO_QSTATE_TRANSITION_V1") or []
    seen_rank: dict[str, int] = {}
    for payload in transitions:
        qid = str(payload.get("qid") or "")
        to_stage = str(payload.get("to_stage") or "")
        rank = _STAGE_RANK.get(to_stage)
        if rank is None:
            continue
        prior = seen_rank.get(qid, -1)
        # Re-entry into the same stage is fine (e.g., reapply via
        # proposal store). Going *backwards* across the funnel is not.
        if rank < prior:
            out.append(InvariantViolation(
                invariant_id="A2",
                invariant_name="monotone_stage_rank",
                qid=qid,
                detail=(
                    f"stage regressed: rank({to_stage})={rank} < "
                    f"prior_rank={prior}"
                ),
                evidence={"to_stage": to_stage, "prior_rank": prior},
            ))
        else:
            seen_rank[qid] = rank
    return out


def _check_a3_no_stage_skip(
    artifacts: Any,
    markers: Mapping[str, list[dict]],
) -> list[InvariantViolation]:
    """A3: no transition skips a stage rank by more than one.

    The funnel allows ``X → next(X)`` or ``X → terminal``. Skipping
    DIAGNOSED → APPLIED would mean the proposal+validation lane was
    bypassed — a structural bug.
    """
    out: list[InvariantViolation] = []
    transitions = markers.get("GSO_QSTATE_TRANSITION_V1") or []
    for payload in transitions:
        qid = str(payload.get("qid") or "")
        from_stage = str(payload.get("from_stage") or "")
        to_stage = str(payload.get("to_stage") or "")
        if not from_stage or not to_stage:
            continue
        if to_stage in _TERMINAL_STAGES:
            # Termination from any non-terminal stage is allowed.
            continue
        if from_stage in _TERMINAL_STAGES:
            # Re-entry from a terminal stage is itself a B-class
            # invariant violation, not an A3 stage-skip.
            continue
        from_rank = _STAGE_RANK.get(from_stage)
        to_rank = _STAGE_RANK.get(to_stage)
        if from_rank is None or to_rank is None:
            continue
        # The state machine permits validation gates to route a
        # rejected proposal back to PROPOSED (legacy escalation
        # ladder), which looks like a "skip back" but is intentional.
        if to_rank <= from_rank:
            continue
        if to_rank - from_rank > 1:
            out.append(InvariantViolation(
                invariant_id="A3",
                invariant_name="no_funnel_skip",
                qid=qid,
                detail=(
                    f"transition {from_stage!r} -> {to_stage!r} skips "
                    f"{to_rank - from_rank - 1} intermediate stage(s)"
                ),
                evidence={"from": from_stage, "to": to_stage},
            ))
    return out


def _check_b1_terminal_kind_nonempty(
    artifacts: Any,
    markers: Mapping[str, list[dict]],
) -> list[InvariantViolation]:
    """B1: every TerminalRecord has a non-empty ``kind``."""
    out: list[InvariantViolation] = []
    for state in artifacts.final_states:
        rec = _terminal(state)
        if rec is None:
            continue
        kind = str(getattr(rec, "kind", "") or "")
        if not kind:
            out.append(InvariantViolation(
                invariant_id="B1",
                invariant_name="terminal_kind_nonempty",
                qid=str(getattr(state, "qid", "")),
                detail="TerminalRecord.kind is empty",
                evidence={"reason": str(getattr(rec, "reason", ""))},
            ))
    return out


def _check_b2_validation_gate_signature(
    artifacts: Any,
    markers: Mapping[str, list[dict]],
) -> list[InvariantViolation]:
    """B2: validation-gate terminals carry a non-empty forbidden_signature.

    This is the Trial 16 wiring guard. Up to Trial 15 the gates emitted
    ``forbidden_signature=""`` so the strategist's existing channel saw
    nothing. The invariant ensures regressions show up locally.

    We approximate "this terminal came from a validation gate" by
    matching the terminal's ``reason`` prefix against a known set
    (``applyability_rejected:``, ``post_apply_eval_failed:``,
    ``target_unchanged:``, ``collateral_regressions=``) — those are
    the only prefixes the four validation gates emit on reject.
    """
    out: list[InvariantViolation] = []
    gate_reason_prefixes = (
        "applyability_rejected:",
        "post_apply_eval_failed:",
        "target_unchanged:",
        "collateral_regressions=",
    )
    for state in artifacts.final_states:
        rec = _terminal(state)
        if rec is None:
            continue
        reason = str(getattr(rec, "reason", "") or "")
        if not any(reason.startswith(p) for p in gate_reason_prefixes):
            continue
        sig = str(getattr(rec, "forbidden_signature", "") or "")
        if not sig:
            out.append(InvariantViolation(
                invariant_id="B2",
                invariant_name="validation_gate_forbidden_signature_nonempty",
                qid=str(getattr(state, "qid", "")),
                detail=(
                    f"validation-gate terminal (reason={reason!r}) "
                    f"emitted empty forbidden_signature; Trial 16 "
                    f"requires the typed signature for the strategist "
                    f"feedback channel"
                ),
                evidence={"reason": reason, "kind": str(getattr(rec, "kind", ""))},
            ))
    return out


def _check_b3_signature_well_formed(
    artifacts: Any,
    markers: Mapping[str, list[dict]],
) -> list[InvariantViolation]:
    """B3: forbidden_signature strings are well-formed.

    Reject leading colon, trailing colon, double colons, leading/
    trailing whitespace, and empty-after-strip — these come from
    sloppy f-string composition (e.g. ``f"{kind}:{maybe_empty}"`` when
    ``maybe_empty=""``) and degrade strategist matching in the next
    iteration. Inner whitespace is allowed because production
    signatures are deliberately human-readable
    (e.g. ``target_unchanged: post_score <= pre_score``).
    """
    out: list[InvariantViolation] = []
    for state in artifacts.final_states:
        rec = _terminal(state)
        if rec is None:
            continue
        sig = str(getattr(rec, "forbidden_signature", "") or "")
        if not sig:
            continue  # B2 handles emptiness
        stripped = sig.strip()
        ill_formed = (
            not stripped
            or stripped.startswith(":")
            or stripped.endswith(":")
            or "::" in stripped
            or sig != stripped  # leading or trailing whitespace
        )
        if ill_formed:
            out.append(InvariantViolation(
                invariant_id="B3",
                invariant_name="signature_well_formed",
                qid=str(getattr(state, "qid", "")),
                detail=f"forbidden_signature has malformed shape: {sig!r}",
                evidence={"forbidden_signature": sig},
            ))
    return out


def _check_c1_qid_conservation(
    artifacts: Any,
    markers: Mapping[str, list[dict]],
) -> list[InvariantViolation]:
    """C1: every QID that appears in any transition has a final state.

    The workbench admits N hard QIDs and must produce exactly N final
    states — no silent drops, no fabricated QIDs mid-run. We
    cross-check the set of QIDs ever appearing in
    ``GSO_QSTATE_TRANSITION_V1`` markers against ``final_states``.

    Transitions emit a real ``from_stage`` even on the first call (the
    transformer dispatches from a non-terminal stage), so we use the
    union of all transition QIDs as the "ever seen" set.
    """
    transitions = markers.get("GSO_QSTATE_TRANSITION_V1") or []
    seen_in_transitions: set[str] = set()
    for payload in transitions:
        qid = str(payload.get("qid") or "")
        if qid:
            seen_in_transitions.add(qid)

    terminated = {str(getattr(s, "qid", "")) for s in artifacts.final_states}
    terminated.discard("")

    out: list[InvariantViolation] = []
    # Note: we do *not* flag (terminated - seen_in_transitions) because
    # final_states is the ground truth — admitted QIDs that never
    # transitioned (e.g. an early-exit gate before the first marker
    # fired) still legitimately appear in final_states. The real
    # structural violation is QIDs that *did* transition but vanished
    # from final_states.
    for qid in seen_in_transitions - terminated:
        out.append(InvariantViolation(
            invariant_id="C1",
            invariant_name="qid_conservation",
            qid=qid,
            detail=(
                "QID emitted a stage transition but no final state was "
                "recorded — silent drop between transformer and orchestrator"
            ),
        ))
    return out


def _check_c2_no_double_terminal(
    artifacts: Any,
    markers: Mapping[str, list[dict]],
) -> list[InvariantViolation]:
    """C2: no QID is both ACCEPTED and TERMINATED."""
    accepted: set[str] = set()
    terminated: set[str] = set()
    for state in artifacts.final_states:
        stage = _stage_value(state)
        qid = str(getattr(state, "qid", ""))
        if stage == FunnelStage.ACCEPTED.value:
            accepted.add(qid)
        elif stage == FunnelStage.TERMINATED.value:
            terminated.add(qid)
    overlap = accepted & terminated
    return [
        InvariantViolation(
            invariant_id="C2",
            invariant_name="no_double_terminal",
            qid=qid,
            detail="QID appears in both accepted and terminated sets",
        )
        for qid in overlap
    ]


def _check_d1_applied_has_downstream(
    artifacts: Any,
    markers: Mapping[str, list[dict]],
) -> list[InvariantViolation]:
    """D1: every ``GSO_PATCH_OUTCOME_V1 outcome=applied`` is followed
    by a downstream verdict for the same QID.

    Specifically: an ``evaluated_gate`` verdict (accept or reject) or
    an ``applier_gate verdict=rejected`` for the same QID. Without
    this, an applied patch goes unobserved — exactly the failure mode
    of pre-Trial-16 production where applied patches sat in APPLIED
    without an accept/reject decision.
    """
    out: list[InvariantViolation] = []
    applied_qids: list[str] = []
    for payload in markers.get("GSO_PATCH_OUTCOME_V1") or []:
        if str(payload.get("outcome") or "") == "applied":
            qid = str(payload.get("qid") or "")
            if qid:
                applied_qids.append(qid)

    downstream_qids: set[str] = set()
    for row in _iter_gate_reasoning(artifacts.stdout_text):
        if row["gate"] in {"evaluated_gate", "acceptance_gate"}:
            downstream_qids.add(row["qid"])
        if row["gate"] == "applier_gate" and row["verdict"] == "rejected":
            downstream_qids.add(row["qid"])

    # Accepted-path QIDs do not emit a GATE_REASONING_V1 marker
    # (markers fire on reject only), so cross-check final state stage.
    accepted_qids = {
        str(getattr(s, "qid", ""))
        for s in artifacts.final_states
        if _stage_value(s) == FunnelStage.ACCEPTED.value
    }
    downstream_qids |= accepted_qids

    for qid in applied_qids:
        if qid not in downstream_qids:
            out.append(InvariantViolation(
                invariant_id="D1",
                invariant_name="applied_has_downstream_verdict",
                qid=qid,
                detail=(
                    "patch was applied but no evaluated_gate / "
                    "acceptance_gate verdict nor applier_gate reject "
                    "followed (acceptance boundary not closed)"
                ),
            ))
    return out


def _check_d2_predicate_inputs_valid_json(
    artifacts: Any,
    markers: Mapping[str, list[dict]],
) -> list[InvariantViolation]:
    """D2: every rejected gate verdict has well-formed JSON predicate_inputs."""
    out: list[InvariantViolation] = []
    for row in _iter_gate_reasoning(artifacts.stdout_text):
        if row["verdict"] != "rejected":
            continue
        try:
            parsed = json.loads(row["predicate_inputs_raw"])
        except json.JSONDecodeError as exc:
            out.append(InvariantViolation(
                invariant_id="D2",
                invariant_name="predicate_inputs_valid_json",
                qid=row["qid"],
                detail=f"predicate_inputs is not valid JSON: {exc}",
                evidence={"raw": row["predicate_inputs_raw"]},
            ))
            continue
        if not isinstance(parsed, dict):
            out.append(InvariantViolation(
                invariant_id="D2",
                invariant_name="predicate_inputs_valid_json",
                qid=row["qid"],
                detail=(
                    f"predicate_inputs decoded to {type(parsed).__name__}, "
                    f"expected object"
                ),
            ))
    return out


def _check_d3_eval_slice_emitted(
    artifacts: Any,
    markers: Mapping[str, list[dict]],
) -> list[InvariantViolation]:
    """D3: Trial-16 RC1 marker fires whenever a post-apply eval ran
    with a non-empty ``eval_qids`` request.

    Today the workbench's post-apply path is a stub, so the production
    ``GSO_POST_APPLY_EVAL_SLICED_V1`` marker only fires when the real
    ``_run_full_evaluation`` is invoked. We assert this conditionally:
    if the marker was emitted, its ``requested_qids`` must be non-empty
    AND ``benchmarks_count`` must be ≤ that count (the slice took
    effect). If it never fires (workbench stub mode), this invariant
    is vacuously satisfied.
    """
    out: list[InvariantViolation] = []
    payloads = markers.get("GSO_POST_APPLY_EVAL_SLICED_V1") or []
    for payload in payloads:
        requested = list(payload.get("requested_qids") or ())
        bench_count = int(payload.get("benchmarks_count") or 0)
        if not requested:
            out.append(InvariantViolation(
                invariant_id="D3",
                invariant_name="eval_slice_marker_well_formed",
                qid="",
                detail=(
                    "GSO_POST_APPLY_EVAL_SLICED_V1 emitted with empty "
                    "requested_qids — the slice contract requires the "
                    "request to carry the patched QID"
                ),
                evidence=dict(payload),
            ))
            continue
        if bench_count > len(requested):
            out.append(InvariantViolation(
                invariant_id="D3",
                invariant_name="eval_slice_actually_sliced",
                qid="",
                detail=(
                    f"slice did not take effect: benchmarks_count="
                    f"{bench_count} > requested={len(requested)}"
                ),
                evidence=dict(payload),
            ))
    return out


def _check_e1_gate_terminal_has_signature(
    artifacts: Any,
    markers: Mapping[str, list[dict]],
) -> list[InvariantViolation]:
    """E1: every gate-emitted terminal carries a forbidden_signature.

    Equivalent to B2 from a different angle: cross-check via the
    GATE_REASONING marker stream — every ``verdict=rejected`` line for
    a validation gate must correspond to a final state with a non-
    empty ``forbidden_signature``.
    """
    out: list[InvariantViolation] = []
    gate_rejected_qids: set[str] = set()
    for row in _iter_gate_reasoning(artifacts.stdout_text):
        if (
            row["verdict"] == "rejected"
            and row["gate"] in _VALIDATION_GATE_NAMES
        ):
            gate_rejected_qids.add(row["qid"])

    by_qid = {
        str(getattr(s, "qid", "")): s for s in artifacts.final_states
    }
    for qid in gate_rejected_qids:
        state = by_qid.get(qid)
        if state is None:
            continue  # C1 catches the missing-state case
        rec = _terminal(state)
        if rec is None:
            # Rejected by a non-terminating gate (e.g. structural_repair
            # reject → re-propose), no signature required.
            continue
        sig = str(getattr(rec, "forbidden_signature", "") or "")
        if not sig:
            out.append(InvariantViolation(
                invariant_id="E1",
                invariant_name="gate_terminal_carries_signature",
                qid=qid,
                detail=(
                    "gate-rejected terminal lacks forbidden_signature; "
                    "strategist's feedback channel will be empty next "
                    "iteration"
                ),
                evidence={
                    "kind": str(getattr(rec, "kind", "")),
                    "reason": str(getattr(rec, "reason", "")),
                },
            ))
    return out


def _check_e2_signature_stable(
    artifacts: Any,
    markers: Mapping[str, list[dict]],
) -> list[InvariantViolation]:
    """E2: signature stability — within a single run, identical
    ``(patch_type, reason)`` pairs must produce identical signatures.

    The cross-permutation stability assertion lives in the chunk-2
    permutation tests (which take the same input and run several
    permutations, comparing signatures). E2 here is the within-run
    coherence check.
    """
    out: list[InvariantViolation] = []
    seen: dict[tuple[str, str], str] = {}
    for state in artifacts.final_states:
        rec = _terminal(state)
        if rec is None:
            continue
        sig = str(getattr(rec, "forbidden_signature", "") or "")
        if not sig:
            continue
        kind = str(getattr(rec, "kind", "") or "")
        reason = str(getattr(rec, "reason", "") or "")
        key = (kind, reason)
        prior = seen.setdefault(key, sig)
        if prior != sig:
            out.append(InvariantViolation(
                invariant_id="E2",
                invariant_name="signature_within_run_stability",
                qid=str(getattr(state, "qid", "")),
                detail=(
                    f"two terminals with kind={kind!r} reason={reason!r} "
                    f"emitted different signatures: {prior!r} vs {sig!r}"
                ),
            ))
    return out


# ─── Registry + entry point ─────────────────────────────────────────


# ─── Trial 18 invariants (group F + G) ──────────────────────────────


def _check_f1_kept_insufficient_distinct_from_accepted(
    artifacts: Any,
    markers: Mapping[str, list[dict]],
) -> list[InvariantViolation]:
    """F1 (Trial 18 Step 3): when ``state.accepted.decision`` is
    ``"kept_insufficient"``, the state must NOT contribute to
    ``accepted_count`` semantics.

    Concretely: a state whose accepted record carries
    ``decision="kept_insufficient"`` is in the KEPT_INSUFFICIENT lane;
    it must also carry a non-empty ``insufficient_repair_signature``
    so the next iteration's strategist receives the typed cumulative-
    learning signal. The pre-Trial-18 ACCEPTED/TERMINATED dichotomy is
    a degenerate case of this invariant (``decision="accepted"`` or
    no acceptance record at all).
    """
    out: list[InvariantViolation] = []
    for state in artifacts.final_states:
        accepted = getattr(state, "accepted", None)
        if accepted is None:
            continue
        decision = str(getattr(accepted, "decision", "") or "")
        if decision != "kept_insufficient":
            continue
        signature = str(
            getattr(accepted, "insufficient_repair_signature", "") or ""
        )
        if not signature:
            out.append(InvariantViolation(
                invariant_id="F1",
                invariant_name="kept_insufficient_emits_signature",
                qid=str(getattr(state, "qid", "")),
                detail=(
                    "decision='kept_insufficient' but "
                    "insufficient_repair_signature is empty — "
                    "cumulative learning signal lost."
                ),
            ))
    return out


def _check_f2_kept_insufficient_excluded_from_accepted_count(
    artifacts: Any,
    markers: Mapping[str, list[dict]],
) -> list[InvariantViolation]:
    """F2 (Trial 18 Step 3): kept_insufficient lane must never inflate
    accepted_count.

    The fuzzer doesn't know the dashboards' aggregation code, but it
    knows the contract: ``decision="kept_insufficient"`` is a
    distinct lane. Any GSO_ACCEPTANCE_GATE_V1 marker carrying
    ``decision="kept_insufficient"`` AND ``counted_as_accepted=true``
    is a contract violation. (The acceptance_gate never stamps
    ``counted_as_accepted``; this invariant guards against future
    regressions that bolt it on.)
    """
    out: list[InvariantViolation] = []
    for payload in markers.get("GSO_ACCEPTANCE_GATE_V1", []) or []:
        decision = str(payload.get("decision") or "")
        if decision != "kept_insufficient":
            continue
        if payload.get("counted_as_accepted") is True:
            out.append(InvariantViolation(
                invariant_id="F2",
                invariant_name="kept_insufficient_not_counted_as_accepted",
                qid=str(payload.get("qid") or ""),
                detail=(
                    "GSO_ACCEPTANCE_GATE_V1 carries decision="
                    "'kept_insufficient' with counted_as_accepted=true"
                ),
                evidence=dict(payload),
            ))
    return out


def _check_f3_kept_insufficient_marker_emitted(
    artifacts: Any,
    markers: Mapping[str, list[dict]],
) -> list[InvariantViolation]:
    """F3 (Trial 18 Step 3): when a final state carries
    ``decision="kept_insufficient"``, the typed marker
    ``GSO_ACCEPTANCE_KEPT_INSUFFICIENT_V1`` must be present in the
    stdout stream — otherwise postmortem cannot reconstruct the
    cumulative-learning evidence trail.

    Cardinality is observational, not strict: one marker per
    kept_insufficient outcome in the run. Fewer markers than states
    is a violation; more is fine (e.g. legacy retry paths).
    """
    out: list[InvariantViolation] = []
    kept_states = [
        s for s in artifacts.final_states
        if (
            getattr(getattr(s, "accepted", None), "decision", "")
            == "kept_insufficient"
        )
    ]
    if not kept_states:
        return out
    emitted = list(
        markers.get("GSO_ACCEPTANCE_KEPT_INSUFFICIENT_V1", []) or []
    )
    if len(emitted) < len(kept_states):
        out.append(InvariantViolation(
            invariant_id="F3",
            invariant_name="kept_insufficient_marker_emitted",
            qid="",
            detail=(
                f"{len(kept_states)} kept_insufficient final states "
                f"but only {len(emitted)} "
                f"GSO_ACCEPTANCE_KEPT_INSUFFICIENT_V1 markers emitted"
            ),
            evidence={
                "kept_states": len(kept_states),
                "markers": len(emitted),
            },
        ))
    return out


# ─── Trial 19 — Workbench v2.0 invariants (G1-G5) ──────────────────


def _check_g1_no_insufficient_signature_repeats(
    artifacts: Any,
    markers: Mapping[str, list[dict]],
) -> list[InvariantViolation]:
    """G1 (Trial 19 A1): no ``insufficient_repair_signature`` value
    repeats across iterations for the same QID.

    The admission gate must reject sole-primary repeats. If we see the
    same signature stamped on two final states for the same QID across
    two iterations, the gate did not fire — which is the production
    bug Trial 19 was designed to catch.
    """
    out: list[InvariantViolation] = []
    seen: dict[tuple[str, str], list[int]] = {}
    for s in getattr(artifacts, "final_states", ()) or ():
        decision = (
            getattr(getattr(s, "accepted", None), "decision", "") or ""
        )
        sig = (
            getattr(
                getattr(s, "accepted", None),
                "insufficient_repair_signature",
                "",
            )
            or ""
        )
        if decision != "kept_insufficient" or not sig:
            continue
        qid = getattr(s, "qid", "") or ""
        if not qid:
            continue
        iteration = int(getattr(s, "iteration", 0) or 0)
        seen.setdefault((qid, sig), []).append(iteration)
    for (qid, sig), iters in seen.items():
        if len(iters) >= 2:
            out.append(InvariantViolation(
                invariant_id="G1",
                invariant_name="no_insufficient_signature_repeats",
                qid=qid,
                detail=(
                    "insufficient_repair_signature emitted across "
                    f"{len(iters)} iterations for the same QID: "
                    "admission gate did not reject the repeat"
                ),
                evidence={
                    "signature": sig,
                    "iterations": sorted(iters),
                },
            ))
    return out


def _check_g2_audit_marker_correlates_with_context(
    artifacts: Any,
    markers: Mapping[str, list[dict]],
) -> list[InvariantViolation]:
    """G2 (Trial 19 A5): whenever the harness *consumes* a non-empty
    ``insufficient_repair_signatures`` tuple (i.e., signatures flow
    into Stage 2 / Stage 3 LLM calls), the
    ``GSO_INSUFFICIENT_SIGNATURES_IN_CONTEXT_V1`` audit marker must
    have been emitted alongside the LLM call. Catches plumbing
    regressions where the signal lands in ``ctx`` but never reaches
    the consumer.

    Detection model: signatures are *consumed* when the input bundle
    or the prior iteration carried them. They are *produced* by the
    acceptance gate on a ``kept_insufficient`` decision and surface on
    ``state.accepted.insufficient_repair_signature``. In a single-
    iteration tape replay (workbench fuzzer), signatures are only
    produced — never consumed — so the audit marker is correctly
    absent. We only flag G2 when we detect a consumer-side signal
    that is unmistakably a downstream LLM consumption: the
    ``GSO_OPTIMIZER_BUNDLE_INSUFFICIENT_SIGNATURES_V1`` marker (emitted
    by the harness when the bundle carries prior signatures into the
    current iteration). Pre-Trial-19 evidence had this marker absent
    too, so the invariant skips when the marker is missing (no
    signatures were consumed → no audit marker required).
    """
    out: list[InvariantViolation] = []
    # Consumer signal: did this iteration receive prior signatures?
    consumed = list(
        markers.get(
            "GSO_OPTIMIZER_BUNDLE_INSUFFICIENT_SIGNATURES_V1", [],
        )
        or []
    )
    if not consumed:
        return out
    audit = list(
        markers.get("GSO_INSUFFICIENT_SIGNATURES_IN_CONTEXT_V1", [])
        or []
    )
    if not audit:
        out.append(InvariantViolation(
            invariant_id="G2",
            invariant_name="audit_marker_correlates_with_context",
            qid="",
            detail=(
                "insufficient_repair_signatures consumed by the "
                "iteration (GSO_OPTIMIZER_BUNDLE_INSUFFICIENT_"
                "SIGNATURES_V1 emitted) but no GSO_INSUFFICIENT_"
                "SIGNATURES_IN_CONTEXT_V1 audit marker reached the "
                "Stage 2 / Stage 3 LLM call: plumbing regression"
            ),
            evidence={
                "audit_marker_count": 0,
                "consumed_marker_count": len(consumed),
            },
        ))
    return out


def _check_g3_no_absent_admit_when_intent_named(
    artifacts: Any,
    markers: Mapping[str, list[dict]],
) -> list[InvariantViolation]:
    """G3 (Trial 19 B4): the structural repair gate must never admit
    a proposal with ``emitted_patch_shape == "absent"`` when a
    non-empty ``intended_patch_shape`` is named. The gate must emit
    a ``retry_with_typed_feedback`` verdict instead.
    """
    out: list[InvariantViolation] = []
    verdicts = list(
        markers.get("GSO_STRUCTURAL_REPAIR_GATE_V1", []) or []
    )
    for v in verdicts:
        outcome = str(v.get("outcome", "") or "")
        emitted = str(v.get("emitted_patch_shape", "") or "")
        intent = str(v.get("intended_patch_shape", "") or "")
        if (
            outcome == "admitted"
            and emitted == "absent"
            and intent
            and intent.lower() != "structural"
        ):
            out.append(InvariantViolation(
                invariant_id="G3",
                invariant_name="no_absent_admit_when_intent_named",
                qid=str(v.get("qid", "") or ""),
                detail=(
                    "structural gate admitted absent shape against "
                    f"non-empty intent '{intent}'"
                ),
                evidence=dict(v),
            ))
    return out


def _check_g4_gt_pending_review_correlation(
    artifacts: Any,
    markers: Mapping[str, list[dict]],
) -> list[InvariantViolation]:
    """G4 (Trial 19 C3): the ``pending_gt_review_count`` reported on
    ``GSO_OPTIMIZER_OUTCOME_V1`` must equal (or be at least as large
    as) the count of detected arbiter-correct-but-GT-disagrees rows
    in the run. Captures a write-path regression.
    """
    out: list[InvariantViolation] = []
    outcome_msgs = list(
        markers.get("GSO_OPTIMIZER_OUTCOME_V1", []) or []
    )
    if not outcome_msgs:
        return out
    reported = sum(
        int(m.get("pending_gt_review_count", 0) or 0)
        for m in outcome_msgs
    )
    detected = int(
        getattr(artifacts, "trial19_pending_review_count", 0) or 0
    )
    if detected > 0 and reported < detected:
        out.append(InvariantViolation(
            invariant_id="G4",
            invariant_name="gt_pending_review_correlation",
            qid="",
            detail=(
                f"detected {detected} arbiter-correct-but-GT-disagrees "
                f"rows but outcome marker reported only {reported}"
            ),
            evidence={"detected": detected, "reported": reported},
        ))
    return out


def _check_g5_no_final_iter_ag_collision(
    artifacts: Any,
    markers: Mapping[str, list[dict]],
) -> list[InvariantViolation]:
    """G5 (Trial 19 A3 / A6): the last iteration of a run must not
    terminate on ``ag_collision_with_forbidden_set``. The regenerator
    wrapper should either produce a non-colliding AG (A3) or emit
    ``fallback_no_new_strategy`` (A6).
    """
    out: list[InvariantViolation] = []
    final_states = getattr(artifacts, "final_states", ()) or ()
    by_qid: dict[str, list[Any]] = {}
    for s in final_states:
        qid = getattr(s, "qid", "") or ""
        if qid:
            by_qid.setdefault(qid, []).append(s)
    for qid, states in by_qid.items():
        if not states:
            continue
        states_sorted = sorted(
            states, key=lambda s: int(getattr(s, "iteration", 0) or 0)
        )
        last = states_sorted[-1]
        terminal = getattr(last, "terminal", None)
        reason = str(
            getattr(terminal, "reason", "") or ""
        )
        if reason == "ag_collision_with_forbidden_set":
            out.append(InvariantViolation(
                invariant_id="G5",
                invariant_name="no_final_iter_ag_collision",
                qid=qid,
                detail=(
                    "final iteration terminated on "
                    "ag_collision_with_forbidden_set; expected typed "
                    "AG variant or fallback_no_new_strategy"
                ),
                evidence={
                    "iteration": int(getattr(last, "iteration", 0)),
                },
            ))
    return out


def _check_k1_no_no_applied_patches_after_kept_insufficient(
    artifacts: Any,
    markers: Mapping[str, list[dict]],
) -> list[InvariantViolation]:
    """K1 (Trial 20 B2): when any QID's state-machine final state
    records ``accepted.decision == "kept_insufficient"`` for an
    iteration, the iteration-terminal selector MUST surface
    ``kept_insufficient`` (or ``accepted``) rather than the legacy
    catch-all ``no_applied_patches``. The
    ``GSO_TRIAL20_KEPT_INSUFFICIENT_TERMINAL_V1`` marker signals that
    the selector overrode the catch-all; its absence when SM final
    state shows ``kept_insufficient`` is the contract split this
    invariant catches.
    """
    out: list[InvariantViolation] = []
    final_states = getattr(artifacts, "final_states", ()) or ()
    kept_iters_by_qid: dict[str, set[int]] = {}
    for s in final_states:
        acc = getattr(s, "accepted", None)
        if acc is None:
            continue
        dec = getattr(acc, "decision", None)
        if str(dec or "") != "kept_insufficient":
            continue
        qid = str(getattr(s, "qid", "") or "")
        it = int(getattr(s, "iteration", 0) or 0)
        if qid:
            kept_iters_by_qid.setdefault(qid, set()).add(it)
    if not kept_iters_by_qid:
        return out
    decided = markers.get("GSO_ITERATION_TERMINAL_DECIDED_V1", []) or []
    k1_marker = markers.get(
        "GSO_TRIAL20_KEPT_INSUFFICIENT_TERMINAL_V1", []
    ) or []
    overridden_iters = {
        int(m.get("iteration", -1) or -1) for m in k1_marker
    }
    for d in decided:
        it = int(d.get("iteration", -1) or -1)
        reason = str(d.get("terminal_reason") or d.get("reason") or "")
        if reason != "no_applied_patches":
            continue
        # If any QID has a kept_insufficient SM final state for this
        # iteration and the override marker did NOT fire, the
        # selector regressed.
        kept_present = any(
            it in iters for iters in kept_iters_by_qid.values()
        )
        if kept_present and it not in overridden_iters:
            out.append(InvariantViolation(
                invariant_id="K1",
                invariant_name="no_no_applied_patches_after_kept_insufficient",
                qid="",
                detail=(
                    "iteration terminated as no_applied_patches but at "
                    "least one QID's SM final state shows "
                    "accepted.decision=kept_insufficient; expected "
                    "kept_insufficient override marker"
                ),
                evidence={"iteration": it},
            ))
    return out


def _check_k2_no_same_family_pivot_after_kept_insufficient(
    artifacts: Any,
    markers: Mapping[str, list[dict]],
) -> list[InvariantViolation]:
    """K2 (Trial 20 C1+C2): after a ``kept_insufficient`` lane on a
    given patch family, Plan 12's ``next_patch_family_for_cluster``
    MUST NOT recommend the same family — the cycle-aware pivot graph
    cycles to a different family. Observed via
    ``GSO_PLAN12_PIVOT_RECOMMENDED_V1`` (or analogous) markers when
    ``prior_patch_family == next_patch_family``.
    """
    out: list[InvariantViolation] = []
    pivots = markers.get("GSO_PLAN12_PIVOT_RECOMMENDED_V1", []) or []
    for p in pivots:
        prior = str(p.get("prior_patch_family") or "")
        nxt = str(p.get("next_patch_family") or "")
        if prior and nxt and prior == nxt:
            out.append(InvariantViolation(
                invariant_id="K2",
                invariant_name="no_same_family_pivot_after_kept_insufficient",
                qid="",
                detail=(
                    "Plan 12 recommended a pivot to the same family "
                    f"({prior!r}); pivot graph should cycle"
                ),
                evidence={
                    "prior_patch_family": prior,
                    "next_patch_family": nxt,
                    "iteration": int(p.get("iteration", -1) or -1),
                },
            ))
    return out


def _check_k3_passing_dependents_stamped(
    artifacts: Any,
    markers: Mapping[str, list[dict]],
) -> list[InvariantViolation]:
    """K3 (Trial 20 E1+E2): the blast-radius gate must never reject
    a proposal for ``passing_dependents_missing``. With E1 plumbing,
    every Stage 3 proposal carries ``passing_dependents`` on its
    ``patch_body``. Any ``GSO_TRIAL20_BLAST_RADIUS_UNSTAMPED_V1``
    marker therefore signals a plumbing regression.
    """
    unstamped = markers.get(
        "GSO_TRIAL20_BLAST_RADIUS_UNSTAMPED_V1", []
    ) or []
    out: list[InvariantViolation] = []
    for u in unstamped:
        out.append(InvariantViolation(
            invariant_id="K3",
            invariant_name="passing_dependents_stamped",
            qid="",
            detail=(
                "blast_radius gate observed missing passing_dependents — "
                "E1 plumbing regression"
            ),
            evidence={
                "patch_type": str(u.get("patch_type") or ""),
                "intent_id": str(u.get("intent_id") or ""),
            },
        ))
    return out


def _check_k4_bundle_when_insufficient_present(
    artifacts: Any,
    markers: Mapping[str, list[dict]],
) -> list[InvariantViolation]:
    """K4 (Trial 20 D1+D3): whenever a cluster carries non-empty
    ``insufficient_repair_signatures`` for an iteration, Stage 3 must
    emit a multi-lever bundle. We detect violation when:

      * ``GSO_INSUFFICIENT_SIGNATURES_IN_CONTEXT_V1`` reports a non-zero
        count for a (run_id, iteration), AND
      * ``GSO_TRIAL20_BUNDLE_EMITTED_V1`` is NOT emitted for the same
        (run_id, iteration), AND
      * Stage 3 produced at least one proposal that iteration
        (``GSO_PLAN11_STAGE3_SYNTHESIS_V1`` outcome=synthesized).
    """
    out: list[InvariantViolation] = []
    sig_in_ctx = markers.get(
        "GSO_INSUFFICIENT_SIGNATURES_IN_CONTEXT_V1", []
    ) or []
    bundle_emitted = markers.get(
        "GSO_TRIAL20_BUNDLE_EMITTED_V1", []
    ) or []
    stage3 = markers.get("GSO_PLAN11_STAGE3_SYNTHESIS_V1", []) or []

    def _key(m: dict) -> tuple:
        return (
            str(m.get("run_id") or m.get("optimization_run_id") or ""),
            int(m.get("iteration", -1) or -1),
        )

    bundle_keys = {_key(b) for b in bundle_emitted}
    stage3_keys_with_proposals = {
        _key(s) for s in stage3
        if str(s.get("outcome") or "") == "synthesized"
    }
    for s in sig_in_ctx:
        count = int(s.get("count", 0) or 0)
        if count <= 0:
            continue
        key = _key(s)
        if key not in stage3_keys_with_proposals:
            continue
        if key not in bundle_keys:
            out.append(InvariantViolation(
                invariant_id="K4",
                invariant_name="bundle_required_when_insufficient_present",
                qid="",
                detail=(
                    "iteration carried insufficient_repair_signatures "
                    "but Stage 3 emitted no bundle"
                ),
                evidence={
                    "run_id": key[0],
                    "iteration": key[1],
                    "insufficient_count": count,
                },
            ))
    return out


def _check_k6_atomic_bundle_apply(
    artifacts: Any,
    markers: Mapping[str, list[dict]],
) -> list[InvariantViolation]:
    """K6 (Phase 2 P2.3) — atomic-bundle apply contract: a bundle's
    patches MUST land all-or-none. If the
    ``GSO_BUNDLE_APPLY_OUTCOME_V1`` marker stream reports
    ``status="partial"`` for any ``bundle_id``, the iteration MUST
    also emit a ``BUNDLE_PARTIAL_APPLY`` terminal_reason. Violations
    are emitted when a partial-apply marker exists without the
    paired terminal.

    The invariant is observational — both signals are already
    emitted by ``bundle_atomic_apply`` and the harness terminal
    bookkeeping; K6 ensures they stay paired across permutations.
    """
    out: list[InvariantViolation] = []
    apply_markers = markers.get("GSO_BUNDLE_APPLY_OUTCOME_V1", []) or []
    terminal_markers = markers.get(
        "GSO_ITERATION_NO_CANDIDATE_V1", []
    ) or []
    # Index terminals by (run_id, iteration) → terminal_reason.
    terminals_by_iter: dict[tuple[str, int], str] = {}
    for t in terminal_markers:
        key = (
            str(t.get("optimization_run_id") or t.get("run_id") or ""),
            int(t.get("iteration", -1) or -1),
        )
        terminals_by_iter[key] = str(t.get("terminal_reason") or "")
    for a in apply_markers:
        status = str(a.get("status") or "").lower()
        if status != "partial":
            continue
        key = (
            str(a.get("optimization_run_id") or a.get("run_id") or ""),
            int(a.get("iteration", -1) or -1),
        )
        reason = terminals_by_iter.get(key, "")
        if reason != "bundle_partial_apply":
            out.append(InvariantViolation(
                invariant_id="K6",
                invariant_name="atomic_bundle_apply",
                qid="",
                detail=(
                    "bundle status=partial but iteration terminal_reason "
                    "is not bundle_partial_apply"
                ),
                evidence={
                    "bundle_id": str(a.get("bundle_id") or ""),
                    "iteration": key[1],
                    "observed_terminal_reason": reason,
                },
            ))
    return out


def _check_k7_kit_completeness(
    artifacts: Any,
    markers: Mapping[str, list[dict]],
) -> list[InvariantViolation]:
    """K7 (Phase 2 P2.2) — KIT_FOR_RCA mandatory-companion contract:
    a proposal whose RCA kind is in ``KIT_FOR_RCA`` MUST carry a
    ``selected_levers`` kit that satisfies the contract (singleton
    kits or kits missing required companions are violations). We
    assert this via the ``GSO_PHASE2_KIT_FOR_RCA_VIOLATION_V1``
    marker emitted by the Stage 3 validator (P2.2). Every entry in
    the marker stream is a violation by construction — K7 lifts
    them to the InvariantResult so fuzzer runs surface kit-contract
    gaps as a single typed surface.
    """
    out: list[InvariantViolation] = []
    violations = markers.get(
        "GSO_PHASE2_KIT_FOR_RCA_VIOLATION_V1", []
    ) or []
    for v in violations:
        out.append(InvariantViolation(
            invariant_id="K7",
            invariant_name="kit_for_rca_completeness",
            qid=str(v.get("qid") or ""),
            detail=(
                f"KIT_FOR_RCA violation: {v.get('reason') or 'kit_incomplete'}"
            ),
            evidence={
                "rca_kind": str(v.get("rca_kind") or ""),
                "selected_levers": list(v.get("selected_levers") or ()),
                "iteration": int(v.get("iteration", -1) or -1),
            },
        ))
    return out


def _check_k8_iteration_itpm_ceiling(
    artifacts: Any,
    markers: Mapping[str, list[dict]],
) -> list[InvariantViolation]:
    """K8 (Phase 0 P0.1) — per-iteration ITPM ceiling: the sum of
    Stage 1 + Stage 3 input tokens consumed in a single iteration
    MUST stay ≤ 120,000 (the Opus rate-limit defense budget). We
    aggregate the ``GSO_ITERATION_TOKEN_BUDGET_V1`` marker stream
    by ``(run_id, iteration)`` and emit a violation for any
    iteration whose ``input_tokens_used`` exceeds the ceiling.

    The ceiling is a hard guard — exceeding it WILL trip Opus
    REQUEST_LIMIT_EXCEEDED at the FMAPI boundary and starve the
    iteration of diagnoses (the Trial 20 ``PLAN11_STAGE1_FMAPI_
    RATE_LIMIT_RUN_STARVED`` postmortem class).
    """
    ITPM_CEILING = 120_000
    out: list[InvariantViolation] = []
    budget_markers = markers.get(
        "GSO_ITERATION_TOKEN_BUDGET_V1", []
    ) or []
    used_by_iter: dict[tuple[str, int], int] = {}
    for b in budget_markers:
        key = (
            str(b.get("optimization_run_id") or b.get("run_id") or ""),
            int(b.get("iteration", -1) or -1),
        )
        used = int(b.get("input_tokens_used", 0) or 0)
        # Take the LATEST observed value per (run, iteration) — the
        # budget context emits intermediate samples and the final
        # close-out reading is the one we want to gate on.
        used_by_iter[key] = used
    for key, used in used_by_iter.items():
        if used > ITPM_CEILING:
            out.append(InvariantViolation(
                invariant_id="K8",
                invariant_name="iteration_itpm_ceiling",
                qid="",
                detail=(
                    f"iteration consumed {used} input tokens > ceiling "
                    f"{ITPM_CEILING}"
                ),
                evidence={
                    "iteration": key[1],
                    "input_tokens_used": used,
                    "ceiling": ITPM_CEILING,
                },
            ))
    return out


_REGISTRY: tuple[
    tuple[
        str,
        Callable[
            [Any, Mapping[str, list[dict]]],
            list[InvariantViolation],
        ],
    ],
    ...,
] = (
    ("A1", _check_a1_single_terminal),
    ("A2", _check_a2_monotone_stage),
    ("A3", _check_a3_no_stage_skip),
    ("B1", _check_b1_terminal_kind_nonempty),
    ("B2", _check_b2_validation_gate_signature),
    ("B3", _check_b3_signature_well_formed),
    ("C1", _check_c1_qid_conservation),
    ("C2", _check_c2_no_double_terminal),
    ("D1", _check_d1_applied_has_downstream),
    ("D2", _check_d2_predicate_inputs_valid_json),
    ("D3", _check_d3_eval_slice_emitted),
    ("E1", _check_e1_gate_terminal_has_signature),
    ("E2", _check_e2_signature_stable),
    # Trial 18 — KEPT_INSUFFICIENT lane purity / observability.
    ("F1", _check_f1_kept_insufficient_distinct_from_accepted),
    ("F2", _check_f2_kept_insufficient_excluded_from_accepted_count),
    ("F3", _check_f3_kept_insufficient_marker_emitted),
    # Trial 19 Workbench v2.0 — enforced-decision invariants.
    ("G1", _check_g1_no_insufficient_signature_repeats),
    ("G2", _check_g2_audit_marker_correlates_with_context),
    ("G3", _check_g3_no_absent_admit_when_intent_named),
    ("G4", _check_g4_gt_pending_review_correlation),
    ("G5", _check_g5_no_final_iter_ag_collision),
    # Trial 20 Workbench v2.1 — outer-rails invariants.
    ("K1", _check_k1_no_no_applied_patches_after_kept_insufficient),
    ("K2", _check_k2_no_same_family_pivot_after_kept_insufficient),
    ("K3", _check_k3_passing_dependents_stamped),
    ("K4", _check_k4_bundle_when_insufficient_present),
    # Phase 2/0 outer-rails invariants. K5 (Trial 20 D1
    # single_lever_justification) is retired in favour of the kit
    # contract (P2.1+P2.2) — single-lever proposals are now a
    # special case of ``selected_levers`` with cardinality 1, and
    # the justification field is deprecated.
    ("K6", _check_k6_atomic_bundle_apply),
    ("K7", _check_k7_kit_completeness),
    ("K8", _check_k8_iteration_itpm_ceiling),
)


def check_all_invariants(artifacts: Any) -> InvariantResult:
    """Run every invariant against the artefacts; return the aggregate.

    ``artifacts`` is a ``LocalRunArtifacts`` (or anything that exposes
    ``final_states`` and ``stdout_text``). Returns an
    :class:`InvariantResult` whose ``ok`` field is ``True`` iff every
    invariant held.
    """
    markers = _aggregate_typed_markers(getattr(artifacts, "stdout_text", ""))
    violations: list[InvariantViolation] = []
    for _id, fn in _REGISTRY:
        try:
            violations.extend(fn(artifacts, markers))
        except Exception as exc:  # noqa: BLE001 — invariants must not crash the suite
            violations.append(InvariantViolation(
                invariant_id=_id,
                invariant_name=fn.__name__,
                qid="",
                detail=f"invariant predicate raised: {exc}",
            ))
    return InvariantResult(violations=tuple(violations))


__all__ = [
    "InvariantResult",
    "InvariantViolation",
    "check_all_invariants",
]
