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
