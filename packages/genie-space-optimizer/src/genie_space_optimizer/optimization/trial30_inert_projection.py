"""Trial 30 W30.3 — pure projection of inert-reroute SM states.

When the acceptance gate fires the ``kit_forced_inert_reroute`` lane the
state machine records the verdict on ``state.accepted`` but the live
harness historically dropped two pieces of forensic evidence:

* no ``Trial29InertPatchDiagnostic`` JSONL was persisted into the run's
  evidence bundle (the persistence helper existed + was unit-tested but
  was never wired into the live acceptance path), and
* the decision never projected into ``genie_eval_lever_loop_decisions``
  (postmortems had to fall back to log-grep for the reroute count).

This module is the **pure** boundary between the SM final states and
those two sinks. It reads the typed SM state records and returns typed
outputs — :class:`Trial29InertPatchDiagnostic` for the JSONL bundle and
:class:`InertDecisionRow` for the decisions table. The harness wiring
(env reads, file writes, Spark inserts) lives in ``harness.py``; this
module performs no IO and no env reads, so it is fully unit-testable and
generalizes across every ``(qid, rca_kind)`` family — no per-QID or
per-anchor branching.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

from pydantic import BaseModel, ConfigDict

from genie_space_optimizer.optimization.inert_patch_diagnostic import (
    Trial29InertPatchDiagnostic,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from genie_space_optimizer.optimization.state_machine.state import (
        QuestionStateInIteration,
    )

# The single decision literal this module projects. Kept in one place so
# the diagnostic builder, the decision-row builder, and the gate agree.
_DECISION_LITERAL = "kit_forced_inert_reroute"
_GATE_NAME = "acceptance_gate"
_DEFAULT_TRIAL = "trial30"


def _is_reroute(state: Any) -> bool:
    """True iff this state's acceptance verdict is the inert-reroute lane.

    Reads ``state.accepted.decision`` defensively (mirrors the existing
    harvest extractors in ``harness.py`` which use ``getattr``) so a
    state that never reached the acceptance gate contributes nothing.
    """
    accepted = getattr(state, "accepted", None)
    return (
        accepted is not None
        and getattr(accepted, "decision", "") == _DECISION_LITERAL
    )


def _canonical_rca(state: Any) -> str:
    """Canonicalize ``state.diagnosed.rca_kind_label`` to the RCA key set.

    Uses the same ``action_groups._normalize_rca_kind`` the acceptance
    gate's reroute lane uses, so the projected ``rca_kind`` matches the
    harvested ``InertMechanismHistory.rca_kind`` and the Stage 3 guard
    key.
    """
    from genie_space_optimizer.optimization.stages.action_groups import (
        _normalize_rca_kind,
    )

    diagnosed = getattr(state, "diagnosed", None)
    raw = str(getattr(diagnosed, "rca_kind_label", "") or "")
    return _normalize_rca_kind(raw)


def _patch_descriptor(state: Any) -> dict[str, Any]:
    """A light, JSON-safe forensic descriptor of the applied patch.

    The full patch body lives in the proposal store keyed by intent-id
    and is not threaded to the harvest site; the diagnostic only needs
    enough to disambiguate which mechanism was inert, so we record the
    applied intent ids, the attempt index, and the latest proposal's
    patch_type. ``patch_json`` on the diagnostic is optional (defaults
    to ``{}``) so a state missing these records still projects cleanly.
    """
    descriptor: dict[str, Any] = {}
    applied = getattr(state, "applied", None)
    if applied is not None:
        descriptor["applied_intent_ids"] = list(
            getattr(applied, "applied_intent_ids", ()) or ()
        )
        descriptor["proposal_attempt_index"] = getattr(
            applied, "proposal_attempt_index", None
        )
    proposals = getattr(state, "proposals", ()) or ()
    if proposals:
        descriptor["patch_type"] = str(
            getattr(proposals[-1], "patch_type", "") or ""
        )
    return descriptor


def build_inert_patch_diagnostics(
    states: Sequence["QuestionStateInIteration"] | None,
    *,
    trial: str = _DEFAULT_TRIAL,
) -> tuple[Trial29InertPatchDiagnostic, ...]:
    """Build one :class:`Trial29InertPatchDiagnostic` per inert reroute.

    Iterates the iteration's final SM states and emits a typed diagnostic
    for every state whose acceptance verdict is ``kit_forced_inert_reroute``.
    Returns an empty tuple when no reroute happened (byte-stable on green
    replays).
    """
    out: list[Trial29InertPatchDiagnostic] = []
    for st in states or ():
        if not _is_reroute(st):
            continue
        accepted = st.accepted  # type: ignore[union-attr]
        evaluated = getattr(st, "evaluated", None)
        out.append(
            Trial29InertPatchDiagnostic(
                qid=str(getattr(st, "qid", "") or ""),
                rca_kind=_canonical_rca(st),
                rejected_mechanism=str(
                    getattr(accepted, "rejected_mechanism", "") or ""
                ),
                patch_json=_patch_descriptor(st),
                pre_arbiter_score=float(
                    getattr(evaluated, "pre_apply_score", 0.0) or 0.0
                ),
                post_arbiter_score=float(
                    getattr(evaluated, "post_apply_score", 0.0) or 0.0
                ),
                behavioral_diff=str(
                    getattr(accepted, "behavioral_diff", "unchanged")
                    or "unchanged"
                ),
                signature=str(
                    getattr(accepted, "insufficient_repair_signature", "")
                    or ""
                ),
                iteration=int(getattr(st, "iteration", 0) or 0),
                trial=trial,
            )
        )
    return tuple(out)


class InertDecisionRow(BaseModel):
    """Typed row for the ``genie_eval_lever_loop_decisions`` projection.

    The decisions table sink (``state.write_lever_loop_decisions``) is a
    pre-existing dict-based API; rather than widen it, this typed model
    is the projection's boundary and :meth:`to_decision_row` adapts to
    the legacy dict shape at the call site.
    """

    model_config = ConfigDict(frozen=True)

    run_id: str
    iteration: int
    ag_id: str = ""
    qid: str
    rca_kind: str
    rejected_mechanism: str
    signature: str
    pre_arbiter_score: float
    post_arbiter_score: float
    behavioral_diff: str = "unchanged"

    def to_decision_row(self) -> dict[str, Any]:
        """Adapt to the ``write_lever_loop_decisions`` row contract.

        ``run_id``, ``gate_name``, and ``decision`` must all be non-empty
        or the writer skips the row.
        """
        return {
            "run_id": self.run_id,
            "iteration": self.iteration,
            "ag_id": self.ag_id or None,
            "gate_name": _GATE_NAME,
            "decision": _DECISION_LITERAL,
            "reason_code": self.rca_kind or None,
            "reason_detail": (
                f"{_DECISION_LITERAL}:rca={self.rca_kind}:"
                f"rejected={self.rejected_mechanism}:"
                f"behavior={self.behavioral_diff}"
            ),
            "affected_qids": [self.qid] if self.qid else [],
            "metrics": {
                "rejected_mechanism": self.rejected_mechanism,
                "rca_kind": self.rca_kind,
                "pre_arbiter_score": self.pre_arbiter_score,
                "post_arbiter_score": self.post_arbiter_score,
                "signature": self.signature,
            },
        }


def build_inert_decision_rows(
    states: Sequence["QuestionStateInIteration"] | None,
    *,
    run_id: str,
    iteration: int,
    ag_id: str = "",
) -> tuple[InertDecisionRow, ...]:
    """Build one :class:`InertDecisionRow` per inert reroute.

    Mirrors :func:`build_inert_patch_diagnostics` but carries the run /
    iteration / ag context needed for the decisions table. Empty tuple
    when no reroute happened.
    """
    out: list[InertDecisionRow] = []
    for st in states or ():
        if not _is_reroute(st):
            continue
        accepted = st.accepted  # type: ignore[union-attr]
        evaluated = getattr(st, "evaluated", None)
        out.append(
            InertDecisionRow(
                run_id=str(run_id or ""),
                iteration=int(iteration or 0),
                ag_id=str(ag_id or ""),
                qid=str(getattr(st, "qid", "") or ""),
                rca_kind=_canonical_rca(st),
                rejected_mechanism=str(
                    getattr(accepted, "rejected_mechanism", "") or ""
                ),
                signature=str(
                    getattr(accepted, "insufficient_repair_signature", "")
                    or ""
                ),
                pre_arbiter_score=float(
                    getattr(evaluated, "pre_apply_score", 0.0) or 0.0
                ),
                post_arbiter_score=float(
                    getattr(evaluated, "post_apply_score", 0.0) or 0.0
                ),
                behavioral_diff=str(
                    getattr(accepted, "behavioral_diff", "unchanged")
                    or "unchanged"
                ),
            )
        )
    return tuple(out)


__all__ = [
    "InertDecisionRow",
    "build_inert_patch_diagnostics",
    "build_inert_decision_rows",
]
