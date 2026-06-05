"""Trial 22 W5 — full-eval ↔ patch/admission lineage reconciliation.

Problem (e943 postmortem)
-------------------------
Run ``e94376a3`` reported an accepted full-eval candidate at 95.8%
(+8.3pp) while the state-machine ledger said ``OPTIMIZER_NO_CANDIDATES``
with zero patch/admission rows. The full-eval acceptance path was
*disconnected* from the patch/admission lineage: a number on a
scoreboard with no candidate that actually reached the applier. That is
not an optimizer win — it is a ledger contradiction. Counting it as
``best_accuracy`` masks the fact that the optimizer produced nothing.

W5.0 — canonical lineage key audit
-----------------------------------
The design review's concern was that keying lineage on
``(optimization_run_id, iteration, intent_id)`` would wrongly orphan
legitimate full-eval candidates, because the full-eval emitter is NOT
keyed on ``intent_id``. The audit of the three emitters confirms this:

================================  ===================================================
Marker                            Key fields actually emitted
================================  ===================================================
``GSO_FULL_EVAL_V1``              ``optimization_run_id`` + payload
                                  ``{iteration, ag_id, target_qids, ...}`` —
                                  **NO** ``intent_id``; ``candidate_id`` only when
                                  the four-tier render carries it.
``GSO_PATCH_OUTCOME_V1``          ``optimization_run_id, iteration, ag_id,
                                  cluster_id, intent_id, applied_patch_id``
``GSO_ADMISSION_DECISION_V1``     ``run_id, iteration, qid, proposal_index`` —
                                  legacy key name ``run_id`` (≡
                                  ``optimization_run_id``); historically **NO**
                                  ``ag_id``. Trial 22 W5.0 adds ``ag_id`` +
                                  ``optimization_run_id`` (additive) at the
                                  emit site so it carries the canonical key.
================================  ===================================================

The only key common to all three (post-W5.0 plumbing) is therefore:

    CANONICAL_LINEAGE_KEY = (optimization_run_id, ag_id, iteration)

``candidate_id`` is used as a finer discriminator WHEN present on both
sides, never as a required key part (full-eval rows predating the
four-tier render do not carry it). ``intent_id`` is explicitly NOT in
the canonical key — that was the design-review trap.

W5.1 — the invariant
---------------------
``GSO_FULL_EVAL_V1.accepted == true`` MUST imply a matching
``GSO_PATCH_OUTCOME_V1`` (an *applied* outcome) AND a matching
``GSO_ADMISSION_DECISION_V1`` (an *admitted* decision) on the canonical
key. When the match is missing, the acceptance is stamped
``provenance = "orphan_acceptance"`` and EXCLUDED from
``scoreboard.best_accuracy`` (it stays at baseline). A
``GSO_TRIAL22_LINEAGE_VIOLATION_V1`` marker records the contradiction
so postmortems classify it as external/partial-harvest evidence, not an
optimizer win.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence


CANONICAL_LINEAGE_KEY: tuple[str, ...] = (
    "optimization_run_id",
    "ag_id",
    "iteration",
)

ORPHAN_ACCEPTANCE: str = "orphan_acceptance"
LINEAGE_OK: str = "ok"


def _run_id_of(row: Mapping[str, Any]) -> str:
    """Normalize the run-id field across emitters.

    ``GSO_ADMISSION_DECISION_V1`` historically used ``run_id`` while
    full-eval / patch-outcome use ``optimization_run_id``. Accept either.
    """
    return str(
        row.get("optimization_run_id")
        or row.get("run_id")
        or ""
    )


def canonical_key(row: Mapping[str, Any]) -> tuple[str, str, int]:
    """Project a marker row onto the canonical lineage key
    ``(optimization_run_id, ag_id, iteration)``.

    ``iteration`` falls back to ``-1`` when absent so a missing
    iteration never silently joins to a real one.
    """
    try:
        iteration = int(row.get("iteration", -1))
    except (TypeError, ValueError):
        iteration = -1
    return (_run_id_of(row), str(row.get("ag_id") or ""), iteration)


@dataclass(frozen=True)
class FullEvalLineageResult:
    """Per-full-eval-row reconciliation verdict."""

    key: tuple[str, str, int]
    candidate_id: str
    accepted: bool
    candidate_accuracy: float
    lineage_verdict: str  # LINEAGE_OK | ORPHAN_ACCEPTANCE
    provenance: str       # "" | ORPHAN_ACCEPTANCE
    has_patch_outcome: bool
    has_admission_decision: bool


@dataclass
class LineageReconciliation:
    """Aggregate result of :func:`enforce_full_eval_lineage`."""

    results: tuple[FullEvalLineageResult, ...]
    best_accuracy: float
    baseline_accuracy: float
    orphan_count: int
    markers: tuple[str, ...] = field(default_factory=tuple)

    def provenance_for(self, key: tuple[str, str, int]) -> str:
        for r in self.results:
            if r.key == key:
                return r.provenance
        return ""


def _candidate_matches(
    fe_key: tuple[str, str, int],
    fe_cand: str,
    row: Mapping[str, Any],
) -> bool:
    """A patch/admission row matches a full-eval row when their
    canonical keys are equal AND, when BOTH carry a ``candidate_id``,
    the candidate_ids agree. A missing candidate_id on either side
    falls back to the canonical-key match (so pre-four-tier full-eval
    rows still reconcile)."""
    if canonical_key(row) != fe_key:
        return False
    row_cand = str(row.get("candidate_id") or "")
    if fe_cand and row_cand:
        return fe_cand == row_cand
    return True


def _is_applied_patch(row: Mapping[str, Any]) -> bool:
    if "applied" in row:
        return bool(row.get("applied"))
    return str(row.get("outcome_kind") or "").lower() == "applied"


def _is_admitted(row: Mapping[str, Any]) -> bool:
    return str(row.get("decision") or "").lower() == "admitted"


def lineage_key_audit_marker() -> str:
    """W5.0 — emit ``GSO_TRIAL22_LINEAGE_KEY_AUDIT_V1`` documenting the
    canonical key and which emitters carry which parts. Lets a
    postmortem confirm the join key without re-deriving it."""
    from genie_space_optimizer.optimization.run_analysis_contract import (
        marker_line,
    )

    return marker_line(
        "GSO_TRIAL22_LINEAGE_KEY_AUDIT_V1",
        {
            "canonical_key": list(CANONICAL_LINEAGE_KEY),
            "intent_id_in_key": False,
            "candidate_id_role": "discriminator_when_present",
            "emitter_key_fields": {
                "GSO_FULL_EVAL_V1": [
                    "optimization_run_id", "ag_id", "iteration",
                ],
                "GSO_PATCH_OUTCOME_V1": [
                    "optimization_run_id", "ag_id", "iteration", "intent_id",
                ],
                "GSO_ADMISSION_DECISION_V1": [
                    "run_id->optimization_run_id", "ag_id(W5.0 added)",
                    "iteration",
                ],
            },
        },
    )


def lineage_violation_marker(
    *,
    result: FullEvalLineageResult,
) -> str:
    """W5.1 — emit ``GSO_TRIAL22_LINEAGE_VIOLATION_V1`` for one orphan
    acceptance."""
    from genie_space_optimizer.optimization.run_analysis_contract import (
        marker_line,
    )

    run_id, ag_id, iteration = result.key
    return marker_line(
        "GSO_TRIAL22_LINEAGE_VIOLATION_V1",
        {
            "optimization_run_id": run_id,
            "ag_id": ag_id,
            "iteration": iteration,
            "candidate_id": result.candidate_id,
            "candidate_accuracy": result.candidate_accuracy,
            "provenance": result.provenance,
            "has_patch_outcome": result.has_patch_outcome,
            "has_admission_decision": result.has_admission_decision,
        },
    )


def enforce_full_eval_lineage(
    *,
    full_eval_rows: Sequence[Mapping[str, Any]],
    patch_outcome_rows: Sequence[Mapping[str, Any]],
    admission_decision_rows: Sequence[Mapping[str, Any]],
    baseline_accuracy: float,
) -> LineageReconciliation:
    """Trial 22 W5.1 — reconcile accepted full-eval rows against
    applied patch outcomes and admitted decisions on the canonical key.

    A full-eval row with ``accepted == true`` contributes to
    ``best_accuracy`` ONLY when it has a matching *applied*
    ``GSO_PATCH_OUTCOME_V1`` AND a matching *admitted*
    ``GSO_ADMISSION_DECISION_V1``. Otherwise it is stamped
    ``orphan_acceptance`` and excluded; ``best_accuracy`` stays at
    ``baseline_accuracy`` (unless another, lineage-clean acceptance
    raises it).

    Always emits the W5.0 key-audit marker; emits one
    ``GSO_TRIAL22_LINEAGE_VIOLATION_V1`` per orphan.
    """
    results: list[FullEvalLineageResult] = []
    markers: list[str] = [lineage_key_audit_marker()]
    best = float(baseline_accuracy)
    orphan_count = 0

    for fe in full_eval_rows or ():
        key = canonical_key(fe)
        cand = str(fe.get("candidate_id") or "")
        accepted = bool(fe.get("accepted"))
        try:
            cand_acc = float(fe.get("candidate_accuracy", 0.0) or 0.0)
        except (TypeError, ValueError):
            cand_acc = 0.0

        has_patch = any(
            _is_applied_patch(p) and _candidate_matches(key, cand, p)
            for p in (patch_outcome_rows or ())
        )
        has_admit = any(
            _is_admitted(a) and _candidate_matches(key, cand, a)
            for a in (admission_decision_rows or ())
        )

        if accepted and has_patch and has_admit:
            verdict = LINEAGE_OK
            provenance = ""
            best = max(best, cand_acc)
        elif accepted:
            verdict = ORPHAN_ACCEPTANCE
            provenance = ORPHAN_ACCEPTANCE
            orphan_count += 1
        else:
            # Not accepted — no lineage obligation, not an orphan.
            verdict = LINEAGE_OK
            provenance = ""

        res = FullEvalLineageResult(
            key=key,
            candidate_id=cand,
            accepted=accepted,
            candidate_accuracy=cand_acc,
            lineage_verdict=verdict,
            provenance=provenance,
            has_patch_outcome=has_patch,
            has_admission_decision=has_admit,
        )
        results.append(res)
        if provenance == ORPHAN_ACCEPTANCE:
            markers.append(lineage_violation_marker(result=res))

    return LineageReconciliation(
        results=tuple(results),
        best_accuracy=best,
        baseline_accuracy=float(baseline_accuracy),
        orphan_count=orphan_count,
        markers=tuple(markers),
    )
