"""Persistent-failure escalation (Task 8).

The retail run printed persistence and quarantine messages but the
loop kept trying broader bundles on the same root cause. After this
module is wired in, a cluster signature that has accumulated
``HUMAN_REQUIRED_CONTENT_ROLLBACK_THRESHOLD`` content-regression
rollbacks within a run is escalated to a human-required queue and
excluded from future strategist input in the same run.

Public surface:

* :func:`compute_human_required_escalations` — pure helper over a
  reflection buffer; returns Delta-shaped escalation records and the
  set of cluster signatures the harness should drop from clustering.
* :data:`HUMAN_REQUIRED_CONTENT_ROLLBACK_THRESHOLD` — config knob
  (default 2). After two same-signature CONTENT_REGRESSION rollbacks,
  the cluster is parked.

The module is leaf-level — no imports from ``harness``. The reflection
buffer entry shape is the one produced by
``harness._build_reflection_entry``: ``rollback_class``,
``source_cluster_signatures``, ``root_cause``, ``rollback_reason``,
``levers``, ``score_deltas``, ``affected_question_ids``, ``iteration``.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Iterable

# Default: two same-signature content rollbacks within a run earn an
# escalation. Override at call time when a tighter or looser policy
# is required.
HUMAN_REQUIRED_CONTENT_ROLLBACK_THRESHOLD: int = 2


@dataclass(frozen=True)
class HumanRequiredCase:
    """Delta-shaped escalation record.

    One row per (cluster_signature, question_id) pair so the
    reviewer queue is keyed at the question level — the same
    cluster can affect many qids and a reviewer typically wants to
    triage qid by qid.
    """

    run_id: str
    cluster_signature: str
    question_id: str
    root_cause: str
    attempt_count: int
    last_iteration: int
    reason_code: str
    evidence: dict = field(default_factory=dict)


def _entry_is_content_rollback(entry: dict) -> bool:
    """True iff the reflection entry represents a content regression
    rollback (not infra / schema)."""
    if entry.get("accepted"):
        return False
    return str(entry.get("rollback_class") or "").lower() == "content_regression"


def _signatures_in_entry(entry: dict) -> list[str]:
    sigs = entry.get("source_cluster_signatures") or []
    if isinstance(sigs, list):
        return [str(s) for s in sigs if s]
    return []


def _iteration_int(entry: dict) -> int:
    val = entry.get("iteration")
    try:
        return int(val) if val is not None else 0
    except (TypeError, ValueError):
        return 0


def compute_human_required_escalations(
    reflection_buffer: Iterable[dict],
    *,
    run_id: str,
    threshold: int = HUMAN_REQUIRED_CONTENT_ROLLBACK_THRESHOLD,
    already_escalated_signatures: set[str] | None = None,
) -> tuple[list[HumanRequiredCase], set[str]]:
    """Walk the reflection buffer and identify cluster signatures that
    have hit the rollback threshold.

    Returns ``(escalation_rows, escalated_signatures)`` where:
      * ``escalation_rows`` are Delta-shaped, one row per affected qid
        per cluster signature, ready for ``state.write_human_required_escalations``.
      * ``escalated_signatures`` is the set of cluster signatures that
        crossed the threshold THIS call (i.e. excludes signatures
        already escalated in a previous call).

    Pure: no I/O, no globals. Idempotent given the same input.
    """
    already = set(already_escalated_signatures or set())
    by_signature: dict[str, list[dict]] = defaultdict(list)
    for entry in reflection_buffer or []:
        if not isinstance(entry, dict):
            continue
        if not _entry_is_content_rollback(entry):
            continue
        for sig in _signatures_in_entry(entry):
            by_signature[sig].append(entry)

    cases: list[HumanRequiredCase] = []
    newly_escalated: set[str] = set()
    for sig, entries in by_signature.items():
        if sig in already:
            continue
        if len(entries) < threshold:
            continue
        # Sort by iteration so ``last_iteration`` reflects the most
        # recent rollback even if the buffer was passed unsorted.
        entries_sorted = sorted(entries, key=_iteration_int)
        last = entries_sorted[-1]
        root_cause = str(
            last.get("root_cause")
            or last.get("dominant_root_cause")
            or "unknown",
        )
        affected_qids = list({
            str(q)
            for e in entries_sorted
            for q in (e.get("affected_question_ids") or [])
            if q
        })
        evidence = {
            "rollback_reasons": [
                str(e.get("rollback_reason") or "")[:240]
                for e in entries_sorted
            ],
            "iterations": [_iteration_int(e) for e in entries_sorted],
            "levers_tried": sorted({
                int(lev)
                for e in entries_sorted
                for lev in (e.get("levers") or [])
                if isinstance(lev, (int, float))
            }),
            "ag_ids": [str(e.get("ag_id") or "") for e in entries_sorted],
        }
        # If the cluster has no recorded affected qids, still emit a
        # single sentinel row keyed off an empty qid so the
        # signature is queryable.
        affected_qids_for_emit = affected_qids or [""]
        for qid in affected_qids_for_emit:
            cases.append(
                HumanRequiredCase(
                    run_id=run_id,
                    cluster_signature=sig,
                    question_id=qid,
                    root_cause=root_cause,
                    attempt_count=len(entries_sorted),
                    last_iteration=_iteration_int(last),
                    reason_code="persistent_content_rollback",
                    evidence=evidence,
                )
            )
        newly_escalated.add(sig)

    return cases, newly_escalated


def case_to_delta_row(case: HumanRequiredCase) -> dict:
    """Convert a :class:`HumanRequiredCase` to a dict shaped for
    :func:`state.write_human_required_escalations`."""
    return {
        "run_id": case.run_id,
        "cluster_signature": case.cluster_signature,
        "question_id": case.question_id,
        "root_cause": case.root_cause,
        "attempt_count": case.attempt_count,
        "last_iteration": case.last_iteration,
        "reason_code": case.reason_code,
        "evidence_json": case.evidence,
    }
