"""Phase 1.1 — consume ``ActionGroupSlate.admission_trace``.

Pure helper. Maps a slate's admission trace + the harness's candidate
AG list to:

  * ``admitted_ags``: the subset of candidates with verdict ADMITTED
    (or with no corresponding trace entry — backward-compat)
  * ``denied_ag_ids``: tuple of AG ids that were DENIED
  * ``pivot_signal``: True iff ANY denial reason was ``AG_RETIRED``
  * ``first_ag_retired_id``: id of the first AG_RETIRED denial in
    trace order (drives the strategist pivot in Phase 1.3)
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

from genie_space_optimizer.optimization.stages.action_groups import (
    AdmissionTrace,
    AdmissionVerdict,
    ForbiddenReason,
)


@dataclass(frozen=True, slots=True)
class AdmissionResult:
    admitted_ags: list[Mapping[str, object]]
    denied_ag_ids: tuple[str, ...]
    pivot_signal: bool
    first_ag_retired_id: str


def _ag_id(ag: Mapping[str, object]) -> str:
    return str(ag.get("ag_id") or ag.get("id") or "")


def apply_admission_trace(
    *,
    slate_traces: Sequence[AdmissionTrace],
    candidate_ags: Sequence[Mapping[str, object]],
) -> AdmissionResult:
    """Filter ``candidate_ags`` against ``slate_traces`` and surface
    the pivot signal.

    Pure: no I/O, no harness imports. Trace entries without a
    corresponding candidate AG are ignored (the slate may
    enumerate forbidden AGs that were already filtered upstream).
    """
    traces_by_id: dict[str, AdmissionTrace] = {
        str(t.ag_id): t for t in (slate_traces or ())
    }

    admitted: list[Mapping[str, object]] = []
    denied: list[str] = []
    pivot = False
    first_retired = ""

    for ag in (candidate_ags or ()):
        agid = _ag_id(ag)
        trace = traces_by_id.get(agid)
        if trace is None:
            admitted.append(ag)
            continue
        if trace.verdict == AdmissionVerdict.ADMITTED:
            admitted.append(ag)
        else:
            denied.append(agid)

    # The pivot signal scans trace order so the FIRST retired
    # AG drives the strategist's next-cluster choice.
    for t in (slate_traces or ()):
        if t.verdict != AdmissionVerdict.DENIED:
            continue
        reason = str(t.denial_reason or "")
        if reason == ForbiddenReason.AG_RETIRED.value:
            pivot = True
            if not first_retired:
                first_retired = str(t.ag_id)

    return AdmissionResult(
        admitted_ags=admitted,
        denied_ag_ids=tuple(denied),
        pivot_signal=pivot,
        first_ag_retired_id=first_retired,
    )
