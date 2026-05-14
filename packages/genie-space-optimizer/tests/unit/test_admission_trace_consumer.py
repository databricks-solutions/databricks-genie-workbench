"""Phase 1.1 — apply_admission_trace pure helper."""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.stages.action_groups import (
    AdmissionTrace,
    AdmissionVerdict,
    ForbiddenReason,
)
from genie_space_optimizer.optimization.admission_trace_consumer import (
    apply_admission_trace,
    AdmissionResult,
)


def _trace(ag_id: str, verdict: str, reason: str = "") -> AdmissionTrace:
    return AdmissionTrace(
        ag_id=ag_id,
        verdict=AdmissionVerdict(verdict),
        denial_reason=reason,
    )


def test_no_trace_returns_all_admitted_no_pivot():
    candidates = [{"id": "ag-1"}, {"id": "ag-2"}]
    result = apply_admission_trace(slate_traces=(), candidate_ags=candidates)
    assert result.admitted_ags == candidates
    assert result.denied_ag_ids == ()
    assert result.pivot_signal is False
    assert result.first_ag_retired_id == ""


def test_denied_ags_are_filtered():
    candidates = [{"id": "ag-1"}, {"id": "ag-2"}, {"id": "ag-3"}]
    traces = (
        _trace("ag-1", "admitted"),
        _trace("ag-2", "denied", "content_regression"),
        _trace("ag-3", "admitted"),
    )
    result = apply_admission_trace(slate_traces=traces, candidate_ags=candidates)
    admitted_ids = [ag["id"] for ag in result.admitted_ags]
    assert admitted_ids == ["ag-1", "ag-3"]
    assert result.denied_ag_ids == ("ag-2",)
    assert result.pivot_signal is False


def test_ag_retired_denial_triggers_pivot_signal():
    """An AG_RETIRED denial reason MUST set pivot_signal=True AND
    record the first retired AG id so the harness can route to the
    next-priority unblocked cluster."""
    candidates = [{"id": "ag-1"}, {"id": "ag-2"}]
    traces = (
        _trace("ag-1", "denied", str(ForbiddenReason.AG_RETIRED.value)),
        _trace("ag-2", "admitted"),
    )
    result = apply_admission_trace(slate_traces=traces, candidate_ags=candidates)
    assert result.pivot_signal is True
    assert result.first_ag_retired_id == "ag-1"
    assert [ag["id"] for ag in result.admitted_ags] == ["ag-2"]


def test_non_ag_retired_denial_does_not_trigger_pivot():
    candidates = [{"id": "ag-1"}, {"id": "ag-2"}]
    traces = (
        _trace("ag-1", "denied", str(ForbiddenReason.CONTENT_REGRESSION.value)),
        _trace("ag-2", "denied", str(ForbiddenReason.NO_PROPOSALS.value)),
    )
    result = apply_admission_trace(slate_traces=traces, candidate_ags=candidates)
    assert result.pivot_signal is False
    assert result.first_ag_retired_id == ""
    assert result.admitted_ags == []
    assert set(result.denied_ag_ids) == {"ag-1", "ag-2"}


def test_candidate_with_no_trace_is_admitted_by_default():
    """A candidate AG without a corresponding admission_trace entry
    is treated as ADMITTED (backward-compat with iter 1 where
    forbidden set is empty)."""
    candidates = [{"id": "ag-1"}, {"id": "ag-untraced"}]
    traces = (_trace("ag-1", "admitted"),)
    result = apply_admission_trace(slate_traces=traces, candidate_ags=candidates)
    admitted_ids = {ag["id"] for ag in result.admitted_ags}
    assert admitted_ids == {"ag-1", "ag-untraced"}


def test_candidates_supporting_both_id_and_ag_id_keys():
    """The harness uses ``ag_id`` in some places, ``id`` in others.
    The helper must look for both."""
    candidates = [{"ag_id": "ag-1"}, {"id": "ag-2"}]
    traces = (
        _trace("ag-1", "denied", "content_regression"),
        _trace("ag-2", "admitted"),
    )
    result = apply_admission_trace(slate_traces=traces, candidate_ags=candidates)
    admitted_ids = [
        str(ag.get("ag_id") or ag.get("id")) for ag in result.admitted_ags
    ]
    assert admitted_ids == ["ag-2"]


def test_first_ag_retired_id_is_first_in_trace_order():
    candidates = [{"id": "a"}, {"id": "b"}, {"id": "c"}]
    traces = (
        _trace("a", "admitted"),
        _trace("b", "denied", "ag_retired"),
        _trace("c", "denied", "ag_retired"),
    )
    result = apply_admission_trace(slate_traces=traces, candidate_ags=candidates)
    assert result.first_ag_retired_id == "b"
