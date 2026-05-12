"""C15 Phase 3 Task 3.1 — ActionGroupsInput / ActionGroupSlate contract.

Verifies:
1. ForbiddenReason / AdmissionVerdict enum values are stable.
2. select() produces per-candidate AdmissionTrace when forbidden_ags
   is non-empty and stage_handlers_chunk_b_enabled() is on.
3. ActionGroupsInput and ActionGroupSlate both subclass JsonRoundTrip.

Note: the pinned class names are ActionGroupsInput / ActionGroupSlate
(from test_stage_io_class_declarations.py). The plan referenced
ActionGroupSelectionInput / ActionGroupSelectionOutput — those names
are NOT used; this test validates the canonical pinned names.
"""

from __future__ import annotations

import os
from unittest import mock

import pytest

from genie_space_optimizer.optimization.stages.action_groups import (
    ActionGroupsInput,
    ActionGroupSlate,
    AdmissionTrace,
    AdmissionVerdict,
    ForbiddenAG,
    ForbiddenReason,
    select,
)
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


# ── Enum stability ─────────────────────────────────────────────────────


def test_forbidden_reason_enum() -> None:
    assert ForbiddenReason.CONTENT_REGRESSION.value == "content_regression"
    assert ForbiddenReason.NO_PROPOSALS.value == "no_proposals"
    assert ForbiddenReason.AG_RETIRED.value == "ag_retired"
    assert ForbiddenReason.OTHER.value == "other"


def test_admission_verdict_enum() -> None:
    assert AdmissionVerdict.ADMITTED.value == "admitted"
    assert AdmissionVerdict.DENIED.value == "denied"


# ── JsonRoundTrip conformance ──────────────────────────────────────────


def test_input_and_output_mix_jsonroundtrip() -> None:
    assert issubclass(ActionGroupsInput, JsonRoundTrip)
    assert issubclass(ActionGroupSlate, JsonRoundTrip)


# ── Admission trace via select() ───────────────────────────────────────


class _MinimalCtx:
    """Minimal ctx for select() calls that don't need real Spark/MLflow."""
    run_id = "r0"
    iteration = 1

    @staticmethod
    def decision_emit(_rec) -> None:
        pass

    @staticmethod
    def journey_emit(**_kw) -> None:
        pass


def test_select_records_admission_trace_when_chunk_b_flag_on(monkeypatch) -> None:
    """When GSO_STAGE_HANDLERS_CHUNK_B=1 and forbidden_ags is non-empty,
    select() should produce admission_trace entries — one per candidate."""
    monkeypatch.setenv("GSO_STAGE_HANDLERS_CHUNK_B", "1")
    inp = ActionGroupsInput(
        action_groups=(
            {"id": "AG1", "source_cluster_ids": [], "affected_questions": [],
             "lever_directives": {}},
            {"id": "AG2", "source_cluster_ids": [], "affected_questions": [],
             "lever_directives": {}},
        ),
        forbidden_ags=(
            ForbiddenAG(ag_id="AG1", reason=ForbiddenReason.NO_PROPOSALS),
        ),
    )
    out = select(ctx=_MinimalCtx(), inp=inp)
    assert len(out.admission_trace) == 2
    by_ag = {t.ag_id: t for t in out.admission_trace}
    assert by_ag["AG1"].verdict is AdmissionVerdict.DENIED
    assert by_ag["AG1"].denial_reason == "no_proposals"
    assert by_ag["AG2"].verdict is AdmissionVerdict.ADMITTED


def test_select_admission_trace_empty_when_chunk_b_flag_off(monkeypatch) -> None:
    """Flag-off: admission_trace must be empty tuple (byte-stable with
    pre-Phase-3 behaviour)."""
    monkeypatch.setenv("GSO_STAGE_HANDLERS_CHUNK_B", "0")
    inp = ActionGroupsInput(
        action_groups=(
            {"id": "AG1", "source_cluster_ids": [], "affected_questions": [],
             "lever_directives": {}},
        ),
        forbidden_ags=(
            ForbiddenAG(ag_id="AG1", reason=ForbiddenReason.CONTENT_REGRESSION),
        ),
    )
    out = select(ctx=_MinimalCtx(), inp=inp)
    assert out.admission_trace == ()


def test_select_admission_trace_empty_when_no_forbidden_ags(monkeypatch) -> None:
    """Even with chunk_b flag on, empty forbidden_ags → empty trace."""
    monkeypatch.setenv("GSO_STAGE_HANDLERS_CHUNK_B", "1")
    inp = ActionGroupsInput(
        action_groups=(
            {"id": "AG1", "source_cluster_ids": [], "affected_questions": [],
             "lever_directives": {}},
        ),
        forbidden_ags=(),
    )
    out = select(ctx=_MinimalCtx(), inp=inp)
    assert out.admission_trace == ()


def test_action_groups_input_carries_blocked_cluster_ids():
    from genie_space_optimizer.optimization.stages.action_groups import (
        ActionGroupsInput,
    )

    inp = ActionGroupsInput(
        action_groups=(),
        blocked_cluster_ids=("H001", "H003"),
    )
    assert inp.blocked_cluster_ids == ("H001", "H003")


def test_action_groups_input_blocked_cluster_ids_defaults_to_empty():
    from genie_space_optimizer.optimization.stages.action_groups import (
        ActionGroupsInput,
    )

    inp = ActionGroupsInput(action_groups=())
    assert inp.blocked_cluster_ids == ()


def test_action_groups_input_blocked_cluster_ids_round_trips():
    from genie_space_optimizer.optimization.stages.action_groups import (
        ActionGroupsInput,
    )

    src = ActionGroupsInput(
        action_groups=(),
        blocked_cluster_ids=("H001", "H002"),
    )
    payload = src.to_json()
    rt = ActionGroupsInput.from_json(payload)
    assert rt.blocked_cluster_ids == ("H001", "H002")
