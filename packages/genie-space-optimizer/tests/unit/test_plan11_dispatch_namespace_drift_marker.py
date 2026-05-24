"""Phase 4 (Trial 13) — Plan 11 dispatch starvation marker gains a
``drift_kind`` field that distinguishes:

* ``starved`` — Plan 11 saw 0 hard QIDs while SM had ≥1.
* ``namespace_mismatch`` — both sets non-empty AND their intersection
  is empty (e.g. SM emits ``airline_..._gs_009`` while dispatch emits
  ``gs_009``).
* ``partial_drift`` — intersection non-empty AND symmetric difference
  non-empty (some QIDs drift between sets).
* ``none`` — sets are identical (no marker emission).

Trial 12's marker used count comparison only; the 98ec8950 run had
``plan11_failing_qids_count=2`` at baseline iteration 0 and
``plan11_hard_qids=[]`` at later iterations, so the count-only
condition was satisfied non-deterministically. The set-aware variant
fires deterministically on the actual mismatch shape.
"""
from __future__ import annotations

import io
import json
from contextlib import redirect_stdout

from genie_space_optimizer.optimization.state_machine.markers import (
    plan11_dispatch_starved_marker,
)


def _emit(**kwargs: object) -> dict | None:
    """Run ``plan11_dispatch_starved_marker(...)`` and return the
    decoded marker payload, or ``None`` if the marker chose not to
    emit (``drift_kind == "none"``).
    """
    line = plan11_dispatch_starved_marker(**kwargs)  # type: ignore[arg-type]
    if not line:
        return None
    assert line.startswith("GSO_PLAN11_DISPATCH_STARVED_V1 "), line
    return json.loads(line.split(" ", 1)[1])


def test_namespace_mismatch_fires_when_sm_namespaces_but_p11_does_not() -> None:
    """SM=`airline_..._gs_009`, P11=`gs_009`. Both non-empty, no overlap.

    Both legacy and new fields populated for backwards-compat.
    """
    payload = _emit(
        run_id="run_namespace_drift",
        iteration=1,
        plan11_failing_qids_count=1,
        sm_hard_qid_count=1,
        sm_hard_qids=["airline_ticketing_and_fare_analysis_gs_009"],
        plan11_dispatch_qids=["gs_009"],
    )
    assert payload is not None, "marker MUST fire on namespace mismatch"
    assert payload["drift_kind"] == "namespace_mismatch", payload
    assert payload["sm_hard_qids"] == [
        "airline_ticketing_and_fare_analysis_gs_009"
    ]
    assert payload["plan11_dispatch_qids"] == ["gs_009"]


def test_starved_fires_when_plan11_empty_and_sm_nonempty() -> None:
    """SM has hard QIDs, P11 dispatch lost them entirely (count == 0).

    Pre-Trial-13 behavior: this case fires with the legacy count
    condition. Trial 13 reclassifies it as ``drift_kind="starved"``.
    """
    payload = _emit(
        run_id="run_starved",
        iteration=2,
        plan11_failing_qids_count=0,
        sm_hard_qid_count=2,
        sm_hard_qids=["q1", "q2"],
        plan11_dispatch_qids=[],
    )
    assert payload is not None
    assert payload["drift_kind"] == "starved", payload


def test_partial_drift_fires_when_sets_overlap_but_differ() -> None:
    """Intersection non-empty, symmetric difference non-empty."""
    payload = _emit(
        run_id="run_partial",
        iteration=3,
        plan11_failing_qids_count=2,
        sm_hard_qid_count=2,
        sm_hard_qids=["q1", "q2"],
        plan11_dispatch_qids=["q1", "q3"],
    )
    assert payload is not None
    assert payload["drift_kind"] == "partial_drift", payload


def test_no_drift_does_not_fire_when_sets_match() -> None:
    """When SM and P11 see the exact same QID set, marker MUST NOT fire."""
    payload = _emit(
        run_id="run_clean",
        iteration=4,
        plan11_failing_qids_count=2,
        sm_hard_qid_count=2,
        sm_hard_qids=["q1", "q2"],
        plan11_dispatch_qids=["q2", "q1"],
    )
    assert payload is None, (
        "marker MUST be silent when SM and P11 sets are equal (any order); "
        f"got payload={payload}"
    )


def test_no_drift_does_not_fire_when_both_empty() -> None:
    """When both sets are empty, no drift to report."""
    payload = _emit(
        run_id="run_zero_zero",
        iteration=0,
        plan11_failing_qids_count=0,
        sm_hard_qid_count=0,
        sm_hard_qids=[],
        plan11_dispatch_qids=[],
    )
    assert payload is None
