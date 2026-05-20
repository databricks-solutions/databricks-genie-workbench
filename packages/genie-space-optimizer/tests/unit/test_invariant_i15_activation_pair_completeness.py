"""Plan 10 Phase B1 — I15 activation marker pair-completeness.

Closes Plan 10 Leak 1's exact signature: the harness-level
``anchor_entered_plan5_dispatch`` marker fired, but no in-dispatcher
status marker followed because the gate's ``rca_evidence_typed``
precondition was empty. Plan 10 Phase A removed the precondition;
Phase B1 locks the invariant so a future regression that re-introduces
silent gate-closure surfaces a typed I15 violation instead of an
inert run summary.

Verification scenarios (both required by the plan spec):

* The marker stream from the two failing production runs
  (59a173d3 airline + ab65fefe 7now) — 4 ``anchor_entered_plan5_dispatch``
  markers with no paired inner status — must surface exactly 4
  violations.

* A synthetic post-fix stream where each entered marker carries a
  paired inner status must report zero violations.

These two scenarios are encoded as direct unit tests below using a
hand-built marker stream rather than the full production fixtures so
the test fails fast and never depends on harness or LLM mocking.
"""

from __future__ import annotations

import json
from collections.abc import Sequence

import pytest

from genie_space_optimizer.optimization.invariants import (
    check_i15_activation_pair_completeness,
    run_invariants,
)


# ── Production marker stream from the failing runs (B1 spec) ─────────────


def _entered(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    cluster_id: str,
) -> dict:
    return {
        "marker_name": "GSO_PLAN5_ANCHOR_ACTIVATION_V1",
        "optimization_run_id": run_id,
        "iteration": iteration,
        "ag_id": ag_id,
        "cluster_id": cluster_id,
        "status": "anchor_entered_plan5_dispatch",
        "reason": "",
        "patch_type": "",
        "intent_id": "",
    }


def _inner(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    cluster_id: str,
    status: str,
    reason: str = "",
) -> dict:
    return {
        "marker_name": "GSO_PLAN5_ANCHOR_ACTIVATION_V1",
        "optimization_run_id": run_id,
        "iteration": iteration,
        "ag_id": ag_id,
        "cluster_id": cluster_id,
        "status": status,
        "reason": reason,
        "patch_type": "",
        "intent_id": "intent-1",
    }


def _two_failing_runs_pre_fix() -> Sequence[dict]:
    """Reconstruct the 2026-05-19 failing-runs stream: 4 entered markers
    (airline gs_009, gs_024 + 7now gs_013, gs_026) and no in-dispatcher
    statuses for any of them. Mirrors the actual postmortem evidence
    fed to Phase A's deploy gate."""
    return [
        _entered(
            run_id="59a173d3-airline",
            iteration=2,
            ag_id="AG_DECOMPOSED_H001",
            cluster_id="C_gs009_plural_top_n_collapse",
        ),
        _entered(
            run_id="59a173d3-airline",
            iteration=2,
            ag_id="AG_DECOMPOSED_H002",
            cluster_id="C_gs024_missing_filter",
        ),
        _entered(
            run_id="ab65fefe-7now",
            iteration=3,
            ag_id="AG_DECOMPOSED_H001",
            cluster_id="C_gs013_wrong_filter_condition",
        ),
        _entered(
            run_id="ab65fefe-7now",
            iteration=3,
            ag_id="AG_DECOMPOSED_H002",
            cluster_id="C_gs026_plural_top_n_collapse",
        ),
    ]


def _post_fix_stream() -> Sequence[dict]:
    """The post-fix stream: each entered marker is paired with a typed
    in-dispatcher terminal status (one of the four allowed ones). This
    is the green state Plan 10 Phase A produces — Phase B1 must report
    zero violations on this stream."""
    stream: list[dict] = []
    for entered in _two_failing_runs_pre_fix():
        stream.append(entered)
        stream.append(_inner(
            run_id=str(entered["optimization_run_id"]),
            iteration=int(entered["iteration"]),
            ag_id=str(entered["ag_id"]),
            cluster_id=str(entered["cluster_id"]),
            status="plan5_intent_routed",
        ))
    return stream


# ── Direct invariant tests ───────────────────────────────────────────────


def test_i15_pre_fix_stream_reports_four_violations() -> None:
    """The marker stream from the two failing production runs flags
    exactly 4 violations — one per ``anchor_entered_plan5_dispatch``
    marker, since none have a paired inner status. This is the
    spec's verification scenario."""
    evidence = {"activation_markers": list(_two_failing_runs_pre_fix())}
    violations = check_i15_activation_pair_completeness(evidence)
    assert len(violations) == 4, (
        f"expected exactly 4 violations on the pre-fix stream, "
        f"got {len(violations)}: {violations!r}"
    )
    assert {v["invariant_id"] for v in violations} == {"I15"}
    ids = {(v["optimization_run_id"], v["ag_id"], v["iteration"])
           for v in violations}
    assert ids == {
        ("59a173d3-airline", "AG_DECOMPOSED_H001", 2),
        ("59a173d3-airline", "AG_DECOMPOSED_H002", 2),
        ("ab65fefe-7now", "AG_DECOMPOSED_H001", 3),
        ("ab65fefe-7now", "AG_DECOMPOSED_H002", 3),
    }


def test_i15_post_fix_stream_reports_zero_violations() -> None:
    """Synthetic post-fix stream where every entered marker has a
    paired in-dispatcher status reports zero violations."""
    evidence = {"activation_markers": list(_post_fix_stream())}
    violations = check_i15_activation_pair_completeness(evidence)
    assert violations == [], (
        f"expected zero violations on the post-fix stream, got: {violations!r}"
    )


# ── Pairing semantics edge cases ─────────────────────────────────────────


@pytest.mark.parametrize(
    "terminal_status",
    [
        "plan5_intent_declined",
        "plan5_intent_validator_rejected",
        "plan5_intent_routed",
        "plan5_intent_materialized",
    ],
)
def test_i15_all_four_terminal_statuses_satisfy_pairing(
    terminal_status: str,
) -> None:
    """Every allowed in-dispatcher terminal status counts as the pair."""
    entered = _entered(
        run_id="run-1",
        iteration=1,
        ag_id="AG_A",
        cluster_id="C_A",
    )
    inner = _inner(
        run_id="run-1",
        iteration=1,
        ag_id="AG_A",
        cluster_id="C_A",
        status=terminal_status,
        reason="typed-reason",
    )
    violations = check_i15_activation_pair_completeness(
        {"activation_markers": [entered, inner]}
    )
    assert violations == [], (
        f"terminal status {terminal_status!r} must satisfy the pair; "
        f"got: {violations!r}"
    )


def test_i15_invoked_alone_is_not_a_pair() -> None:
    """``plan5_intent_invoked`` is the pre-outcome marker — it does NOT
    satisfy the pairing. An invoked marker without one of the four
    terminal statuses is itself a silent-exit violation."""
    entered = _entered(
        run_id="run-1",
        iteration=1,
        ag_id="AG_A",
        cluster_id="C_A",
    )
    invoked_only = _inner(
        run_id="run-1",
        iteration=1,
        ag_id="AG_A",
        cluster_id="C_A",
        status="plan5_intent_invoked",
    )
    violations = check_i15_activation_pair_completeness(
        {"activation_markers": [entered, invoked_only]}
    )
    assert len(violations) == 1
    assert violations[0]["invariant_id"] == "I15"
    assert violations[0]["ag_id"] == "AG_A"


def test_i15_pairing_keyed_on_run_ag_iteration_triple() -> None:
    """A terminal status on a different iteration / ag / run does NOT
    count as the pair. The triple key isolates each dispatch."""
    entered = _entered(
        run_id="run-1",
        iteration=1,
        ag_id="AG_A",
        cluster_id="C_A",
    )
    wrong_iter = _inner(
        run_id="run-1",
        iteration=2,
        ag_id="AG_A",
        cluster_id="C_A",
        status="plan5_intent_routed",
    )
    wrong_ag = _inner(
        run_id="run-1",
        iteration=1,
        ag_id="AG_B",
        cluster_id="C_A",
        status="plan5_intent_routed",
    )
    wrong_run = _inner(
        run_id="run-2",
        iteration=1,
        ag_id="AG_A",
        cluster_id="C_A",
        status="plan5_intent_routed",
    )
    violations = check_i15_activation_pair_completeness({
        "activation_markers": [entered, wrong_iter, wrong_ag, wrong_run],
    })
    assert len(violations) == 1
    assert violations[0]["ag_id"] == "AG_A"
    assert violations[0]["iteration"] == 1


def test_i15_silent_on_legacy_fixtures_without_activation_markers() -> None:
    """Evidence dicts from pre-Plan-9 fixtures carry no
    ``activation_markers`` key. I15 must stay silent so back-compat
    with the legacy invariant suite is preserved."""
    violations = check_i15_activation_pair_completeness(
        {"iterations": [{"iteration": 1, "decision_records": []}]}
    )
    assert violations == []


def test_i15_violation_payload_is_json_serialisable() -> None:
    """Violations bubble through ``run_invariants`` -> contract-health
    summary -> stdout marker. The payload must be JSON-serialisable
    without losing the per-violation context fields."""
    evidence = {"activation_markers": list(_two_failing_runs_pre_fix())}
    violations = check_i15_activation_pair_completeness(evidence)
    encoded = json.dumps(violations)
    decoded = json.loads(encoded)
    assert len(decoded) == 4
    keys_of_first = set(decoded[0].keys())
    assert {"invariant_id", "title", "detail", "optimization_run_id",
            "ag_id", "iteration", "cluster_id"}.issubset(keys_of_first)


def test_i15_wired_into_run_invariants_aggregator() -> None:
    """``run_invariants`` aggregates every implemented check. I15 must
    surface via the aggregator so the contract-health summary picks
    it up automatically."""
    evidence = {"activation_markers": list(_two_failing_runs_pre_fix())}
    out = run_invariants(evidence)
    i15s = [v for v in out if v.get("invariant_id") == "I15"]
    assert len(i15s) == 4
