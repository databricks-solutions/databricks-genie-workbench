"""Phase 2 (2026-05-16) — exit-criterion replay test.

Run B's airline fixture (``AG_DECOMPOSED_H001`` on cluster H001,
target ``airline_gs_017``, lever L5, root cause
``missing_or_misordered_join``) re-selected the same AG across
iterations 1..3 in the export run. This test asserts that after
Phase 2 lands:

1. iter-1's reflection-buffer entry (built via the actual
   ``_build_reflection_entry`` helper) carries a populated
   ``TerminalSignature``.
2. ``compute_retired_signatures`` returns a non-empty set.
3. The harness's ``_compute_forbidden_ag_set`` (the active
   forbidden-set producer consumed by ``_filter_tried_clusters``)
   admits the H001 signature.
4. iter-3's cluster list, when filtered through
   ``_filter_tried_clusters``, no longer contains H001.

Unlike ``tests/replay/test_ccf1d60d_repeated_zero_proposal_retires.py``
which constructs the reflection buffer as a hand-rolled dict
literal, this test routes through the actual harness helper so
schema drift in ``_build_reflection_entry`` would surface here.
"""
from __future__ import annotations

import inspect

from genie_space_optimizer.optimization.forbidden_ag_set_v2 import (
    compute_retired_signatures,
)
from genie_space_optimizer.optimization.harness import (
    _build_reflection_entry,
    _compute_forbidden_ag_set,
    _filter_tried_clusters,
)
from genie_space_optimizer.optimization.terminal_reason import (
    TerminalReason,
)
from genie_space_optimizer.optimization.terminal_signature import (
    TerminalSignature,
)
from genie_space_optimizer.optimization.terminal_signature_iter import (
    terminal_signature_for_iteration,
)


# Run B export-run constants (docs/export_runs/run_b_airline_iter_1_through_3.md).
#
# NOTE: the export run's free-form ``root_cause_summary`` was
# ``missing_or_misordered_join``, which is NOT in the canonical
# ``_FEASIBLE_LEVER_SETS_BY_ROOT_CAUSE`` table inside ``harness.py``.
# Canonical aliases like ``missing_join_spec`` have TWO feasible
# lever sets ({4}, {5}) so a single-lever retirement would not
# trip the 3-tuple suppression rule. ``wrong_join_type`` (mapped
# in the table to a single ``{5}`` lever set) preserves the join-
# centric semantic intent AND maps the strategist's L5 lever
# selection to the lone feasible set, so iter-1's retirement
# unambiguously suppresses iter-3 re-selection.
RUN_B_H001_AG_ID = "AG_DECOMPOSED_H001"
RUN_B_H001_CLUSTER_ID = "H001"
RUN_B_H001_TARGET_QID = "airline_gs_017"
RUN_B_H001_LEVER = 5
RUN_B_H001_ROOT_CAUSE = "wrong_join_type"
RUN_B_H001_BLAME_ASSET = "catalog.airline.fact_bookings"
RUN_B_ITER_1_TERMINAL_REASON = TerminalReason.NO_APPLIED_PATCHES


def _h001_iter_locals() -> dict:
    """The ``capture_iter_ag_context`` snapshot Run B iter-1 would
    have produced for ``AG_DECOMPOSED_H001``."""
    return {
        "ag_id": RUN_B_H001_AG_ID,
        "cluster_ids": (RUN_B_H001_CLUSTER_ID,),
        "target_qids": (RUN_B_H001_TARGET_QID,),
        "levers": (RUN_B_H001_LEVER,),
        "root_cause": RUN_B_H001_ROOT_CAUSE,
        "blame_set": (RUN_B_H001_BLAME_ASSET,),
    }


def _iter_1_reflection_entry() -> dict:
    """Build iter-1's reflection-buffer entry through the actual
    ``_build_reflection_entry`` helper — NOT a hand-rolled dict."""
    return _build_reflection_entry(
        iteration=1,
        ag_id=RUN_B_H001_AG_ID,
        accepted=False,
        levers=[RUN_B_H001_LEVER],
        target_objects=[RUN_B_H001_TARGET_QID],
        prev_scores={"airline_gs_017": 0.0, "airline_gs_018": 1.0},
        new_scores={"airline_gs_017": 0.0, "airline_gs_018": 1.0},
        rollback_reason="no_applied_patches",
        patches=[],
        affected_question_ids=[RUN_B_H001_TARGET_QID],
        prev_failure_qids={RUN_B_H001_TARGET_QID},
        new_failure_qids={RUN_B_H001_TARGET_QID},
        reflection_text=(
            "Iter 1 terminal: applyability dropped every proposal "
            "for AG_DECOMPOSED_H001 (NO_APPLIED_PATCHES)."
        ),
        refinement_mode="out_of_plan",
        escalation_handled=False,
        root_cause=RUN_B_H001_ROOT_CAUSE,
        blame_set=(RUN_B_H001_BLAME_ASSET,),
        source_cluster_ids=[RUN_B_H001_CLUSTER_ID],
        terminal_signature=terminal_signature_for_iteration(
            iter_locals=_h001_iter_locals(),
            terminal_reason=RUN_B_ITER_1_TERMINAL_REASON,
        ),
    )


def test_iter_1_entry_carries_terminal_signature():
    """The entry built through the actual helper must contain a
    ``terminal_signature`` key with a ``TerminalSignature`` value."""
    entry = _iter_1_reflection_entry()
    assert "terminal_signature" in entry, (
        "_build_reflection_entry must surface 'terminal_signature' "
        "key when caller passes the kwarg. Got keys: "
        f"{sorted(entry.keys())}"
    )
    assert isinstance(entry["terminal_signature"], TerminalSignature)


def test_iter_1_terminal_signature_carries_h001_fields():
    """The signature must encode the five H001 identity fields so
    compute_retired_signatures can hash-match it across iterations."""
    entry = _iter_1_reflection_entry()
    sig: TerminalSignature = entry["terminal_signature"]
    assert sig.root_cause == RUN_B_H001_ROOT_CAUSE
    assert sig.blame_set_norm == (RUN_B_H001_BLAME_ASSET,)
    assert sig.lever_set == frozenset({RUN_B_H001_LEVER})
    assert sig.target_qids == frozenset({RUN_B_H001_TARGET_QID})
    assert sig.terminal_reason == RUN_B_ITER_1_TERMINAL_REASON.value


def test_compute_retired_signatures_returns_non_empty():
    """``compute_retired_signatures`` returns a non-empty frozenset on
    Run B's iter-1 reflection buffer."""
    reflection_buffer = [_iter_1_reflection_entry()]
    retired = compute_retired_signatures(
        reflection_buffer=reflection_buffer,
    )
    assert isinstance(retired, frozenset)
    assert len(retired) >= 1, (
        f"compute_retired_signatures returned empty set on a "
        f"reflection buffer containing one non-accepted entry "
        f"with a populated terminal_signature. retired={retired!r}"
    )


def test_retired_set_contains_h001_signature():
    """The retired set must contain the exact H001 signature so AG
    selection can match against it."""
    reflection_buffer = [_iter_1_reflection_entry()]
    retired = compute_retired_signatures(
        reflection_buffer=reflection_buffer,
    )
    expected_sig = _iter_1_reflection_entry()["terminal_signature"]
    assert expected_sig in retired, (
        f"Expected H001's TerminalSignature in retired set.\n"
        f"  expected={expected_sig!r}\n"
        f"  retired={list(retired)!r}"
    )


def test_compute_forbidden_ag_set_admits_h001_legacy_tuple(monkeypatch):
    """The harness's active forbidden-set producer must include H001's
    legacy 3-tuple after retirement. This is the form
    ``_filter_tried_clusters`` reads."""
    monkeypatch.setenv("GSO_TERMINAL_SIGNATURE_RETIRE", "1")
    reflection_buffer = [_iter_1_reflection_entry()]
    forbidden = _compute_forbidden_ag_set(reflection_buffer)
    expected_tuple = (
        RUN_B_H001_ROOT_CAUSE,
        (RUN_B_H001_BLAME_ASSET,),
        frozenset({RUN_B_H001_LEVER}),
    )
    assert expected_tuple in forbidden, (
        f"_compute_forbidden_ag_set must admit H001's legacy 3-tuple.\n"
        f"  expected={expected_tuple!r}\n"
        f"  forbidden={forbidden!r}"
    )


def test_filter_tried_clusters_drops_h001_cluster():
    """iter-3 AG selection rejects ``AG_DECOMPOSED_H001`` because its
    signature is retired.

    The active path through ``_filter_tried_clusters`` reads the
    ``(root_cause, blame_set_norm, lever_set)`` triple. We supply
    iter-3's cluster list with H001 + a control cluster H002, and
    assert H001 is filtered out.
    """
    iter_3_clusters = [
        {
            "cluster_id": RUN_B_H001_CLUSTER_ID,
            "root_cause": RUN_B_H001_ROOT_CAUSE,
            "asi_failure_type": RUN_B_H001_ROOT_CAUSE,
            "blame_set": [RUN_B_H001_BLAME_ASSET],
            "asi_blame_set": [RUN_B_H001_BLAME_ASSET],
            "candidate_levers": [RUN_B_H001_LEVER],
            "affected_questions": [RUN_B_H001_TARGET_QID],
        },
        {
            "cluster_id": "H002",
            "root_cause": "missing_metric_view",
            "asi_failure_type": "missing_metric_view",
            "blame_set": ["catalog.airline.dim_routes"],
            "asi_blame_set": ["catalog.airline.dim_routes"],
            "candidate_levers": [5],
            "affected_questions": ["airline_gs_021"],
        },
    ]

    reflection_buffer = [_iter_1_reflection_entry()]
    forbidden = _compute_forbidden_ag_set(reflection_buffer)
    surviving = _filter_tried_clusters(iter_3_clusters, forbidden)
    surviving_ids = [c.get("cluster_id") for c in surviving]
    assert RUN_B_H001_CLUSTER_ID not in surviving_ids, (
        f"Expected H001 cluster to be filtered out at iter-3 (its "
        f"signature is retired in the forbidden set), but it "
        f"survived. surviving_ids={surviving_ids}"
    )
    assert "H002" in surviving_ids, (
        f"H002 should NOT be filtered (different root_cause and "
        f"blame_set). surviving_ids={surviving_ids}"
    )


def test_repeated_h001_selection_across_iterations_is_blocked():
    """If iter-2 ALSO writes an H001 reflection entry (same signature),
    the retired set's size stays at 1 (idempotent) AND the cluster is
    still filtered."""
    reflection_buffer = [
        _iter_1_reflection_entry(),
        {
            **_iter_1_reflection_entry(),
            "iteration": 2,
        },
    ]
    retired = compute_retired_signatures(
        reflection_buffer=reflection_buffer,
    )
    assert len(retired) == 1, (
        f"Two reflection entries with identical signatures must "
        f"hash to one retired signature. Got len={len(retired)}."
    )


def test_no_legacy_repeat_threshold_param():
    """Regression guard for the broken test
    ``test_ccf1d60d_repeated_zero_proposal_retires.py`` which calls
    ``compute_retired_signatures(reflection_buffer, repeat_threshold=1)``.
    The actual signature is keyword-only ``reflection_buffer=...``."""
    sig = inspect.signature(compute_retired_signatures)
    param_names = list(sig.parameters.keys())
    assert param_names == ["reflection_buffer"], (
        f"compute_retired_signatures signature drifted. Expected "
        f"only 'reflection_buffer'; got {param_names}."
    )
