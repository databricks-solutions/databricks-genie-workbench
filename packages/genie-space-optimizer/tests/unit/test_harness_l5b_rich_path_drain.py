"""Unit test for the harness step that drains ``_L5B_RICH_PATH_DECLINES``
and emits typed ``NO_STRUCTURAL_CANDIDATE`` decision records.

We test the drain step in isolation via a helper
``_emit_l5b_rich_path_decline_records(run_id, iteration, ag_id_resolver,
emit_decision_record)``. The helper is the production entry point so we
can call it without spinning up the full harness loop.
"""
from __future__ import annotations

from typing import Any


def test_drain_emits_one_record_per_decline() -> None:
    from genie_space_optimizer.optimization.harness import (
        _emit_l5b_rich_path_decline_records,
    )
    from genie_space_optimizer.optimization.l5b_rich_dispatch import (
        _L5B_RICH_PATH_DECLINES, drain_l5b_rich_path_declines,
    )
    drain_l5b_rich_path_declines()  # reset
    _L5B_RICH_PATH_DECLINES.append({
        "cluster_id": "C1",
        "root_cause": "plural_top_n_collapse",
        "asi_failure_type": "wrong_aggregation",
        "attempted_archetypes": ("single_row_top_n",),
        "skipped_reason": "no_viable_archetype",
        "question_ids": ("q1",),
    })
    _L5B_RICH_PATH_DECLINES.append({
        "cluster_id": "C2",
        "root_cause": "wrong_filter_condition",
        "asi_failure_type": "",
        "attempted_archetypes": ("ordered_list_by_metric",),
        "skipped_reason": "gate:firewall:contains_q1",
        "question_ids": ("q2",),
    })

    emitted: list[Any] = []

    def _emit(rec):
        emitted.append(rec)

    def _ag_id_for_cluster(cluster_id):
        return f"AG_DECOMPOSED_{cluster_id}"

    _emit_l5b_rich_path_decline_records(
        run_id="test_run",
        iteration=3,
        ag_id_resolver=_ag_id_for_cluster,
        emit_decision_record=_emit,
    )

    assert len(emitted) == 2
    rec1 = emitted[0]
    assert rec1.cluster_id == "C1"
    assert rec1.ag_id == "AG_DECOMPOSED_C1"
    assert rec1.root_cause == "wrong_aggregation"
    assert rec1.call_site == "l5b_rich_path"

    rec2 = emitted[1]
    assert rec2.cluster_id == "C2"
    assert rec2.ag_id == "AG_DECOMPOSED_C2"
    assert rec2.root_cause == "wrong_filter_condition"
    assert rec2.skipped_reason == "gate:firewall:contains_q1"

    # Drain is destructive: subsequent invocations emit nothing.
    _emit_l5b_rich_path_decline_records(
        run_id="test_run",
        iteration=3,
        ag_id_resolver=_ag_id_for_cluster,
        emit_decision_record=_emit,
    )
    assert len(emitted) == 2  # no new records


def test_drain_no_op_when_ledger_empty() -> None:
    from genie_space_optimizer.optimization.harness import (
        _emit_l5b_rich_path_decline_records,
    )
    from genie_space_optimizer.optimization.l5b_rich_dispatch import (
        drain_l5b_rich_path_declines,
    )
    drain_l5b_rich_path_declines()

    emitted: list[Any] = []
    _emit_l5b_rich_path_decline_records(
        run_id="r",
        iteration=1,
        ag_id_resolver=lambda c: "AG",
        emit_decision_record=emitted.append,
    )
    assert emitted == []


def test_drain_uses_unknown_ag_when_resolver_returns_none() -> None:
    """If the resolver can't find an AG for a cluster_id (cluster not in
    any AG's source_clusters for this iteration), the record's ag_id is
    set to ``UNKNOWN_AG`` — still emitted (so we don't silently lose the
    observability) but flagged for triage.
    """
    from genie_space_optimizer.optimization.harness import (
        _emit_l5b_rich_path_decline_records,
    )
    from genie_space_optimizer.optimization.l5b_rich_dispatch import (
        _L5B_RICH_PATH_DECLINES, drain_l5b_rich_path_declines,
    )
    drain_l5b_rich_path_declines()
    _L5B_RICH_PATH_DECLINES.append({
        "cluster_id": "C_ORPHAN",
        "root_cause": "wrong_aggregation",
        "asi_failure_type": "",
        "attempted_archetypes": ("single_row_top_n",),
        "skipped_reason": "no_viable_archetype",
        "question_ids": ("q1",),
    })

    emitted: list[Any] = []
    _emit_l5b_rich_path_decline_records(
        run_id="r",
        iteration=1,
        ag_id_resolver=lambda c: None,
        emit_decision_record=emitted.append,
    )
    assert len(emitted) == 1
    assert emitted[0].ag_id == "UNKNOWN_AG"


def test_drain_wrapper_routes_through_decision_emit_fn() -> None:
    """The ``_drain_l5b_rich_path_after_stage2`` wrapper must build a
    resolver from ``action_groups`` and pass it to the underlying
    drain helper. Records flow through the caller-supplied
    ``decision_emit_fn`` (a list-append lambda in tests, the
    iteration body's ``_decision_emit`` closure in production)."""
    from genie_space_optimizer.optimization.harness import (
        _drain_l5b_rich_path_after_stage2,
    )
    from genie_space_optimizer.optimization.l5b_rich_dispatch import (
        _L5B_RICH_PATH_DECLINES, drain_l5b_rich_path_declines,
    )
    drain_l5b_rich_path_declines()
    _L5B_RICH_PATH_DECLINES.append({
        "cluster_id": "C_ALPHA",
        "root_cause": "wrong_aggregation",
        "asi_failure_type": "wrong_aggregation",
        "attempted_archetypes": ("single_row_top_n",),
        "skipped_reason": "no_viable_archetype",
        "question_ids": ("q1",),
    })

    action_groups = [
        {"id": "AG_DECOMPOSED_C_ALPHA", "source_cluster_ids": ["C_ALPHA"]},
    ]
    emitted: list = []
    _drain_l5b_rich_path_after_stage2(
        run_id="run_t1",
        iteration=2,
        action_groups=action_groups,
        decision_emit_fn=emitted.append,
    )
    assert len(emitted) == 1
    record = emitted[0]
    assert record.cluster_id == "C_ALPHA"
    assert record.ag_id == "AG_DECOMPOSED_C_ALPHA"
    assert record.call_site == "l5b_rich_path"
    assert record.run_id == "run_t1"
    assert record.iteration == 2


def test_drain_wrapper_no_op_when_ledger_empty() -> None:
    """A no-op call is the default-flag-OFF path. The wrapper must
    accept an empty ledger without raising and without emitting any
    record — this is the invariant that lets the wiring land before
    the default-on flip."""
    from genie_space_optimizer.optimization.harness import (
        _drain_l5b_rich_path_after_stage2,
    )
    from genie_space_optimizer.optimization.l5b_rich_dispatch import (
        drain_l5b_rich_path_declines,
    )
    drain_l5b_rich_path_declines()  # ensure empty

    emitted: list = []
    _drain_l5b_rich_path_after_stage2(
        run_id="run_empty",
        iteration=0,
        action_groups=[{"id": "AG_X", "source_cluster_ids": ["C_X"]}],
        decision_emit_fn=emitted.append,
    )
    assert emitted == []


def test_drain_wrapper_records_unknown_ag_when_cluster_orphaned() -> None:
    """When no AG in ``action_groups`` owns the cluster, the wrapper
    must still emit the record with ``ag_id="UNKNOWN_AG"`` rather
    than silently dropping it. This matches the underlying drain
    helper's contract — observability before silence."""
    from genie_space_optimizer.optimization.harness import (
        _drain_l5b_rich_path_after_stage2,
    )
    from genie_space_optimizer.optimization.l5b_rich_dispatch import (
        _L5B_RICH_PATH_DECLINES, drain_l5b_rich_path_declines,
    )
    drain_l5b_rich_path_declines()
    _L5B_RICH_PATH_DECLINES.append({
        "cluster_id": "C_ORPHAN",
        "root_cause": "wrong_aggregation",
        "asi_failure_type": "",
        "attempted_archetypes": ("single_row_top_n",),
        "skipped_reason": "no_viable_archetype",
        "question_ids": ("q1",),
    })

    emitted: list = []
    # action_groups holds an AG whose source_cluster_ids does NOT
    # include C_ORPHAN — the resolver returns None and the drain
    # helper falls back to "UNKNOWN_AG".
    _drain_l5b_rich_path_after_stage2(
        run_id="run_orphan",
        iteration=4,
        action_groups=[
            {"id": "AG_OTHER", "source_cluster_ids": ["C_DIFFERENT"]},
        ],
        decision_emit_fn=emitted.append,
    )
    assert len(emitted) == 1
    assert emitted[0].ag_id == "UNKNOWN_AG"
    assert emitted[0].cluster_id == "C_ORPHAN"


def test_drain_wrapper_does_not_mutate_action_groups() -> None:
    """The wrapper passes ``action_groups`` to the resolver but must
    not mutate the list or its contents. Pin this invariant so a
    future "enrich AG with decline info" refactor can't silently
    corrupt the iteration's strategist output."""
    from genie_space_optimizer.optimization.harness import (
        _drain_l5b_rich_path_after_stage2,
    )
    from genie_space_optimizer.optimization.l5b_rich_dispatch import (
        _L5B_RICH_PATH_DECLINES, drain_l5b_rich_path_declines,
    )
    import copy

    drain_l5b_rich_path_declines()
    _L5B_RICH_PATH_DECLINES.append({
        "cluster_id": "C_MUT",
        "root_cause": "x",
        "asi_failure_type": "x",
        "attempted_archetypes": ("a",),
        "skipped_reason": "r",
        "question_ids": ("q1",),
    })

    action_groups = [
        {"id": "AG_M", "source_cluster_ids": ["C_MUT"]},
    ]
    snapshot = copy.deepcopy(action_groups)
    _drain_l5b_rich_path_after_stage2(
        run_id="r",
        iteration=1,
        action_groups=action_groups,
        decision_emit_fn=lambda _: None,
    )
    assert action_groups == snapshot, (
        "wrapper must not mutate action_groups"
    )
