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
