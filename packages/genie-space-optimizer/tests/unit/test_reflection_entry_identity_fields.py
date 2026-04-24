"""Regression tests for Phase C2 — identity fields on reflection entries.

The DO-NOT-RETRY guard (D2) and lever-aware tried bookkeeping (D3) rely
on every reflection entry carrying ``root_cause``, ``blame_set``,
``source_cluster_ids``, ``lever_set``, and ``rollback_class``. These
tests pin the shape so a future refactor can't silently drop them.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.harness import _build_reflection_entry
from genie_space_optimizer.optimization.rollback_class import RollbackClass


def _kwargs(**overrides) -> dict:
    base = dict(
        iteration=1,
        ag_id="AG_1",
        accepted=False,
        levers=[5],
        target_objects=[],
        prev_scores={"result_correctness": 95.0},
        new_scores={"result_correctness": 95.0},
        rollback_reason="slice_gate: result_correctness",
        patches=[],
        root_cause="missing_filter",
        blame_set=["fact.is_active"],
        source_cluster_ids=["C001"],
    )
    base.update(overrides)
    return base


def test_reflection_entry_contains_identity_fields() -> None:
    entry = _build_reflection_entry(**_kwargs())
    assert entry["root_cause"] == "missing_filter"
    # blame_set is normalised to a tuple so it's hashable.
    assert entry["blame_set"] == ("fact.is_active",)
    assert entry["source_cluster_ids"] == ["C001"]
    assert entry["lever_set"] == [5]
    assert entry["rollback_class"] == RollbackClass.CONTENT_REGRESSION.value


def test_reflection_entry_normalises_blame_list() -> None:
    entry = _build_reflection_entry(
        **_kwargs(blame_set=["zone_name", "market_description"])
    )
    # Sorted alphabetically so identity keys are stable across runs.
    assert entry["blame_set"] == ("market_description", "zone_name")


def test_reflection_entry_handles_missing_identity_gracefully() -> None:
    """Callers that haven't yet been updated still produce a valid entry;
    the identity fields are simply empty."""
    entry = _build_reflection_entry(
        iteration=1,
        ag_id="AG_1",
        accepted=False,
        levers=[],
        target_objects=[],
        prev_scores={},
        new_scores={},
        rollback_reason=None,
        patches=[],
    )
    assert entry["root_cause"] == ""
    assert entry["blame_set"] == ""
    assert entry["source_cluster_ids"] == []
    assert entry["lever_set"] == []
    assert entry["rollback_class"] == RollbackClass.OTHER.value


def test_reflection_entry_classifies_patch_deploy_failure() -> None:
    entry = _build_reflection_entry(
        **_kwargs(rollback_reason="patch_deploy_failed: 500 Internal Server Error")
    )
    assert entry["rollback_class"] == RollbackClass.INFRA_FAILURE.value


def test_reflection_entry_classifies_schema_fatal() -> None:
    entry = _build_reflection_entry(
        **_kwargs(
            rollback_reason=(
                "patch_deploy_failed: Invalid serialized_space: "
                "Cannot find field: failure_clusters"
            ),
        )
    )
    assert entry["rollback_class"] == RollbackClass.SCHEMA_FAILURE.value
