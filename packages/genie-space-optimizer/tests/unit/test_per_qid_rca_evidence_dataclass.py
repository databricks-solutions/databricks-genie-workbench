"""Plan 3 Task 2 — PerQidRcaEvidence frozen+slots+JsonRoundTrip dataclass."""
from __future__ import annotations

import dataclasses

from genie_space_optimizer.optimization.rca_evidence_typed import (
    PerQidRcaEvidence,
)
from genie_space_optimizer.optimization.repair_intent import PatchType
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


def test_dataclass_is_frozen_with_slots() -> None:
    assert dataclasses.is_dataclass(PerQidRcaEvidence)
    assert PerQidRcaEvidence.__dataclass_params__.frozen is True  # type: ignore[attr-defined]
    assert "__slots__" in PerQidRcaEvidence.__dict__


def test_dataclass_mixes_in_json_round_trip() -> None:
    assert issubclass(PerQidRcaEvidence, JsonRoundTrip)


def test_round_trip_through_to_json_from_json() -> None:
    inst = PerQidRcaEvidence(
        qid="gs_009",
        observed_failure="returned 1 row instead of top 3",
        generated_sql_issue="missing LIMIT 3 and ORDER BY revenue DESC",
        expected_sql_shape="SELECT product, SUM(revenue) GROUP BY 1 ORDER BY 2 DESC LIMIT 3",
        blame_set=("sales.fact_sales.revenue",),
        suggested_repair_family="top_n_with_ordering",
        repair_hint_patch_type=PatchType.ADD_EXAMPLE_SQL,
        confidence="high",
        quoted_evidence=("judge: 'expected 3 rows, got 1'",),
    )
    payload = inst.to_json()
    assert payload["repair_hint_patch_type"] == "add_example_sql"
    assert payload["blame_set"] == ["sales.fact_sales.revenue"]
    rebuilt = PerQidRcaEvidence.from_json(payload)
    assert rebuilt == inst


def test_blame_set_and_quoted_evidence_are_tuples_not_lists() -> None:
    inst = PerQidRcaEvidence(
        qid="gs_001",
        observed_failure="x",
        generated_sql_issue="x",
        expected_sql_shape="x",
        blame_set=("a", "b"),
        suggested_repair_family="x",
        repair_hint_patch_type=PatchType.ADD_INSTRUCTION,
        confidence="medium",
        quoted_evidence=("q1",),
    )
    assert isinstance(inst.blame_set, tuple)
    assert isinstance(inst.quoted_evidence, tuple)


def test_repair_hint_patch_type_is_patch_type_enum_at_runtime() -> None:
    inst = PerQidRcaEvidence(
        qid="gs_001",
        observed_failure="x",
        generated_sql_issue="x",
        expected_sql_shape="x",
        blame_set=(),
        suggested_repair_family="x",
        repair_hint_patch_type=PatchType.UPDATE_INSTRUCTION,
        confidence="low",
        quoted_evidence=(),
    )
    assert inst.repair_hint_patch_type is PatchType.UPDATE_INSTRUCTION
    rebuilt = PerQidRcaEvidence.from_json(inst.to_json())
    assert str(rebuilt.repair_hint_patch_type) in {
        "PatchType.UPDATE_INSTRUCTION",
        "update_instruction",
    }


def test_pretty_render_includes_qid_and_repair_family() -> None:
    inst = PerQidRcaEvidence(
        qid="gs_042",
        observed_failure="wrong dimension grain",
        generated_sql_issue="grouped by region, expected state",
        expected_sql_shape="GROUP BY state",
        blame_set=("dim.state.name",),
        suggested_repair_family="grain_correction_to_state",
        repair_hint_patch_type=PatchType.ADD_INSTRUCTION,
        confidence="high",
        quoted_evidence=(),
    )
    rendered = inst.to_pretty()
    assert "gs_042" in rendered
    assert "grain_correction_to_state" in rendered
