"""Plan 3 Task 3 — pin field-name alignment between the Pydantic
schema and the dataclass carrier.
"""
from __future__ import annotations

import dataclasses

from genie_space_optimizer.optimization.rca_evidence_typed import (
    PerQidRcaEvidence,
)
from genie_space_optimizer.skills.rca_evidence_extraction.output_schema import (
    PerQidRcaEvidenceOutput,
)


def test_pydantic_and_dataclass_have_identical_field_names() -> None:
    pydantic_fields = set(PerQidRcaEvidenceOutput.model_fields.keys())
    dataclass_fields = {f.name for f in dataclasses.fields(PerQidRcaEvidence)}
    assert pydantic_fields == dataclass_fields, (
        f"Pydantic ⇄ dataclass drift: "
        f"in_pydantic_only={pydantic_fields - dataclass_fields}, "
        f"in_dataclass_only={dataclass_fields - pydantic_fields}"
    )


def test_field_order_matches() -> None:
    """Field order must match because to_legacy_dict and the JSON
    serializer both iterate fields in declaration order."""
    pydantic_order = list(PerQidRcaEvidenceOutput.model_fields.keys())
    dataclass_order = [f.name for f in dataclasses.fields(PerQidRcaEvidence)]
    assert pydantic_order == dataclass_order


def test_pydantic_to_dataclass_construction_round_trips() -> None:
    """A Pydantic instance can be passed to the dataclass constructor
    as kwargs (converting list → tuple for sequence fields)."""
    from genie_space_optimizer.optimization.repair_intent import PatchType
    pydantic_inst = PerQidRcaEvidenceOutput(
        qid="gs_001",
        observed_failure="x",
        generated_sql_issue="x",
        expected_sql_shape="x",
        blame_set=["a", "b"],
        suggested_repair_family="x",
        repair_hint_patch_type=PatchType.ADD_EXAMPLE_SQL,
        confidence="high",
        quoted_evidence=["q1"],
    )
    payload = pydantic_inst.model_dump()
    payload["blame_set"] = tuple(payload["blame_set"])
    payload["quoted_evidence"] = tuple(payload["quoted_evidence"])
    payload["repair_hint_patch_type"] = PatchType(payload["repair_hint_patch_type"])
    dc_inst = PerQidRcaEvidence(**payload)
    assert dc_inst.qid == "gs_001"
    assert dc_inst.blame_set == ("a", "b")
