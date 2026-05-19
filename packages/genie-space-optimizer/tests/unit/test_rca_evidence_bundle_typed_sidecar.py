"""Plan 3 Task 12 — RcaEvidenceBundle gains per_qid_evidence_typed sidecar."""
from __future__ import annotations

import dataclasses

from genie_space_optimizer.optimization.rca_evidence_typed import (
    PerQidRcaEvidence,
)
from genie_space_optimizer.optimization.repair_intent import PatchType
from genie_space_optimizer.optimization.stages.rca_evidence import (
    RcaEvidenceBundle,
)


def test_bundle_has_per_qid_evidence_typed_field() -> None:
    field_names = {f.name for f in dataclasses.fields(RcaEvidenceBundle)}
    assert "per_qid_evidence_typed" in field_names


def test_per_qid_evidence_typed_defaults_to_empty_dict() -> None:
    """Default must be a callable factory (empty dict), not the
    sentinel ``{}`` literal — mutable default would alias across
    instances."""
    out = RcaEvidenceBundle(
        per_qid_evidence={},
        rca_kinds_by_qid={},
        evidence_refs={},
        promoted_to_top_n_qids=(),
    )
    assert out.per_qid_evidence_typed == {}
    out2 = RcaEvidenceBundle(
        per_qid_evidence={},
        rca_kinds_by_qid={},
        evidence_refs={},
        promoted_to_top_n_qids=(),
    )
    out.per_qid_evidence_typed["a"] = "x"  # type: ignore[assignment]
    assert "a" not in out2.per_qid_evidence_typed


def test_bundle_round_trips_with_typed_sidecar_populated() -> None:
    evidence = PerQidRcaEvidence(
        qid="gs_001",
        observed_failure="x",
        generated_sql_issue="x",
        expected_sql_shape="x",
        blame_set=("t.c",),
        suggested_repair_family="top_n_with_ordering",
        repair_hint_patch_type=PatchType.ADD_EXAMPLE_SQL,
        confidence="high",
        quoted_evidence=(),
    )
    out = RcaEvidenceBundle(
        per_qid_evidence={
            "gs_001": {"rca_kind": "top_n_cardinality_collapse"},
        },
        rca_kinds_by_qid={"gs_001": "top_n_cardinality_collapse"},
        evidence_refs={"gs_001": ("trace://x",)},
        promoted_to_top_n_qids=(),
        per_qid_evidence_typed={"gs_001": evidence},
    )
    payload = out.to_json()
    assert "per_qid_evidence_typed" in payload
    assert payload["per_qid_evidence_typed"]["gs_001"]["qid"] == "gs_001"
    rebuilt = RcaEvidenceBundle.from_json(payload)
    assert rebuilt.per_qid_evidence_typed.keys() == {"gs_001"}
    # JsonRoundTrip rebuilds nested dataclasses only when the field
    # type is explicit; this carrier is ``dict[str, Any]`` so the
    # value re-emerges as a plain dict from JSON. The typed
    # round-trip is verified per-dataclass in T2.
    assert rebuilt.per_qid_evidence_typed["gs_001"]["qid"] == "gs_001"


def test_existing_contract_test_still_passes() -> None:
    """Sanity: instantiation without the new field still works
    (default factory)."""
    out = RcaEvidenceBundle(
        per_qid_evidence={
            "gs_001": {
                "rca_kind": "wrong_join_spec",
                "judge_verdict": "wrong_join_spec",
                "sql_diff": "SELECT *",
                "rca_id": "rca-001",
            }
        },
        rca_kinds_by_qid={"gs_001": "wrong_join_spec"},
        evidence_refs={"gs_001": ("trace://run1/iter/1/judge/gs_001",)},
        promoted_to_top_n_qids=(),
    )
    assert out.per_qid_evidence_typed == {}
