"""C15-P2.3: RcaEvidenceInput / RcaEvidenceBundle typed contract tests.

The existing rca_evidence stage preserves its production-shape classes
(RcaEvidenceInput with eval_rows / hard_failure_qids / per_qid_judge /
asi_metadata, RcaEvidenceBundle with per_qid_evidence / rca_kinds_by_qid /
evidence_refs / promoted_to_top_n_qids) and adds JsonRoundTrip as a mixin
so boundary-fixture replay can serialize / deserialize stage I/O.

Naming note: test_stage_io_class_declarations.py pins:
  INPUT_CLASS  = RcaEvidenceInput   (not a simplified form)
  OUTPUT_CLASS = RcaEvidenceBundle  (not RcaEvidenceOutput)
These tests conform to those pinned names.

The plan's RcaGrounding StrEnum / RcaCard sub-dataclass is a NEW concept
for the strategist_context stage boundary. Here we test the actual production
output boundary that feeds into clustering (rca_kinds_by_qid, per_qid_evidence).
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.stages.rca_evidence import (
    RcaEvidenceInput,
    RcaEvidenceBundle,
)
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


def test_input_mixes_jsonroundtrip() -> None:
    assert issubclass(RcaEvidenceInput, JsonRoundTrip)


def test_output_mixes_jsonroundtrip() -> None:
    assert issubclass(RcaEvidenceBundle, JsonRoundTrip)


def test_input_to_json_round_trips_fields() -> None:
    """RcaEvidenceInput.to_json() / from_json() preserves all fields."""
    inp = RcaEvidenceInput(
        eval_rows=(
            {"question_id": "gs_001", "genie_sql": "SELECT * FROM t"},
        ),
        hard_failure_qids=("gs_001",),
        soft_signal_qids=(),
        per_qid_judge={"gs_001": {"verdict": "wrong_join_spec"}},
        asi_metadata={"gs_001": {"failure_type": "wrong_join_spec"}},
    )
    payload = inp.to_json()
    restored = RcaEvidenceInput.from_json(payload)
    assert restored.hard_failure_qids == ("gs_001",)
    assert restored.per_qid_judge["gs_001"]["verdict"] == "wrong_join_spec"
    assert len(restored.eval_rows) == 1


def test_output_to_json_round_trips_fields() -> None:
    """RcaEvidenceBundle.to_json() / from_json() preserves evidence fields."""
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
    payload = out.to_json()
    restored = RcaEvidenceBundle.from_json(payload)
    assert restored.rca_kinds_by_qid["gs_001"] == "wrong_join_spec"
    assert restored.promoted_to_top_n_qids == ()
    assert restored.per_qid_evidence["gs_001"]["rca_id"] == "rca-001"


def test_input_and_output_mix_jsonroundtrip() -> None:
    assert issubclass(RcaEvidenceInput, JsonRoundTrip)
    assert issubclass(RcaEvidenceBundle, JsonRoundTrip)
