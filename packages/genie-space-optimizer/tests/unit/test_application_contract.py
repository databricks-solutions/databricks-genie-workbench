"""C15 Phase 4.3: ApplicationInput / AppliedPatchSet JsonRoundTrip contract.

The plan (Task 4.3) refers to ApplicationOutput but the class-declarations
test pins the natural-noun name AppliedPatchSet. We test AppliedPatchSet.
"""

from __future__ import annotations

from genie_space_optimizer.optimization.stages.application import (
    AppliedPatch,
    ApplicationInput,
    AppliedPatchSet,
)
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


def test_input_mixes_jsonroundtrip() -> None:
    assert issubclass(ApplicationInput, JsonRoundTrip)


def test_output_mixes_jsonroundtrip() -> None:
    assert issubclass(AppliedPatchSet, JsonRoundTrip)


def test_input_round_trip() -> None:
    inp = ApplicationInput(
        applied_entries_by_ag={
            "AG1": (
                {
                    "patch": {
                        "proposal_id": "p001#1",
                        "patch_type": "synonym",
                        "target_qids": ["gs_001"],
                        "content_fingerprint": "fp1",
                    }
                },
            )
        },
        ags=({"ag_id": "AG1"},),
        rca_id_by_cluster={"C1": "rca-1"},
        cluster_root_cause_by_id={"C1": "missing synonym"},
    )
    payload = inp.to_json()
    restored = ApplicationInput.from_json(payload)
    assert "AG1" in restored.applied_entries_by_ag
    assert restored.rca_id_by_cluster == {"C1": "rca-1"}


def test_output_round_trip() -> None:
    out = AppliedPatchSet(
        applied=(
            AppliedPatch(
                proposal_id="p001#1",
                ag_id="AG1",
                patch_type="synonym",
                target_qids=("gs_001",),
                content_fingerprint="fp1",
            ),
        ),
        applied_signature="abc123",
    )
    payload = out.to_json()
    restored = AppliedPatchSet.from_json(payload)
    assert len(restored.applied) == 1
    assert restored.applied[0].proposal_id == "p001#1"
    assert restored.applied[0].target_qids == ("gs_001",)
    assert restored.applied_signature == "abc123"
