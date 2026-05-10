"""C15 Phase 4.1: ProposalsInput / ProposalSlate JsonRoundTrip contract.

The plan (Task 4.1) targets ``ProposalGenerationInput/Output`` but the
class-declarations test pins the names ``ProposalsInput`` / ``ProposalSlate``
(natural-noun convention, Phase H Task 3). We test those canonical names.
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.stages.proposals import (
    ProposalsInput,
    ProposalSlate,
)
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


def test_input_mixes_jsonroundtrip() -> None:
    assert issubclass(ProposalsInput, JsonRoundTrip)


def test_output_mixes_jsonroundtrip() -> None:
    assert issubclass(ProposalSlate, JsonRoundTrip)


def test_input_round_trip() -> None:
    inp = ProposalsInput(
        proposals_by_ag={"AG1": ({"id": "p001", "content_fingerprint": "fp1"},)},
        rca_id_by_cluster={"C1": "rca-1"},
        cluster_root_cause_by_id={"C1": "missing synonym"},
    )
    payload = inp.to_json()
    restored = ProposalsInput.from_json(payload)
    assert restored.rca_id_by_cluster == {"C1": "rca-1"}


def test_output_round_trip() -> None:
    out = ProposalSlate(
        proposals_by_ag={"AG1": ({"id": "p001", "content_fingerprint": "fp1"},)},
        content_fingerprints_emitted=("fp1",),
    )
    payload = out.to_json()
    restored = ProposalSlate.from_json(payload)
    assert restored.content_fingerprints_emitted == ("fp1",)
