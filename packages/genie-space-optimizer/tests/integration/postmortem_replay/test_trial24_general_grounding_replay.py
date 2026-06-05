"""Trial 24 replay-readiness gate — generalized instruction grounding.

Follow-on B's FB2 grounds a solo corrective ``add_instruction``'s
justification only when the cluster ``rca_kind`` is in the Trial 24
forced-kit map (``extra_defensive_filter`` / ``top_n_cardinality_collapse``).
A broader multi-RCA replay can hit the SAME e943 death mode
(``unjustified_single_lever``) for any other ``rca_kind``.

``GSO_TRIAL24_GENERAL_INSTRUCTION_GROUNDING`` widens the FB2 fallback
(``single_lever_justification`` -> ``expected_behavioral_change`` ->
``rationale``) to ANY ``INSTRUCTION_TEXT`` proposal regardless of
``rca_kind``. This fixture-backed bright-line drives the live
``run_plan11_synthesis_for_single_cluster`` boundary for a lone
``add_instruction`` on a NON-allowlist RCA (``ambiguous_terminology``)
carrying an EMPTY ``single_lever_justification`` but a populated
``expected_behavioral_change``:

  * general grounding ON  -> the corrective instruction lands solo.
  * general grounding OFF -> it dies ``unjustified_single_lever``.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import patch as mock_patch

import pytest

from genie_space_optimizer.optimization.stages import synthesize as syn
from genie_space_optimizer.optimization.stages.plan11_types import (
    FailureCluster,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures"
FIXTURE = FIXTURE_DIR / "run_general_grounding_nonallowlist.json"


def _load() -> dict[str, Any]:
    return json.loads(FIXTURE.read_text())["trial24_general_grounding_replay"]


@pytest.fixture(scope="module")
def block() -> dict[str, Any]:
    return _load()


def _stub_reasoning_response(proposals: list[dict]):
    class _Resp:
        succeeded = True
        declined = None
        parsed_output = {"proposals": proposals}
        tokens_input = 100
        tokens_output = 100

    return _Resp()


def _invoke(block: dict[str, Any]):
    c = block["cluster"]
    cluster = FailureCluster(
        cluster_id=c["cluster_id"],
        semantic_theme=c["semantic_theme"],
        member_qids=tuple(c["member_qids"]),
        unifying_evidence=c["unifying_evidence"],
        repair_hypothesis=c["repair_hypothesis"],
        primary_blame_set=tuple(c["primary_blame_set"]),
        confidence=c["confidence"],
        root_cause=c["root_cause"],
    )
    with mock_patch.object(
        syn.LlmReasoningCall,
        "invoke",
        return_value=_stub_reasoning_response([block["llm_proposal"]]),
    ):
        return syn.run_plan11_synthesis_for_single_cluster(
            cluster=cluster,
            schema_slice=block["schema_slice"],
            history=[],
            member_qid_evidence=block["member_qid_evidence"],
            optimization_run_id="r24_genreplay",
            iteration=2,
            ag_id="AG_T24_GENREPLAY",
            w=None,
        )


def test_general_grounding_replay_lands_solo_flag_on(block, monkeypatch) -> None:
    monkeypatch.setenv("GSO_TRIAL24_KIT_AT_SOURCE", "1")
    monkeypatch.delenv(
        "GSO_TRIAL24_GENERAL_INSTRUCTION_GROUNDING", raising=False
    )
    result = _invoke(block)
    assert (result.proposal is not None) == block["expected_general_on"][
        "proposal_survives"
    ], (
        "general-grounding replay: the grounded non-allowlist solo "
        "instruction MUST land flag-on; "
        f"skipped_reason={result.skipped_reason}"
    )


def test_general_grounding_replay_drops_flag_off(block, monkeypatch) -> None:
    monkeypatch.setenv("GSO_TRIAL24_KIT_AT_SOURCE", "1")
    monkeypatch.setenv("GSO_TRIAL24_GENERAL_INSTRUCTION_GROUNDING", "0")
    result = _invoke(block)
    assert (result.proposal is not None) == block["expected_general_off"][
        "proposal_survives"
    ], (
        "general-grounding replay: with general grounding OFF and a "
        "non-allowlist RCA, the ungrounded lone instruction must drop"
    )
