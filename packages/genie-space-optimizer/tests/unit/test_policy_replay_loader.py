"""Unit tests for tools.policy_replay loader.

These tests cover the pure loader layer that turns the hand-curated
policy_replay fixture JSON into a ReplayPayload dataclass. They do
NOT exercise the policy classifier (Task 5).
"""
from __future__ import annotations

import json
import pathlib
import textwrap

import pytest

from genie_space_optimizer.tools.policy_replay import (
    ReplayPayload,
    load_payload,
)


def test_load_payload_present_parses_all_buckets(tmp_path: pathlib.Path) -> None:
    fixture = tmp_path / "happy.json"
    fixture.write_text(textwrap.dedent("""\
        {
          "fixture_id": "happy_iter1",
          "run_id": "00000000-0000-0000-0000-000000000000",
          "iteration": 1,
          "ag_id": "AG1",
          "payload_present": true,
          "baseline_post_arbiter": 87.0,
          "candidate_post_arbiter": 91.3,
          "baseline_pre_arbiter": 56.5,
          "candidate_pre_arbiter": 65.2,
          "target_qids": ["q026"],
          "target_fixed_qids": [],
          "target_still_hard_qids": ["q026"],
          "out_of_target_regressed_qids": ["q012"],
          "soft_to_hard_regressed_qids": [],
          "passing_to_hard_regressed_qids": [],
          "unknown_to_hard_regressed_qids": ["q012"],
          "accepted_in_recorded_run": false,
          "reason_code_in_recorded_run": "target_qids_not_improved",
          "source_notes": "synthetic happy-path fixture for the loader test"
        }
    """))

    payload = load_payload(fixture)

    assert isinstance(payload, ReplayPayload)
    assert payload.fixture_id == "happy_iter1"
    assert payload.iteration == 1
    assert payload.payload_present is True
    assert payload.baseline_post_arbiter == 87.0
    assert payload.candidate_post_arbiter == 91.3
    assert payload.target_qids == ("q026",)
    assert payload.target_fixed_qids == ()
    assert payload.out_of_target_regressed_qids == ("q012",)
    assert payload.unknown_to_hard_regressed_qids == ("q012",)
    assert payload.accepted_in_recorded_run is False
    assert payload.reason_code_in_recorded_run == "target_qids_not_improved"


def test_load_payload_absent_returns_sentinel(tmp_path: pathlib.Path) -> None:
    """payload_present=False means no candidate was ever built; the
    classifier must short-circuit without synthesizing a decision."""
    fixture = tmp_path / "absent.json"
    fixture.write_text(textwrap.dedent("""\
        {
          "fixture_id": "no_candidate",
          "run_id": "ffffffff-ffff-ffff-ffff-ffffffffffff",
          "iteration": 0,
          "ag_id": null,
          "payload_present": false,
          "baseline_post_arbiter": 91.7,
          "candidate_post_arbiter": 91.7,
          "baseline_pre_arbiter": null,
          "candidate_pre_arbiter": null,
          "target_qids": [],
          "target_fixed_qids": [],
          "target_still_hard_qids": [],
          "out_of_target_regressed_qids": [],
          "soft_to_hard_regressed_qids": [],
          "passing_to_hard_regressed_qids": [],
          "unknown_to_hard_regressed_qids": [],
          "accepted_in_recorded_run": false,
          "reason_code_in_recorded_run": "no_candidate_built",
          "source_notes": "no proposals were generated in any iteration"
        }
    """))

    payload = load_payload(fixture)

    assert payload.payload_present is False
    assert payload.ag_id is None
    assert payload.baseline_pre_arbiter is None
    assert payload.target_qids == ()


def test_load_payload_rejects_missing_required_field(tmp_path: pathlib.Path) -> None:
    fixture = tmp_path / "broken.json"
    fixture.write_text('{"fixture_id": "missing_run_id"}')
    with pytest.raises(KeyError, match="run_id"):
        load_payload(fixture)
