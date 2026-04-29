"""The proposal log must mark column-target invalidity."""

from __future__ import annotations

from genie_space_optimizer.optimization.harness import (
    _classify_proposal_log_status,
)


def test_invalid_empty_column_for_column_targeted_type() -> None:
    proposal = {
        "type": "update_column_description",
        "column": [],
        "rationale": "valid JSON",
    }
    status = _classify_proposal_log_status(proposal)
    assert status == "INVALID_TARGET"


def test_invalid_multi_token_column() -> None:
    proposal = {
        "type": "update_column_description",
        "column": ["x", "y"],
        "rationale": "valid JSON",
    }
    status = _classify_proposal_log_status(proposal)
    assert status == "INVALID_TARGET"


def test_valid_single_element_list_column() -> None:
    proposal = {
        "type": "update_column_description",
        "column": ["time_window"],
        "rationale": "valid JSON",
    }
    status = _classify_proposal_log_status(proposal)
    assert status == "OK"


def test_failed_non_json_takes_precedence() -> None:
    proposal = {
        "type": "update_column_description",
        "column": "time_window",
        "rationale": "not valid JSON: ParserError",
    }
    status = _classify_proposal_log_status(proposal)
    assert status.startswith("FAILED")


def test_non_column_targeted_type_unaffected() -> None:
    proposal = {
        "type": "add_instruction",
        "column": [],
        "rationale": "valid JSON",
    }
    status = _classify_proposal_log_status(proposal)
    assert status == "OK"
