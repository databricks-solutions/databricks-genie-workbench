"""Trial 20 Workstream F1 — ``validate_sql_snippet`` stamping audit.

The applier (``applier.py:3171``) refuses to apply ``add_sql_snippet_*``
patches unless ``patch_body["validation_passed"]`` is set to ``True``.
Trial 20 F1 closes the gap where Stage 3 ran the validator but never
wrote the field back, causing every SM/Plan-11 snippet repair to
RuntimeError at apply-time.

Pins:

* After a successful :func:`validate_sql_snippet`, the dispatcher
  stamps ``validation_passed=True`` on the patch body.
* For ``add_sql_snippet_*``, the dispatcher also materializes a
  ``sql_snippet`` payload when the LLM did not supply one.
* For ``add_example_sql`` the stamp is set but no ``sql_snippet``
  payload is synthesized (it's not a snippet patch).
* On validation failure, ``validation_passed`` is NOT stamped (so the
  applier's hard assertion still fires).
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)
from genie_space_optimizer.optimization.repair_proposal_typed import (
    RepairProposal,
)
from genie_space_optimizer.optimization.stages.validate_patch import (
    validate_patch,
)


def _make_proposal(patch_type: str, patch_body: dict) -> RepairProposal:
    return RepairProposal(
        intent_id="intent_t20f1",
        intent_name="trial20 f1",
        intent_description="trial20 f1 stamping",
        repair_shape=RepairShape.OTHER,
        patch_type=patch_type,
        rationale="r",
        confidence="high",
        patch_body=patch_body,
        blame_set=(),
        repair_hypothesis="h",
        target_qids=("gs_001",),
    )


@pytest.fixture
def mock_context():
    return dict(
        config={},
        metadata_snapshot={"tables": [], "columns": [], "joins": []},
        spark=MagicMock(),
        w=MagicMock(),
        catalog="main",
        gold_schema="gso",
        warehouse_id="abc",
    )


@patch(
    "genie_space_optimizer.optimization.stages.validate_patch._validate_sql_snippet_entry",
    return_value=(True, []),
)
@patch(
    "genie_space_optimizer.optimization.stages.validate_patch.validate_sql_snippet",
    return_value=(True, "amount > 0"),
)
def test_f1_add_sql_snippet_filter_stamps_validation_passed(
    mock_validate_sql, mock_validate_entry, mock_context,
):
    body = {
        "name": "positive_amount",
        "sql_expression": "amount > 0",
        "target_table": "main.shop.orders",
    }
    proposal = _make_proposal(PatchType.ADD_SQL_SNIPPET_FILTER, body)

    result = validate_patch(proposal, **mock_context)

    assert result.is_valid is True
    assert body.get("validation_passed") is True


@patch(
    "genie_space_optimizer.optimization.stages.validate_patch._validate_sql_snippet_entry",
    return_value=(True, []),
)
@patch(
    "genie_space_optimizer.optimization.stages.validate_patch.validate_sql_snippet",
    return_value=(True, "amount > 0"),
)
def test_f1_add_sql_snippet_filter_materializes_payload(
    mock_validate_sql, mock_validate_entry, mock_context,
):
    body = {
        "name": "positive_amount",
        "sql_expression": "amount > 0",
        "target_table": "main.shop.orders",
    }
    proposal = _make_proposal(PatchType.ADD_SQL_SNIPPET_FILTER, body)
    validate_patch(proposal, **mock_context)
    payload = body.get("sql_snippet")
    assert isinstance(payload, dict)
    assert payload.get("sql") == "amount > 0"
    assert payload.get("target_table") == "main.shop.orders"


@patch(
    "genie_space_optimizer.optimization.stages.validate_patch._validate_example_sql_entry",
    return_value=(True, []),
)
@patch(
    "genie_space_optimizer.optimization.stages.validate_patch.validate_sql_snippet",
    return_value=(True, "SELECT 1"),
)
def test_f1_add_example_sql_stamps_but_no_snippet_payload(
    mock_validate_sql, mock_validate_entry, mock_context,
):
    body = {
        "example_question": "How many?",
        "example_sql": "SELECT 1",
    }
    proposal = _make_proposal(PatchType.ADD_EXAMPLE_SQL, body)
    validate_patch(proposal, **mock_context)
    assert body.get("validation_passed") is True
    assert "sql_snippet" not in body


@patch(
    "genie_space_optimizer.optimization.stages.validate_patch._validate_sql_snippet_entry",
    return_value=(True, []),
)
@patch(
    "genie_space_optimizer.optimization.stages.validate_patch.validate_sql_snippet",
    return_value=(False, "syntax error near amout"),
)
def test_f1_validation_failure_does_not_stamp(
    mock_validate_sql, mock_validate_entry, mock_context,
):
    body = {
        "name": "bad",
        "sql_expression": "amout > 0",
        "target_table": "main.shop.orders",
    }
    proposal = _make_proposal(PatchType.ADD_SQL_SNIPPET_FILTER, body)
    result = validate_patch(proposal, **mock_context)
    assert result.is_valid is False
    assert "validation_passed" not in body
