"""Trial 13i — pin ``Stage1InputEvidenceContract.validate_schema_columns``.

Before Trial 13i the Stage 1 input contract only validated the per-QID
card; the run-level ``schema_columns`` channel was never checked, and
the SM lane shipped ``"schema_columns": []`` to every Stage 1 LLM
call. The post-13h Trial 13h prompt then correctly declined every call
with ``insufficient_blame_set``.

Trial 13i adds ``schema_columns_min_size`` (default 1) and a paired
``validate_schema_columns`` method that returns a single
``"missing_schema_columns"`` violation when the channel is empty.
``_invoke_stage1_llm`` and the batch lane short-circuit on that
violation instead of burning LLM tokens on a guaranteed decline.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.stage1_input_evidence_contract import (
    DEFAULT_STAGE1_CONTRACT,
    Stage1InputCardEmptyError,
    Stage1InputEvidenceContract,
)


def test_default_min_size_is_one() -> None:
    """Production default — single FQN suffices to ground a diagnosis."""
    assert DEFAULT_STAGE1_CONTRACT.schema_columns_min_size == 1


def test_validate_schema_columns_accepts_non_empty_list() -> None:
    violations = DEFAULT_STAGE1_CONTRACT.validate_schema_columns(
        ["main.public.orders.revenue"]
    )
    assert violations == []


def test_validate_schema_columns_rejects_empty_list() -> None:
    violations = DEFAULT_STAGE1_CONTRACT.validate_schema_columns([])
    assert len(violations) == 1
    assert violations[0].field == "missing_schema_columns"


def test_validate_schema_columns_rejects_none() -> None:
    violations = DEFAULT_STAGE1_CONTRACT.validate_schema_columns(None)
    assert len(violations) == 1
    assert violations[0].field == "missing_schema_columns"


def test_validate_schema_columns_treats_whitespace_as_empty() -> None:
    """A list of blank strings must NOT satisfy the minimum count."""
    violations = DEFAULT_STAGE1_CONTRACT.validate_schema_columns(
        ["", "   ", None]  # type: ignore[list-item]
    )
    assert len(violations) == 1
    assert violations[0].field == "missing_schema_columns"


def test_violation_renders_via_input_card_empty_error() -> None:
    """Pre-flight short-circuit uses ``Stage1InputCardEmptyError`` so the
    declined reason is shaped like ``evidence_card_empty:missing_schema_columns``
    — postmortems grep on the field name, not the prefix."""
    violations = DEFAULT_STAGE1_CONTRACT.validate_schema_columns([])
    err = Stage1InputCardEmptyError(violations)
    assert err.as_declined_reason() == (
        "evidence_card_empty:missing_schema_columns"
    )


@pytest.mark.parametrize("min_size", [2, 3, 5])
def test_configurable_min_size_lets_strict_lanes_require_more(min_size: int) -> None:
    """Future per-lane contract instances may tighten the requirement."""
    contract = Stage1InputEvidenceContract(
        schema_columns_min_size=min_size,
    )
    # Below the threshold -> violation.
    violations = contract.validate_schema_columns(
        ["main.public.orders.revenue"]
    )
    assert len(violations) == 1
    assert violations[0].field == "missing_schema_columns"
    # At the threshold -> ok.
    ok = contract.validate_schema_columns(
        [f"main.s.t.c{i}" for i in range(min_size)]
    )
    assert ok == []


def test_contract_still_validates_card_fields_independently() -> None:
    """Adding ``validate_schema_columns`` must not change ``validate``."""
    # An otherwise-valid card must still pass per-card validation.
    card = {
        "qid": "gs_001",
        "question_text": "Top 10?",
        "ground_truth_sql": "SELECT * FROM x ORDER BY r DESC LIMIT 10",
        "generated_sql": "SELECT * FROM x",
        "judge_rationale": "wrong",
        "blame_set_seed": ["main.public.orders.revenue"],
        "rca_evidence": {
            "observed_failure": "wrong rows",
            "generated_sql_issue": "",
            "expected_sql_shape": "",
            "suggested_repair_family": "",
        },
    }
    assert DEFAULT_STAGE1_CONTRACT.validate(card) == []
