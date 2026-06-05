"""P4 C3 unit tests — producer-side SQL snippet validator + stamper."""
from __future__ import annotations

from unittest import mock

import pytest

from genie_space_optimizer.optimization.llm_abstain import AbstainReason
from genie_space_optimizer.optimization.producer_snippet_validator import (
    _mint_snippet_id,
    stamp_snippet_validation_on_body,
    validate_and_stamp_snippet_patch_body,
)


def test_mint_snippet_id_is_deterministic():
    import re

    a = _mint_snippet_id("intent_X", "SELECT 1")
    b = _mint_snippet_id("intent_X", "SELECT 1")
    assert a == b
    # 32-char lowercase hex to satisfy Genie serialized_space ID rules
    # (the prior 16-char id passed validator stamping but failed
    # genie_schema validation downstream — d139/e943 postmortems).
    assert len(a) == 32
    assert re.fullmatch(r"[0-9a-f]{32}", a)


def test_mint_snippet_id_pins_sql_snippet_finalizer():
    """The producer and L6 finalizer must mint the same id for the
    same ``(intent_id, sql)`` pair so a proposal validated via either
    path is idempotent."""
    from genie_space_optimizer.optimization.sql_snippet_finalizer import (
        _snippet_id_for,
    )

    sql = "SELECT amount FROM main.sales.orders"
    intent_id = "intent_pin_check"

    class _Proposal:
        pass

    p = _Proposal()
    p.intent_id = intent_id  # type: ignore[attr-defined]
    finalizer_id = _snippet_id_for(p, sql)  # type: ignore[arg-type]
    producer_id = _mint_snippet_id(intent_id, sql)
    assert finalizer_id == producer_id


def test_stamp_snippet_validation_on_body_mutates_in_place():
    body = {"name": "top3", "sql_expression": "SELECT * FROM t LIMIT 3"}
    stamp_snippet_validation_on_body(
        body,
        intent_id="intent_X",
        snippet_name="top3",
        normalized_sql="SELECT * FROM t LIMIT 3",
        snippet_type="filter",
        description="top 3 rows",
    )
    assert body["validation_passed"] is True
    assert body["snippet_id"] == _mint_snippet_id(
        "intent_X", "SELECT * FROM t LIMIT 3"
    )
    assert body["sql_snippet"]["id"] == body["snippet_id"]
    assert body["sql_snippet"]["sql"] == "SELECT * FROM t LIMIT 3"
    assert body["sql_snippet"]["type"] == "filter"


def test_stamp_replaces_sql_when_validator_normalized_it():
    body = {"name": "f", "sql_expression": "select 1"}
    stamp_snippet_validation_on_body(
        body,
        intent_id="i",
        snippet_name="f",
        normalized_sql="SELECT 1",  # canonicalized
        snippet_type="expression",
        description="",
    )
    assert body["sql_expression"] == "SELECT 1"
    assert body["sql_snippet"]["sql"] == "SELECT 1"


def test_validate_and_stamp_returns_stamped_on_success():
    body = {
        "name": "top3_orders",
        "sql_expression": "ORDER BY amount DESC LIMIT 3",
    }
    with mock.patch(
        "genie_space_optimizer.optimization.stages.validate_patch."
        "validate_sql_snippet",
        return_value=(True, "ORDER BY amount DESC LIMIT 3"),
    ):
        verdict = validate_and_stamp_snippet_patch_body(
            body,
            intent_id="intent_top3",
            patch_type_wire="add_sql_snippet_filter",
            metadata_snapshot={},
            # Provide a backend so the producer consults the (mocked)
            # canonical validator instead of the no-backend defer path.
            w=object(),
            warehouse_id="wh",
        )
    assert verdict.outcome == "stamped"
    assert verdict.abstain_reason is None
    assert body["validation_passed"] is True
    assert body["snippet_id"]
    # The stamped ID must be a Genie-valid 32-char lowercase hex string.
    import re as _re

    assert _re.fullmatch(r"[0-9a-f]{32}", body["snippet_id"])
    assert body["sql_snippet"]["type"] == "filter"


def test_stamped_id_validator_helper_rejects_16_char_id():
    from genie_space_optimizer.optimization.producer_snippet_validator import (
        _stamped_id_is_genie_valid,
    )

    bad = {
        "snippet_id": "01dc008002e45949",
        "sql_snippet": {"id": "01dc008002e45949"},
    }
    ok, err = _stamped_id_is_genie_valid(bad)
    assert ok is False
    assert "not a valid 32-char" in err

    good_id = "0" * 32
    good = {"snippet_id": good_id, "sql_snippet": {"id": good_id}}
    ok2, _ = _stamped_id_is_genie_valid(good)
    assert ok2 is True


def test_validate_and_stamp_declines_when_minted_id_is_malformed():
    # Defence-in-depth: if a regression ever reintroduces a short id,
    # the post-stamp Genie-schema guard must convert stamped->declined
    # rather than ship the invalid asset (e943 postmortem).
    body = {
        "name": "top3_orders",
        "sql_expression": "ORDER BY amount DESC LIMIT 3",
    }
    with mock.patch(
        "genie_space_optimizer.optimization.stages.validate_patch."
        "validate_sql_snippet",
        return_value=(True, "ORDER BY amount DESC LIMIT 3"),
    ), mock.patch(
        "genie_space_optimizer.optimization.producer_snippet_validator."
        "_mint_snippet_id",
        return_value="01dc008002e45949",  # 16-char — invalid
    ):
        verdict = validate_and_stamp_snippet_patch_body(
            body,
            intent_id="intent_top3",
            patch_type_wire="add_sql_snippet_filter",
            metadata_snapshot={},
            w=object(),
            warehouse_id="wh",
        )
    assert verdict.outcome == "declined"
    assert verdict.abstain_reason == AbstainReason.SNIPPET_INVALID
    assert "Genie schema" in verdict.error_message


def test_validate_and_stamp_returns_declined_on_validator_failure():
    body = {"name": "broken", "sql_expression": "ORDER BY unknown_col"}
    with mock.patch(
        "genie_space_optimizer.optimization.stages.validate_patch."
        "validate_sql_snippet",
        return_value=(False, "unknown column: unknown_col"),
    ):
        verdict = validate_and_stamp_snippet_patch_body(
            body,
            intent_id="intent_broken",
            patch_type_wire="add_sql_snippet_filter",
            metadata_snapshot={},
            # A backend must be present for the producer to run (and
            # honour) the canonical validator's failure verdict; with no
            # backend the producer defers to the applier-side gate.
            w=object(),
            warehouse_id="wh",
        )
    assert verdict.outcome == "declined"
    assert verdict.abstain_reason == AbstainReason.SNIPPET_INVALID
    assert "unknown column" in verdict.error_message
    # patch_body must NOT have been stamped on failure.
    assert "validation_passed" not in body
    assert "snippet_id" not in body
    assert "sql_snippet" not in body


def test_validate_and_stamp_passes_through_when_no_backend():
    """No SQL execution backend (offline / replay / unit harness): the
    producer fast-check cannot run live validation, so it stamps the
    parse-clean snippet and defers to the authoritative applier-side
    ``gate_validate_sql_snippet``. This restores pre-producer-validator
    behaviour for offline replay anchors (which have no warehouse)."""
    body = {"name": "top3", "sql_expression": "ORDER BY amount DESC LIMIT 3"}
    # No spark, no w/warehouse_id, AND the canonical validator is mocked
    # to FAIL — proving the producer never consults it when no backend
    # exists (the guard short-circuits before the canonical call).
    with mock.patch(
        "genie_space_optimizer.optimization.stages.validate_patch."
        "validate_sql_snippet",
        return_value=(False, "should not be called offline"),
    ):
        verdict = validate_and_stamp_snippet_patch_body(
            body,
            intent_id="intent_offline",
            patch_type_wire="add_sql_snippet_filter",
            metadata_snapshot={},
        )
    assert verdict.outcome == "stamped"
    assert verdict.abstain_reason is None
    assert body["validation_passed"] is True
    assert body["snippet_id"]
    assert body["sql_snippet"]["type"] == "filter"


def test_validate_and_stamp_declines_on_empty_sql():
    body = {"name": "empty"}  # no sql_expression / example_sql
    verdict = validate_and_stamp_snippet_patch_body(
        body,
        intent_id="i",
        patch_type_wire="add_sql_snippet_measure",
        metadata_snapshot={},
    )
    assert verdict.outcome == "declined"
    assert verdict.abstain_reason == AbstainReason.SNIPPET_INVALID
    assert "missing" in verdict.error_message


def test_validate_and_stamp_noop_for_non_snippet_patch_type():
    body = {"instruction_text": "Use ORDER BY amount DESC"}
    verdict = validate_and_stamp_snippet_patch_body(
        body,
        intent_id="i",
        patch_type_wire="add_instruction",
        metadata_snapshot={},
    )
    assert verdict.outcome == "stamped"
    assert verdict.abstain_reason is None
    # No stamping happens for non-snippet patch types.
    assert "validation_passed" not in body


def test_snippet_invalid_abstain_reason_value():
    assert AbstainReason.SNIPPET_INVALID.value == "snippet_invalid"


# ── Trial 24 Follow-on B — no-op suppression snippet guard ────────────


@pytest.mark.parametrize(
    "sql",
    [
        "1=1",
        "1 = 1",
        "TRUE",
        "true",
        "WHERE 1=1",
        "where TRUE",
        "(1=1)",
        "TRUE;",
        "  where   1 = 1  ",
    ],
)
def test_is_noop_suppression_sql_detects_tautologies(sql):
    from genie_space_optimizer.optimization.producer_snippet_validator import (
        _is_noop_suppression_sql,
    )

    assert _is_noop_suppression_sql(sql) is True


@pytest.mark.parametrize(
    "sql",
    [
        "status <> 'cancelled'",
        "WHERE amount > 0",
        "region = 'US'",
        "1=2",
        "",
    ],
)
def test_is_noop_suppression_sql_passes_real_predicates(sql):
    from genie_space_optimizer.optimization.producer_snippet_validator import (
        _is_noop_suppression_sql,
    )

    assert _is_noop_suppression_sql(sql) is False


def test_validate_and_stamp_declines_noop_with_typed_reason():
    # A tautology snippet is declined with the DISTINCT
    # ``snippet_noop_suppression`` reason BEFORE the canonical validator
    # is consulted (so no mock is needed), leaving the body unstamped.
    body = {"name": "noop", "sql_expression": "1=1"}
    verdict = validate_and_stamp_snippet_patch_body(
        body,
        intent_id="intent_noop",
        patch_type_wire="add_sql_snippet_filter",
        metadata_snapshot={},
    )
    assert verdict.outcome == "declined"
    assert verdict.abstain_reason == AbstainReason.SNIPPET_NOOP_SUPPRESSION
    assert "tautology" in verdict.error_message
    assert "validation_passed" not in body
    assert "snippet_id" not in body


def test_snippet_noop_suppression_abstain_reason_value():
    assert (
        AbstainReason.SNIPPET_NOOP_SUPPRESSION.value
        == "snippet_noop_suppression"
    )
