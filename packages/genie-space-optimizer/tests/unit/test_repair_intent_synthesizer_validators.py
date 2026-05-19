"""Plan 5 Task 6 — deterministic post-LLM validators.

Three pure validators (the reviewer's "safety rails — all stay
code-driven" from roadmap.md:353):

  _validate_patch_body_against_patch_type — per-patch-type required
                                            field check.
  _validate_blame_set_in_identifier_allowlist — every blame_set entry
                                                must be allowlisted.
  _validate_benchmark_leakage_relaxed_for_other — n-gram firewall
                                                  for OTHER shape.

Plus the deterministic ``_stamp_intent_id`` helper (mirrors Plan 4's
_stamp_cluster_id; framework-stamped intent IDs, NOT LLM-minted).
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)
from genie_space_optimizer.optimization.repair_intent_synthesizer import (
    _stamp_intent_id,
    _validate_benchmark_leakage_relaxed_for_other,
    _validate_blame_set_in_identifier_allowlist,
    _validate_patch_body_against_patch_type,
)
from genie_space_optimizer.optimization.repair_proposal_typed import (
    PatchBodyValidationError,
    RepairProposal,
)


def _make(
    patch_type: PatchType,
    patch_body: dict,
    *,
    blame: tuple[str, ...] = (),
    repair_shape: RepairShape = RepairShape.TOP_N_BY_METRIC,
) -> RepairProposal:
    return RepairProposal(
        intent_id="intent_H001_AG3_001",
        intent_name="x", intent_description="x",
        repair_shape=repair_shape, patch_type=patch_type,
        rationale="x", confidence="high",
        patch_body=patch_body, blame_set=blame,
    )


# ── _validate_patch_body_against_patch_type ───────────────────────────


def test_patch_body_validator_accepts_complete_add_example_sql_body() -> None:
    rp = _make(PatchType.ADD_EXAMPLE_SQL, {
        "example_question": "q", "example_sql": "SELECT 1",
    })
    _validate_patch_body_against_patch_type(rp)


def test_patch_body_validator_rejects_missing_example_sql() -> None:
    rp = _make(PatchType.ADD_EXAMPLE_SQL, {"example_question": "q"})
    with pytest.raises(PatchBodyValidationError) as exc:
        _validate_patch_body_against_patch_type(rp)
    assert "example_sql" in str(exc.value)


def test_patch_body_validator_accepts_complete_join_spec_body() -> None:
    rp = _make(PatchType.ADD_JOIN_SPEC, {
        "left": "crm.customer", "right": "crm.orders", "on": "customer_id",
    })
    _validate_patch_body_against_patch_type(rp)


def test_patch_body_validator_rejects_missing_join_on() -> None:
    rp = _make(PatchType.ADD_JOIN_SPEC, {
        "left": "crm.customer", "right": "crm.orders",
    })
    with pytest.raises(PatchBodyValidationError) as exc:
        _validate_patch_body_against_patch_type(rp)
    assert "'on'" in str(exc.value) or "on" in str(exc.value)


def test_patch_body_validator_passes_through_unenumerated_patch_types() -> None:
    """ADD_TVF is not in _REQUIRED_PATCH_BODY_FIELDS — accepted
    permissively at this layer; the cross-lever router's compatible-
    shape check will reject the override later."""
    rp = _make(PatchType.ADD_TVF, {"tvf_name": "fn_x"})
    _validate_patch_body_against_patch_type(rp)


# ── _validate_blame_set_in_identifier_allowlist ───────────────────────


def test_blame_set_validator_accepts_subset_of_allowlist() -> None:
    rp = _make(
        PatchType.ADD_EXAMPLE_SQL,
        {"example_question": "q", "example_sql": "SELECT 1"},
        blame=("sales.fact_sales.revenue", "sales.fact_sales.region"),
    )
    _validate_blame_set_in_identifier_allowlist(
        rp, identifier_allowlist={
            "sales.fact_sales.revenue", "sales.fact_sales.region",
            "sales.fact_sales.order_date",
        },
    )


def test_blame_set_validator_rejects_unknown_identifier() -> None:
    rp = _make(
        PatchType.ADD_EXAMPLE_SQL,
        {"example_question": "q", "example_sql": "SELECT 1"},
        blame=("sales.fact_sales.revenue", "bogus.schema.col"),
    )
    with pytest.raises(ValueError) as exc:
        _validate_blame_set_in_identifier_allowlist(
            rp, identifier_allowlist={"sales.fact_sales.revenue"},
        )
    assert "bogus.schema.col" in str(exc.value)


def test_blame_set_validator_accepts_empty_blame_for_instruction_patch() -> None:
    """ADD_INSTRUCTION + UPDATE_INSTRUCTION are prose patches — empty
    blame_set is acceptable."""
    rp = _make(
        PatchType.ADD_INSTRUCTION,
        {"instruction_text": "Always group by region."},
        blame=(),
    )
    _validate_blame_set_in_identifier_allowlist(
        rp, identifier_allowlist=set(),
    )


def test_blame_set_validator_is_case_sensitive() -> None:
    """UC identifiers in Genie Spaces are case-sensitive — validator
    must NOT case-fold."""
    rp = _make(
        PatchType.ADD_EXAMPLE_SQL,
        {"example_question": "q", "example_sql": "SELECT 1"},
        blame=("Sales.Fact_Sales.Revenue",),
    )
    with pytest.raises(ValueError):
        _validate_blame_set_in_identifier_allowlist(
            rp, identifier_allowlist={"sales.fact_sales.revenue"},
        )


# ── _validate_benchmark_leakage_relaxed_for_other ─────────────────────


def test_leakage_validator_passes_when_no_benchmark_text_overlap() -> None:
    """For OTHER repair_shape with ADD_EXAMPLE_SQL, run a permissive
    n-gram firewall against the benchmark corpus. Pass when overlap
    is below the threshold."""
    rp = _make(
        PatchType.ADD_EXAMPLE_SQL,
        {
            "example_question": "What is total revenue by region this year?",
            "example_sql": "SELECT region, SUM(revenue) FROM sales GROUP BY region",
        },
        repair_shape=RepairShape.OTHER,
    )
    benchmarks = [
        {"question": "How many distinct customers visited last week?"},
        {"question": "What was the average order value in Q1?"},
    ]
    _validate_benchmark_leakage_relaxed_for_other(rp, benchmarks=benchmarks)


def test_leakage_validator_rejects_when_example_question_paraphrases_benchmark() -> None:
    rp = _make(
        PatchType.ADD_EXAMPLE_SQL,
        {
            "example_question": "How many distinct customers visited last week?",
            "example_sql": "SELECT COUNT(DISTINCT id) FROM sales",
        },
        repair_shape=RepairShape.OTHER,
    )
    benchmarks = [
        {"question": "How many distinct customers visited last week?"},
    ]
    with pytest.raises(ValueError) as exc:
        _validate_benchmark_leakage_relaxed_for_other(rp, benchmarks=benchmarks)
    assert "leakage" in str(exc.value).lower()


def test_leakage_validator_no_op_for_non_other_shape() -> None:
    """For closed repair_shapes, the standard leakage gate inside the
    L5b synthesis pipeline runs — this relaxed validator only fires
    when repair_shape == OTHER (the closed-gate bypass case)."""
    rp = _make(
        PatchType.ADD_EXAMPLE_SQL,
        {
            "example_question": "How many distinct customers visited last week?",
            "example_sql": "SELECT 1",
        },
        repair_shape=RepairShape.TOP_N_BY_METRIC,  # not OTHER
    )
    benchmarks = [
        {"question": "How many distinct customers visited last week?"},
    ]
    _validate_benchmark_leakage_relaxed_for_other(rp, benchmarks=benchmarks)


def test_leakage_validator_no_op_for_non_example_sql_patch_type() -> None:
    """SQL snippets / instructions don't carry example_question — the
    leakage gate only checks example_question / example_sql text."""
    rp = _make(
        PatchType.ADD_SQL_SNIPPET_EXPRESSION,
        {"name": "x", "sql_expression": "SUM(revenue)"},
        repair_shape=RepairShape.OTHER,
    )
    benchmarks = [
        {"question": "anything"},
    ]
    _validate_benchmark_leakage_relaxed_for_other(rp, benchmarks=benchmarks)


# ── _stamp_intent_id ──────────────────────────────────────────────────


def test_stamp_intent_id_format() -> None:
    """intent_<cluster_id>_<ag_id>_<seq:03d> — mirrors Plan 1's
    intent_from_archetype intent_id format (repair_intent.py:292-294)."""
    assert _stamp_intent_id(
        cluster_id="H001", ag_id="AG3", seq=1,
    ) == "intent_H001_AG3_001"
    assert _stamp_intent_id(
        cluster_id="H010", ag_id="AG7", seq=42,
    ) == "intent_H010_AG7_042"


def test_stamp_intent_id_rejects_empty_cluster_id() -> None:
    with pytest.raises(ValueError):
        _stamp_intent_id(cluster_id="", ag_id="AG3", seq=1)


def test_stamp_intent_id_rejects_empty_ag_id() -> None:
    with pytest.raises(ValueError):
        _stamp_intent_id(cluster_id="H001", ag_id="", seq=1)


def test_stamp_intent_id_rejects_zero_or_negative_seq() -> None:
    with pytest.raises(ValueError):
        _stamp_intent_id(cluster_id="H001", ag_id="AG3", seq=0)
    with pytest.raises(ValueError):
        _stamp_intent_id(cluster_id="H001", ag_id="AG3", seq=-1)
