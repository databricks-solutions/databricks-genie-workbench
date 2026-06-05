"""P4 C5 unit tests — evidence-based mechanism-coverage check."""
from __future__ import annotations

import json

import pytest

from genie_space_optimizer.optimization.mechanism_coverage import (
    BehaviorDeltaCategory,
    adequate_mechanisms_for_category,
    check_mechanism_coverage,
    classify_behavior_delta,
    mechanism_coverage_marker,
)
from genie_space_optimizer.optimization.patch_mechanism import PatchMechanism


@pytest.mark.parametrize(
    "text,expected",
    [
        ("results need top 3 ordering by amount", BehaviorDeltaCategory.RANK_ORDER_TOPN),
        ("ORDER BY amount DESC LIMIT 3 missing", BehaviorDeltaCategory.RANK_ORDER_TOPN),
        ("rank within group missing", BehaviorDeltaCategory.RANK_ORDER_TOPN),
        ("highest revenue customers", BehaviorDeltaCategory.RANK_ORDER_TOPN),
        ("column meaning ambiguous between revenue and net_revenue", BehaviorDeltaCategory.COLUMN_AMBIGUITY),
        ("synonym needed for net_amount", BehaviorDeltaCategory.COLUMN_AMBIGUITY),
        ("status code labels are encoded values", BehaviorDeltaCategory.VALUE_MAPPING),
        ("missing join between orders and customers", BehaviorDeltaCategory.JOIN_GROUNDING),
        ("wrong join key on orders.user_id", BehaviorDeltaCategory.JOIN_GROUNDING),
        ("not a recognized category", BehaviorDeltaCategory.OTHER),
        ("", BehaviorDeltaCategory.OTHER),
    ],
)
def test_classify_behavior_delta(text, expected):
    assert classify_behavior_delta(text) == expected


def test_join_grounding_precedence_over_rank_order():
    """A delta containing BOTH join AND order keywords resolves to
    join_grounding (most-specific wins)."""
    category = classify_behavior_delta(
        "missing join on customer_id AND missing ordering"
    )
    assert category == BehaviorDeltaCategory.JOIN_GROUNDING


def test_rank_order_topn_requires_structural_mechanism():
    verdict = check_mechanism_coverage(
        behavior_delta="results need top 3 ordering by amount",
        proposed_mechanisms=(PatchMechanism.INSTRUCTION_TEXT,),
    )
    assert verdict.outcome == "uncovered"
    assert verdict.inferred_category == BehaviorDeltaCategory.RANK_ORDER_TOPN
    assert "instruction_text" in verdict.feedback


def test_rank_order_topn_covered_by_example_sql():
    verdict = check_mechanism_coverage(
        behavior_delta="results need top 3 ordering by amount",
        proposed_mechanisms=(PatchMechanism.EXAMPLE_SQL,),
    )
    assert verdict.outcome == "covered"


def test_rank_order_topn_covered_by_sql_snippet():
    verdict = check_mechanism_coverage(
        behavior_delta="results need top 3 ordering by amount",
        proposed_mechanisms=(PatchMechanism.SQL_SNIPPET,),
    )
    assert verdict.outcome == "covered"


def test_column_ambiguity_covered_by_metadata_description():
    verdict = check_mechanism_coverage(
        behavior_delta="column meaning ambiguous between revenue and net_revenue",
        proposed_mechanisms=(PatchMechanism.METADATA_DESCRIPTION,),
    )
    assert verdict.outcome == "covered"


def test_join_grounding_uncovered_by_instruction_only():
    verdict = check_mechanism_coverage(
        behavior_delta="missing join between orders and customers",
        proposed_mechanisms=(PatchMechanism.INSTRUCTION_TEXT,),
    )
    assert verdict.outcome == "uncovered"


def test_join_grounding_covered_by_join_spec():
    verdict = check_mechanism_coverage(
        behavior_delta="missing join between orders and customers",
        proposed_mechanisms=(PatchMechanism.METADATA_JOIN,),
    )
    assert verdict.outcome == "covered"


def test_value_mapping_uncovered_by_instruction_only():
    verdict = check_mechanism_coverage(
        behavior_delta="status code labels need value mapping",
        proposed_mechanisms=(PatchMechanism.INSTRUCTION_TEXT,),
    )
    assert verdict.outcome == "uncovered"


def test_value_mapping_covered_by_metadata_description():
    verdict = check_mechanism_coverage(
        behavior_delta="status code labels need value mapping",
        proposed_mechanisms=(PatchMechanism.METADATA_DESCRIPTION,),
    )
    assert verdict.outcome == "covered"


def test_other_category_fail_open():
    """No recognized pattern → all mechanisms covered (fail-open)."""
    verdict = check_mechanism_coverage(
        behavior_delta="some weird unrecognized failure mode",
        proposed_mechanisms=(PatchMechanism.INSTRUCTION_TEXT,),
    )
    assert verdict.outcome == "covered"
    assert verdict.inferred_category == BehaviorDeltaCategory.OTHER


def test_override_path_with_justification():
    verdict = check_mechanism_coverage(
        behavior_delta="results need top 3 ordering by amount",
        proposed_mechanisms=(PatchMechanism.INSTRUCTION_TEXT,),
        mechanism_coverage_override_justification=(
            "team explicitly chose to test instruction-only patch "
            "against this delta because the SQL shape can be taught "
            "via guidance text alone"
        ),
    )
    assert verdict.outcome == "override"
    assert "team explicitly" in verdict.override_justification


def test_override_path_empty_justification_uncovered():
    verdict = check_mechanism_coverage(
        behavior_delta="results need top 3 ordering by amount",
        proposed_mechanisms=(PatchMechanism.INSTRUCTION_TEXT,),
        mechanism_coverage_override_justification="   ",  # whitespace only
    )
    assert verdict.outcome == "uncovered"


def test_multi_mechanism_proposal_covered_when_any_adequate():
    """Even if some mechanisms are inadequate, at least one adequate
    mechanism makes the proposal covered."""
    verdict = check_mechanism_coverage(
        behavior_delta="results need top 3 ordering by amount",
        proposed_mechanisms=(
            PatchMechanism.INSTRUCTION_TEXT,  # inadequate alone
            PatchMechanism.EXAMPLE_SQL,  # adequate
        ),
    )
    assert verdict.outcome == "covered"


def test_adequate_mechanisms_pinned():
    """Per-category mechanism sets are stable across changes."""
    assert (
        PatchMechanism.EXAMPLE_SQL
        in adequate_mechanisms_for_category(BehaviorDeltaCategory.RANK_ORDER_TOPN)
    )
    assert (
        PatchMechanism.INSTRUCTION_TEXT
        not in adequate_mechanisms_for_category(BehaviorDeltaCategory.RANK_ORDER_TOPN)
    )
    assert (
        PatchMechanism.METADATA_JOIN
        in adequate_mechanisms_for_category(BehaviorDeltaCategory.JOIN_GROUNDING)
    )


def test_marker_payload():
    verdict = check_mechanism_coverage(
        behavior_delta="top 3 by amount",
        proposed_mechanisms=(PatchMechanism.INSTRUCTION_TEXT,),
    )
    line = mechanism_coverage_marker(
        optimization_run_id="run_X",
        iteration=2,
        qid="gs_009",
        behavior_delta="top 3 by amount",
        verdict=verdict,
    )
    name, _, payload_json = line.partition(" ")
    assert name == "GSO_MECHANISM_COVERAGE_V1"
    payload = json.loads(payload_json)
    assert payload["outcome"] == "uncovered"
    assert payload["inferred_category"] == "rank_order_topn"
    assert payload["proposed_mechanisms"] == ["instruction_text"]
    assert "example_sql" in payload["adequate_mechanisms"]
    assert "sql_snippet" in payload["adequate_mechanisms"]
