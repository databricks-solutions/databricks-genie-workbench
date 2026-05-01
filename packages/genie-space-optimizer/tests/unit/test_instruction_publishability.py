from __future__ import annotations

from genie_space_optimizer.optimization.instruction_publishability import (
    compile_publishable_fallback,
    instruction_section_for_failure,
    validate_publishable_instruction_text,
)


def test_validate_accepts_canonical_asset_grounded_instruction() -> None:
    result = validate_publishable_instruction_text(
        "AGGREGATION RULES:\n"
        "- Treat PAYMENT_AMT as USD-denominated; do not add a "
        "PAYMENT_CURRENCY_CD = 'USD' filter solely because the user asks "
        "for total payment amount in USD.",
        known_assets={"payment_amt", "payment_currency_cd"},
    )

    assert result.ok is True
    assert result.reasons == []


def test_validate_rejects_internal_diagnostics_even_with_known_assets() -> None:
    result = validate_publishable_instruction_text(
        "Guidance for wrong_aggregation:\n"
        "- Summary: Root cause: wrong_aggregation; Blamed: PAYMENT_CURRENCY_CD\n"
        "- Affected: PAYMENT_CURRENCY_CD",
        known_assets={"payment_currency_cd"},
    )

    assert result.ok is False
    assert "internal_diagnostic_text" in result.reasons
    assert "missing_canonical_section" in result.reasons


def test_validate_rejects_optimizer_repair_plan_voice() -> None:
    result = validate_publishable_instruction_text(
        "DATA QUALITY NOTES:\n"
        "- Add an instruction in the Genie Space metadata clarifying that "
        "PAYMENT_AMT is already in USD.",
        known_assets={"payment_amt"},
    )

    assert result.ok is False
    assert "optimizer_repair_plan_voice" in result.reasons


def test_validate_rejects_sql_in_text_instructions() -> None:
    result = validate_publishable_instruction_text(
        "QUERY PATTERNS:\n"
        "- Use SELECT PAYMENT_CURRENCY_CD, SUM(PAYMENT_AMT) FROM payments "
        "GROUP BY PAYMENT_CURRENCY_CD for payment totals.",
        known_assets={"payment_currency_cd", "payment_amt", "payments"},
    )

    assert result.ok is False
    assert "sql_in_text_instruction" in result.reasons


def test_instruction_section_for_failure_routes_sql_shape_to_structured_sections() -> None:
    assert instruction_section_for_failure("wrong_aggregation") == "AGGREGATION RULES"
    assert instruction_section_for_failure("missing_filter") == "QUERY RULES"
    assert instruction_section_for_failure("wrong_join") == "JOIN GUIDANCE"
    assert instruction_section_for_failure("asset_routing_error") == "ASSET ROUTING"
    assert instruction_section_for_failure("unknown") == "CONSTRAINTS"


def test_compile_publishable_fallback_declines_sql_shape_failures() -> None:
    proposal = compile_publishable_fallback({
        "cluster_id": "C_payment",
        "failure_type": "wrong_aggregation",
        "blame_set": ["PAYMENT_CURRENCY_CD", "PAYMENT_AMT"],
        "counterfactual_fixes": [
            "Remove the PAYMENT_CURRENCY_CD = USD filter since the user asked "
            "for total payment amount in USD.",
        ],
        "suggested_fix_summary": (
            "Root cause: wrong_aggregation; Blamed: PAYMENT_CURRENCY_CD"
        ),
    })

    assert proposal is None


def test_compile_publishable_fallback_uses_explicit_candidate_only() -> None:
    proposal = compile_publishable_fallback({
        "cluster_id": "C_payment",
        "failure_type": "missing_instruction",
        "blame_set": ["PAYMENT_AMT", "PAYMENT_CURRENCY_CD"],
        "publishable_instruction_candidates": [
            {
                "section_name": "DATA QUALITY NOTES",
                "text": (
                    "PAYMENT_AMT is USD-denominated; do not infer that a "
                    "PAYMENT_CURRENCY_CD = 'USD' filter is required from the "
                    "phrase total payment amount in USD."
                ),
                "assets": ["PAYMENT_AMT", "PAYMENT_CURRENCY_CD"],
            }
        ],
    })

    assert proposal is not None
    assert proposal["patch_type"] == "update_instruction_section"
    assert proposal["section_name"] == "DATA QUALITY NOTES"
    assert "PAYMENT_AMT is USD-denominated" in proposal["new_text"]
    assert "Root cause" not in proposal["new_text"]
    assert "Guidance for" not in proposal["new_text"]
