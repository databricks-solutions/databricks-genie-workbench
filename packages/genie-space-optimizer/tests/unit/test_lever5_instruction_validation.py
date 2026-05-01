from __future__ import annotations

from genie_space_optimizer.optimization.optimizer import _validate_lever5_proposals


def _snapshot() -> dict:
    return {
        "tables": [
            {
                "name": "catalog.schema.payments",
                "column_configs": [
                    {"name": "PAYMENT_AMT"},
                    {"name": "PAYMENT_CURRENCY_CD"},
                ],
            }
        ],
        "functions": [],
        "metric_views": [],
        "instructions": {"text_instructions": []},
    }


def test_validate_lever5_rejects_polluted_add_instruction() -> None:
    proposals = [
        {
            "patch_type": "add_instruction",
            "new_text": (
                "Guidance for wrong_aggregation:\n"
                "- Summary: Root cause: wrong_aggregation; Blamed: PAYMENT_CURRENCY_CD\n"
                "- Affected: PAYMENT_CURRENCY_CD"
            ),
        }
    ]

    valid = _validate_lever5_proposals(proposals, _snapshot(), benchmarks=[])

    assert valid == []


def test_validate_lever5_rejects_operator_repair_plan_voice() -> None:
    proposals = [
        {
            "patch_type": "add_instruction",
            "new_text": (
                "DATA QUALITY NOTES:\n"
                "- Add an instruction in the Genie Space metadata clarifying "
                "that PAYMENT_AMT is already in USD."
            ),
        }
    ]

    valid = _validate_lever5_proposals(proposals, _snapshot(), benchmarks=[])

    assert valid == []


def test_validate_lever5_accepts_publishable_instruction() -> None:
    proposals = [
        {
            "patch_type": "add_instruction",
            "new_text": (
                "DATA QUALITY NOTES:\n"
                "- PAYMENT_AMT is USD-denominated; do not infer that "
                "PAYMENT_CURRENCY_CD = 'USD' is required when the user asks "
                "for total payment amount in USD."
            ),
        }
    ]

    valid = _validate_lever5_proposals(proposals, _snapshot(), benchmarks=[])

    assert len(valid) == 1
    assert valid[0]["patch_type"] == "add_instruction"


def test_validate_lever5_rejects_payment_currency_pollution_regression() -> None:
    polluted = (
        "INSTRUCTIONS YOU MUST FOLLOW WHEN PROVIDING SUMMARIES:\n"
        "- Always state the currency code alongside any monetary amount.\n"
        "\n"
        "Guidance for wrong_aggregation:\n"
        "- Remove the PAYMENT_CURRENCY_CD = USD filter since the user asked "
        "for total payment amount in USD which likely refers to the label/alias "
        "of the amount column rather than filtering by currency code.\n"
        "- Add an instruction in the Genie Space metadata clarifying that "
        "PAYMENT_AMT is already in USD and does not require filtering by "
        "PAYMENT_CURRENCY_CD = USD.\n"
        "- Summary: Root cause: wrong_aggregation; Blamed: PAYMENT_CURRENCY_CD; "
        "1 question(s) affected\n"
        "- Affected: PAYMENT_CURRENCY_CD, PAYMENT_CURRENCY_CD = USD filter"
    )

    valid = _validate_lever5_proposals(
        [{"patch_type": "add_instruction", "new_text": polluted}],
        _snapshot(),
        benchmarks=[],
    )

    assert valid == []
