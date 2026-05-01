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
