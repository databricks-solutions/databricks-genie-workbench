from __future__ import annotations

import mlflow
from mlflow.genai.datasets import create_dataset


DATASET_NAME = "gso-instruction-publishability-v1"


records = [
    {
        "inputs": {
            "instruction_text": (
                "Guidance for wrong_aggregation:\n"
                "- Summary: Root cause: wrong_aggregation; Blamed: PAYMENT_CURRENCY_CD\n"
                "- Affected: PAYMENT_CURRENCY_CD"
            )
        },
        "expectations": {
            "publishable": "no",
            "reason": "contains internal RCA diagnostics",
        },
    },
    {
        "inputs": {
            "instruction_text": (
                "DATA QUALITY NOTES:\n"
                "- Add an instruction in the Genie Space metadata clarifying "
                "that PAYMENT_AMT is already in USD."
            )
        },
        "expectations": {
            "publishable": "no",
            "reason": "optimizer repair plan voice",
        },
    },
    {
        "inputs": {
            "instruction_text": (
                "DATA QUALITY NOTES:\n"
                "- PAYMENT_AMT is USD-denominated; do not infer that "
                "PAYMENT_CURRENCY_CD = 'USD' is required when the user asks "
                "for total payment amount in USD."
            )
        },
        "expectations": {
            "publishable": "yes",
            "reason": "Genie-facing natural-language rule",
        },
    },
]


def main() -> None:
    dataset = create_dataset(name=DATASET_NAME)
    dataset.merge_records(records)
    print(f"Created or updated dataset {DATASET_NAME} with {len(records)} records")


if __name__ == "__main__":
    main()
