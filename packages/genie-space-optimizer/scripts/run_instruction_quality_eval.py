from __future__ import annotations

import mlflow
from mlflow.entities import Feedback
from mlflow.genai.scorers import scorer

from genie_space_optimizer.optimization.instruction_publishability import (
    validate_publishable_instruction_text,
)


DATASET_NAME = "gso-instruction-publishability-v1"


def predict_fn(instruction_text: str) -> dict:
    result = validate_publishable_instruction_text(
        instruction_text,
        known_assets={"payment_amt", "payment_currency_cd"},
    )
    return {
        "publishable": "yes" if result.ok else "no",
        "reasons": result.reasons,
    }


@scorer
def exact_publishability_scorer(
    inputs: dict,
    outputs: dict,
    expectations: dict,
) -> Feedback:
    actual = str(outputs.get("publishable") or "")
    expected = str(expectations.get("publishable") or "")
    correct = actual == expected
    return Feedback(
        name="exact_publishability",
        value="yes" if correct else "no",
        rationale=(
            f"Expected publishable={expected}, got publishable={actual}. "
            f"Reasons: {outputs.get('reasons', [])}"
        ),
        metadata={
            "expected_publishable": expected,
            "actual_publishable": actual,
            "reasons": outputs.get("reasons", []),
        },
    )


def main() -> None:
    dataset = mlflow.genai.datasets.get_dataset(name=DATASET_NAME)
    result = mlflow.genai.evaluate(
        data=dataset,
        predict_fn=predict_fn,
        scorers=[exact_publishability_scorer],
    )
    print(result.metrics)


if __name__ == "__main__":
    main()
