"""Asset routing scorer — Layer 1 CODE judge.

Checks whether Genie used the correct asset type (MV, TVF, or TABLE).
When results match or both sides return empty, asset-type differences
are treated as soft preferences rather than hard failures.
"""

from __future__ import annotations

from mlflow.entities import Feedback
from mlflow.genai.scorers import scorer

from genie_space_optimizer.common.genie_client import detect_asset_type, sanitize_sql
from genie_space_optimizer.optimization.evaluation import (
    CODE_SOURCE,
    _extract_response_text,
    build_asi_metadata,
    format_asi_markdown,
)


_VALID_ASSET_TYPES = frozenset({"MV", "TVF", "TABLE"})


@scorer
def asset_routing_scorer(inputs: dict, outputs: dict, expectations: dict) -> Feedback:
    """Check if Genie selected the correct asset type."""
    question_id = inputs.get("question_id", "")
    sql = sanitize_sql(_extract_response_text(outputs) or "").lower()
    raw_expected = expectations.get("expected_asset", "").upper()
    expected_sql = expectations.get("expected_response", "")

    if raw_expected in _VALID_ASSET_TYPES:
        expected_type = raw_expected
    else:
        expected_type = detect_asset_type(expected_sql)

    actual_asset = detect_asset_type(sql)

    correct = actual_asset == expected_type

    cmp = outputs.get("comparison", {}) if isinstance(outputs, dict) else {}
    result_matched = cmp.get("match", False)

    gt_rows = cmp.get("gt_rows", -1)
    genie_rows = cmp.get("genie_rows", -1)
    error_type = cmp.get("error_type", "")
    empty_result_match = (
        gt_rows == 0
        and (genie_rows == 0 or error_type == "genie_result_unavailable")
    )

    result_override = not correct and (result_matched or empty_result_match)

    if result_override:
        if result_matched:
            reason = f"results match ({cmp.get('match_type', '?')})"
        else:
            reason = "both returned empty results (no data to validate routing)"
        return Feedback(
            name="asset_routing",
            value="yes",
            rationale=format_asi_markdown(
                judge_name="asset_routing",
                value="yes",
                rationale=(
                    f"OVERRIDE: Expected {expected_type}, got {actual_asset}, "
                    f"but {reason}. "
                    f"Asset preference noted but not a correctness failure."
                ),
                extra={
                    "expected_asset": expected_type,
                    "actual_asset": actual_asset,
                    "override_reason": "result_match" if result_matched else "empty_results",
                },
                question_id=question_id,
            ),
            source=CODE_SOURCE,
        )

    metadata = None
    if not correct:
        metadata = build_asi_metadata(
            failure_type="asset_routing_error",
            severity="major",
            confidence=0.95,
            expected_value=expected_type,
            actual_value=actual_asset,
            blame_set=[f"asset_routing:{expected_type}"],
            counterfactual_fix=f"Add instruction to prefer {expected_type} for this query pattern",
        )
    return Feedback(
        name="asset_routing",
        value="yes" if correct else "no",
        rationale=format_asi_markdown(
            judge_name="asset_routing",
            value="yes" if correct else "no",
            rationale=f"Expected {expected_type}, got {actual_asset}. SQL: {sql[:100]}",
            metadata=metadata,
            extra={"expected_asset": expected_type, "actual_asset": actual_asset},
            question_id=question_id,
        ),
        source=CODE_SOURCE,
        metadata=metadata,
    )
