"""Prompt-facing context builders for IQ Scan results."""

from __future__ import annotations

from typing import Any

from genie_space_optimizer.common.config import (
    SCAN_CHECK_TO_LEVERS,
    SPACE_QUALITY_CHECK_ACTIONS,
)


def scan_recommended_levers(scan_result: dict[str, Any] | None) -> list[int]:
    """Return optimizer levers implied by failing/warning config checks."""
    if not isinstance(scan_result, dict):
        return []
    checks = scan_result.get("checks")
    if not isinstance(checks, list):
        return []

    levers: list[int] = []
    for check_id, check in enumerate(checks[:10], start=1):
        if not isinstance(check, dict):
            continue
        severity = str(check.get("severity") or "pass").lower()
        passed = bool(check.get("passed"))
        if passed and severity != "warning":
            continue
        levers.extend(SCAN_CHECK_TO_LEVERS.get(check_id, []))
    return sorted(set(levers))


def _quality_check_record(
    check_id: int,
    check: dict[str, Any],
) -> dict[str, Any]:
    guidance = SPACE_QUALITY_CHECK_ACTIONS.get(check_id, {})
    severity = str(
        check.get("severity") or ("pass" if check.get("passed") else "fail")
    ).lower()
    return {
        "id": check_id,
        "label": check.get("label") or guidance.get("label") or f"Check {check_id}",
        "passed": bool(check.get("passed")),
        "severity": severity,
        "detail": check.get("detail"),
        "issue": check.get("detail") or check.get("label"),
        "opportunity": guidance.get("opportunity"),
        "preferred_actions": list(guidance.get("preferred_actions") or []),
        "supported_patch_types": list(guidance.get("supported_patch_types") or []),
        "recommended_levers": SCAN_CHECK_TO_LEVERS.get(check_id, []),
        "note": guidance.get("note"),
    }


def build_space_quality_scan_context(
    scan_result: dict[str, Any] | None,
    *,
    recommended_levers: list[int] | None = None,
) -> dict[str, Any] | None:
    """Convert a raw IQ Scan result into compact optimizer prompt context.

    Checks 1-10 are actionable space-curation checks. Checks 11-12 are outcome
    status, so they are included separately and never treated as patch targets.
    """
    if not isinstance(scan_result, dict):
        return None
    checks = scan_result.get("checks")
    if not isinstance(checks, list):
        return None

    failed_checks: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    outcome_checks: list[dict[str, Any]] = []
    scan_levers = scan_recommended_levers(scan_result)

    for check_id, raw_check in enumerate(checks, start=1):
        if not isinstance(raw_check, dict):
            continue
        record = _quality_check_record(check_id, raw_check)
        if check_id > 10:
            outcome_checks.append(
                {
                    "id": record["id"],
                    "label": record["label"],
                    "passed": record["passed"],
                    "severity": record["severity"],
                    "detail": record["detail"],
                    "note": "Outcome status, not a direct patch target.",
                }
            )
            continue
        if not record["passed"]:
            failed_checks.append(record)
        elif record["severity"] == "warning":
            warnings.append(record)

    merged_levers = sorted(set(scan_levers + list(recommended_levers or [])))
    return {
        "score": scan_result.get("score"),
        "total": scan_result.get("total", 12),
        "maturity": scan_result.get("maturity"),
        "failed_checks": failed_checks[:10],
        "warnings": warnings[:10],
        "outcome_checks": outcome_checks,
        "recommended_levers": merged_levers,
        "guidance": (
            "Use failed_checks and warnings as advisory context. Prefer patches "
            "that fix benchmark failures and also advance related IQ checks; "
            "do not invent unsupported patch types or add unrelated quality-only "
            "changes."
        ),
    }
