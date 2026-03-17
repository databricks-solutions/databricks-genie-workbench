"""Suggestion endpoints: list, review, apply."""

from __future__ import annotations

import json
import logging
import re

from fastapi import HTTPException

from ..core import Dependencies, create_router
from ..models import SuggestionOut, SuggestionReviewRequest
from .._spark import get_spark

router = create_router()
logger = logging.getLogger(__name__)

_CREATE_MV_RE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?METRIC\s+VIEW\s+"
    r"(?:`?(\w+)`?\.)?(?:`?(\w+)`?\.)?`?(\w+)`?",
    re.IGNORECASE,
)
_CREATE_FUNC_RE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+"
    r"(?:`?(\w+)`?\.)?(?:`?(\w+)`?\.)?`?(\w+)`?",
    re.IGNORECASE,
)


def _row_to_suggestion(row: dict) -> SuggestionOut:
    aff_raw = row.get("affected_questions", "[]")
    try:
        affected = json.loads(aff_raw) if isinstance(aff_raw, str) else (aff_raw or [])
    except (json.JSONDecodeError, TypeError):
        affected = []

    return SuggestionOut(
        suggestionId=row.get("suggestion_id", ""),
        runId=row.get("run_id", ""),
        spaceId=row.get("space_id", ""),
        iteration=row.get("iteration"),
        suggestionType=row.get("type", ""),
        title=row.get("title", ""),
        rationale=row.get("rationale"),
        definition=row.get("definition"),
        affectedQuestions=affected,
        estimatedImpact=row.get("estimated_impact"),
        status=row.get("status", "PROPOSED"),
        reviewedBy=row.get("reviewed_by"),
        reviewedAt=row.get("reviewed_at"),
    )


@router.get(
    "/runs/{run_id}/suggestions",
    response_model=list[SuggestionOut],
    operation_id="getImprovementSuggestions",
)
def get_suggestions(run_id: str, config: Dependencies.Config):
    from genie_space_optimizer.optimization.state import load_suggestions

    spark = get_spark()
    df = load_suggestions(spark, run_id, config.catalog, config.schema_name)

    if df.empty:
        return []

    return [_row_to_suggestion(row.to_dict()) for _, row in df.iterrows()]


@router.post(
    "/suggestions/{suggestion_id}/review",
    response_model=SuggestionOut,
    operation_id="reviewSuggestion",
)
def review_suggestion(
    suggestion_id: str,
    body: SuggestionReviewRequest,
    config: Dependencies.Config,
    user_ws: Dependencies.UserClient,
):
    from genie_space_optimizer.optimization.state import (
        load_suggestion_by_id,
        update_suggestion_status,
    )

    if body.status not in {"ACCEPTED", "REJECTED"}:
        raise HTTPException(status_code=400, detail="status must be ACCEPTED or REJECTED")

    spark = get_spark()

    existing = load_suggestion_by_id(spark, suggestion_id, config.catalog, config.schema_name)
    if not existing:
        raise HTTPException(status_code=404, detail="Suggestion not found")

    try:
        reviewer = user_ws.current_user.me().user_name
    except Exception:
        reviewer = None

    update_suggestion_status(
        spark, suggestion_id, config.catalog, config.schema_name,
        status=body.status, reviewed_by=reviewer,
    )

    updated = load_suggestion_by_id(spark, suggestion_id, config.catalog, config.schema_name)
    if not updated:
        raise HTTPException(status_code=404, detail="Suggestion not found after update")
    return _row_to_suggestion(updated)


@router.post(
    "/suggestions/{suggestion_id}/apply",
    response_model=SuggestionOut,
    operation_id="applySuggestion",
)
def apply_suggestion(
    suggestion_id: str,
    config: Dependencies.Config,
    ws: Dependencies.Client,
):
    from genie_space_optimizer.optimization.state import (
        load_suggestion_by_id,
        update_suggestion_status,
    )

    spark = get_spark()

    row = load_suggestion_by_id(spark, suggestion_id, config.catalog, config.schema_name)
    if not row:
        raise HTTPException(status_code=404, detail="Suggestion not found")

    if row.get("status") != "ACCEPTED":
        raise HTTPException(status_code=400, detail="Suggestion must be ACCEPTED before applying")

    definition = (row.get("definition") or "").strip()
    if not definition:
        raise HTTPException(status_code=400, detail="Suggestion has no SQL definition")

    suggestion_type = row.get("type", "")
    space_id = row.get("space_id", "")

    try:
        ws.statement_execution.execute_statement(
            warehouse_id=config.warehouse_id,
            statement=definition,
            wait_timeout="120s",
        )
    except Exception as exc:
        logger.error("Failed to apply suggestion %s: %s", suggestion_id, exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"SQL execution failed: {exc}") from exc

    if space_id and suggestion_type in ("METRIC_VIEW", "FUNCTION"):
        try:
            _register_in_genie_space(ws, space_id, suggestion_type, definition)
        except Exception:
            logger.warning(
                "SQL executed but failed to register %s in Genie Space %s",
                suggestion_type, space_id, exc_info=True,
            )

    update_suggestion_status(
        spark, suggestion_id, config.catalog, config.schema_name,
        status="IMPLEMENTED",
    )

    updated = load_suggestion_by_id(spark, suggestion_id, config.catalog, config.schema_name)
    if not updated:
        raise HTTPException(status_code=404, detail="Suggestion not found after apply")
    return _row_to_suggestion(updated)


def _extract_identifier(definition: str, suggestion_type: str) -> str | None:
    """Parse the three-level name from a CREATE METRIC VIEW or CREATE FUNCTION statement."""
    pattern = _CREATE_MV_RE if suggestion_type == "METRIC_VIEW" else _CREATE_FUNC_RE
    m = pattern.search(definition)
    if not m:
        return None
    parts = [p for p in m.groups() if p]
    return ".".join(parts)


def _register_in_genie_space(
    ws: "WorkspaceClient",  # type: ignore[name-defined]
    space_id: str,
    suggestion_type: str,
    definition: str,
) -> None:
    """Add the newly created object to the Genie Space data_sources config."""
    from genie_space_optimizer.common.genie_client import (
        fetch_space_config,
        patch_space_config,
    )

    identifier = _extract_identifier(definition, suggestion_type)
    if not identifier:
        logger.warning("Could not extract identifier from definition — skipping Genie Space registration")
        return

    config = fetch_space_config(ws, space_id)
    parsed = config.get("_parsed_space", {})
    if not isinstance(parsed, dict):
        parsed = {}

    ds = parsed.setdefault("data_sources", {})
    ds_key = "metric_views" if suggestion_type == "METRIC_VIEW" else "functions"
    entries = ds.setdefault(ds_key, [])

    existing_ids = {e.get("identifier", "") for e in entries if isinstance(e, dict)}
    if identifier in existing_ids:
        logger.info("%s %s already registered in space %s", ds_key, identifier, space_id)
        return

    entries.append({"identifier": identifier})
    logger.info("Registering %s %s in Genie Space %s", ds_key, identifier, space_id)

    patch_space_config(ws, space_id, parsed)
