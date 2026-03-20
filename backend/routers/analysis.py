"""
REST API endpoints for Genie Space operations.

Provides endpoints for fetching/parsing spaces, optimization, Genie queries,
SQL execution, benchmarking, and space creation.
"""

import json
from pathlib import Path

import logging

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

from backend.services.genie_client import get_serialized_space
from backend.models import (
    CompareResultsRequest,
    ComparisonResult,
    ConfigMergeRequest,
    ConfigMergeResponse,
    GenieCreateRequest,
    GenieCreateResponse,
    LabelingFeedbackItem,
    OptimizationRequest,
    OptimizationResponse,
    OptimizationSuggestion,
)
from backend.services.optimizer import get_optimizer

router = APIRouter(prefix="/api")


def _safe_error(e: Exception, status_code: int, context: str) -> HTTPException:
    """Create an HTTP exception with safe error message.

    Logs detailed error server-side but returns generic message to client.
    """
    logger.exception(f"{context}: {e}")

    generic_messages = {
        400: "Invalid request. Please check your input and try again.",
        404: "The requested resource was not found.",
        500: "An internal error occurred. Please try again later.",
        504: "The operation timed out. Please try again.",
    }

    message = generic_messages.get(status_code, "An error occurred.")
    return HTTPException(status_code=status_code, detail=message)


# Request/Response models
class FetchSpaceRequest(BaseModel):
    """Request to fetch a Genie Space."""

    genie_space_id: str = Field(
        ..., min_length=1, max_length=64, pattern=r"^[a-zA-Z0-9\-_]+$"
    )


class FetchSpaceResponse(BaseModel):
    """Response containing the fetched Genie Space data."""

    genie_space_id: str
    space_data: dict


class ParseJsonRequest(BaseModel):
    """Request to parse pasted JSON."""

    json_content: str = Field(..., min_length=1, max_length=1_000_000)  # 1MB limit


class GenieQueryRequest(BaseModel):
    """Request to query Genie for SQL."""

    genie_space_id: str = Field(
        ..., min_length=1, max_length=64, pattern=r"^[a-zA-Z0-9\-_]+$"
    )
    question: str = Field(..., min_length=1, max_length=10000)


class GenieQueryResponse(BaseModel):
    """Response containing generated SQL from Genie."""
    sql: str | None
    status: str
    error: str | None
    conversation_id: str
    message_id: str


class ExecuteSqlRequest(BaseModel):
    """Request to execute SQL on a warehouse."""

    sql: str = Field(..., min_length=1, max_length=100_000)  # 100KB limit
    warehouse_id: str | None = Field(None, max_length=64)


class ExecuteSqlResponse(BaseModel):
    """Response from SQL execution."""
    columns: list[dict]
    data: list[list]
    row_count: int
    truncated: bool
    error: str | None


class SettingsResponse(BaseModel):
    """Application settings response."""
    genie_space_id: str | None
    llm_model: str
    sql_warehouse_id: str | None
    databricks_host: str | None
    workspace_directory: str | None


@router.post("/space/fetch", response_model=FetchSpaceResponse)
async def fetch_space(request: FetchSpaceRequest):
    """Fetch and parse a Genie Space by ID.

    Returns the space data.
    """
    try:
        space_data = get_serialized_space(request.genie_space_id)

        return FetchSpaceResponse(
            genie_space_id=request.genie_space_id,
            space_data=space_data,
        )
    except Exception as e:
        raise _safe_error(e, 400, "Failed to fetch Genie space")


@router.post("/space/parse", response_model=FetchSpaceResponse)
async def parse_space_json(request: ParseJsonRequest):
    """Parse pasted Genie Space JSON.

    Accepts the raw API response from GET /api/2.0/genie/spaces/{id}?include_serialized_space=true
    Requires valid JSON format.
    """
    from datetime import datetime

    try:
        try:
            raw_response = json.loads(request.json_content)
        except json.JSONDecodeError as e:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Invalid JSON at line {e.lineno}, column {e.colno}: {e.msg}. "
                    "Please ensure you are pasting valid JSON from the Databricks API response."
                ),
            )

        # Extract and parse the serialized_space field
        if "serialized_space" not in raw_response:
            raise HTTPException(
                status_code=400,
                detail="Invalid input: missing 'serialized_space' field"
            )

        serialized = raw_response["serialized_space"]
        if isinstance(serialized, str):
            space_data = json.loads(serialized)
        else:
            space_data = serialized

        # Generate placeholder ID
        genie_space_id = f"pasted-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

        return FetchSpaceResponse(
            genie_space_id=genie_space_id,
            space_data=space_data,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {str(e)}")


@router.post("/genie/query", response_model=GenieQueryResponse)
async def query_genie(request: GenieQueryRequest):
    """Query a Genie Space with a natural language question.

    Calls the Databricks Genie API to generate SQL for the given question.
    Returns the generated SQL if successful.
    """
    try:
        from backend.services.genie_client import query_genie_for_sql

        result = query_genie_for_sql(
            genie_space_id=request.genie_space_id,
            question=request.question,
        )

        return GenieQueryResponse(**result)
    except TimeoutError as e:
        raise _safe_error(e, 504, "Genie query timed out")
    except Exception as e:
        raise _safe_error(e, 500, "Genie query failed")


@router.get("/debug/auth")
async def debug_auth():
    """Debug endpoint to check authentication status.

    Returns information about the current authentication context.
    Only available in development mode (not on Databricks Apps).
    """
    import os

    from backend.services.auth import get_workspace_client, is_running_on_databricks_apps

    # Disable in production to avoid exposing auth info
    if is_running_on_databricks_apps():
        raise HTTPException(status_code=404, detail="Not found")

    try:
        client = get_workspace_client()

        # Try to get current user/service principal to verify auth is working
        try:
            current_user = client.current_user.me()
            user_info = {
                "user_name": current_user.user_name,
                "display_name": current_user.display_name,
            }
        except Exception as e:
            user_info = {"error": str(e)}

        return {
            "running_on_databricks_apps": is_running_on_databricks_apps(),
            "host": client.config.host,
            "auth_type": client.config.auth_type,
            "current_user": user_info,
            "env_vars": {
                "DATABRICKS_HOST": os.environ.get("DATABRICKS_HOST", "[not set]"),
                "DATABRICKS_APP_PORT": os.environ.get("DATABRICKS_APP_PORT", "[not set]"),
                "DATABRICKS_CLIENT_ID": os.environ.get("DATABRICKS_CLIENT_ID", "[not set]")[:8] + "..." if os.environ.get("DATABRICKS_CLIENT_ID") else "[not set]",
            }
        }
    except Exception as e:
        return {
            "error": str(e),
            "running_on_databricks_apps": is_running_on_databricks_apps(),
        }


@router.post("/sql/execute", response_model=ExecuteSqlResponse)
async def execute_sql_endpoint(request: ExecuteSqlRequest):
    """Execute SQL on a Databricks SQL Warehouse.

    Returns tabular results for display in the UI.
    Limited to 1000 rows to prevent memory issues.
    Only read-only SELECT queries are allowed.
    """
    from backend.sql_executor import execute_sql

    try:
        result = execute_sql(
            sql=request.sql,
            warehouse_id=request.warehouse_id,
        )
        return ExecuteSqlResponse(**result)
    except Exception as e:
        raise _safe_error(e, 500, "SQL execution failed")


@router.get("/settings", response_model=SettingsResponse)
async def get_settings():
    """Get application settings for the Settings page.

    Returns read-only configuration values.
    """
    import os
    from backend.services.auth import get_databricks_host
    from backend.sql_executor import get_sql_warehouse_id

    return SettingsResponse(
        genie_space_id=None,  # This is session-specific, passed from frontend
        llm_model=os.environ.get("LLM_MODEL", "databricks-claude-sonnet-4-6"),
        sql_warehouse_id=get_sql_warehouse_id(),
        databricks_host=get_databricks_host(),
        workspace_directory=os.environ.get("GENIE_TARGET_DIRECTORY", "").strip() or None,
    )


@router.post("/benchmark/compare", response_model=ComparisonResult)
async def compare_benchmark_results(request: CompareResultsRequest):
    """Compare Genie SQL results against expected SQL results.

    Uses LLM-based semantic comparison considering SQL, results, and question context.
    Returns a detailed comparison with match type, confidence,
    discrepancies, and an auto-label suggestion.
    """
    import asyncio

    from backend.services.auth import run_in_context
    from backend.services.result_comparator import compare_results

    try:
        # Run in thread pool since compare_results may call LLM (blocking I/O)
        result = await asyncio.get_running_loop().run_in_executor(
            None,
            run_in_context(
                compare_results,
                genie_result=request.genie_result,
                expected_result=request.expected_result,
                genie_sql=request.genie_sql,
                expected_sql=request.expected_sql,
                question=request.question,
            ),
        )
        return result
    except Exception as e:
        raise _safe_error(e, 500, "Result comparison failed")


@router.post("/optimize")
async def stream_optimizations(request: OptimizationRequest):
    """Stream optimization progress with heartbeats to prevent proxy timeouts.

    Returns Server-Sent Events with:
    - {"status": "processing", "message": "...", "elapsed_seconds": N} - heartbeats every 15s
    - {"status": "complete", "data": {...}} - final result
    - {"status": "error", "message": "..."} - if optimization fails
    """
    import asyncio
    import concurrent.futures

    from backend.services.auth import run_in_context
    from backend.services.lakebase import save_optimization_run

    logger.info(f"Received streaming optimization request for space: {request.genie_space_id}")
    logger.info(f"Feedback items count: {len(request.labeling_feedback)}")

    async def generate():
        """Async SSE generator with heartbeats."""
        loop = asyncio.get_event_loop()
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

        # Run optimizer in thread pool
        def run_optimizer():
            optimizer = get_optimizer()
            return optimizer.generate_optimizations(
                space_data=request.space_data,
                labeling_feedback=request.labeling_feedback,
            )

        future = loop.run_in_executor(executor, run_in_context(run_optimizer))
        start_time = asyncio.get_event_loop().time()
        heartbeat_interval = 15  # seconds

        while True:
            try:
                # Wait for result with timeout (heartbeat interval)
                result = await asyncio.wait_for(
                    asyncio.shield(future), timeout=heartbeat_interval
                )
                # Success - send complete event
                logger.info(f"Generated {len(result.suggestions)} suggestions, sending complete event")
                yield f"data: {json.dumps({'status': 'complete', 'data': result.model_dump()})}\n\n"
                logger.info("Complete event sent")

                # Persist optimization run for scoring
                try:
                    total = len(request.labeling_feedback)
                    correct = sum(1 for f in request.labeling_feedback if f.is_correct is True)
                    await save_optimization_run(request.genie_space_id, total, correct)
                except Exception as e:
                    logger.warning(f"Failed to save optimization run: {e}")

                break
            except asyncio.TimeoutError:
                # Still running - send heartbeat
                elapsed = int(asyncio.get_event_loop().time() - start_time)
                logger.info(f"Sending heartbeat at {elapsed}s")
                yield f"data: {json.dumps({'status': 'processing', 'message': f'Generating suggestions... ({elapsed}s elapsed)', 'elapsed_seconds': elapsed})}\n\n"
            except Exception as e:
                # Error - send error event
                logger.exception(f"Optimization failed: {e}")
                yield f"data: {json.dumps({'status': 'error', 'message': str(e)})}\n\n"
                break

    # Headers to prevent proxy buffering
    headers = {
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(generate(), media_type="text/event-stream", headers=headers)


@router.post("/config/merge", response_model=ConfigMergeResponse)
async def merge_config(request: ConfigMergeRequest):
    """Merge optimization suggestions into a config programmatically.

    This is a fast operation that applies field-level changes without LLM calls.
    """
    logger.info(f"Received config merge request with {len(request.suggestions)} suggestions")

    try:
        optimizer = get_optimizer()
        result = optimizer.merge_config(
            space_data=request.space_data,
            suggestions=request.suggestions,
        )
        return result
    except Exception as e:
        raise _safe_error(e, 500, "Config merge failed")


@router.post("/genie/create", response_model=GenieCreateResponse)
async def create_genie_space(request: GenieCreateRequest):
    """Create a new Genie Space with the merged configuration.

    Creates a new Genie Space in the target directory using the optimized
    configuration. Requires GENIE_TARGET_DIRECTORY to be configured.
    """
    from backend.genie_creator import create_genie_space as do_create

    logger.info(f"Creating new Genie Space: {request.display_name}")

    try:
        result = do_create(
            display_name=request.display_name,
            merged_config=request.merged_config,
            parent_path=request.parent_path,
        )
        return GenieCreateResponse(**result)
    except ValueError as e:
        # Invalid config or missing env var
        raise HTTPException(status_code=400, detail=str(e))
    except PermissionError as e:
        # No write permission
        raise HTTPException(status_code=403, detail=str(e))
    except TimeoutError as e:
        raise HTTPException(status_code=504, detail=str(e))
    except Exception as e:
        raise _safe_error(e, 500, "Failed to create Genie Space")
