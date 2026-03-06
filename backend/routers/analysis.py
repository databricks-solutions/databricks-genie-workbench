"""
REST API endpoints for the Genie Space Analyzer.

Provides endpoints for the React frontend to fetch spaces, analyze sections,
and stream analysis progress.
"""

import json
from pathlib import Path

import logging

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

from backend.services.analyzer import GenieSpaceAnalyzer, SECTIONS, get_analyzer
from backend.services.genie_client import get_serialized_space
from backend.models import (
    AgentInput,
    AgentOutput,
    ConfigMergeRequest,
    ConfigMergeResponse,
    GenieCreateRequest,
    GenieCreateResponse,
    LabelingFeedbackItem,
    OptimizationRequest,
    OptimizationResponse,
    OptimizationSuggestion,
    SectionAnalysis,
    SynthesisResult,
)
from backend.services.optimizer import get_optimizer
from backend.synthesizer import synthesize_analysis

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
    sections: list[dict]  # List of {name, data, has_data}


class ParseJsonRequest(BaseModel):
    """Request to parse pasted JSON."""

    json_content: str = Field(..., min_length=1, max_length=1_000_000)  # 1MB limit


class AnalyzeSectionRequest(BaseModel):
    """Request to analyze a single section."""
    section_name: str
    section_data: dict | list | None
    full_space: dict


class StreamAnalysisRequest(BaseModel):
    """Request for streaming analysis."""

    genie_space_id: str = Field(
        ..., min_length=1, max_length=64, pattern=r"^[a-zA-Z0-9\-_]+$"
    )


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


class AnalyzeAllSectionsRequest(BaseModel):
    """Request to analyze all sections with cross-sectional synthesis."""
    sections: list[dict]  # List of {name, data} for sections to analyze
    full_space: dict


class AnalyzeAllSectionsResponse(BaseModel):
    """Response with all section analyses and synthesis."""
    analyses: list[SectionAnalysis]
    synthesis: SynthesisResult | None  # Only present for full analysis
    is_full_analysis: bool


@router.post("/space/fetch", response_model=FetchSpaceResponse)
async def fetch_space(request: FetchSpaceRequest):
    """Fetch and parse a Genie Space by ID.
    
    Returns the space data and list of sections with their data.
    """
    try:
        space_data = get_serialized_space(request.genie_space_id)
        analyzer = get_analyzer()
        all_sections = analyzer.get_all_sections(space_data)

        sections = [
            {
                "name": name,
                "data": data,
                "has_data": data is not None
            }
            for name, data in all_sections
        ]

        return FetchSpaceResponse(
            genie_space_id=request.genie_space_id,
            space_data=space_data,
            sections=sections,
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
        
        analyzer = get_analyzer()
        all_sections = analyzer.get_all_sections(space_data)
        
        sections = [
            {
                "name": name,
                "data": data,
                "has_data": data is not None
            }
            for name, data in all_sections
        ]
        
        return FetchSpaceResponse(
            genie_space_id=genie_space_id,
            space_data=space_data,
            sections=sections
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {str(e)}")


@router.post("/analyze/section")
async def analyze_section(request: AnalyzeSectionRequest) -> SectionAnalysis:
    """Analyze a single section of the Genie Space."""
    try:
        analyzer = get_analyzer()
        analysis = analyzer.analyze_section(
            request.section_name,
            request.section_data,
            full_space=request.full_space,
        )
        return analysis
    except Exception as e:
        raise _safe_error(e, 500, "Section analysis failed")


@router.post("/analyze/all", response_model=AnalyzeAllSectionsResponse)
async def analyze_all_sections(request: AnalyzeAllSectionsRequest):
    """Analyze all selected sections with cross-sectional synthesis.

    Returns style detection (heuristic), section analyses (LLM),
    and synthesis (LLM, for full analysis).

    Full analysis = analyzed all CONFIGURED sections (sections with data).
    This means a space with only tables configured gets full synthesis
    when tables are analyzed, without being penalized for not having
    metric views or other optional sections.
    """
    try:
        analyzer = get_analyzer()
        analyzer.start_session()

        try:
            # Analyze each section
            analyses = []
            for section in request.sections:
                analysis = analyzer.analyze_section(
                    section["name"],
                    section.get("data"),
                    full_space=request.full_space,
                )
                analyses.append(analysis)

            # Determine configured sections in the full space
            all_sections = analyzer.get_all_sections(request.full_space)
            configured_section_names = {name for name, data in all_sections if data is not None}
            analyzed_section_names = {s["name"] for s in request.sections}

            # Full analysis = analyzed all configured sections
            # (user analyzed everything they have set up)
            is_full_analysis = configured_section_names <= analyzed_section_names

            # Run synthesis for full analysis
            synthesis = None
            if is_full_analysis:
                synthesis = synthesize_analysis(analyses, is_full_analysis)

            return AnalyzeAllSectionsResponse(
                analyses=analyses,
                synthesis=synthesis,
                is_full_analysis=is_full_analysis,
            )
        finally:
            analyzer.end_session()
    except Exception as e:
        raise _safe_error(e, 500, "Analysis failed")


@router.post("/analyze/stream")
async def stream_analysis(request: StreamAnalysisRequest):
    """Stream analysis progress for all sections.
    
    Returns Server-Sent Events with progress updates and final results.
    """
    def generate():
        analyzer = get_analyzer()
        input_obj = AgentInput(genie_space_id=request.genie_space_id)
        gen = analyzer.predict_streaming(input_obj)
        
        result = None
        try:
            while True:
                progress = next(gen)
                yield f"data: {json.dumps(progress)}\n\n"
        except StopIteration as e:
            result = e.value
        
        if result:
            from backend.services.analyzer import save_analysis_output
            save_analysis_output(result)
            yield f"data: {json.dumps({'status': 'result', 'data': result.model_dump()})}\n\n"
    
    return StreamingResponse(generate(), media_type="text/event-stream")


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


@router.get("/checklist")
async def get_checklist():
    """Get the checklist markdown documentation."""
    docs_path = Path(__file__).parent.parent / "docs" / "checklist-by-schema.md"
    try:
        content = docs_path.read_text()
        return {"content": content}
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Checklist documentation not found")


@router.get("/sections")
async def get_sections():
    """Get the list of all section names."""
    return {"sections": SECTIONS}


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

        future = loop.run_in_executor(executor, run_optimizer)
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

