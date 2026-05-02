"""
REST API endpoints for Genie Space operations.

Provides endpoints for fetching/parsing spaces and application settings.
"""

import json

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

from backend.services.genie_client import get_serialized_space, normalize_metric_view_sources

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
        if isinstance(space_data, dict):
            normalize_metric_view_sources(space_data)

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
