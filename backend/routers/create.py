"""
/api/create — UC discovery + config validation + Genie Space creation wizard + agent chat.
"""
import asyncio
import json
import logging
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from backend.models import CreateSpaceRequest, CreateSpaceResponse
from backend.services.uc_client import (
    list_catalogs,
    list_schemas,
    list_tables,
    get_table_columns,
)
from backend.genie_creator import create_genie_space

router = APIRouter(prefix="/api/create")
logger = logging.getLogger(__name__)


# ── Preflight check ──────────────────────────────────────────────────────────

@router.get("/preflight")
async def create_preflight(request: Request):
    """Check warehouse availability and OBO auth status for the Create flow."""
    import os
    from backend.services.auth import get_workspace_client

    obo_enabled = bool(getattr(request.state, "user_token", ""))
    app_name = os.environ.get("DATABRICKS_APP_NAME", "this app")

    warehouses_available = False
    try:
        from backend.services.create_agent_tools import get_sql_warehouse_id
        configured_id = get_sql_warehouse_id()
        if configured_id:
            # App has an explicitly granted warehouse resource — confirmed access
            warehouses_available = True
        else:
            # No app resource assigned; fall back to checking OBO user's warehouses
            if obo_enabled:
                client = get_workspace_client()
                for wh in client.warehouses.list():
                    is_serverless = getattr(wh, "enable_serverless_compute", False)
                    wh_type = str(getattr(wh, "warehouse_type", ""))
                    if is_serverless or wh_type == "PRO":
                        warehouses_available = True
                        break
    except Exception as e:
        logger.warning("preflight: warehouse check failed: %s", e)

    return {
        "warehouses_available": warehouses_available,
        "obo_enabled": obo_enabled,
        "app_name": app_name,
    }


# ── UC discovery ──────────────────────────────────────────────────────────────

@router.get("/discover/catalogs")
async def discover_catalogs():
    return {"catalogs": list_catalogs()}


@router.get("/discover/schemas")
async def discover_schemas(catalog: str):
    return {"schemas": list_schemas(catalog)}


@router.get("/discover/tables")
async def discover_tables(catalog: str, schema: str):
    return {"tables": list_tables(catalog, schema)}


@router.get("/discover/columns")
async def discover_columns(catalog: str, schema: str, table: str):
    return {"columns": get_table_columns(catalog, schema, table)}


@router.get("/discover/search")
async def discover_search(keywords: str, catalogs: str | None = None):
    """Search for tables across Unity Catalog by keywords.

    Args:
        keywords: Comma-separated search terms (e.g., "bank,loan,customer")
        catalogs: Optional comma-separated catalog names to scope search
    """
    from backend.services.uc_client import search_tables
    kw_list = [k.strip() for k in keywords.split(",") if k.strip()]
    cat_list = [c.strip() for c in catalogs.split(",") if c.strip()] if catalogs else None
    return search_tables(kw_list, catalogs=cat_list)


# ── Config validation ─────────────────────────────────────────────────────────

class ValidateRequest(BaseModel):
    serialized_space: dict


@router.post("/validate")
async def validate_config(body: ValidateRequest):
    errors: list[str] = []
    warnings: list[str] = []
    s = body.serialized_space

    tables = s.get("data_sources", {}).get("tables") or []
    if len(tables) == 0:
        errors.append("At least one table is required")
    if len(tables) > 30:
        errors.append(f"Maximum 30 tables allowed (found {len(tables)})")
    elif len(tables) > 10:
        warnings.append(f"More than 10 tables ({len(tables)}) may reduce accuracy")

    questions = s.get("instructions", {}).get("example_question_sqls") or []
    if len(questions) < 5:
        warnings.append(f"Fewer than 5 sample questions (found {len(questions)})")

    ti = s.get("instructions", {}).get("text_instructions") or []
    total_chars = sum(len(str(i.get("content") or i)) for i in ti)
    if total_chars > 500:
        warnings.append(f"Text instructions exceed 500 chars ({total_chars})")

    snippets = s.get("instructions", {}).get("sql_snippets") or {}
    total_instructions = (
        len(snippets.get("expressions") or [])
        + len(snippets.get("measures") or [])
        + len(snippets.get("filters") or [])
        + len(s.get("instructions", {}).get("example_question_sqls") or [])
    )
    if total_instructions > 100:
        errors.append(f"Instruction budget exceeded: {total_instructions}/100")

    return {"valid": len(errors) == 0, "errors": errors, "warnings": warnings}


# ── Space creation ────────────────────────────────────────────────────────────

@router.post("", response_model=CreateSpaceResponse)
async def create_space_endpoint(body: CreateSpaceRequest):
    try:
        result = create_genie_space(
            display_name=body.display_name,
            merged_config=body.serialized_space,
            parent_path=body.parent_path,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except TimeoutError as e:
        raise HTTPException(status_code=504, detail=str(e))
    except Exception as e:
        logger.exception(f"create_genie_space failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to create Genie Space")

    # genie_creator returns genie_space_id; our response model uses space_id
    return CreateSpaceResponse(
        space_id=result["genie_space_id"],
        display_name=result["display_name"],
        space_url=result["space_url"],
    )


# ── Agent chat (agentic create flow) ─────────────────────────────────────────

class AgentChatRequest(BaseModel):
    """Request body for the agent chat endpoint.

    ``message`` may be empty for auto-continuation rounds (the frontend
    sends an empty message to resume the agent loop after a tool batch).
    """
    message: str = Field("", max_length=10000)
    session_id: str | None = Field(None, description="Existing session ID. Omit to start a new session.")
    selections: dict | None = Field(None, description="UI selections from interactive elements")
    space_id: str | None = Field(None, description="Pre-seed session with existing space ID for fix/update flows")


@router.post("/agent/chat")
async def agent_chat(body: AgentChatRequest, request: Request):
    """Conversational endpoint for the Create Genie agent.

    Returns a streaming SSE response with typed events:
    - thinking: agent is processing
    - tool_call: agent is calling a tool
    - tool_result: tool returned a result
    - message: agent text response (may include ui_elements)
    - created: space was created successfully
    - error: something went wrong
    - done: turn is complete
    """
    from backend.services.create_agent import get_create_agent
    from backend.services.create_agent_session import (
        create_session, get_session_async, persist_session,
    )
    from backend.services.auth import set_obo_user_token, clear_obo_user_token

    agent = get_create_agent()

    is_continuation = not body.message.strip()
    if is_continuation and not body.session_id:
        raise HTTPException(status_code=400, detail="session_id is required for continuation")

    if body.session_id:
        session = await get_session_async(body.session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Session not found or expired")
    else:
        session = create_session()
        if body.space_id:
            session.space_id = body.space_id
            # Pre-load existing space config so update_config works immediately
            try:
                from backend.services.genie_client import get_serialized_space
                session.space_config = await asyncio.to_thread(
                    get_serialized_space, body.space_id
                )
                logger.info("Pre-loaded space config for fix flow: %s", body.space_id)
            except Exception as e:
                logger.warning("Could not pre-load space config for %s: %s", body.space_id, e)

    user_message = body.message
    selections = body.selections
    if selections:
        # Embed selections in message for LLM context (non-fast paths)
        user_message += f"\n\n[User selections: {json.dumps(selections)}]"

        # Populate session state from structured selections
        if "selected_tables" in selections and isinstance(selections["selected_tables"], list):
            session.selected_tables = selections["selected_tables"]
            logger.info("Session tables updated from selections: %d tables", len(session.selected_tables))

    # Capture the user token so the streaming generator can re-establish
    # the OBO context (ContextVars don't propagate into async generators
    # that outlive the middleware's call_next).
    user_token = getattr(request.state, "user_token", "")

    _KEEPALIVE_INTERVAL = 15  # seconds between SSE keepalive comments

    async def event_stream():
        if user_token:
            set_obo_user_token(user_token)
        try:
            yield _sse_event("session", {"session_id": session.session_id})

            async with session._lock:
                agent_iter = agent.chat(session, user_message, selections=selections).__aiter__()
                next_coro = None
                while True:
                    if next_coro is None:
                        next_coro = asyncio.ensure_future(agent_iter.__anext__())
                    try:
                        event = await asyncio.wait_for(
                            asyncio.shield(next_coro), timeout=_KEEPALIVE_INTERVAL
                        )
                        next_coro = None
                        yield _sse_event(event["event"], event["data"])
                    except asyncio.TimeoutError:
                        yield ": keepalive\n\n"
                    except StopAsyncIteration:
                        break

            await persist_session(session)
        finally:
            clear_obo_user_token()

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/agent/sessions/{session_id}")
async def get_agent_session(session_id: str):
    """Get session history for page refresh / reconnection."""
    from backend.services.create_agent_session import get_session_async

    session = await get_session_async(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Session not found or expired")

    display_history = []
    for msg in session.history:
        if msg["role"] == "user":
            display_history.append({"role": "user", "content": msg["content"]})
        elif msg["role"] == "assistant" and msg.get("content"):
            display_history.append({"role": "assistant", "content": msg["content"]})

    return {
        "session_id": session.session_id,
        "history": display_history,
        "space_id": session.space_id,
        "space_url": session.space_url,
        "has_config": session.space_config is not None,
    }


@router.delete("/agent/sessions/{session_id}")
async def delete_agent_session(session_id: str):
    """Delete a session."""
    from backend.services.create_agent_session import delete_session_async

    deleted = await delete_session_async(session_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Session not found")
    return {"deleted": True}


def _sse_event(event_type: str, data: dict) -> str:
    """Format a dict as an SSE event string."""
    payload = json.dumps(data, default=str)
    return f"event: {event_type}\ndata: {payload}\n\n"
