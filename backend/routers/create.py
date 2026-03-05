"""
/api/create — UC discovery + config validation + Genie Space creation wizard + agent chat.
"""
import json
import logging
from fastapi import APIRouter, HTTPException
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
    if len(tables) > 25:
        errors.append(f"Maximum 25 tables allowed (found {len(tables)})")
    elif len(tables) > 5:
        warnings.append(f"More than 5 tables ({len(tables)}) may reduce accuracy")

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
    """Request body for the agent chat endpoint."""
    message: str = Field(..., min_length=1, max_length=10000)
    session_id: str | None = Field(None, description="Existing session ID. Omit to start a new session.")
    selections: dict | None = Field(None, description="UI selections from interactive elements")


@router.post("/agent/chat")
async def agent_chat(body: AgentChatRequest):
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

    agent = get_create_agent()

    if body.session_id:
        session = await get_session_async(body.session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Session not found or expired")
    else:
        session = create_session()

    user_message = body.message
    if body.selections:
        user_message += f"\n\n[User selections: {json.dumps(body.selections)}]"

    async def event_stream():
        yield _sse_event("session", {"session_id": session.session_id})
        async for event in agent.chat(session, user_message):
            yield _sse_event(event["event"], event["data"])
        # Persist session to Lakebase after the turn completes
        await persist_session(session)

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
