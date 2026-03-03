"""
/api/create — UC discovery + config validation + Genie Space creation wizard.
"""
import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

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
    except Exception as e:
        logger.exception(f"create_genie_space failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to create Genie Space")

    # genie_creator returns genie_space_id; our response model uses space_id
    return CreateSpaceResponse(
        space_id=result["genie_space_id"],
        display_name=result["display_name"],
        space_url=result["space_url"],
    )
