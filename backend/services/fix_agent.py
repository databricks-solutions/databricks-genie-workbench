"""AI Fix Agent - applies targeted fixes to Genie Space configurations.

Addresses each finding individually with a separate LLM call, then applies
all patches together in a single Databricks API call. This avoids token-limit
truncation and produces more reliable JSON per patch.
"""

import copy
import json
import logging
from typing import AsyncGenerator

import mlflow
from mlflow.entities import SpanType

from backend.services.llm_utils import call_serving_endpoint, get_llm_model
from backend.services.auth import get_workspace_client, run_in_context
from backend.prompts import get_fix_agent_single_prompt

logger = logging.getLogger(__name__)


class FixAgent:
    """AI agent that applies fixes to Genie Space configurations."""

    def __init__(self):
        self.model = get_llm_model()

    async def run(
        self,
        space_id: str,
        findings: list[str],
        space_config: dict,
    ) -> AsyncGenerator[dict, None]:
        """Run the fix agent — one LLM call per finding, then apply all patches.

        Yields dicts with:
            - {"status": "thinking", "message": str}
            - {"status": "patch", "field_path": str, "old_value": any, "new_value": any, "rationale": str}
            - {"status": "applying", "message": str}
            - {"status": "complete", "patches_applied": int, "summary": str, "diff": dict}
            - {"status": "error", "message": str}
        """
        if not findings:
            yield {"status": "complete", "patches_applied": 0, "summary": "No findings to fix.", "diff": {}}
            return

        yield {"status": "thinking", "message": f"Analyzing {len(findings)} issue(s)..."}

        new_config = copy.deepcopy(space_config)
        applied_patches = []

        try:
            for i, finding in enumerate(findings):
                yield {
                    "status": "thinking",
                    "message": f"Fixing issue {i + 1}/{len(findings)}: {finding[:80]}...",
                }

                # One focused LLM call for this finding (may return multiple patches)
                patches = _generate_patches_for_finding(
                    space_id=space_id,
                    finding=finding,
                    space_config=new_config,
                    model=self.model,
                )

                if not patches:
                    logger.info(f"No patch generated for finding: {finding[:80]}")
                    yield {
                        "status": "patch",
                        "field_path": "",
                        "old_value": None,
                        "new_value": None,
                        "rationale": "No fix needed or could not determine a patch",
                    }
                    continue

                for patch in patches:
                    field_path = patch.get("field_path", "")
                    new_value = patch.get("new_value")
                    old_value = _get_value_at_path(new_config, field_path) if field_path else None
                    rationale = patch.get("rationale", "")

                    if not field_path:
                        continue
                    if not _validate_field_path(field_path):
                        logger.warning(f"Skipping patch with invalid field path: {field_path}")
                        continue

                    try:
                        _set_value_at_path(new_config, field_path, new_value)
                        applied_patches.append({
                            "field_path": field_path,
                            "old_value": old_value,
                            "new_value": new_value,
                            "rationale": rationale,
                        })
                    except Exception as e:
                        logger.warning(f"Failed to apply patch at {field_path}: {e}")

                # Emit one patch event per finding (summarizing all sub-patches)
                first_patch = patches[0] if patches else {}
                yield {
                    "status": "patch",
                    "field_path": first_patch.get("field_path", ""),
                    "old_value": None,
                    "new_value": None,
                    "rationale": first_patch.get("rationale", f"Applied {len(patches)} patch(es)"),
                }

            # Apply all patches at once to Databricks
            if applied_patches:
                yield {"status": "applying", "message": f"Applying {len(applied_patches)} fix(es) to space configuration..."}
                try:
                    await _apply_config_to_databricks(space_id, new_config)
                    yield {
                        "status": "complete",
                        "patches_applied": len(applied_patches),
                        "summary": f"Successfully applied {len(applied_patches)} fix(es).",
                        "diff": {
                            "patches": applied_patches,
                            "original_config": space_config,
                            "updated_config": new_config,
                        },
                    }
                except Exception as e:
                    logger.error(f"Failed to apply config to Databricks: {e}")
                    yield {
                        "status": "error",
                        "message": f"Generated fixes but failed to apply: {e}",
                        "diff": {"patches": applied_patches},
                    }
            else:
                yield {"status": "complete", "patches_applied": 0, "summary": "No applicable patches found.", "diff": {}}

        except Exception as e:
            logger.exception(f"Fix agent failed: {e}")
            yield {"status": "error", "message": str(e)}


def _generate_patches_for_finding(
    space_id: str,
    finding: str,
    space_config: dict,
    model: str,
) -> list[dict]:
    """Generate patch(es) for one finding. Returns list of patch dicts."""
    prompt = get_fix_agent_single_prompt(
        space_id=space_id,
        finding=finding,
        space_config=space_config,
    )

    content = _call_llm_for_patch(
        messages=[{"role": "user", "content": prompt}],
        model=model,
        max_tokens=4096,
    )

    return _parse_patches(content)


@mlflow.trace(name="fix_generate_patch", span_type=SpanType.LLM)
def _call_llm_for_patch(messages: list[dict], model: str, max_tokens: int) -> str:
    """Traced wrapper around LLM call for a single patch."""
    return call_serving_endpoint(messages=messages, model=model, max_tokens=max_tokens)


@mlflow.trace(name="fix_parse_patch", span_type=SpanType.TOOL)
def _parse_patches(content: str) -> list[dict]:
    """Parse patch(es) from LLM response. Returns list of patch dicts."""
    from backend.services.llm_utils import parse_json_from_llm_response

    try:
        result = parse_json_from_llm_response(content)
    except (json.JSONDecodeError, ValueError) as e:
        logger.warning(f"Failed to parse patch JSON: {e}. Content preview: {content[:300]}")
        return []

    # Handle {"patches": [{...}, {...}]} format
    if "patches" in result and isinstance(result["patches"], list):
        return [p for p in result["patches"] if isinstance(p, dict)]
    # Handle single {"field_path": ...} format
    if "field_path" in result:
        return [result]
    return []


def _get_value_at_path(config: dict, field_path: str):
    """Navigate a config dict using dot-notation path."""
    import re
    parts = []
    for part in field_path.split("."):
        match = re.match(r"^(.+?)\[(\d+)\]$", part)
        if match:
            parts.append(match.group(1))
            parts.append(int(match.group(2)))
        else:
            parts.append(part)

    current = config
    for part in parts:
        try:
            if isinstance(part, int):
                current = current[part]
            else:
                current = current[part]
        except (KeyError, IndexError, TypeError):
            return None
    return current


def _set_value_at_path(config: dict, field_path: str, value) -> None:
    """Set a value in a config dict using dot-notation path."""
    import re
    parts = []
    for part in field_path.split("."):
        match = re.match(r"^(.+?)\[(\d+)\]$", part)
        if match:
            parts.append(match.group(1))
            parts.append(int(match.group(2)))
        else:
            parts.append(part)

    current = config
    for i, part in enumerate(parts[:-1]):
        if isinstance(part, int):
            while len(current) <= part:
                current.append(None)
            if current[part] is None:
                next_part = parts[i + 1]
                current[part] = [] if isinstance(next_part, int) else {}
            current = current[part]
        else:
            if part not in current:
                next_part = parts[i + 1]
                current[part] = [] if isinstance(next_part, int) else {}
            current = current[part]

    final_key = parts[-1]
    if isinstance(final_key, int):
        while len(current) <= final_key:
            current.append(None)
        current[final_key] = value
    else:
        current[final_key] = value


def _validate_field_path(field_path: str) -> bool:
    """Check that a patch field_path uses only known Genie API field names.

    Returns True if valid, False if the path contains an unknown field name.
    See: https://docs.databricks.com/aws/en/genie/conversation-api#understanding-the-serialized_space-field
    """
    # Known top-level and nested field names in serialized_space
    VALID_FIELDS = {
        # top-level
        "version", "config", "data_sources", "instructions", "benchmarks",
        # config
        "sample_questions",
        # data_sources
        "tables", "metric_views", "identifier", "description", "column_configs",
        "column_name", "synonyms", "exclude",
        "enable_entity_matching", "enable_format_assistance",
        # instructions
        "text_instructions", "example_question_sqls", "sql_functions",
        "join_specs", "sql_snippets",
        "content", "question", "sql", "usage_guidance", "parameters",
        "left", "right", "comment", "instruction",
        "filters", "expressions", "measures",
        "display_name", "alias", "id",
        # benchmarks
        "questions", "answer", "format",
    }
    import re
    parts = field_path.split(".")
    for part in parts:
        # Strip array index: "tables[0]" → "tables"
        name = re.sub(r"\[\d+\]$", "", part)
        if name not in VALID_FIELDS:
            logger.warning(f"Unknown field name '{name}' in path '{field_path}'")
            return False
    return True


@mlflow.trace(name="fix_apply_config", span_type=SpanType.TOOL)
def _apply_config_sync(space_id: str, new_config: dict) -> None:
    """Synchronous, traced config application via the Genie API."""
    from backend.genie_creator import _enforce_constraints, _clean_config
    constrained = _enforce_constraints(new_config)
    cleaned = _clean_config(constrained)
    client = get_workspace_client()
    client.api_client.do(
        method="PATCH",
        path=f"/api/2.0/genie/spaces/{space_id}",
        body={"serialized_space": json.dumps(cleaned)},
    )


async def _apply_config_to_databricks(space_id: str, new_config: dict) -> None:
    """Apply the updated config to Databricks via the Genie API."""
    import asyncio

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, run_in_context(lambda: _apply_config_sync(space_id, new_config)))


# Lazy singleton
_fix_agent: FixAgent | None = None


def get_fix_agent() -> FixAgent:
    """Get or create the fix agent instance."""
    global _fix_agent
    if _fix_agent is None:
        _fix_agent = FixAgent()
    return _fix_agent
