"""AI Fix Agent - applies targeted fixes to Genie Space configurations.

Replaces the notebook generator from GenieIQ. Uses LLM tool-calling to:
1. Reason over findings and prioritize fixes
2. Generate specific config patch operations
3. Apply patches via Genie API
4. Return structured results with before/after diffs
"""

import json
import logging
from typing import AsyncGenerator

import mlflow
from mlflow.entities import SpanType

from backend.services.llm_utils import call_serving_endpoint, get_llm_model
from backend.services.genie_client import get_genie_space, get_serialized_space
from backend.services.auth import get_workspace_client, run_in_context
from backend.prompts import get_fix_agent_prompt

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
        """Run the fix agent and stream progress updates.

        Yields dicts with:
            - {"status": "thinking", "message": str}
            - {"status": "patch", "field_path": str, "old_value": any, "new_value": any, "rationale": str}
            - {"status": "applying", "message": str}
            - {"status": "complete", "patches_applied": int, "summary": str, "diff": dict}
            - {"status": "error", "message": str}

        Args:
            space_id: The Genie Space ID
            findings: List of finding strings from IQ scan
            space_config: The current space configuration dict
        """
        import asyncio

        yield {"status": "thinking", "message": "Analyzing findings and planning fixes..."}

        try:
            prompt = get_fix_agent_prompt(
                space_id=space_id,
                findings=findings,
                space_config=space_config,
            )

            # Call LLM to generate fix plan
            content = _generate_fix_plan(
                messages=[{"role": "user", "content": prompt}],
                model=self.model,
                max_tokens=4096,
            )

            # Parse the fix plan
            fix_plan = _parse_fix_plan(content)
            patches = fix_plan.get("patches", [])
            summary = fix_plan.get("summary", "")

            if not patches:
                yield {"status": "complete", "patches_applied": 0, "summary": "No fixes needed.", "diff": {}}
                return

            yield {
                "status": "thinking",
                "message": f"Identified {len(patches)} fix(es) to apply...",
            }

            # Apply patches
            applied_patches = []
            import copy
            new_config = copy.deepcopy(space_config)

            for patch in patches:
                field_path = patch.get("field_path", "")
                new_value = patch.get("new_value")
                old_value = _get_value_at_path(space_config, field_path)
                rationale = patch.get("rationale", "")

                yield {
                    "status": "patch",
                    "field_path": field_path,
                    "old_value": old_value,
                    "new_value": new_value,
                    "rationale": rationale,
                }

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

            if applied_patches:
                yield {"status": "applying", "message": f"Applying {len(applied_patches)} fix(es) to space configuration..."}

                # Apply to Databricks via API
                try:
                    await _apply_config_to_databricks(space_id, new_config)
                    yield {
                        "status": "complete",
                        "patches_applied": len(applied_patches),
                        "summary": summary or f"Successfully applied {len(applied_patches)} fix(es).",
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
                        "message": f"Generated fixes but failed to apply: {e}. Config diff is available.",
                        "diff": {"patches": applied_patches},
                    }
            else:
                yield {"status": "complete", "patches_applied": 0, "summary": "Could not apply any patches.", "diff": {}}

        except Exception as e:
            logger.exception(f"Fix agent failed: {e}")
            yield {"status": "error", "message": str(e)}


@mlflow.trace(name="fix_generate_plan", span_type=SpanType.LLM)
def _generate_fix_plan(messages: list[dict], model: str, max_tokens: int) -> str:
    """Traced wrapper around LLM call for fix plan generation."""
    return call_serving_endpoint(messages=messages, model=model, max_tokens=max_tokens)


@mlflow.trace(name="fix_parse_plan", span_type=SpanType.TOOL)
def _parse_fix_plan(content: str) -> dict:
    """Parse LLM response into a structured fix plan."""
    content = content.strip()

    if content.startswith("```"):
        lines = content.split("\n")
        start_idx = 1
        end_idx = len(lines)
        for i in range(len(lines) - 1, 0, -1):
            if lines[i].strip() == "```":
                end_idx = i
                break
        content = "\n".join(lines[start_idx:end_idx])

    if not content.startswith("{"):
        json_start = content.find("{")
        if json_start != -1:
            content = content[json_start:]

    try:
        return json.loads(content)
    except json.JSONDecodeError:
        logger.warning("Failed to parse fix plan JSON, returning empty plan")
        return {"patches": [], "summary": "Could not parse fix plan."}


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
