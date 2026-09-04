"""Shared backend-side LLM JSON helpers + config resolver.

The LLM transport is the single wheel-native client
(:func:`genie_space_optimizer.common.llm.call_llm_core`, MV-D65) — backend
callers pass their OBO ``WorkspaceClient`` into it directly. This module keeps
only the transport-free helpers: JSON extraction/repair for LLM responses and
the model-name resolver.
"""

import json
import logging

from genie_space_optimizer.common.config import get_llm_endpoint

logger = logging.getLogger(__name__)


def get_llm_model() -> str:
    """Return the configured LLM serving-endpoint name.

    Delegates to the single resolver :func:`get_llm_endpoint`
    (``GSO_LLM_ENDPOINT`` → ``LLM_MODEL`` → default) so backend and wheel agree.
    """
    return get_llm_endpoint()


def _repair_json(content: str) -> str:
    """Attempt to repair common JSON syntax errors from LLM responses.

    Fixes:
    - Missing commas between array elements or object properties
    - Trailing commas (not valid JSON but LLMs often add them)
    """
    import re

    # Remove trailing commas before closing brackets/braces
    content = re.sub(r",\s*([}\]])", r"\1", content)

    # Fix missing commas between string values and opening braces/brackets
    # e.g., "value"{ -> "value",{  or "value" \n { -> "value",{
    content = re.sub(r'(")\s*\n?\s*([{\[])', r'\1,\n\2', content)

    # Fix missing commas between closing and opening braces/brackets
    # e.g., }{ -> },{  and ][ -> ],[  (with optional whitespace/newlines)
    content = re.sub(r"([}\]])\s*\n?\s*([{\[])", r"\1,\n\2", content)

    # Fix missing commas between string values (including across newlines)
    # e.g., "value" "key" -> "value", "key"
    # e.g., "value"\n"key" -> "value",\n"key"
    content = re.sub(r'(")\s*\n\s*(")', r'\1,\n\2', content)
    content = re.sub(r'(")\s+(")', r'\1, \2', content)

    # Fix missing commas after closing brace/bracket before string (including newlines)
    # e.g., } "key" -> }, "key"  or }\n"key" -> },\n"key"
    content = re.sub(r'([}\]])\s*\n\s*(")', r'\1,\n\2', content)
    content = re.sub(r'([}\]])\s+(")', r'\1, \2', content)

    # Fix missing commas after values before keys (number/bool/null followed by string key)
    # e.g., true\n"key" -> true,\n"key"
    content = re.sub(r'(true|false|null|\d+)\s*\n\s*(")', r'\1,\n\2', content)

    return content


def _extract_first_json_object(content: str) -> str:
    """Return the first balanced JSON object from content, if one exists."""
    json_start = content.find("{")
    if json_start == -1:
        return content

    brace_count = 0
    in_string = False
    escaped = False

    for i, char in enumerate(content[json_start:], json_start):
        if escaped:
            escaped = False
            continue
        if char == "\\" and in_string:
            escaped = True
            continue
        if char == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if char == "{":
            brace_count += 1
        elif char == "}":
            brace_count -= 1
            if brace_count == 0:
                return content[json_start:i + 1]

    return content


def parse_json_from_llm_response(content: str) -> dict:
    """Parse JSON from an LLM response, handling markdown code blocks.

    LLM responses often wrap JSON in ```json ... ``` code blocks.
    This function extracts and parses the JSON content, with automatic
    repair for common LLM JSON errors.

    Args:
        content: The raw LLM response content

    Returns:
        Parsed JSON as a dict

    Raises:
        json.JSONDecodeError: If JSON parsing fails even after repair
        ValueError: If no valid JSON found
    """
    content = content.strip()

    # Handle markdown code blocks
    if content.startswith("```"):
        lines = content.split("\n")
        # Skip first line (```json or ```)
        start_idx = 1
        # Find closing ```
        end_idx = len(lines)
        for i in range(len(lines) - 1, 0, -1):
            if lines[i].strip() == "```":
                end_idx = i
                break
        content = "\n".join(lines[start_idx:end_idx])

    # Handle text before/after JSON, including multiple JSON objects.
    # LLMs sometimes answer with a valid object followed by "Wait..." and
    # another object; downstream callers expect the first complete object.
    content = _extract_first_json_object(content)

    if not content:
        raise ValueError("LLM returned empty response after parsing")

    # Try parsing as-is first
    try:
        return json.loads(content)
    except json.JSONDecodeError as e:
        logger.warning(f"Initial JSON parse failed: {e}. Attempting repair...")

        # Try to repair and parse again
        repaired = _repair_json(content)
        try:
            result = json.loads(repaired)
            logger.info("JSON repair successful")
            return result
        except json.JSONDecodeError:
            # Re-raise original error with context
            logger.error(f"JSON repair failed. Content preview: {content[:500]}...")
            raise e
