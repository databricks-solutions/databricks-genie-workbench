"""Shared LLM utilities for calling serving endpoints and parsing responses."""

import json
import logging
import os

from backend.services.auth import get_workspace_client

logger = logging.getLogger(__name__)


def get_llm_model() -> str:
    """Get the configured LLM model name."""
    return os.environ.get("LLM_MODEL", "databricks-claude-sonnet-4-6")


def call_serving_endpoint(
    messages: list[dict],
    model: str | None = None,
    max_tokens: int | None = None,
) -> str:
    """Call the LLM serving endpoint using the SDK's API client.

    Uses the SDK's api_client.do() which handles OBO authentication
    automatically on Databricks Apps.

    Args:
        messages: List of chat messages in OpenAI format
        model: Model name to use. Defaults to LLM_MODEL env var.
        max_tokens: Optional max tokens for response.

    Returns:
        The assistant's response content

    Raises:
        ValueError: If response format is unexpected or content is empty
    """
    if model is None:
        model = get_llm_model()

    client = get_workspace_client()

    body: dict = {"messages": messages}
    if max_tokens is not None:
        body["max_tokens"] = max_tokens

    logger.info(f"Calling serving endpoint: {model}")

    response = client.api_client.do(
        method="POST",
        path=f"/serving-endpoints/{model}/invocations",
        body=body,
    )

    # Response is in OpenAI-compatible format
    if not isinstance(response, dict):
        raise ValueError(f"Unexpected response type: {type(response)}")

    if "choices" not in response:
        logger.error(f"Response missing 'choices': {response}")
        raise ValueError(f"Response missing 'choices' key: {list(response.keys())}")

    if not response["choices"]:
        raise ValueError("Response has empty 'choices' list")

    content = response["choices"][0]["message"]["content"]
    if not content:
        raise ValueError("LLM returned empty content")

    return content


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

    # Handle case where response might have text before JSON
    if not content.startswith("{"):
        json_start = content.find("{")
        if json_start != -1:
            # Find matching closing brace
            brace_count = 0
            json_end = -1
            for i, char in enumerate(content[json_start:], json_start):
                if char == "{":
                    brace_count += 1
                elif char == "}":
                    brace_count -= 1
                    if brace_count == 0:
                        json_end = i + 1
                        break
            if json_end != -1:
                content = content[json_start:json_end]

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
