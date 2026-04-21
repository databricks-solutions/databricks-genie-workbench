"""AI Fix Agent - applies targeted fixes to Genie Space configurations.

Addresses each finding individually with a separate LLM call, then applies
all patches together in a single Databricks API call. This avoids token-limit
truncation and produces more reliable JSON per patch.
"""

import asyncio
import copy
import json
import logging
import re
import time
import uuid
from typing import AsyncGenerator

import mlflow
from mlflow.entities import SpanType

from backend.services.llm_utils import call_serving_endpoint, get_llm_model
from backend.services.auth import get_workspace_client, run_in_context
from backend.prompts import get_fix_agent_single_prompt

logger = logging.getLogger(__name__)

# Known field names in the Genie API serialized_space schema.
# See: https://docs.databricks.com/aws/en/genie/conversation-api#understanding-the-serialized_space-field
_VALID_FIELDS: frozenset[str] = frozenset({
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
})


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
            - {"status": "skipped", "field_path": "", "old_value": None, "new_value": None, "rationale": str}
              (emitted when the agent declines a patch — e.g., the fix would erase a canonical GSL section header)
            - {"status": "applying", "message": str}
            - {"status": "complete", "patches_applied": int, "summary": str, "diff": dict}
            - {"status": "error", "message": str}
        """
        if not findings:
            yield {"status": "complete", "patches_applied": 0, "summary": "No findings to fix.", "diff": {}}
            return

        yield {"status": "thinking", "message": f"Analyzing {len(findings)} issue(s)..."}

        # Frozen snapshot for all parallel LLM calls; mutable copy for applying patches
        config_snapshot = copy.deepcopy(space_config)
        new_config = copy.deepcopy(space_config)
        applied_patches = []

        try:
            # Launch ALL LLM calls in parallel — total wall time ≈ slowest call
            # instead of sum of all calls, keeping under the ~120s proxy timeout.
            loop = asyncio.get_running_loop()
            tasks = []
            for finding in findings:
                task = loop.run_in_executor(
                    None,
                    run_in_context(lambda f=finding: _generate_patches_for_finding(
                        space_id=space_id, finding=f, space_config=config_snapshot, model=self.model,
                    )),
                )
                tasks.append(task)

            # Await results in order, yielding progress events as each completes
            for i, (finding, task) in enumerate(zip(findings, tasks)):
                yield {
                    "status": "thinking",
                    "message": f"Fixing issue {i + 1}/{len(findings)}: {finding[:80]}...",
                }

                try:
                    patches = await task
                except Exception as e:
                    logger.warning(f"LLM call failed for finding: {finding[:80]}: {e}")
                    patches = []

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

                # Agent declined to patch (e.g., fix would erase a canonical GSL
                # section header in text_instructions). Emit a "skipped" event
                # carrying the LLM's rationale instead of attempting to apply.
                if patches[0].get("declined"):
                    rationale = patches[0].get("rationale") or "Patch declined by agent."
                    logger.info(f"Fix declined for finding: {finding[:80]} — {rationale[:120]}")
                    yield {
                        "status": "skipped",
                        "field_path": "",
                        "old_value": None,
                        "new_value": None,
                        "rationale": rationale,
                    }
                    continue

                finding_patches = []
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
                        finding_patches.append({
                            "field_path": field_path,
                            "old_value": old_value,
                            "new_value": new_value,
                            "rationale": rationale,
                        })
                    except Exception as e:
                        logger.warning(f"Failed to apply patch at {field_path}: {e}")

                applied_patches.extend(finding_patches)

                # Emit one patch event per finding with actual values from first sub-patch
                first = finding_patches[0] if finding_patches else patches[0]
                yield {
                    "status": "patch",
                    "field_path": first.get("field_path", ""),
                    "old_value": first.get("old_value"),
                    "new_value": first.get("new_value"),
                    "rationale": first.get("rationale", f"Applied {len(finding_patches)} patch(es)"),
                }

            # Apply all patches at once to Databricks
            if applied_patches:
                yield {"status": "applying", "message": f"Applying {len(applied_patches)} fix(es) to space configuration..."}
                try:
                    await _apply_config_to_databricks(space_id, applied_patches)
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
        max_tokens=10000,
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

    # Handle {"decline": true, "rationale": "..."} — agent refuses to patch because
    # the fix would erase a canonical GSL section header. Return a single marker
    # "patch" with declined=True so the caller can emit a skipped event.
    if result.get("decline") is True:
        rationale = result.get("rationale") or "Patch declined by agent (no rationale provided)."
        return [{"field_path": "", "new_value": None, "rationale": rationale, "declined": True}]

    # Handle {"patches": [{...}, {...}]} format
    if "patches" in result and isinstance(result["patches"], list):
        return [p for p in result["patches"] if isinstance(p, dict)]
    # Handle single {"field_path": ...} format
    if "field_path" in result:
        return [result]
    return []


def _get_value_at_path(config: dict, field_path: str):
    """Navigate a config dict using dot-notation path."""
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
    Uses the module-level _VALID_FIELDS frozenset.
    """
    for part in field_path.split("."):
        name = re.sub(r"\[\d+\]$", "", part)
        if name not in _VALID_FIELDS:
            logger.warning(f"Unknown field name '{name}' in path '{field_path}'")
            return False
    return True


_HEX32_RE = re.compile(r"^[0-9a-f]{32}$")

# Arrays whose dict entries require an `id` field per the Genie API schema.
_ID_REQUIRED_ARRAYS = frozenset({
    "sample_questions", "text_instructions", "example_question_sqls",
    "join_specs", "filters", "expressions", "measures", "questions",
    "sql_functions",
})


def _sanitize_ids(obj, _parent_key=None):
    """Recursively fix invalid or missing IDs in the config.

    The Genie API requires all `id` fields to be 32-character lowercase hex
    strings (UUID without hyphens). LLMs sometimes generate IDs with non-hex
    characters, wrong formats, null values, or omit them entirely. This
    replaces any invalid ID with a fresh one and injects missing IDs into
    entries inside known ID-required arrays.
    """
    if isinstance(obj, dict):
        # Inject missing id for entries inside known ID-required arrays
        if _parent_key in _ID_REQUIRED_ARRAYS and "id" not in obj:
            obj["id"] = uuid.uuid4().hex
            logger.info("Injected missing id '%s' in %s entry", obj["id"], _parent_key)
        for k, v in obj.items():
            if k == "id" and (not isinstance(v, str) or not _HEX32_RE.match(v)):
                obj[k] = uuid.uuid4().hex
                logger.info("Replaced invalid id %r with '%s'", v, obj[k])
            else:
                _sanitize_ids(v, _parent_key=k)
    elif isinstance(obj, list):
        for item in obj:
            _sanitize_ids(item, _parent_key=_parent_key)


@mlflow.trace(name="fix_apply_config", span_type=SpanType.TOOL)
def _apply_config_sync(space_id: str, patches: list[dict]) -> None:
    """Re-fetch the space config, apply patches, then PATCH to Databricks.

    Re-fetching avoids "Space configuration has been modified since this export
    was taken" errors that occur when the config becomes stale between the scan
    and the patch application.  If the PATCH still fails (e.g. due to
    server-side background processing between GET and PATCH), we retry up to
    2 additional times with back-off, re-fetching on each attempt.

    Only lightweight transforms (ID sanitization, join relationship
    normalization, sorting) are applied — NOT the full _enforce_constraints /
    _clean_config pipeline, which can modify existing entries (e.g. removing
    empty column_names) and make the config diverge from the stored version.
    """
    from backend.genie_creator import _normalize_join_relationships, _clean_config
    from backend.services.genie_client import get_serialized_space

    max_attempts = 3
    retry_delays = [2, 4]  # seconds between attempt 1→2 and 2→3

    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            fresh_config = get_serialized_space(space_id)
            for patch in patches:
                field_path = patch.get("field_path", "")
                if field_path:
                    _set_value_at_path(fresh_config, field_path, patch["new_value"])

            _sanitize_ids(fresh_config)
            _normalize_join_relationships(fresh_config)

            # Strip incomplete join_specs (missing required left/right fields)
            join_specs = fresh_config.get("instructions", {}).get("join_specs", [])
            if isinstance(join_specs, list):
                fresh_config.setdefault("instructions", {})["join_specs"] = [
                    js for js in join_specs
                    if isinstance(js, dict) and js.get("left") and js.get("right")
                ]

            # Deduplicate column_configs: the API rejects duplicate column_name
            # entries (including empty names) even though it may return them.
            for tbl in fresh_config.get("data_sources", {}).get("tables", []):
                if not isinstance(tbl, dict):
                    continue
                ccs = tbl.get("column_configs", [])
                if not isinstance(ccs, list):
                    continue
                seen: set[str] = set()
                deduped = []
                for cc in ccs:
                    if not isinstance(cc, dict):
                        continue
                    col = cc.get("column_name", "")
                    if not col or col in seen:
                        continue
                    seen.add(col)
                    deduped.append(cc)
                tbl["column_configs"] = deduped

            # Deduplicate instruction IDs: the API rejects duplicate IDs
            # across instruction arrays even though it may return them.
            # IDs must be unique across all instruction types.
            seen_ids: set[str] = set()
            for arr_name in (
                "text_instructions", "example_question_sqls",
                "sql_functions", "join_specs",
            ):
                arr = fresh_config.get("instructions", {}).get(arr_name, [])
                if not isinstance(arr, list):
                    continue
                deduped_arr = []
                for entry in arr:
                    if not isinstance(entry, dict):
                        continue
                    eid = entry.get("id", "")
                    if eid and eid in seen_ids:
                        logger.info("Dropping duplicate instruction id %s from %s", eid, arr_name)
                        continue
                    if eid:
                        seen_ids.add(eid)
                    deduped_arr.append(entry)
                fresh_config.get("instructions", {})[arr_name] = deduped_arr

            # Same for sql_snippets sub-arrays
            snippets = fresh_config.get("instructions", {}).get("sql_snippets", {})
            if isinstance(snippets, dict):
                for snippet_key in ("filters", "expressions", "measures"):
                    arr = snippets.get(snippet_key, [])
                    if not isinstance(arr, list):
                        continue
                    deduped_arr = []
                    for entry in arr:
                        if not isinstance(entry, dict):
                            continue
                        eid = entry.get("id", "")
                        if eid and eid in seen_ids:
                            logger.info("Dropping duplicate instruction id %s from sql_snippets.%s", eid, snippet_key)
                            continue
                        if eid:
                            seen_ids.add(eid)
                        deduped_arr.append(entry)
                    snippets[snippet_key] = deduped_arr

            # Also deduplicate sample_questions and benchmark questions by id.
            # One shared set across both sections — matches _enforce_constraints().
            seen_q: set[str] = set()
            for section, arr_name in (("config", "sample_questions"), ("benchmarks", "questions")):
                section_dict = fresh_config.get(section, {})
                if not isinstance(section_dict, dict):
                    continue
                arr = section_dict.get(arr_name, [])
                if not isinstance(arr, list):
                    continue
                deduped_arr = []
                for entry in arr:
                    if not isinstance(entry, dict):
                        continue
                    eid = entry.get("id", "")
                    if eid and eid in seen_q:
                        logger.info("Dropping duplicate id %s from %s.%s", eid, section, arr_name)
                        continue
                    if eid:
                        seen_q.add(eid)
                    deduped_arr.append(entry)
                section_dict[arr_name] = deduped_arr

            # Sort-only pass: _clean_config handles sorting, null removal, and
            # string-to-array coercion for LLM-generated values — but does NOT
            # remove or rewrite existing entries the way _enforce_constraints does.
            cleaned = _clean_config(fresh_config)
            client = get_workspace_client()
            client.api_client.do(
                method="PATCH",
                path=f"/api/2.0/genie/spaces/{space_id}",
                body={"serialized_space": json.dumps(cleaned)},
            )
            if attempt > 1:
                logger.info("PATCH succeeded for space %s on attempt %d/%d", space_id, attempt, max_attempts)
            return

        except Exception as exc:
            last_exc = exc
            if attempt < max_attempts:
                delay = retry_delays[attempt - 1]
                logger.warning(
                    "PATCH attempt %d/%d failed for space %s: %s — retrying in %ds",
                    attempt, max_attempts, space_id, exc, delay,
                )
                time.sleep(delay)
            else:
                logger.error(
                    "PATCH attempt %d/%d failed for space %s: %s — no retries left",
                    attempt, max_attempts, space_id, exc,
                )

    raise last_exc  # type: ignore[misc]


async def _apply_config_to_databricks(space_id: str, patches: list[dict]) -> None:
    """Apply patches to Databricks via the Genie API."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, run_in_context(lambda: _apply_config_sync(space_id, patches)))


# Lazy singleton
_fix_agent: FixAgent | None = None


def get_fix_agent() -> FixAgent:
    """Get or create the fix agent instance."""
    global _fix_agent
    if _fix_agent is None:
        _fix_agent = FixAgent()
    return _fix_agent
