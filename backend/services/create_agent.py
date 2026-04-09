"""Create Genie agent — LLM-driven conversational workflow for building Genie spaces.

Uses a tool-calling loop: the LLM decides which tools to call and when,
guided by the system prompt (SKILL.md workflow + schema reference).
"""

import asyncio
import json
import logging
import re
import time
from pathlib import Path
from typing import AsyncGenerator, Generator

from backend.services.llm_utils import get_llm_model
from backend.services.auth import get_workspace_client, run_in_context
from backend.services.create_agent_session import AgentSession
from backend.services.create_agent_tools import TOOL_DEFINITIONS, handle_tool_call, _present_plan
from backend.services import plan_builder
from backend.prompts_create import assemble_system_prompt, detect_step

logger = logging.getLogger(__name__)

MAX_TOOL_ROUNDS = 15

# Strip fake tool-call XML the LLM writes when tools are unavailable
_FAKE_TOOL_RE = re.compile(
    r"<tool_call>.*?</tool_call>\s*(?:<tool_response>.*?</tool_response>)?",
    re.DOTALL,
)

STEP_LABELS: dict[str, str] = {
    "requirements": "Understanding requirements",
    "discovery": "Discovering data sources",
    "feasibility": "Assessing feasibility",
    "inspection": "Inspecting tables",
    "plan": "Building plan",
    "config_create": "Generating configuration",
    "post_creation": "Finalizing space",
}

STEP_THINKING: dict[str, str] = {
    "requirements": "Understanding your requirements…",
    "discovery": "Exploring your data catalog…",
    "feasibility": "Assessing data feasibility…",
    "inspection": "Analyzing table structure and data quality…",
    "plan": "Designing your Genie Space plan…",
    "config_create": "Generating the configuration…",
    "post_creation": "Finalizing your Genie Space…",
}

# Tools allowed per step — structural guardrail to prevent the LLM from
# calling tools outside the current step's scope.
# Empty set = no tools (pure conversation). Missing key = all tools (fallback).
STEP_TOOLS: dict[str, set[str]] = {
    # Requirements has discovery tools so the agent can transition naturally
    # when the user says "go find my data." The prompt guides the agent to
    # gather requirements first — tools are available but not encouraged.
    "requirements": {"search_tables", "discover_catalogs", "discover_schemas", "discover_tables"},
    "discovery": {"search_tables", "discover_catalogs", "discover_schemas", "discover_tables"},
    # Feasibility has inspection tools available so the LLM can call
    # describe_table after the user confirms. The prompt instructs it to
    # present an assessment and WAIT before calling tools.
    "feasibility": {"describe_table", "profile_columns", "assess_data_quality", "profile_table_usage", "test_sql"},
    "inspection": {"describe_table", "profile_columns", "assess_data_quality", "profile_table_usage", "test_sql"},
    "plan": {"generate_plan", "present_plan", "test_sql"},
    "config_create": {"discover_warehouses", "generate_config", "validate_config", "create_space"},
    "post_creation": {"update_config", "validate_config", "update_space"},
}

STEP_ORDER = [
    "requirements",
    "discovery",
    "feasibility",
    "inspection",
    "plan",
    "config_create",
    "post_creation",
]


class CreateGenieAgent:
    """Conversational agent that guides users through Genie space creation."""

    def __init__(self):
        self.model = get_llm_model()
        self._schema_content: str | None = None

    def _get_schema_content(self) -> str:
        if self._schema_content is None:
            schema_path = Path(__file__).parent.parent / "references" / "schema.md"
            self._schema_content = schema_path.read_text()
        return self._schema_content

    async def chat(
        self,
        session: AgentSession,
        user_message: str,
        selections: dict | None = None,
    ) -> AsyncGenerator[dict, None]:
        """Process a user message (or empty continuation) and stream events.

        Args:
            session: The agent session.
            user_message: User's message (may include embedded selections for LLM context).
            selections: Structured selections from frontend (passed directly, not parsed
                from message string). Used for fast path detection.

        Each call performs exactly ONE LLM inference + ONE tool batch, then
        closes.  When the LLM requests tools, the ``done`` event carries
        ``needs_continuation: true`` and the frontend immediately opens a
        new stream with an empty message to start the next round.  This
        keeps each HTTP response short enough to survive the Databricks
        Apps reverse-proxy timeout (~120 s).

        Yields dicts with:
            {"event": "thinking",      "data": {"message": str}}
            {"event": "tool_call",     "data": {"tool": str, "args": dict}}
            {"event": "tool_result",   "data": {"tool": str, "result": dict}}
            {"event": "message_delta", "data": {"content": str}}
            {"event": "message",       "data": {"content": str, "ui_elements": list | None}}
            {"event": "created",       "data": {"space_id": str, "url": str}}
            {"event": "error",         "data": {"message": str}}
            {"event": "done",          "data": {"needs_continuation": bool}}
        """
        is_continuation = not user_message.strip()

        if not is_continuation:
            session.add_message("user", user_message)
            session.continuation_count = 0
        else:
            session.continuation_count += 1
            if session.continuation_count > MAX_TOOL_ROUNDS:
                yield {"event": "error", "data": {"message": "Agent exceeded maximum tool rounds"}}
                yield {"event": "done", "data": {"needs_continuation": False}}
                return

        # ── Fast path: deterministic create (no LLM needed) ──────────────
        # When the user clicks "Approve & Create", selections contain
        # action: "create" + the edited plan.  Instead of 3 LLM round trips
        # (generate_config → validate_config → create_space), run them
        # directly.  This cuts ~60-90s of LLM latency.
        #
        # Check structured selections first (reliable), fall back to parsing
        # from message string (for backward compatibility).
        if not is_continuation:
            sel = selections  # Prefer structured selections from router
            if not sel:
                sel = self._extract_selections(user_message)  # Fallback: parse from message
            if sel and sel.get("action") == "create" and sel.get("edited_plan"):
                logger.info("Fast path triggered: action=create, plan keys=%s", list(sel["edited_plan"].keys()))
                async for event in self._fast_create(session, sel):
                    yield event
                return

        step = detect_step(session)
        step_idx = STEP_ORDER.index(step) if step in STEP_ORDER else 0
        round_num = session.continuation_count

        yield {"event": "step", "data": {
            "step": step,
            "label": STEP_LABELS.get(step, step),
            "index": step_idx,
            "total": len(STEP_ORDER),
        }}
        yield {"event": "thinking", "data": {
            "message": "Processing tool results…" if is_continuation else STEP_THINKING.get(step, "Processing…"),
            "step": step,
            "round": round_num,
        }}

        tools_used: list[str] = []
        error_msg: str | None = None
        needs_continuation = False

        try:
            messages = self._build_messages(session)

            # Filter tools based on current step
            allowed_tools = STEP_TOOLS.get(step)
            if allowed_tools is not None:
                # Step has an explicit tool set (possibly empty for requirements/feasibility)
                step_tool_defs = [
                    td for td in TOOL_DEFINITIONS
                    if td.get("function", {}).get("name") in allowed_tools
                ]
            else:
                # Step not in STEP_TOOLS — fallback to all tools
                step_tool_defs = TOOL_DEFINITIONS

            content_parts: list[str] = []
            tool_calls_acc: dict[int, dict] = {}
            tool_call_signaled = False
            async for chunk in self._async_stream_llm(messages, tools=step_tool_defs):
                choices = chunk.get("choices", [])
                if not choices:
                    continue
                delta = choices[0].get("delta", {})

                if delta.get("content"):
                    token = delta["content"]
                    content_parts.append(token)
                    yield {"event": "message_delta", "data": {"content": token}}

                if delta.get("tool_calls"):
                    if not tool_call_signaled:
                        tool_call_signaled = True
                        yield {"event": "thinking", "data": {"message": "Planning next steps…", "step": step, "round": round_num}}
                    for tc_delta in delta["tool_calls"]:
                        idx = tc_delta.get("index", 0)
                        if idx not in tool_calls_acc:
                            fn = tc_delta.get("function", {})
                            tool_calls_acc[idx] = {
                                "id": tc_delta.get("id", ""),
                                "type": "function",
                                "function": {
                                    "name": fn.get("name", ""),
                                    "arguments": fn.get("arguments", ""),
                                },
                            }
                        else:
                            if tc_delta.get("id"):
                                tool_calls_acc[idx]["id"] = tc_delta["id"]
                            fn = tc_delta.get("function", {})
                            if fn.get("name"):
                                tool_calls_acc[idx]["function"]["name"] = fn["name"]
                            if fn.get("arguments"):
                                tool_calls_acc[idx]["function"]["arguments"] += fn["arguments"]

            accumulated_content = "".join(content_parts)

            if tool_calls_acc:
                for tc in tool_calls_acc.values():
                    args_str = tc["function"].get("arguments", "")
                    if not args_str or not args_str.strip():
                        tc["function"]["arguments"] = "{}"
                tool_calls = [tool_calls_acc[i] for i in sorted(tool_calls_acc)]

                tc_content = accumulated_content.strip() if accumulated_content else None
                assistant_msg: dict = {
                    "role": "assistant",
                    "content": tc_content,
                    "tool_calls": tool_calls,
                }
                session.history.append(assistant_msg)
                session.last_active = time.time()

                for tc in tool_calls:
                    fn = tc["function"]
                    tool_name = fn["name"]
                    try:
                        tool_args = json.loads(fn["arguments"])
                    except json.JSONDecodeError:
                        args_preview = fn.get("arguments", "")[:200]
                        args_len = len(fn.get("arguments", ""))
                        logger.error(
                            "JSONDecodeError parsing %s args (%d chars, preview: %s). "
                            "Likely truncated due to max_tokens. Attempting repair.",
                            tool_name, args_len, args_preview,
                        )
                        tool_args = self._try_repair_json(fn.get("arguments", ""))
                        fn["arguments"] = json.dumps(tool_args)

                    if tool_name in ("generate_config", "present_plan"):
                        injected = self._backfill_generate_config_args(session, tool_args)
                        if injected:
                            fn["arguments"] = json.dumps(tool_args)
                            logger.info("Backfilled %s args from session: %s", tool_name, ", ".join(injected))

                    yield {"event": "tool_call", "data": {"tool": tool_name, "args": tool_args}}

                    if session.space_config is None and tool_name in (
                        "update_config", "validate_config", "create_space", "update_space",
                    ):
                        recovered = self._recover_config_from_history(session)
                        if recovered:
                            session.space_config = recovered
                            logger.info("Recovered space_config from session history for %s", tool_name)

                    if tool_name in ("generate_plan", "present_plan"):
                        plan_item_count = sum(
                            len(v) for v in tool_args.values() if isinstance(v, list)
                        )
                        use_parallel = (
                            tool_name == "generate_plan"
                            or plan_item_count < 15
                        )
                        if use_parallel:
                            if tool_name == "present_plan":
                                logger.info("Redirecting sparse present_plan (%d items) to parallel generate_plan", plan_item_count)
                            loop = asyncio.get_event_loop()
                            future = loop.run_in_executor(
                                None, run_in_context(self._run_generate_plan, session, tool_args)
                            )
                        else:
                            loop = asyncio.get_event_loop()
                            future = loop.run_in_executor(
                                None, run_in_context(handle_tool_call, tool_name, tool_args, session.space_config)
                            )
                    else:
                        loop = asyncio.get_event_loop()
                        future = loop.run_in_executor(
                            None, run_in_context(handle_tool_call, tool_name, tool_args, session.space_config)
                        )
                    while not future.done():
                        try:
                            await asyncio.wait_for(asyncio.shield(future), timeout=3.0)
                        except asyncio.TimeoutError:
                            yield {"event": "heartbeat", "data": {"tool": tool_name}}
                    result = future.result()

                    tools_used.append(tool_name)

                    yield {"event": "tool_result", "data": {"tool": tool_name, "result": result}}

                    if tool_name == "create_space" and result.get("success"):
                        session.space_id = result.get("space_id")
                        session.space_url = result.get("space_url")
                        yield {"event": "created", "data": {
                            "space_id": result["space_id"],
                            "url": result["space_url"],
                            "display_name": result.get("display_name", ""),
                        }}

                    if tool_name == "update_space" and result.get("success"):
                        yield {"event": "updated", "data": {
                            "space_id": result["space_id"],
                            "url": result["url"],
                        }}

                    if tool_name in ("generate_config", "update_config") and "config" in result:
                        session.space_config = result["config"]

                    session.add_tool_result(tc["id"], json.dumps(result, default=str))

                    # ── Auto-chain: config tools → deploy ──────────────────
                    # Once generate_config or update_config succeeds, the
                    # remaining steps are deterministic.  Chain them here
                    # instead of burning extra LLM round trips (~30s each).
                    if tool_name in ("generate_config", "update_config") and "config" in result:
                        config = result["config"]

                        if session.space_id:
                            # Space already exists → update it (include display_name if provided)
                            update_args: dict = {"space_id": session.space_id}
                            dn = self._derive_display_name(None, tool_args, session)
                            if dn and dn != "New Genie Space":
                                update_args["display_name"] = dn
                            yield {"event": "tool_call", "data": {"tool": "update_space", "args": update_args}}
                            u_result = await loop.run_in_executor(
                                None, run_in_context(handle_tool_call, "update_space", update_args, config)
                            )
                            yield {"event": "tool_result", "data": {"tool": "update_space", "result": u_result}}
                            tools_used.append("update_space")

                            if u_result.get("success"):
                                yield {"event": "updated", "data": {
                                    "space_id": u_result["space_id"],
                                    "url": u_result["url"],
                                }}
                        else:
                            # New space: validate → create
                            yield {"event": "tool_call", "data": {"tool": "validate_config", "args": {}}}
                            v_result = await loop.run_in_executor(
                                None, run_in_context(handle_tool_call, "validate_config", {}, config)
                            )
                            yield {"event": "tool_result", "data": {"tool": "validate_config", "result": v_result}}
                            tools_used.append("validate_config")

                            if not v_result.get("errors"):
                                dn = self._derive_display_name(None, tool_args, session)
                                async for event in self._create_space_with_repair(session, config, dn):
                                    yield event
                                tools_used.append("create_space")

                new_step = detect_step(session)
                if new_step != step:
                    step = new_step
                    step_idx = STEP_ORDER.index(step) if step in STEP_ORDER else 0
                    yield {"event": "step", "data": {
                        "step": step,
                        "label": STEP_LABELS.get(step, step),
                        "index": step_idx,
                        "total": len(STEP_ORDER),
                    }}

                _STOP_TOOLS = {"generate_plan", "present_plan", "create_space"}
                if _STOP_TOOLS.intersection(tools_used):
                    needs_continuation = False
                else:
                    needs_continuation = True

            else:
                # Text-only response — conversation turn is done.
                # Strip fake <tool_call> XML the LLM may emit when tools are blocked.
                cleaned = _FAKE_TOOL_RE.sub("", accumulated_content).strip()
                if cleaned != accumulated_content:
                    logger.warning("Stripped fake tool_call XML from response (%d -> %d chars)", len(accumulated_content), len(cleaned))
                session.add_message("assistant", cleaned)
                ui_elements = self._extract_ui_hints(session)
                # The frontend's onMessage replaces streamed content with this final version,
                # so the cleaned content overwrites any <tool_call> junk from message_delta.
                yield {"event": "message", "data": {"content": cleaned, "ui_elements": ui_elements}}

        except Exception as e:
            logger.exception("Create agent chat failed")
            error_msg = str(e)
            yield {"event": "error", "data": {"message": str(e)}}

        if tools_used:
            logger.info(
                "Agent round complete: round=%d step=%s tools=%s continuation=%s error=%s",
                round_num, step, tools_used, needs_continuation, error_msg,
            )

        yield {"event": "done", "data": {"needs_continuation": needs_continuation}}

    _TOOL_RESULT_CHAR_LIMIT = 3000
    _COMPRESSIBLE_TOOLS = frozenset({
        "describe_table", "profile_columns", "profile_table_usage",
        "assess_data_quality", "test_sql",
    })

    @classmethod
    def _compress_tool_result(cls, tool_name: str, content: str) -> str:
        """Trim large tool results to keep the conversation within token limits.

        Only compresses results from data-heavy profiling tools; leaves
        plan/config tool results untouched since those are needed verbatim.
        """
        if tool_name not in cls._COMPRESSIBLE_TOOLS:
            return content
        if len(content) <= cls._TOOL_RESULT_CHAR_LIMIT:
            return content

        try:
            data = json.loads(content)
        except (json.JSONDecodeError, TypeError):
            return content[:cls._TOOL_RESULT_CHAR_LIMIT] + "\n...(truncated)"

        if tool_name == "describe_table":
            # Keep column metadata but cap sample rows
            if "sample_rows" in data:
                data["sample_rows"] = data["sample_rows"][:2]
            if "columns" in data and len(data["columns"]) > 30:
                data["columns"] = data["columns"][:30]
                data["_note"] = "Showing first 30 columns only"

        elif tool_name == "profile_columns":
            # Limit distinct values per column
            for col, profile in data.get("profiles", {}).items():
                vals = profile.get("distinct_values", [])
                if len(vals) > 8:
                    profile["distinct_values"] = vals[:8]
                    profile["has_more"] = True

        elif tool_name == "profile_table_usage":
            # Trim query history per table
            for tbl, info in data.get("tables", {}).items():
                qs = info.get("recent_queries", [])
                if len(qs) > 3:
                    info["recent_queries"] = qs[:3]

        elif tool_name == "assess_data_quality":
            # Only keep the summary, drop per-column detail if large
            for tbl, info in data.get("tables", {}).items():
                if isinstance(info, dict) and "columns" in info:
                    cols = info["columns"]
                    if len(cols) > 20:
                        info["columns"] = {k: v for i, (k, v) in enumerate(cols.items()) if i < 20}
                        info["_note"] = "Showing first 20 columns only"

        elif tool_name == "test_sql":
            # Cap data rows
            if "data" in data:
                data["data"] = data.get("data", [])[:3]

        compressed = json.dumps(data, default=str)
        if len(compressed) > cls._TOOL_RESULT_CHAR_LIMIT:
            return compressed[:cls._TOOL_RESULT_CHAR_LIMIT] + "\n...(truncated)"
        return compressed

    _COMPRESSIBLE_TOOLS_SET = frozenset({
        "describe_table", "profile_columns", "profile_table_usage",
        "assess_data_quality", "test_sql",
    })

    def _build_messages(self, session: AgentSession) -> list[dict]:
        """Build the full message list for the LLM call.

        The Databricks serving endpoint for Claude converts OpenAI-format
        messages to Anthropic-native format. We need to be careful about:

        1. Assistant messages with tool_calls: include ``content: null``
           (not empty string, which becomes an empty text block that Claude rejects)
        2. Tool result messages: include the ``name`` field (some endpoints need it)
        3. Orphaned tool_use blocks: if the user stopped the stream mid-tool-call,
           inject synthetic "cancelled" results so Claude doesn't reject the history

        Individual tool results are compressed (capped at 3000 chars) for
        data-heavy tools, but the full conversation history is always
        preserved so the LLM can see which tools have already been called.
        """
        self._heal_orphaned_tool_calls(session)

        prompt = assemble_system_prompt(session, self._get_schema_content())
        messages: list[dict] = [{"role": "system", "content": prompt}]

        tc_id_to_name: dict[str, str] = {}
        for msg in session.history:
            if msg["role"] == "assistant" and msg.get("tool_calls"):
                for tc in msg["tool_calls"]:
                    tc_id_to_name[tc["id"]] = tc["function"]["name"]

        messages = self._build_full_messages(
            session, messages, tc_id_to_name,
        )

        messages = self._sanitize_messages(messages)
        return messages

    @staticmethod
    def _sanitize_messages(messages: list[dict]) -> list[dict]:
        """Fix message structure issues that cause Claude API 400 errors.

        - Merges consecutive assistant messages (without tool/user in between)
        - Ensures conversation ends with a user message (required by Databricks
          Claude endpoint — no assistant prefill support)
        """
        if len(messages) < 2:
            return messages

        sanitized: list[dict] = [messages[0]]

        for msg in messages[1:]:
            prev = sanitized[-1]

            if (
                msg["role"] == "assistant"
                and prev["role"] == "assistant"
                and not prev.get("tool_calls")
                and not msg.get("tool_calls")
            ):
                prev_content = prev.get("content") or ""
                new_content = msg.get("content") or ""
                merged = (prev_content + "\n\n" + new_content).strip()
                prev["content"] = merged
                logger.warning("Merged consecutive assistant messages (%d + %d chars)", len(prev_content), len(new_content))
                continue

            sanitized.append(msg)

        last = sanitized[-1]
        if last["role"] != "user":
            sanitized.append({"role": "user", "content": "Continue."})
            logger.info("Appended 'Continue.' user message — endpoint requires user-last")

        return sanitized

    def _build_full_messages(
        self,
        session: AgentSession,
        messages: list[dict],
        tc_id_to_name: dict[str, str],
    ) -> list[dict]:
        """Append all history messages with standard compression."""
        for msg in session.history:
            if msg["role"] == "tool":
                tool_name = tc_id_to_name.get(msg["tool_call_id"], "unknown")
                raw_content = msg.get("content") or "{}"
                messages.append({
                    "role": "tool",
                    "tool_call_id": msg["tool_call_id"],
                    "name": tool_name,
                    "content": self._compress_tool_result(tool_name, raw_content),
                })
            elif msg["role"] == "assistant" and msg.get("tool_calls"):
                tc_content = (msg.get("content") or "").strip() or None
                normalized_tcs = []
                for tc in msg["tool_calls"]:
                    ntc = {
                        "id": tc["id"],
                        "type": "function",
                        "function": {
                            "name": tc["function"]["name"],
                            "arguments": tc["function"].get("arguments") or "{}",
                        },
                    }
                    normalized_tcs.append(ntc)
                tc_msg: dict = {"role": "assistant", "content": tc_content, "tool_calls": normalized_tcs}
                messages.append(tc_msg)
            elif msg["role"] == "assistant":
                content = msg.get("content") or ""
                if not content.strip():
                    continue
                messages.append({"role": "assistant", "content": content})
            else:
                messages.append({
                    "role": msg["role"],
                    "content": msg.get("content") or " ",
                })
        return messages

    @staticmethod
    def _heal_orphaned_tool_calls(session: AgentSession) -> None:
        """Inject synthetic tool results for any tool_call IDs missing a result.

        When the user stops the stream mid-execution, assistant messages with
        tool_calls may exist without corresponding tool-result messages. Claude
        requires every tool_use to be followed by a tool_result, so we patch
        the gap with a ``{"cancelled": true}`` placeholder.
        """
        # Collect all tool_call IDs that have results
        result_ids: set[str] = set()
        for msg in session.history:
            if msg["role"] == "tool" and "tool_call_id" in msg:
                result_ids.add(msg["tool_call_id"])

        # Find orphans and inject results right after the assistant message
        inserts: list[tuple[int, dict]] = []
        for i, msg in enumerate(session.history):
            if msg["role"] == "assistant" and msg.get("tool_calls"):
                for tc in msg["tool_calls"]:
                    if tc["id"] not in result_ids:
                        logger.info(f"Healing orphaned tool_call {tc['id']} ({tc['function']['name']})")
                        inserts.append((i + 1, {
                            "role": "tool",
                            "tool_call_id": tc["id"],
                            "content": json.dumps({"cancelled": True, "message": "Operation was cancelled by the user."}),
                        }))

        # Insert in reverse order to preserve indices
        for idx, msg in reversed(inserts):
            session.history.insert(idx, msg)

    def _repair_config(self, config: dict, error_msg: str) -> dict | None:
        """Use the LLM to repair a config that failed space creation.

        Sends the error message and the failing config section to the LLM,
        which returns a corrected config.  Returns None if repair fails.
        """
        from backend.services.llm_utils import call_serving_endpoint, parse_json_from_llm_response, get_llm_model
        try:
            # Send both sections — use a generous limit so the LLM sees the full config
            repair_context = {
                "instructions": config.get("instructions", {}),
                "data_sources": config.get("data_sources", {}),
            }
            config_json = json.dumps(repair_context, default=str)
            # Truncate only if truly enormous (>24k chars); 16k is typical for 9 tables
            if len(config_json) > 24000:
                config_json = config_json[:24000] + "\n... (truncated)"
            prompt = (
                "The following Genie Space config failed with this API error:\n\n"
                f"**Error:** {error_msg}\n\n"
                f"**Config (relevant sections):**\n```json\n{config_json}\n```\n\n"
                "Fix ONLY the specific issue described in the error. Return the FULL corrected config "
                "(with both 'instructions' and 'data_sources' sections intact).\n"
                "Return ONLY valid JSON: {\"instructions\": {...}, \"data_sources\": {...}}"
            )
            response = call_serving_endpoint(
                [{"role": "user", "content": prompt}],
                model=get_llm_model(),
                max_tokens=16000,
            )
            repaired = parse_json_from_llm_response(response)
            # Merge repaired sections back into original config
            fixed = {**config}
            if "instructions" in repaired:
                fixed["instructions"] = repaired["instructions"]
            if "data_sources" in repaired:
                fixed["data_sources"] = repaired["data_sources"]
            return fixed
        except Exception as e:
            logger.warning("Config repair failed: %s", e)
            return None

    @staticmethod
    def _extract_selections(user_message: str) -> dict | None:
        """Extract the [User selections: <json>] payload from a user message.

        Format: ...text...[User selections: <json>]
        The JSON object starts with { and we find its end by matching braces,
        rather than relying on the trailing ] which could appear inside the JSON.
        """
        marker = "[User selections: "
        idx = user_message.find(marker)
        if idx < 0:
            return None
        json_start = idx + len(marker)
        # Find the JSON object by matching braces (the value is always a dict)
        remainder = user_message[json_start:]
        brace_start = remainder.find("{")
        if brace_start < 0:
            return None
        depth = 0
        in_string = False
        escape = False
        for i, ch in enumerate(remainder[brace_start:], start=brace_start):
            if escape:
                escape = False
                continue
            if ch == "\\":
                escape = True
                continue
            if ch == '"':
                in_string = not in_string
                continue
            if in_string:
                continue
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(remainder[brace_start:i + 1])
                    except (json.JSONDecodeError, ValueError):
                        return None
        return None

    @staticmethod
    def _derive_display_name(
        selections: dict | None,
        tool_args: dict | None,
        session: AgentSession,
    ) -> str:
        """Derive a display name from the best available source."""
        # 1. Explicit from selections
        if selections and selections.get("display_name"):
            return selections["display_name"]
        # 2. From tool_args (LLM may have specified it)
        if tool_args and tool_args.get("display_name"):
            return tool_args["display_name"]
        # 3. From any user selection in history (scan all, not just last)
        for m in reversed(session.history):
            if m["role"] != "user":
                continue
            sel = CreateGenieAgent._extract_selections(m.get("content", ""))
            if sel and sel.get("display_name"):
                return sel["display_name"]
        # 4. Derive from table names
        tables = (tool_args or {}).get("tables") or []
        if not tables:
            for m in reversed(session.history):
                if m["role"] != "tool":
                    continue
                try:
                    r = json.loads(m.get("content", "{}"))
                    if isinstance(r, dict) and r.get("table"):
                        tables.append({"identifier": r["table"]})
                except (json.JSONDecodeError, KeyError):
                    continue
        if tables:
            short = [t.get("identifier", "").split(".")[-1] for t in tables[:3]]
            return " + ".join(n for n in short if n) + " Space"
        return "New Genie Space"

    async def _create_space_with_repair(
        self,
        session: AgentSession,
        config: dict,
        display_name: str,
    ) -> AsyncGenerator[dict, None]:
        """Run create_space with automatic LLM repair on config errors.

        Yields SSE events (tool_call, tool_result, thinking, created).
        Updates session.space_config/space_id/space_url on success.
        """
        from backend.services.create_agent_tools import handle_tool_call
        loop = asyncio.get_event_loop()

        yield {"event": "tool_call", "data": {"tool": "create_space", "args": {"display_name": display_name}}}
        result = await loop.run_in_executor(
            None, run_in_context(handle_tool_call, "create_space", {"display_name": display_name}, config)
        )

        # If creation failed with a config error, try LLM-assisted repair once
        if not result.get("success") and result.get("error"):
            err = result["error"]
            if "Invalid" in err or "configuration" in err.lower() or "proto" in err.lower():
                yield {"event": "tool_result", "data": {"tool": "create_space", "result": {"repairing": True, "original_error": err}}}
                yield {"event": "thinking", "data": {"message": "Config rejected by API — repairing automatically...", "step": "create", "round": 0}}

                fixed = await loop.run_in_executor(None, run_in_context(self._repair_config, config, err))
                if fixed:
                    config = fixed
                    session.space_config = config
                    result = await loop.run_in_executor(
                        None, run_in_context(handle_tool_call, "create_space", {"display_name": display_name}, config)
                    )

        yield {"event": "tool_result", "data": {"tool": "create_space", "result": result}}

        if result.get("success"):
            session.space_id = result.get("space_id")
            session.space_url = result.get("space_url")
            yield {"event": "created", "data": {
                "space_id": result["space_id"],
                "url": result["space_url"],
                "display_name": result.get("display_name", display_name),
            }}

    async def _fast_create(
        self,
        session: AgentSession,
        selections: dict,
    ) -> AsyncGenerator[dict, None]:
        """Deterministic create path — skips the LLM entirely.

        Runs generate_config → validate_config → create_space directly using
        the edited plan from selections.  This eliminates 3 LLM round trips
        (~60-90s) for what are purely programmatic operations.
        """
        from backend.services.create_agent_tools import handle_tool_call

        edited_plan = selections["edited_plan"]
        display_name = self._derive_display_name(selections, None, session)

        yield {"event": "step", "data": {
            "step": "create",
            "label": "Creating Space",
            "index": STEP_ORDER.index("config_create") if "config_create" in STEP_ORDER else len(STEP_ORDER) - 1,
            "total": len(STEP_ORDER),
        }}

        # Build config args from edited plan, then backfill gaps from session
        config_args: dict = {}
        for key in ("tables", "sample_questions", "text_instructions", "example_sqls",
                     "join_specs", "measures", "filters", "expressions", "benchmarks"):
            val = edited_plan.get(key)
            if val:
                config_args[key] = val
        # text_instructions: frontend sends as single string, backend expects list
        ti = config_args.get("text_instructions")
        if isinstance(ti, str):
            config_args["text_instructions"] = [ti] if ti.strip() else []
        # Backfill tables + any missing sections from session history
        self._backfill_generate_config_args(session, config_args)

        try:
            loop = asyncio.get_event_loop()

            # Step 1: generate_config
            yield {"event": "tool_call", "data": {"tool": "generate_config", "args": {"tables": f"({len(config_args.get('tables', []))} tables)"}}}
            config_result = await loop.run_in_executor(
                None, run_in_context(handle_tool_call, "generate_config", config_args, session.space_config)
            )
            yield {"event": "tool_result", "data": {"tool": "generate_config", "result": config_result}}

            if "error" in config_result:
                yield {"event": "error", "data": {"message": f"Config generation failed: {config_result['error']}"}}
                yield {"event": "done", "data": {"needs_continuation": False}}
                return

            config = config_result.get("config")
            session.space_config = config

            if session.space_id:
                # Space already exists → update it (include display_name for rename support)
                update_args: dict = {"space_id": session.space_id}
                if display_name and display_name != "New Genie Space":
                    update_args["display_name"] = display_name
                yield {"event": "tool_call", "data": {"tool": "update_space", "args": update_args}}
                u_result = await loop.run_in_executor(
                    None, run_in_context(handle_tool_call, "update_space", update_args, config)
                )
                yield {"event": "tool_result", "data": {"tool": "update_space", "result": u_result}}
                if u_result.get("success"):
                    yield {"event": "updated", "data": {"space_id": u_result["space_id"], "url": u_result["url"]}}
                    yield {"event": "message", "data": {"content": f"Space **{display_name}** updated successfully!", "ui_elements": None}}
                else:
                    yield {"event": "error", "data": {"message": f"Space update failed: {u_result.get('error', 'unknown')}"}}
            else:
                # Step 2: validate_config
                yield {"event": "tool_call", "data": {"tool": "validate_config", "args": {}}}
                validate_result = await loop.run_in_executor(
                    None, run_in_context(handle_tool_call, "validate_config", {}, config)
                )
                yield {"event": "tool_result", "data": {"tool": "validate_config", "result": validate_result}}

                if validate_result.get("errors"):
                    yield {"event": "error", "data": {"message": f"Config validation failed: {validate_result['errors']}"}}
                    yield {"event": "done", "data": {"needs_continuation": False}}
                    return

                # Step 3: create_space (with LLM repair on failure)
                async for event in self._create_space_with_repair(session, config, display_name):
                    yield event

                if session.space_id:
                    yield {"event": "message", "data": {
                        "content": f"Space **{display_name}** created successfully!",
                        "ui_elements": None,
                    }}

        except Exception as e:
            logger.exception("Fast create failed")
            yield {"event": "error", "data": {"message": str(e)}}

        yield {"event": "done", "data": {"needs_continuation": False}}

    _MAX_LLM_RETRIES = 4
    _RETRY_BACKOFF_BASE = 2  # seconds

    def _stream_llm(self, messages: list[dict], tools: list[dict] | None = None) -> Generator[dict, None, None]:
        """Stream LLM response chunks from the serving endpoint (sync).

        Uses the SDK's pre-authenticated requests.Session so auth works
        across all methods (PAT, OAuth/M2M, CLI profile).
        Retries automatically on 429 (rate limit) and 502/503 (transient)
        with exponential backoff.
        """
        client = get_workspace_client()
        host = (client.config.host or "").rstrip("/")

        body = {
            "messages": messages,
            "max_tokens": 16384,
            "stream": True,
        }
        # Only include tools if the step has tools available
        effective_tools = tools if tools is not None else TOOL_DEFINITIONS
        if effective_tools:
            body["tools"] = effective_tools

        url = f"{host}/serving-endpoints/{self.model}/invocations"
        logger.info("Streaming LLM call to %s with %d messages", self.model, len(messages))
        if logger.isEnabledFor(logging.DEBUG):
            for i, m in enumerate(messages):
                role = m.get("role", "?")
                has_tc = bool(m.get("tool_calls"))
                tc_id = m.get("tool_call_id", "")
                content_preview = str(m.get("content", ""))[:80] if m.get("content") else "(none)"
                logger.debug("  msg[%d] role=%s tool_calls=%s tc_id=%s content=%r", i, role, has_tc, tc_id, content_preview)
                if has_tc:
                    for j, tc in enumerate(m["tool_calls"]):
                        logger.debug("    tc[%d] id=%s fn=%s args_len=%d", j, tc.get("id", "?"), tc.get("function", {}).get("name", "?"), len(tc.get("function", {}).get("arguments", "")))

        _RETRYABLE_STATUSES = {429, 502, 503}
        session = client.api_client._api_client._session
        for attempt in range(self._MAX_LLM_RETRIES + 1):
            resp = session.post(url, json=body, stream=True, timeout=120)
            if resp.status_code in _RETRYABLE_STATUSES:
                resp.close()
                if attempt >= self._MAX_LLM_RETRIES:
                    logger.error("LLM endpoint returned %d after %d retries, giving up", resp.status_code, self._MAX_LLM_RETRIES)
                    raise RuntimeError(f"LLM endpoint returned {resp.status_code} after {self._MAX_LLM_RETRIES} retries.")
                retry_after = resp.headers.get("Retry-After")
                delay = float(retry_after) if retry_after else self._RETRY_BACKOFF_BASE * (2 ** attempt)
                logger.warning("%d from LLM endpoint, retrying in %.1fs (attempt %d/%d)", resp.status_code, delay, attempt + 1, self._MAX_LLM_RETRIES)
                time.sleep(delay)
                continue
            break

        try:
            if not resp.ok:
                error_body = resp.text[:1000]
                logger.error("LLM endpoint returned %s: %s", resp.status_code, error_body)
                raise RuntimeError(
                    f"LLM endpoint returned {resp.status_code}: {error_body[:300]}"
                )

            resp.encoding = "utf-8"
            for line in resp.iter_lines(decode_unicode=True):
                if not line or not line.startswith("data: "):
                    continue
                data = line[6:].strip()
                if data == "[DONE]":
                    break
                try:
                    yield json.loads(data)
                except json.JSONDecodeError:
                    continue
        finally:
            resp.close()

    async def _async_stream_llm(self, messages: list[dict], tools: list[dict] | None = None) -> AsyncGenerator[dict, None]:
        """Async wrapper that bridges the sync streaming generator to async.

        Captures context once and reuses across all iterations (can't use
        run_in_context here because we need a single shared ctx for the generator).
        """
        import contextvars as _cv
        loop = asyncio.get_event_loop()
        ctx = _cv.copy_context()
        gen = self._stream_llm(messages, tools=tools)
        _sentinel = object()

        while True:
            chunk = await loop.run_in_executor(None, lambda: ctx.run(next, gen, _sentinel))
            if chunk is _sentinel:
                break
            yield chunk

    @staticmethod
    def _try_repair_json(s: str) -> dict:
        """Attempt to repair truncated JSON from an LLM tool call.

        The LLM output was cut off mid-JSON (due to max_tokens). Try
        progressively closing open brackets/braces to recover partial data.
        """
        if not s or not s.strip():
            return {}
        s = s.strip()

        # Try as-is first
        try:
            return json.loads(s)
        except json.JSONDecodeError:
            pass

        # Find last complete value by trimming trailing partial tokens
        # Remove trailing comma or partial key
        for trim_char in (",", ":"):
            idx = s.rfind(trim_char)
            if idx > 0:
                candidate = s[:idx]
                # Close all open brackets/braces
                open_brackets = 0
                open_braces = 0
                in_string = False
                escape = False
                for c in candidate:
                    if escape:
                        escape = False
                        continue
                    if c == "\\":
                        escape = True
                        continue
                    if c == '"' and not escape:
                        in_string = not in_string
                        continue
                    if in_string:
                        continue
                    if c == "[":
                        open_brackets += 1
                    elif c == "]":
                        open_brackets -= 1
                    elif c == "{":
                        open_braces += 1
                    elif c == "}":
                        open_braces -= 1

                suffix = "]" * max(open_brackets, 0) + "}" * max(open_braces, 0)
                try:
                    result = json.loads(candidate + suffix)
                    if isinstance(result, dict):
                        logger.info("Repaired truncated JSON (%d chars → %d keys)", len(s), len(result))
                        return result
                except json.JSONDecodeError:
                    continue

        logger.warning("Could not repair truncated JSON (%d chars)", len(s))
        return {}

    @staticmethod
    def _run_generate_plan(session: AgentSession, tool_args: dict) -> dict:
        """Run parallel plan generation using plan_builder.

        Extracts tables_context and inspection_summaries from session history,
        then calls plan_builder.generate_plan() which makes 4 parallel LLM calls.
        Wraps the result through _present_plan for frontend rendering.
        """
        tables_context = []
        inspection_summaries: dict = {}

        for msg in session.history:
            if msg["role"] != "tool":
                continue
            try:
                result = json.loads(msg.get("content", "{}"))
                if not isinstance(result, dict):
                    continue

                # describe_table results → tables_context
                table_id = result.get("table")
                if table_id and "columns" in result:
                    existing_ids = {t.get("table") or t.get("table_name") for t in tables_context}
                    if table_id not in existing_ids:
                        tables_context.append(result)

                # assess_data_quality results — returns {"tables": {...}, "summary": {"tables_assessed": N, ...}}
                summary_val = result.get("summary")
                if isinstance(summary_val, dict) and "tables_assessed" in summary_val:
                    inspection_summaries["quality"] = result

                # profile_table_usage results — tables is a dict keyed by table id
                # (discover_tables also has "tables" but as a list — guard with isinstance)
                tables_val = result.get("tables")
                if isinstance(tables_val, dict) and any(
                    "recent_queries" in v for v in tables_val.values() if isinstance(v, dict)
                ):
                    inspection_summaries["usage"] = result

                # profile_columns results — accumulate across tables
                if "profiles" in result and result.get("table"):
                    if "profiles" not in inspection_summaries:
                        inspection_summaries["profiles"] = {}
                    inspection_summaries["profiles"][result["table"]] = result["profiles"]

            except (json.JSONDecodeError, KeyError, TypeError, AttributeError):
                continue

        user_requirements = tool_args.get("user_requirements", "")

        # If the LLM specified which tables to include (e.g., after user removed some),
        # filter tables_context to only those tables.
        requested_tables = tool_args.get("tables")
        if requested_tables and isinstance(requested_tables, list):
            requested_ids = set()
            for t in requested_tables:
                if isinstance(t, str):
                    requested_ids.add(t)
                elif isinstance(t, dict):
                    requested_ids.add(t.get("identifier", ""))
            if requested_ids:
                tables_context = [
                    t for t in tables_context
                    if (t.get("table") or t.get("table_name") or "") in requested_ids
                ]
                logger.info("Filtered tables_context to %d tables (requested: %s)", len(tables_context), requested_ids)

        if not tables_context:
            return {
                "error": "No table inspection data found in session. Run describe_table first.",
                "hint": "Call describe_table on each table before calling generate_plan.",
            }

        logger.info(
            "Running parallel plan generation: %d tables, %d inspection sections, requirements=%d chars",
            len(tables_context), len(inspection_summaries), len(user_requirements),
        )

        raw_plan = plan_builder.generate_plan(tables_context, inspection_summaries, user_requirements)

        if "error" in raw_plan and "tables" not in raw_plan:
            return raw_plan

        warnings = raw_plan.pop("_generation_warnings", None)

        _PLAN_KEYS = {
            "tables", "sample_questions", "text_instructions", "example_sqls",
            "measures", "filters", "expressions", "join_specs", "benchmarks",
            "metric_views",
        }
        plan_args = {k: v for k, v in raw_plan.items() if k in _PLAN_KEYS}

        total_items = sum(len(v) for v in plan_args.values() if isinstance(v, list))
        if total_items == 0:
            logger.error("generate_plan produced an empty plan. raw_plan keys: %s, warnings: %s", list(raw_plan.keys()), warnings)
            return {
                "error": "Plan generation produced empty results. This usually means the parallel LLM calls failed.",
                "details": warnings or "No warnings captured — check server logs.",
                "hint": "Try again, or use present_plan with manually constructed data.",
            }

        result = _present_plan(**plan_args)
        if raw_plan.get("suggested_display_name"):
            result["suggested_display_name"] = raw_plan["suggested_display_name"]
        if warnings:
            result["_generation_warnings"] = warnings
        return result

    @staticmethod
    def _backfill_generate_config_args(session: AgentSession, tool_args: dict) -> list[str]:
        """Backfill missing generate_config arguments from session history.

        Scans for describe_table results (tables + columns) and the most
        recent present_plan result (tables, sample_questions, text_instructions,
        example_sqls, join_specs, measures, filters, expressions, benchmarks, metric_views).

        Mutates tool_args in-place and returns the list of keys that were injected.
        """
        injected: list[str] = []

        # --- Extract tables and metric_views from session history ---
        tables_by_id: dict[str, dict] = {}
        mvs_by_id: dict[str, dict] = {}
        for msg in session.history:
            if msg["role"] != "tool":
                continue
            try:
                result = json.loads(msg.get("content", "{}"))
                if not isinstance(result, dict):
                    continue
                # describe_table results → tables or metric_views
                table_id = result.get("table")
                if table_id and "columns" in result:
                    cols = []
                    for col in result["columns"]:
                        entry: dict = {"column_name": col["name"]}
                        if col.get("description"):
                            entry["description"] = col["description"]
                        cols.append(entry)
                    ttype = result.get("table_type", "")
                    if ttype and "METRIC_VIEW" in ttype:
                        # Metric views go into mvs_by_id with column_configs
                        # (only enable_format_assistance, NOT enable_entity_matching)
                        mv_cols = [{"column_name": c["column_name"], "enable_format_assistance": True} for c in cols]
                        mvs_by_id[table_id] = {
                            "identifier": table_id,
                            "description": result.get("comment") or "",
                            "column_configs": mv_cols,
                        }
                    else:
                        tables_by_id[table_id] = {
                            "identifier": table_id,
                            "description": result.get("comment") or "",
                            "column_configs": cols,
                        }
                # discover_tables results → metric_views (dedup by identifier)
                mvs = result.get("metric_views")
                if isinstance(mvs, list) and mvs:
                    for mv in mvs:
                        full_name = mv.get("full_name") or mv.get("identifier")
                        if full_name and full_name not in mvs_by_id:
                            mvs_by_id[full_name] = {
                                "identifier": full_name,
                                "description": mv.get("description", ""),
                            }
            except (json.JSONDecodeError, KeyError, TypeError):
                continue
        if "tables" not in tool_args and tables_by_id:
            tool_args["tables"] = list(tables_by_id.values())
            injected.append(f"tables({len(tables_by_id)})")
        if "metric_views" not in tool_args and mvs_by_id:
            tool_args["metric_views"] = list(mvs_by_id.values())
            injected.append(f"metric_views({len(mvs_by_id)})")

        # --- Extract user's edited plan from selections (preferred source) ---
        edited_plan: dict | None = None
        last_user_msg = next(
            (m for m in reversed(session.history) if m["role"] == "user"), None
        )
        if last_user_msg:
            sel = CreateGenieAgent._extract_selections(last_user_msg.get("content", ""))
            if sel and "edited_plan" in sel:
                edited_plan = sel["edited_plan"]

        # --- Extract plan data from the most recent present_plan result ---
        plan_sections: dict | None = None
        for msg in reversed(session.history):
            if msg["role"] != "tool":
                continue
            try:
                result = json.loads(msg.get("content", "{}"))
                if isinstance(result, dict) and "sections" in result:
                    plan_sections = result["sections"]
                    break
            except (json.JSONDecodeError, KeyError, TypeError):
                continue

        # Merge: edited_plan overrides plan_sections where present
        merged: dict = {}
        if plan_sections:
            merged.update(plan_sections)
        if edited_plan:
            # text_instructions comes as a single string from frontend; convert to list
            if "text_instructions" in edited_plan:
                ti = edited_plan["text_instructions"]
                if isinstance(ti, str):
                    edited_plan["text_instructions"] = [ti] if ti.strip() else []
            merged.update({k: v for k, v in edited_plan.items() if v})

        if merged:
            mapping = {
                "tables": "tables",
                "sample_questions": "sample_questions",
                "text_instructions": "text_instructions",
                "example_sqls": "example_sqls",
                "join_specs": "join_specs",
                "measures": "measures",
                "filters": "filters",
                "expressions": "expressions",
                "benchmarks": "benchmarks",
                "metric_views": "metric_views",
            }
            source = "edited_plan" if edited_plan else "plan_sections"
            for plan_key, arg_key in mapping.items():
                if arg_key not in tool_args:
                    val = merged.get(plan_key)
                    if val:
                        tool_args[arg_key] = val
                        count = len(val) if isinstance(val, list) else 1
                        injected.append(f"{arg_key}({count}|{source})")

        return injected

    @staticmethod
    def _recover_config_from_history(session: AgentSession) -> dict | None:
        """Scan session history for the most recent generate_config or update_config result
        that contains a 'config' key, and return it.

        This covers the case where session.space_config was lost (e.g. server
        restart, session restore) but the config is still in the tool results.
        """
        for msg in reversed(session.history):
            if msg["role"] != "tool":
                continue
            try:
                result = json.loads(msg.get("content", "{}"))
                if isinstance(result, dict) and "config" in result and isinstance(result["config"], dict):
                    cfg = result["config"]
                    if "tables" in cfg or "instructions" in cfg:
                        return cfg
            except (json.JSONDecodeError, KeyError, TypeError):
                continue
        return None

    def _extract_ui_hints(self, session: AgentSession) -> list[dict] | None:
        """Extract UI hint metadata from tool results in the current turn.

        Called right after session.add_message("assistant", content), so the
        latest entry in history is the current assistant response. We skip it
        and scan backward through tool results + tool-call-only assistant
        messages. We stop at the previous turn's real text response.

        Mergeable multi-select elements (catalogs, schemas, tables) accumulate
        across multiple tool calls and emit one combined UI element each.
        """
        ui_elements = []
        seen_ids: set[str] = set()
        merged_catalogs: list[dict] = []
        merged_schemas: list[dict] = []
        merged_tables: list[dict] = []
        skipped_current = False

        for msg in reversed(session.history):
            if msg["role"] == "assistant" and not skipped_current:
                skipped_current = True
                continue

            if msg["role"] == "tool":
                try:
                    result = json.loads(msg["content"])
                    if isinstance(result, dict) and "ui_hint" in result:
                        hint = dict(result["ui_hint"])

                        if hint.get("type") == "multi_select" and "catalogs" in result:
                            for c in result["catalogs"]:
                                merged_catalogs.append({
                                    "value": c["name"],
                                    "label": c["name"],
                                    "description": c.get("comment", ""),
                                })
                            continue

                        if hint.get("type") == "multi_select" and "schemas" in result:
                            for s in result["schemas"]:
                                cat = s.get("catalog_name", "")
                                full = f"{cat}.{s['name']}" if cat else s["name"]
                                merged_schemas.append({
                                    "value": full,
                                    "label": full,
                                    "description": s.get("comment", ""),
                                })
                            continue

                        if hint.get("type") == "multi_select" and "tables" in result:
                            for t in result["tables"]:
                                full = t.get("full_name", t.get("name", ""))
                                merged_tables.append({
                                    "value": full,
                                    "label": full,
                                    "description": t.get("comment", ""),
                                })
                            continue

                        hint_id = hint.get("id", "")
                        if hint_id in seen_ids:
                            continue
                        seen_ids.add(hint_id)

                        if hint.get("type") == "single_select" and "warehouses" in result:
                            hint["options"] = [
                                {"value": w["id"], "label": f"{w['name']} ({w['type']})", "description": w.get("state", "")}
                                for w in result["warehouses"]
                            ]
                        elif hint.get("type") == "config_preview" and session.space_config:
                            hint["config"] = session.space_config

                        if hint.get("options") or hint.get("config"):
                            ui_elements.append(hint)
                except (json.JSONDecodeError, KeyError):
                    pass
            elif msg["role"] == "assistant":
                has_text = bool(msg.get("content") and msg["content"].strip())
                if has_text:
                    break

        def _dedupe_and_sort(options: list[dict]) -> list[dict]:
            seen: set[str] = set()
            deduped: list[dict] = []
            for opt in reversed(options):
                if opt["value"] not in seen:
                    seen.add(opt["value"])
                    deduped.append(opt)
            deduped.reverse()
            deduped.sort(key=lambda o: o["value"])
            return deduped

        if merged_catalogs:
            ui_elements.append({
                "type": "multi_select",
                "id": "catalog_selection",
                "label": "Select catalogs",
                "options": _dedupe_and_sort(merged_catalogs),
            })
        if merged_schemas:
            ui_elements.append({
                "type": "multi_select",
                "id": "schema_selection",
                "label": "Select schemas",
                "options": _dedupe_and_sort(merged_schemas),
            })
        if merged_tables:
            ui_elements.append({
                "type": "multi_select",
                "id": "table_selection",
                "label": "Select tables to include",
                "options": _dedupe_and_sort(merged_tables),
            })

        ui_elements.reverse()
        return ui_elements if ui_elements else None


_agent: CreateGenieAgent | None = None


def get_create_agent() -> CreateGenieAgent:
    global _agent
    if _agent is None:
        _agent = CreateGenieAgent()
    return _agent
