"""Create Genie agent — LLM-driven conversational workflow for building Genie spaces.

Uses a tool-calling loop: the LLM decides which tools to call and when,
guided by the system prompt (SKILL.md workflow + schema reference).
"""

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import AsyncGenerator, Generator

from backend.services.llm_utils import get_llm_model
from backend.services.auth import get_workspace_client
from backend.services.create_agent_session import AgentSession
from backend.services.create_agent_tools import TOOL_DEFINITIONS, handle_tool_call
from backend.prompts_create import assemble_system_prompt, detect_step

logger = logging.getLogger(__name__)

MAX_TOOL_ROUNDS = 15

STEP_LABELS: dict[str, str] = {
    "requirements": "Understanding requirements",
    "data_sources": "Exploring data sources",
    "inspection": "Inspecting tables",
    "plan": "Building plan",
    "config_create": "Generating configuration",
    "post_creation": "Finalizing space",
}

STEP_THINKING: dict[str, str] = {
    "requirements": "Understanding your requirements…",
    "data_sources": "Exploring your data catalog…",
    "inspection": "Analyzing table structure and data quality…",
    "plan": "Designing your Genie Space plan…",
    "config_create": "Generating the configuration…",
    "post_creation": "Finalizing your Genie Space…",
}

STEP_ORDER = [
    "requirements",
    "data_sources",
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
    ) -> AsyncGenerator[dict, None]:
        """Process a user message (or empty continuation) and stream events.

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
            session._continuation_count = 0
        else:
            cnt = getattr(session, "_continuation_count", 0) + 1
            session._continuation_count = cnt
            if cnt > MAX_TOOL_ROUNDS:
                yield {"event": "error", "data": {"message": "Agent exceeded maximum tool rounds"}}
                yield {"event": "done", "data": {"needs_continuation": False}}
                return

        step = detect_step(session)
        step_idx = STEP_ORDER.index(step) if step in STEP_ORDER else 0
        round_num = getattr(session, "_continuation_count", 0)

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

            content_parts: list[str] = []
            tool_calls_acc: dict[int, dict] = {}
            tool_call_signaled = False

            async for chunk in self._async_stream_llm(messages):
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

                    loop = asyncio.get_event_loop()
                    future = loop.run_in_executor(
                        None, lambda n=tool_name, a=tool_args: handle_tool_call(n, a, session.space_config)
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

                needs_continuation = True

            else:
                # Text-only response — conversation turn is done
                session.add_message("assistant", accumulated_content)
                ui_elements = self._extract_ui_hints(session)
                yield {"event": "message", "data": {"content": accumulated_content, "ui_elements": ui_elements}}

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

    _INSPECTION_TOOLS = frozenset({
        "describe_table", "profile_columns", "profile_table_usage",
        "assess_data_quality", "test_sql",
    })
    _POST_INSPECTION_STEPS = frozenset({"plan", "config_create", "post_creation"})

    def _build_messages(self, session: AgentSession) -> list[dict]:
        """Build the full message list for the LLM call.

        The Databricks serving endpoint for Claude converts OpenAI-format
        messages to Anthropic-native format. We need to be careful about:

        1. Assistant messages with tool_calls: include ``content: null``
           (not empty string, which becomes an empty text block that Claude rejects)
        2. Tool result messages: include the ``name`` field (some endpoints need it)
        3. Orphaned tool_use blocks: if the user stopped the stream mid-tool-call,
           inject synthetic "cancelled" results so Claude doesn't reject the history

        When the workflow has moved past inspection (plan step or later),
        old inspection tool-call/result pairs are replaced with a compact
        summary to cut context size by ~70%.
        """
        self._heal_orphaned_tool_calls(session)

        step = detect_step(session)
        compact = step in self._POST_INSPECTION_STEPS

        prompt = assemble_system_prompt(session, self._get_schema_content())
        messages: list[dict] = [{"role": "system", "content": prompt}]

        tc_id_to_name: dict[str, str] = {}
        for msg in session.history:
            if msg["role"] == "assistant" and msg.get("tool_calls"):
                for tc in msg["tool_calls"]:
                    tc_id_to_name[tc["id"]] = tc["function"]["name"]

        if compact:
            messages = self._build_compacted_messages(
                session, messages, tc_id_to_name,
            )
        else:
            messages = self._build_full_messages(
                session, messages, tc_id_to_name,
            )

        return messages

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

    def _build_compacted_messages(
        self,
        session: AgentSession,
        messages: list[dict],
        tc_id_to_name: dict[str, str],
    ) -> list[dict]:
        """Build messages with inspection-phase tool results compacted.

        Replaces verbose tool call/result pairs from profiling tools with
        a single summary message, cutting context by ~70% for post-inspection
        steps (plan, config_create, post_creation).
        """
        inspection_findings: list[str] = []
        skip_tool_ids: set[str] = set()

        # First pass: collect inspection tool results into summaries
        for msg in session.history:
            if msg["role"] == "tool":
                tool_name = tc_id_to_name.get(msg["tool_call_id"], "unknown")
                if tool_name in self._INSPECTION_TOOLS:
                    skip_tool_ids.add(msg["tool_call_id"])
                    summary = self._summarize_tool_result(tool_name, msg.get("content") or "{}")
                    if summary:
                        inspection_findings.append(summary)

        # Also mark assistant messages whose tool_calls are ALL inspection tools
        # so we can skip those too
        skip_assistant_indices: set[int] = set()
        for i, msg in enumerate(session.history):
            if msg["role"] == "assistant" and msg.get("tool_calls"):
                tc_ids = {tc["id"] for tc in msg["tool_calls"]}
                if tc_ids and tc_ids.issubset(skip_tool_ids):
                    skip_assistant_indices.add(i)

        # Inject the compacted summary after the last user message before
        # the first skipped block
        summary_injected = False

        for i, msg in enumerate(session.history):
            if msg["role"] == "tool":
                if msg["tool_call_id"] in skip_tool_ids:
                    # Inject summary right before we start skipping
                    if not summary_injected and inspection_findings:
                        summary_text = (
                            "Here is a summary of the data inspection findings:\n\n"
                            + "\n\n".join(inspection_findings)
                        )
                        messages.append({"role": "assistant", "content": summary_text})
                        summary_injected = True
                    continue
                tool_name = tc_id_to_name.get(msg["tool_call_id"], "unknown")
                raw_content = msg.get("content") or "{}"
                messages.append({
                    "role": "tool",
                    "tool_call_id": msg["tool_call_id"],
                    "name": tool_name,
                    "content": self._compress_tool_result(tool_name, raw_content),
                })
            elif i in skip_assistant_indices:
                continue
            elif msg["role"] == "assistant" and msg.get("tool_calls"):
                # Mixed: some inspection, some not — keep only non-inspection calls
                kept_tcs = [tc for tc in msg["tool_calls"] if tc["id"] not in skip_tool_ids]
                if not kept_tcs:
                    continue
                tc_content = (msg.get("content") or "").strip() or None
                normalized_tcs = []
                for tc in kept_tcs:
                    normalized_tcs.append({
                        "id": tc["id"],
                        "type": "function",
                        "function": {
                            "name": tc["function"]["name"],
                            "arguments": tc["function"].get("arguments") or "{}",
                        },
                    })
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

        # If we collected findings but never injected (edge case), append now
        if not summary_injected and inspection_findings:
            summary_text = (
                "Here is a summary of the data inspection findings:\n\n"
                + "\n\n".join(inspection_findings)
            )
            messages.append({"role": "assistant", "content": summary_text})

        return messages

    @staticmethod
    def _summarize_tool_result(tool_name: str, content: str) -> str | None:
        """Extract a compact summary from an inspection tool result."""
        try:
            data = json.loads(content)
        except (json.JSONDecodeError, TypeError):
            return None

        if tool_name == "describe_table":
            table = data.get("table_name", "?")
            cols = data.get("columns", [])
            col_names = [c.get("name", "?") for c in cols[:50]]
            row_count = data.get("row_count", "?")
            return (
                f"**{table}**: {len(cols)} columns ({', '.join(col_names)}), "
                f"~{row_count} rows"
            )

        if tool_name == "profile_columns":
            parts = []
            for col, profile in data.get("profiles", {}).items():
                dtype = profile.get("data_type", "?")
                nulls = profile.get("null_pct", "?")
                distinct = profile.get("distinct_count", "?")
                vals = profile.get("distinct_values", [])[:5]
                vals_str = ", ".join(str(v) for v in vals)
                parts.append(f"  - {col} ({dtype}): {distinct} distinct, {nulls}% null, samples=[{vals_str}]")
            if parts:
                return "Column profiles:\n" + "\n".join(parts)
            return None

        if tool_name == "profile_table_usage":
            parts = []
            for tbl, info in data.get("tables", {}).items():
                queries = info.get("recent_queries", [])
                freq = len(queries)
                users = len({q.get("executed_by") for q in queries if q.get("executed_by")})
                lin = info.get("lineage", {})
                up = len(lin.get("upstream", []))
                down = len(lin.get("downstream", []))
                parts.append(f"  - {tbl}: {freq} queries, {users} users, {up} upstream, {down} downstream")
            if parts:
                return "Usage profiles:\n" + "\n".join(parts)
            return None

        if tool_name == "assess_data_quality":
            parts = []
            for tbl, info in data.get("tables", {}).items():
                if isinstance(info, dict):
                    score = info.get("overall_score", info.get("quality_score", "?"))
                    issues = info.get("issues", [])
                    issue_str = "; ".join(str(i) for i in issues[:3]) if issues else "none"
                    parts.append(f"  - {tbl}: score={score}, issues: {issue_str}")
            if parts:
                return "Data quality:\n" + "\n".join(parts)
            return None

        if tool_name == "test_sql":
            status = "OK" if data.get("success") or "data" in data else "FAILED"
            query = data.get("query", data.get("sql", "?"))[:120]
            row_ct = len(data.get("data", []))
            cols = data.get("columns", [])
            col_str = ", ".join(str(c) for c in cols[:10]) if cols else "?"
            error = data.get("error", "")
            if error:
                return f"SQL test ({status}): `{query}` → error: {error[:200]}"
            return f"SQL test ({status}): `{query}` → {row_ct} rows, columns=[{col_str}]"

        return None

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

    _MAX_LLM_RETRIES = 4
    _RETRY_BACKOFF_BASE = 2  # seconds

    def _stream_llm(self, messages: list[dict]) -> Generator[dict, None, None]:
        """Stream LLM response chunks from the serving endpoint (sync).

        Uses the SDK's pre-authenticated requests.Session so auth works
        across all methods (PAT, OAuth/M2M, CLI profile).
        Retries automatically on 429 (rate limit) with exponential backoff.
        """
        client = get_workspace_client()
        host = (client.config.host or "").rstrip("/")

        body = {
            "messages": messages,
            "tools": TOOL_DEFINITIONS,
            "max_tokens": 16384,
            "stream": True,
        }

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

        session = client.api_client._api_client._session
        for attempt in range(self._MAX_LLM_RETRIES + 1):
            resp = session.post(url, json=body, stream=True, timeout=120)
            if resp.status_code == 429:
                resp.close()
                if attempt >= self._MAX_LLM_RETRIES:
                    logger.error("Rate-limited after %d retries, giving up", self._MAX_LLM_RETRIES)
                    raise RuntimeError("LLM endpoint rate-limited (429). Please try again in a moment.")
                retry_after = resp.headers.get("Retry-After")
                delay = float(retry_after) if retry_after else self._RETRY_BACKOFF_BASE * (2 ** attempt)
                logger.warning("429 rate-limited, retrying in %.1fs (attempt %d/%d)", delay, attempt + 1, self._MAX_LLM_RETRIES)
                time.sleep(delay)
                continue
            break

        try:
            if not resp.ok:
                error_body = resp.text[:500]
                logger.error("LLM endpoint returned %s: %s", resp.status_code, error_body)
                resp.raise_for_status()

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

    async def _async_stream_llm(self, messages: list[dict]) -> AsyncGenerator[dict, None]:
        """Async wrapper that bridges the sync streaming generator to async."""
        loop = asyncio.get_event_loop()
        gen = self._stream_llm(messages)
        _sentinel = object()

        while True:
            chunk = await loop.run_in_executor(None, lambda: next(gen, _sentinel))
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
    def _backfill_generate_config_args(session: AgentSession, tool_args: dict) -> list[str]:
        """Backfill missing generate_config arguments from session history.

        Scans for describe_table results (tables + columns) and the most
        recent present_plan result (tables, sample_questions, text_instructions,
        example_sqls, join_specs, measures, filters, expressions, benchmarks, metric_views).

        Mutates tool_args in-place and returns the list of keys that were injected.
        """
        injected: list[str] = []

        # --- Extract tables from describe_table results ---
        if "tables" not in tool_args:
            tables_by_id: dict[str, dict] = {}
            for msg in session.history:
                if msg["role"] != "tool":
                    continue
                try:
                    result = json.loads(msg.get("content", "{}"))
                    if not isinstance(result, dict):
                        continue
                    table_id = result.get("table")
                    if table_id and "columns" in result:
                        cols = []
                        for col in result["columns"]:
                            entry: dict = {"column_name": col["name"]}
                            if col.get("description"):
                                entry["description"] = col["description"]
                            cols.append(entry)
                        tables_by_id[table_id] = {
                            "identifier": table_id,
                            "description": result.get("comment") or "",
                            "column_configs": cols,
                        }
                except (json.JSONDecodeError, KeyError, TypeError):
                    continue
            if tables_by_id:
                tool_args["tables"] = list(tables_by_id.values())
                injected.append(f"tables({len(tables_by_id)})")

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

        if plan_sections:
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
            for plan_key, arg_key in mapping.items():
                if arg_key not in tool_args:
                    val = plan_sections.get(plan_key)
                    if val:
                        tool_args[arg_key] = val
                        count = len(val) if isinstance(val, list) else 1
                        injected.append(f"{arg_key}({count})")

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
