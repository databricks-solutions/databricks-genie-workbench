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

import mlflow
from mlflow.entities import SpanType

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
        """Process a user message and stream agent events.

        Yields dicts with:
            {"event": "thinking",      "data": {"message": str}}
            {"event": "tool_call",     "data": {"tool": str, "args": dict}}
            {"event": "tool_result",   "data": {"tool": str, "result": dict}}
            {"event": "message_delta", "data": {"content": str}}
            {"event": "message",       "data": {"content": str, "ui_elements": list | None}}
            {"event": "created",       "data": {"space_id": str, "url": str}}
            {"event": "error",         "data": {"message": str}}
            {"event": "done",          "data": {}}
        """
        session.add_message("user", user_message)
        step = detect_step(session)
        step_idx = STEP_ORDER.index(step) if step in STEP_ORDER else 0

        yield {"event": "step", "data": {
            "step": step,
            "label": STEP_LABELS.get(step, step),
            "index": step_idx,
            "total": len(STEP_ORDER),
        }}
        yield {"event": "thinking", "data": {
            "message": STEP_THINKING.get(step, "Processing…"),
            "step": step,
            "round": 0,
        }}

        tools_used: list[str] = []
        error_msg: str | None = None

        try:
            for round_num in range(MAX_TOOL_ROUNDS):
                if round_num > 0:
                    yield {"event": "thinking", "data": {
                        "message": "Processing tool results…",
                        "step": step,
                        "round": round_num,
                    }}

                messages = self._build_messages(session)

                content_parts: list[str] = []
                tool_calls_acc: dict[int, dict] = {}

                with mlflow.start_span(name="llm_call", span_type=SpanType.LLM) as llm_span:
                    llm_span.set_inputs({
                        "model": self.model,
                        "message_count": len(messages),
                        "round": round_num,
                        "session_id": session.session_id,
                        "workflow_step": step,
                    })

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
                    llm_span.set_outputs({
                        "has_tool_calls": bool(tool_calls_acc),
                        "tool_count": len(tool_calls_acc),
                        "response_preview": accumulated_content[:200],
                    })

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
                            tool_args = {}

                        yield {"event": "tool_call", "data": {"tool": tool_name, "args": tool_args}}

                        with mlflow.start_span(name=f"tool:{tool_name}", span_type=SpanType.TOOL) as tool_span:
                            tool_span.set_inputs({
                                "tool": tool_name,
                                "args": tool_args,
                                "session_id": session.session_id,
                            })
                            result = await asyncio.get_event_loop().run_in_executor(
                                None, lambda n=tool_name, a=tool_args: handle_tool_call(n, a, session.space_config)
                            )
                            tool_span.set_outputs({
                                "success": "error" not in result,
                                "result_keys": list(result.keys()),
                            })

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
                    else:
                        step = new_step
                    continue

                # Text-only response — conversation turn is done
                session.add_message("assistant", accumulated_content)
                ui_elements = self._extract_ui_hints(session)
                yield {"event": "message", "data": {"content": accumulated_content, "ui_elements": ui_elements}}
                break

            else:
                error_msg = "Agent exceeded maximum tool rounds"
                yield {"event": "error", "data": {"message": error_msg}}

        except Exception as e:
            logger.exception("Create agent chat failed")
            error_msg = str(e)
            yield {"event": "error", "data": {"message": str(e)}}

        if tools_used:
            logger.info(
                "Agent turn complete: step=%s tools=%s error=%s",
                step, tools_used, error_msg,
            )

        yield {"event": "done", "data": {}}

    def _build_messages(self, session: AgentSession) -> list[dict]:
        """Build the full message list for the LLM call.

        The Databricks serving endpoint for Claude converts OpenAI-format
        messages to Anthropic-native format. We need to be careful about:

        1. Assistant messages with tool_calls: include ``content: null``
           (not empty string, which becomes an empty text block that Claude rejects)
        2. Tool result messages: include the ``name`` field (some endpoints need it)
        3. Orphaned tool_use blocks: if the user stopped the stream mid-tool-call,
           inject synthetic "cancelled" results so Claude doesn't reject the history
        """
        # First, heal any orphaned tool calls in the session history.
        # This happens when the user clicks "stop" while tools are running.
        self._heal_orphaned_tool_calls(session)

        prompt = assemble_system_prompt(session, self._get_schema_content())
        messages: list[dict] = [{"role": "system", "content": prompt}]

        # Build a lookup from tool_call_id → tool_name for annotating results
        tc_id_to_name: dict[str, str] = {}
        for msg in session.history:
            if msg["role"] == "assistant" and msg.get("tool_calls"):
                for tc in msg["tool_calls"]:
                    tc_id_to_name[tc["id"]] = tc["function"]["name"]

        for msg in session.history:
            if msg["role"] == "tool":
                tool_name = tc_id_to_name.get(msg["tool_call_id"], "unknown")
                messages.append({
                    "role": "tool",
                    "tool_call_id": msg["tool_call_id"],
                    "name": tool_name,
                    "content": msg.get("content") or "{}",
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
                # content MUST appear before tool_calls — the Databricks
                # serving endpoint mis-translates to Anthropic format
                # when tool_calls comes first in the JSON object.
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

    def _stream_llm(self, messages: list[dict]) -> Generator[dict, None, None]:
        """Stream LLM response chunks from the serving endpoint (sync).

        Uses the SDK's pre-authenticated requests.Session so auth works
        across all methods (PAT, OAuth/M2M, CLI profile).
        """
        client = get_workspace_client()
        host = (client.config.host or "").rstrip("/")

        body = {
            "messages": messages,
            "tools": TOOL_DEFINITIONS,
            "max_tokens": 4096,
            "stream": True,
        }

        url = f"{host}/serving-endpoints/{self.model}/invocations"
        logger.info(f"Streaming LLM call to {self.model} with {len(messages)} messages")
        for i, m in enumerate(messages):
            role = m.get("role", "?")
            has_tc = bool(m.get("tool_calls"))
            tc_id = m.get("tool_call_id", "")
            content_preview = str(m.get("content", ""))[:80] if m.get("content") else "(none)"
            logger.info(f"  msg[{i}] role={role} tool_calls={has_tc} tc_id={tc_id} content={content_preview!r}")
            if has_tc:
                for j, tc in enumerate(m["tool_calls"]):
                    logger.info(f"    tc[{j}] id={tc.get('id','?')} type={tc.get('type','?')} fn={tc.get('function',{}).get('name','?')} args_len={len(tc.get('function',{}).get('arguments',''))}")

        resp = client.api_client._api_client._session.post(
            url,
            json=body,
            stream=True,
            timeout=120,
        )
        try:
            if not resp.ok:
                error_body = resp.text[:500]
                logger.error("LLM endpoint returned %s: %s", resp.status_code, error_body)
                import tempfile, os
                debug_path = os.path.join(tempfile.gettempdir(), "llm_debug_body.json")
                with open(debug_path, "w") as f:
                    json.dump(body, f, indent=2, default=str)
                logger.error("Wrote failing request body to %s", debug_path)
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
