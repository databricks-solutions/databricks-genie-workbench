"""Create Genie agent — LLM-driven conversational workflow for building Genie spaces.

Uses a tool-calling loop: the LLM decides which tools to call and when,
guided by the system prompt (SKILL.md workflow + schema reference).
"""

import json
import logging
from pathlib import Path
from typing import AsyncGenerator

from backend.services.llm_utils import get_llm_model
from backend.services.auth import get_workspace_client
from backend.services.create_agent_session import AgentSession
from backend.services.create_agent_tools import TOOL_DEFINITIONS, handle_tool_call
from backend.prompts import get_create_agent_system_prompt

logger = logging.getLogger(__name__)

MAX_TOOL_ROUNDS = 15


class CreateGenieAgent:
    """Conversational agent that guides users through Genie space creation."""

    def __init__(self):
        self.model = get_llm_model()
        self._system_prompt: str | None = None

    def _get_system_prompt(self) -> str:
        if self._system_prompt is None:
            schema_path = Path(__file__).parent.parent / "references" / "schema.md"
            schema_content = schema_path.read_text()
            self._system_prompt = get_create_agent_system_prompt(schema_content)
        return self._system_prompt

    async def chat(
        self,
        session: AgentSession,
        user_message: str,
    ) -> AsyncGenerator[dict, None]:
        """Process a user message and stream agent events.

        Yields dicts with:
            {"event": "thinking",    "data": {"message": str}}
            {"event": "tool_call",   "data": {"tool": str, "args": dict}}
            {"event": "tool_result", "data": {"tool": str, "result": dict}}
            {"event": "message",     "data": {"content": str, "ui_elements": list | None}}
            {"event": "created",     "data": {"space_id": str, "url": str}}
            {"event": "error",       "data": {"message": str}}
            {"event": "done",        "data": {}}
        """
        import asyncio

        session.add_message("user", user_message)

        yield {"event": "thinking", "data": {"message": "Processing..."}}

        try:
            for _ in range(MAX_TOOL_ROUNDS):
                messages = self._build_messages(session)

                response = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: self._call_llm(messages)
                )

                choice = response["choices"][0]
                message = choice["message"]
                finish_reason = choice.get("finish_reason", "stop")

                # If the LLM wants to call tools
                # Check for tool_calls regardless of finish_reason — some
                # Claude endpoints report "tool_use" or "stop" instead of "tool_calls"
                tool_calls = message.get("tool_calls")
                if tool_calls:
                    raw_content = message.get("content")
                    # Normalize content — Claude can return a list of blocks
                    if isinstance(raw_content, list):
                        raw_content = " ".join(
                            b.get("text", "") if isinstance(b, dict) else str(b)
                            for b in raw_content
                        ).strip()

                    # Always store content as a string — None breaks some
                    # serving endpoints when the message is sent back
                    assistant_msg = {
                        "role": "assistant",
                        "content": raw_content or "",
                        "tool_calls": tool_calls,
                    }
                    session.history.append(assistant_msg)
                    session.last_active = __import__("time").time()

                    # Surface reasoning text so the user sees WHY the agent
                    # is about to call tools, not just the tool names.
                    if raw_content:
                        yield {"event": "message", "data": {"content": raw_content, "ui_elements": None}}

                    for tc in tool_calls:
                        fn = tc["function"]
                        tool_name = fn["name"]
                        try:
                            tool_args = json.loads(fn["arguments"])
                        except json.JSONDecodeError:
                            tool_args = {}

                        yield {"event": "tool_call", "data": {"tool": tool_name, "args": tool_args}}

                        result = await asyncio.get_event_loop().run_in_executor(
                            None, lambda n=tool_name, a=tool_args: handle_tool_call(n, a, session.space_config)
                        )

                        yield {"event": "tool_result", "data": {"tool": tool_name, "result": result}}

                        # Track space creation
                        if tool_name == "create_space" and result.get("success"):
                            session.space_id = result.get("space_id")
                            session.space_url = result.get("space_url")
                            yield {"event": "created", "data": {
                                "space_id": result["space_id"],
                                "url": result["space_url"],
                                "display_name": result.get("display_name", ""),
                            }}

                        # Track space update
                        if tool_name == "update_space" and result.get("success"):
                            yield {"event": "updated", "data": {
                                "space_id": result["space_id"],
                                "url": result["url"],
                            }}

                        # Track generated/updated config
                        if tool_name in ("generate_config", "update_config") and "config" in result:
                            session.space_config = result["config"]

                        session.add_tool_result(tc["id"], json.dumps(result, default=str))

                    continue  # Loop back for the LLM to process tool results

                # LLM returned a text response — conversation turn is done
                raw_content = message.get("content", "")
                if isinstance(raw_content, list):
                    raw_content = " ".join(
                        b.get("text", "") if isinstance(b, dict) else str(b)
                        for b in raw_content
                    ).strip()
                content = raw_content or ""
                session.add_message("assistant", content)

                ui_elements = self._extract_ui_hints(session)

                yield {"event": "message", "data": {"content": content, "ui_elements": ui_elements}}
                break

            else:
                yield {"event": "error", "data": {"message": "Agent exceeded maximum tool rounds"}}

        except Exception as e:
            logger.exception("Create agent chat failed")
            yield {"event": "error", "data": {"message": str(e)}}

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

        messages: list[dict] = [{"role": "system", "content": self._get_system_prompt()}]

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
                tc_content = (msg.get("content") or "").strip()
                messages.append({
                    "role": "assistant",
                    "content": tc_content if tc_content else None,
                    "tool_calls": msg["tool_calls"],
                })
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

    def _call_llm(self, messages: list[dict]) -> dict:
        """Call the LLM serving endpoint with tools."""
        client = get_workspace_client()

        body = {
            "messages": messages,
            "tools": TOOL_DEFINITIONS,
            "max_tokens": 4096,
        }

        logger.info(f"Calling LLM with {len(messages)} messages and {len(TOOL_DEFINITIONS)} tools")
        for i, m in enumerate(messages):
            role = m["role"]
            content = m.get("content", "")
            if isinstance(content, list):
                types = [b.get("type", "?") for b in content]
                logger.info(f"  msg[{i}] role={role} content_blocks={types}")
            else:
                logger.info(f"  msg[{i}] role={role} content='{str(content)[:60]}'")

        response = client.api_client.do(
            method="POST",
            path=f"/serving-endpoints/{self.model}/invocations",
            body=body,
        )

        if not isinstance(response, dict) or "choices" not in response:
            raise ValueError(f"Unexpected LLM response: {type(response)}")

        return response

    def _extract_ui_hints(self, session: AgentSession) -> list[dict] | None:
        """Extract UI hint metadata from tool results in the current turn.

        Called right after session.add_message("assistant", content), so the
        latest entry in history is the current assistant response. We skip it
        and scan backward through tool results + tool-call-only assistant
        messages. We stop at the previous turn's real text response.
        """
        ui_elements = []
        seen_ids: set[str] = set()
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
                        hint_id = hint.get("id", "")
                        if hint_id in seen_ids:
                            continue
                        seen_ids.add(hint_id)

                        if hint.get("type") == "multi_select" and "tables" in result:
                            hint["options"] = [
                                {
                                    "value": t.get("full_name", t.get("name", "")),
                                    "label": t.get("name", ""),
                                    "description": t.get("comment", ""),
                                }
                                for t in result["tables"]
                            ]
                        elif hint.get("type") == "single_select":
                            if "catalogs" in result:
                                hint["options"] = [
                                    {"value": c["name"], "label": c["name"], "description": c.get("comment", "")}
                                    for c in result["catalogs"]
                                ]
                            elif "schemas" in result:
                                hint["options"] = [
                                    {"value": s["name"], "label": s["name"], "description": s.get("comment", "")}
                                    for s in result["schemas"]
                                ]
                            elif "warehouses" in result:
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

        ui_elements.reverse()
        return ui_elements if ui_elements else None


_agent: CreateGenieAgent | None = None


def get_create_agent() -> CreateGenieAgent:
    global _agent
    if _agent is None:
        _agent = CreateGenieAgent()
    return _agent
