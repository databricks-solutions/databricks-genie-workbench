# AI Gateway Migration — Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Route the three *chat* LLM call sites (llm_utils, create_agent streaming, GSO `call_llm`) through the Phase 0 seam so gateway traffic carries the cost-attribution tag — default-off, byte-identical on `classic`.

**Architecture:** Each call site stops hard-coding the classic `/serving-endpoints/{model}/invocations` URL and instead asks the already-shipped seam (`llm_route.resolve_chat` / helpers) for `(url, model, extra_headers)`, keeping its own transport (httpx / SDK requests.Session / OpenAI SDK). Under `GENIE_LLM_ROUTE=classic` (the default) the seam returns today's exact values, so behavior is unchanged; under `gateway` the calls hit `/ai-gateway/mlflow/v1/chat/completions`, map the model to `system.ai.*`, and attach the `Databricks-Ai-Gateway-Request-Tags` header.

**Tech Stack:** Python 3.12, FastAPI, httpx, Databricks SDK (`requests.Session`), OpenAI SDK; pytest (`./scripts/test.sh`, i.e. `uv run --frozen --extra dev pytest`).

**Spec:** `scripts/ai-gateway-validation/ai-gateway-migration-spec.md` — §1 (the seam & five sites), §1.3 (the tag), §2 (`reasoning_effort` retry), §6 Phase 1, §9 (Decision 1: scoped extras), Appendix A.2 (per-site adoption deltas). Executors read the spec **and** this plan; where they differ, the recorded Rulings below win and the spec is the binding authority for everything else.

## Global Constraints

- **Default-off / fail-safe.** `GENIE_LLM_ROUTE` (env) selects the route; anything but the exact string `gateway` → `CLASSIC` (the Phase 0 seam already guarantees this). With `classic`, every site in this plan MUST behave exactly as today: same URL (`{host}/serving-endpoints/{model}/invocations`), **no** tag header, **no** `model` key added to the request body.
- **The seam is NOT modified in Phase 1.** `backend/services/llm_route.py` and `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/llm_route.py` (byte-identical twins from Phase 0) are consumed, never edited. `backend/tests/test_llm_route_parity.py` MUST stay green untouched.
- **Tag (§1.3, Decision 1).** Header key exactly `Databricks-Ai-Gateway-Request-Tags`; base value `{"application":"genie-workbench","component":<feature>}`; scoped extras `run_id` (GSO job calls) / `space_id` (create-agent) attached only when present (the seam's `request_tags` drops empties). Build headers only via the seam (`resolve_chat(...).extra_headers` or `tag_header(...)`), never a hand-written literal.
- **Component vocabulary (this phase only):** `create-agent` (create_agent streaming + repair call), `plan-builder` (plan_builder), `gso-optimize` (GSO `call_llm`). The site-1 default is `workbench`. (`iq-scan`, `mv-suggest`, `leakage-embed` belong to later phases — do not add them here.)
- **Model mapping = seam only.** Under gateway, map via `gateway_model_name` (curated `databricks-*` → `system.ai.*`). **No** `model_catalog` / BYOK / curated-list change (that is Phase 2). **No** embeddings change (site 4 = Phase 1b).
- **No new dependency.** Use existing `httpx`, `openai`, Databricks SDK, stdlib. Do not touch `pyproject.toml` / `uv.lock` / `requirements.txt`.
- **`run_id` source = env, not a new param.** GSO job notebooks already export `os.environ["GSO_RUN_ID"] = run_id` (`jobs/run_intake_and_snapshot.py:160`, `jobs/run_benchmark_qc_and_repair.py:160`). Site 3 reads `os.getenv("GSO_RUN_ID")` — do **not** thread a `run_id` parameter through `call_llm`'s ~7 callers.
- **Test invocation.** Always `./scripts/test.sh <paths>` (runs `uv run --frozen --extra dev pytest`). A bare `pytest` omits `pytest-asyncio` and fails 12 unrelated async backend tests — an invocation defect, never a code defect.
- **Baseline.** Post-Phase-0 the full suite is **2751 passed** (1218 backend + 1533 GSO). Phase 1 only *adds* tests + edits call sites; the count rises — a count below 2751 is a regression to investigate. **Do NOT edit the mv-advisor rule or playbook baseline line:** `test_rules_parity.py` pins that the two mv-rule copies match *each other*, not the suite count; this AI-Gateway work rides no doc-freeze (spec preamble) and is not an mv-advisor change. Track expected counts in this plan only.

### Recorded Rulings (deviations from the spec's literal Appendix A.2 — carry into the relevant task's dispatch)

- **R1 — `reasoning_effort` retry lands ONLY at site 2 (the tool path).** Appendix A.2 also asks for it at GSO `call_llm` (`:200`), but `call_llm` sends **no function tools** (no `tools` param; GSO uses `response_format` JSON mode), so the `reasoning_effort` 400 is unreachable there and the retry would be untestable dead code (YAGNI). Implement it at site 2, where tools are sent and it is testable. *If wrong:* a future GSO tool-calling path via `call_llm` would 400 on a reasoning model until the retry is added there — flagged here, revisit when/if `call_llm` grows a `tools` param.
- **R2 — site 3 tags attach per-request, not on the cached client.** Appendix A.2 says `default_headers=tag_header(...)` on `OpenAI(...)`, but `get_openai_client` caches the client by host (`llm_client.py:126`). Attach the tag per-call via the OpenAI SDK's `extra_headers=` on `chat.completions.create(...)` instead, and key the client cache by `(host, route)` since only `base_url` is route-dependent (and route is process-stable). *If wrong:* nothing functional — tags still land; this only avoids baking a stale header into a shared client.

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `backend/services/llm_utils.py` | Non-streaming Workbench chat (`call_serving_endpoint`) | Route via seam; add `component` param |
| `backend/services/plan_builder.py` | Plan-generation caller of site 1 | Pass `component="plan-builder"` |
| `backend/services/create_agent.py` | Create-agent: streaming (`_stream_llm`/`_async_stream_llm`) + repair caller of site 1 | Route streaming via seam + tag + `space_id`; `reasoning_effort` retry; pass `component="create-agent"` at the repair call |
| `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/llm_client.py` | GSO OpenAI chat (`get_openai_client` + `call_llm`) | Route-aware `base_url`; model map + per-request tag header |
| `backend/tests/test_llm_utils.py` | Site 1 tests | Add classic/gateway routing tests |
| `backend/tests/test_create_agent_gateway.py` (new) | Site 2 tests | Route/tag/`space_id`, terminators, `reasoning_effort` retry |
| `packages/genie-space-optimizer/tests/unit/test_llm_client_gateway.py` (new) | Site 3 tests | Route base_url, model map, per-request tag header |

**Consumed seam surface (unchanged, from Phase 0):** `resolve_chat(host, endpoint, component, *, route=None, run_id=None, space_id=None) -> ResolvedCall(url, model, extra_headers, use_legacy_sdk)`, `gateway_model_name(endpoint) -> str`, `tag_header(component, **scope) -> dict`, `is_reasoning_effort_400(status, body_text) -> bool`, `get_llm_route() -> LLMRoute`, `LLMRoute.{CLASSIC,GATEWAY}`.

---

## Task 1: Site 1 — `call_serving_endpoint` routes through the seam (+ component)

**Files:**
- Modify: `backend/services/llm_utils.py` (import; `call_serving_endpoint` signature `:52-56`; URL/body/headers `:80-97`)
- Modify: `backend/services/plan_builder.py:178` (caller — pass component)
- Modify: `backend/services/create_agent.py:703` (repair caller — pass component)
- Test: `backend/tests/test_llm_utils.py`

**Interfaces:**
- Consumes: seam `resolve_chat` (backend copy).
- Produces: `call_serving_endpoint(messages, model=None, max_tokens=None, timeout=600, component="workbench")` — later callers/phases pass a component; default keeps existing callers working.

- [ ] **Step 1: Write the failing tests** (append to `backend/tests/test_llm_utils.py`)

```python
def _capture_post(monkeypatch, status=200):
    """Stub get_workspace_client + httpx.post; return the captured-call list."""
    captured = []
    client = SimpleNamespace(
        config=SimpleNamespace(
            host="https://example.databricks.com/",
            authenticate=lambda: {"Authorization": "Bearer test"},
        )
    )
    response = SimpleNamespace(
        status_code=status,
        text="",
        json=lambda: {"choices": [{"message": {"content": "ok"}}]},
    )

    def fake_post(url, json=None, headers=None, timeout=None):
        captured.append(SimpleNamespace(url=url, json=json, headers=headers))
        return response

    monkeypatch.setattr(llm_utils, "get_workspace_client", lambda: client)
    monkeypatch.setattr(llm_utils.httpx, "post", fake_post)
    return captured


def test_call_serving_endpoint_classic_is_unchanged(monkeypatch):
    monkeypatch.delenv("GENIE_LLM_ROUTE", raising=False)
    captured = _capture_post(monkeypatch)
    llm_utils.call_serving_endpoint(
        [{"role": "user", "content": "hi"}], model="databricks-claude-sonnet-4-6"
    )
    call = captured[0]
    assert call.url == "https://example.databricks.com/serving-endpoints/databricks-claude-sonnet-4-6/invocations"
    assert "model" not in call.json                       # model stays in the URL
    assert "Databricks-Ai-Gateway-Request-Tags" not in call.headers


def test_call_serving_endpoint_gateway_tags_and_maps(monkeypatch):
    monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")
    captured = _capture_post(monkeypatch)
    llm_utils.call_serving_endpoint(
        [{"role": "user", "content": "hi"}],
        model="databricks-claude-sonnet-4-6",
        component="plan-builder",
    )
    call = captured[0]
    assert call.url == "https://example.databricks.com/ai-gateway/mlflow/v1/chat/completions"
    assert call.json["model"] == "system.ai.claude-sonnet-4-6"   # mapped, in the body
    tags = json.loads(call.headers["Databricks-Ai-Gateway-Request-Tags"])
    assert tags == {"application": "genie-workbench", "component": "plan-builder"}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./scripts/test.sh backend/tests/test_llm_utils.py -k "classic_is_unchanged or gateway_tags_and_maps" -v`
Expected: FAIL — `call_serving_endpoint()` has no `component` kwarg / gateway assertions fail (URL still `/invocations`).

- [ ] **Step 3: Route `call_serving_endpoint` through the seam**

Add the import near the top of `backend/services/llm_utils.py` (after `from backend.services.auth import get_workspace_client`):

```python
from backend.services.llm_route import resolve_chat
```

Change the signature (`:52-56`) to add the param:

```python
def call_serving_endpoint(
    messages: list[dict],
    model: str | None = None,
    max_tokens: int | None = None,
    timeout: float = 600,
    component: str = "workbench",
) -> str:
```

Replace the URL/body construction (`:86-89`) and the `httpx.post` headers arg (`:97`):

```python
    auth_headers = client.config.authenticate()

    rc = resolve_chat(host, model, component)
    url = rc.url
    body: dict = {"messages": messages}
    if rc.model is not None:
        body["model"] = rc.model          # gateway puts the model in the body
    if max_tokens is not None:
        body["max_tokens"] = max_tokens
    headers = {**auth_headers, **rc.extra_headers}
```

```python
        resp = httpx.post(
            url,
            json=body,
            headers=headers,
            timeout=timeout,
        )
```

- [ ] **Step 4: Update the two site-1 callers**

`backend/services/plan_builder.py:178` — add the component:

```python
        response = call_serving_endpoint(
            # ...existing messages/model/max_tokens args unchanged...
            component="plan-builder",
        )
```

`backend/services/create_agent.py:703` — add the component:

```python
            response = call_serving_endpoint(
                [{"role": "user", "content": prompt}],
                model=model or get_llm_model(),
                max_tokens=16000,
                component="create-agent",
            )
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `./scripts/test.sh backend/tests/test_llm_utils.py -v`
Expected: PASS (new tests green; the existing `test_call_serving_endpoint_normalizes_structured_content` still green — default route `classic`, default component `workbench`).

- [ ] **Step 6: Commit**

```bash
git add backend/services/llm_utils.py backend/services/plan_builder.py backend/services/create_agent.py backend/tests/test_llm_utils.py
git commit -m "feat(ai-gateway): Phase 1 site 1 — route call_serving_endpoint through the seam (+component tag)"
```

---

## Task 2: Site 2 — create_agent streaming routes through the seam (+ tag + space_id)

**Files:**
- Modify: `backend/services/create_agent.py` — `_stream_llm` (`:979-1055`, URL `:1006`, `session.post` `:1022`), `_async_stream_llm` (`:1057-1078`, forward `:1071`), caller (`:204`)
- Test: `backend/tests/test_create_agent_gateway.py` (new)

**Interfaces:**
- Consumes: seam `resolve_chat` (backend copy); Task 1's import already present in the module tree (import independently in this file).
- Produces: `_stream_llm(messages, tools=None, model=None, space_id=None)` and `_async_stream_llm(messages, tools=None, model=None, space_id=None)` — Task 3 extends `_stream_llm`'s post loop.

- [ ] **Step 1: Write the failing tests** (new file `backend/tests/test_create_agent_gateway.py`)

```python
"""Site-2 (create_agent streaming) AI-Gateway routing tests."""
import json
from types import SimpleNamespace

from backend.services import create_agent


class _Resp:
    def __init__(self, status_code=200, lines=None, text=""):
        self.status_code = status_code
        self.ok = status_code == 200
        self.headers = {}
        self.text = text
        self.encoding = None
        self._lines = lines or []
        self.closed = False

    def iter_lines(self, decode_unicode=True):
        yield from self._lines

    def close(self):
        self.closed = True


class _Session:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def post(self, url, json=None, stream=None, timeout=None, headers=None):
        self.calls.append(SimpleNamespace(url=url, json=json, headers=headers))
        return self._responses.pop(0)


def _agent_with_session(monkeypatch, responses):
    session = _Session(responses)
    client = SimpleNamespace(
        config=SimpleNamespace(host="https://example.databricks.com/"),
        api_client=SimpleNamespace(_api_client=SimpleNamespace(_session=session)),
    )
    monkeypatch.setattr(create_agent, "get_workspace_client", lambda: client)
    return create_agent.CreateGenieAgent(), session


_TOOLS = [{"type": "function", "function": {"name": "noop"}}]
_LINE = 'data: {"choices":[{"delta":{"content":"hi"}}]}'


def test_stream_classic_is_unchanged(monkeypatch):
    monkeypatch.delenv("GENIE_LLM_ROUTE", raising=False)
    agent, session = _agent_with_session(monkeypatch, [_Resp(lines=[_LINE, "data: [DONE]"])])
    list(agent._stream_llm([{"role": "user", "content": "x"}], tools=_TOOLS,
                           model="databricks-claude-sonnet-4-6", space_id="sp1"))
    call = session.calls[0]
    assert call.url == "https://example.databricks.com/serving-endpoints/databricks-claude-sonnet-4-6/invocations"
    assert "model" not in call.json
    assert not call.headers or "Databricks-Ai-Gateway-Request-Tags" not in call.headers


def test_stream_gateway_tags_maps_and_scopes_space(monkeypatch):
    monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")
    agent, session = _agent_with_session(monkeypatch, [_Resp(lines=[_LINE, "data: [DONE]"])])
    list(agent._stream_llm([{"role": "user", "content": "x"}], tools=_TOOLS,
                           model="databricks-claude-sonnet-4-6", space_id="sp1"))
    call = session.calls[0]
    assert call.url == "https://example.databricks.com/ai-gateway/mlflow/v1/chat/completions"
    assert call.json["model"] == "system.ai.claude-sonnet-4-6"
    tags = json.loads(call.headers["Databricks-Ai-Gateway-Request-Tags"])
    assert tags == {"application": "genie-workbench", "component": "create-agent", "space_id": "sp1"}


def test_stream_terminates_on_done(monkeypatch):
    monkeypatch.delenv("GENIE_LLM_ROUTE", raising=False)
    agent, _ = _agent_with_session(monkeypatch, [_Resp(lines=[
        _LINE, "data: [DONE]", 'data: {"choices":[{"delta":{"content":"AFTER"}}]}'])])
    chunks = list(agent._stream_llm([{"role": "user", "content": "x"}], tools=_TOOLS,
                                    model="databricks-claude-sonnet-4-6"))
    assert len(chunks) == 1                                   # stops at [DONE]


def test_stream_terminates_on_iterlines_end_without_done(monkeypatch):
    """GPT-style ending: no [DONE], finish_reason line then the stream simply ends."""
    monkeypatch.delenv("GENIE_LLM_ROUTE", raising=False)
    fr = 'data: {"choices":[{"delta":{"content":"hi"},"finish_reason":"stop"}]}'
    agent, _ = _agent_with_session(monkeypatch, [_Resp(lines=[fr])])
    chunks = list(agent._stream_llm([{"role": "user", "content": "x"}], tools=_TOOLS,
                                    model="databricks-claude-sonnet-4-6"))
    assert len(chunks) == 1 and chunks[0]["choices"][0]["finish_reason"] == "stop"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./scripts/test.sh backend/tests/test_create_agent_gateway.py -v`
Expected: FAIL — `_stream_llm()` has no `space_id` kwarg; gateway URL/model/tag assertions fail.

- [ ] **Step 3: Route `_stream_llm` through the seam**

Add the import near the top of `backend/services/create_agent.py` (with the other `backend.services` imports):

```python
from backend.services.llm_route import resolve_chat, is_reasoning_effort_400
```

Change `_stream_llm`'s signature (`:979-984`) to accept `space_id`:

```python
    def _stream_llm(
        self,
        messages: list[dict],
        tools: list[dict] | None = None,
        model: str | None = None,
        space_id: str | None = None,
    ) -> Generator[dict, None, None]:
```

Replace the URL construction (`:1005-1006`) with the seam call, and set the model in the body:

```python
        effective_model = model or get_llm_model()
        rc = resolve_chat(host, effective_model, "create-agent", space_id=space_id)
        url = rc.url
        if rc.model is not None:
            body["model"] = rc.model
        logger.info("Streaming LLM call to %s with %d messages", effective_model, len(messages))
```

Attach the tag header on the POST (`:1022`) — pass `headers=rc.extra_headers` (the SDK `requests.Session` merges per-request headers over its pre-auth headers, so this adds the tag without dropping auth):

```python
            resp = session.post(url, json=body, stream=True, timeout=120, headers=rc.extra_headers)
```

> The `[DONE]` break (`:1048`) and the `iter_lines` end both stay — no parser change; the terminator tests pin both.

- [ ] **Step 4: Forward `space_id` through `_async_stream_llm` and the caller**

`_async_stream_llm` signature (`:1057-1062`) + forward (`:1071`):

```python
    async def _async_stream_llm(
        self,
        messages: list[dict],
        tools: list[dict] | None = None,
        model: str | None = None,
        space_id: str | None = None,
    ) -> AsyncGenerator[dict, None]:
```

```python
        gen = self._stream_llm(messages, tools=tools, model=model, space_id=space_id)
```

Caller at `:204` — pass the session's space id (it is in scope; the same method already calls `self._build_messages(session)` / `self._effective_model(session)`):

```python
            async for chunk in self._async_stream_llm(
                messages, tools=step_tool_defs, model=effective_model, space_id=session.space_id
            ):
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `./scripts/test.sh backend/tests/test_create_agent_gateway.py -v`
Expected: PASS (4 tests). Also run the existing streaming suite to confirm no regression: `./scripts/test.sh backend/tests/test_create_agent_streaming.py backend/tests/test_create_agent_hook.py -v` (they stub `_async_stream_llm`, so the new kwarg is transparent).

- [ ] **Step 6: Commit**

```bash
git add backend/services/create_agent.py backend/tests/test_create_agent_gateway.py
git commit -m "feat(ai-gateway): Phase 1 site 2 — route create_agent streaming through the seam (+space-scoped tag)"
```

---

## Task 3: Site 2 — `reasoning_effort` one-shot retry on the tool path (§2, R1)

**Files:**
- Modify: `backend/services/create_agent.py` — `_stream_llm` post loop (between the 429 loop end `:1033` and the `if not resp.ok` raise `:1036`)
- Test: `backend/tests/test_create_agent_gateway.py` (extend)

**Interfaces:**
- Consumes: Task 2's `_stream_llm` (post loop + `is_reasoning_effort_400`, already imported in Task 2).
- Produces: nothing new — behavior only (one extra POST on a `reasoning_effort` 400).

- [ ] **Step 1: Write the failing tests** (append to `backend/tests/test_create_agent_gateway.py`)

```python
def test_reasoning_model_retries_once_with_none(monkeypatch):
    """First plain attempt 400s on reasoning_effort; retry adds reasoning_effort='none' and succeeds."""
    monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")
    bad = _Resp(status_code=400, text='{"error":"reasoning_effort is required"}')
    good = _Resp(lines=[_LINE, "data: [DONE]"])
    agent, session = _agent_with_session(monkeypatch, [bad, good])
    chunks = list(agent._stream_llm([{"role": "user", "content": "x"}], tools=_TOOLS,
                                    model="system.ai.gpt-5-6-sol"))
    assert len(session.calls) == 2
    assert "reasoning_effort" not in session.calls[0].json          # first attempt is plain
    assert session.calls[1].json["reasoning_effort"] == "none"      # retry sends the flag
    assert len(chunks) == 1


def test_claude_style_200_does_not_add_flag(monkeypatch):
    """A model that accepts tools plainly (200) never gets reasoning_effort added."""
    monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")
    agent, session = _agent_with_session(monkeypatch, [_Resp(lines=[_LINE, "data: [DONE]"])])
    list(agent._stream_llm([{"role": "user", "content": "x"}], tools=_TOOLS,
                           model="databricks-claude-sonnet-4-6"))
    assert len(session.calls) == 1
    assert "reasoning_effort" not in session.calls[0].json
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./scripts/test.sh backend/tests/test_create_agent_gateway.py -k "reasoning_model_retries or claude_style_200" -v`
Expected: FAIL — only one POST is made; no retry logic yet (the 400 falls straight through to the `not resp.ok` raise).

- [ ] **Step 3: Insert the one-shot retry**

In `_stream_llm`, immediately after the 429 retry `for` loop ends (`:1033`, right before the `try:` at `:1035`):

```python
        # reasoning_effort retry (§2): reasoning-backed models 400 on tool calls unless
        # reasoning_effort='none' is sent; non-reasoning models 400 if it IS sent. So send
        # plain first, and retry once only when the 400 body names reasoning_effort.
        if is_reasoning_effort_400(resp.status_code, resp.text):
            resp.close()
            body["reasoning_effort"] = "none"
            resp = session.post(url, json=body, stream=True, timeout=120, headers=rc.extra_headers)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `./scripts/test.sh backend/tests/test_create_agent_gateway.py -v`
Expected: PASS (all 6 tests). A `reasoning_effort` 400 triggers exactly one retry; a 200 makes exactly one POST.

- [ ] **Step 5: Commit**

```bash
git add backend/services/create_agent.py backend/tests/test_create_agent_gateway.py
git commit -m "feat(ai-gateway): Phase 1 site 2 — one-shot reasoning_effort retry on the tool path"
```

---

## Task 4: Site 3 — GSO `call_llm` routes through the seam (+ per-request tag, R2)

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/llm_client.py` — imports (`:19-23`), `get_openai_client` (`:110-133`), `call_llm` model + kwargs (`:169-181`)
- Test: `packages/genie-space-optimizer/tests/unit/test_llm_client_gateway.py` (new)

**Interfaces:**
- Consumes: seam `get_llm_route`, `LLMRoute`, `gateway_model_name`, `tag_header` (GSO copy); `os.getenv("GSO_RUN_ID")`.
- Produces: route-aware `get_openai_client` (cache key `(host, route)`); `call_llm` maps the model + attaches `extra_headers` on gateway.

- [ ] **Step 1: Write the failing tests** (new file `packages/genie-space-optimizer/tests/unit/test_llm_client_gateway.py`)

```python
"""Site-3 (GSO call_llm) AI-Gateway routing tests."""
from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from genie_space_optimizer.optimization import llm_client


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    llm_client._openai_client_cache.clear()
    monkeypatch.setattr(llm_client, "get_llm_endpoint", lambda: "databricks-claude-sonnet-4-6")
    yield
    llm_client._openai_client_cache.clear()


def _fake_wc():
    return SimpleNamespace(config=SimpleNamespace(
        host="https://example.databricks.com/", token="tok",
        authenticate=lambda: {"Authorization": "Bearer tok"}))


def _fake_openai_client(monkeypatch):
    completions = MagicMock()
    completions.create.return_value = SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content="ok"))])
    client = SimpleNamespace(chat=SimpleNamespace(completions=completions), api_key="tok")
    monkeypatch.setattr(llm_client, "get_openai_client", lambda w: client)
    return completions


def test_get_openai_client_base_url_by_route(monkeypatch):
    import openai
    seen = {}

    class FakeOpenAI:
        def __init__(self, api_key=None, base_url=None):
            seen["base_url"] = base_url
            self.api_key = api_key

    monkeypatch.setattr(openai, "OpenAI", FakeOpenAI)
    monkeypatch.setattr(llm_client, "_resolve_bearer_token", lambda wc: "tok")

    monkeypatch.delenv("GENIE_LLM_ROUTE", raising=False)
    llm_client.get_openai_client(_fake_wc())
    assert seen["base_url"] == "https://example.databricks.com/serving-endpoints"

    monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")
    llm_client.get_openai_client(_fake_wc())
    assert seen["base_url"] == "https://example.databricks.com/ai-gateway/mlflow/v1"


def test_call_llm_classic_no_headers_unmapped_model(monkeypatch):
    monkeypatch.delenv("GENIE_LLM_ROUTE", raising=False)
    completions = _fake_openai_client(monkeypatch)
    llm_client.call_llm(_fake_wc(), messages=[{"role": "user", "content": "hi"}])
    kwargs = completions.create.call_args.kwargs
    assert kwargs["model"] == "databricks-claude-sonnet-4-6"
    assert "extra_headers" not in kwargs


def test_call_llm_gateway_maps_model_and_tags_with_run_id(monkeypatch):
    monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")
    monkeypatch.setenv("GSO_RUN_ID", "run-42")
    completions = _fake_openai_client(monkeypatch)
    llm_client.call_llm(_fake_wc(), messages=[{"role": "user", "content": "hi"}])
    kwargs = completions.create.call_args.kwargs
    assert kwargs["model"] == "system.ai.claude-sonnet-4-6"
    tags = json.loads(kwargs["extra_headers"]["Databricks-Ai-Gateway-Request-Tags"])
    assert tags == {"application": "genie-workbench", "component": "gso-optimize", "run_id": "run-42"}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./scripts/test.sh packages/genie-space-optimizer/tests/unit/test_llm_client_gateway.py -v`
Expected: FAIL — gateway `base_url` still `/serving-endpoints`; `call_llm` neither maps the model nor sends `extra_headers`.

- [ ] **Step 3: Add the seam import**

Extend the import block in `llm_client.py` (after the `common.config` import, `:23`):

```python
from genie_space_optimizer.optimization.llm_route import (
    LLMRoute,
    gateway_model_name,
    get_llm_route,
    tag_header,
)
```

- [ ] **Step 4: Make `get_openai_client` route-aware**

Replace the cache/construct block (`:126-133`):

```python
    route = get_llm_route()
    base_url = (
        f"{host}/ai-gateway/mlflow/v1" if route is LLMRoute.GATEWAY
        else f"{host}/serving-endpoints"
    )
    cache_key = f"{host}|{route.value}"
    if cache_key not in _openai_client_cache:
        _openai_client_cache[cache_key] = OpenAI(api_key=token, base_url=base_url)
    else:
        _openai_client_cache[cache_key].api_key = token
    return _openai_client_cache[cache_key]
```

- [ ] **Step 5: Map the model + attach the tag in `call_llm`**

After `model = get_llm_endpoint()` (`:170`), map on gateway; and add `extra_headers` when building `call_kwargs` (`:172-176`):

```python
    client = get_openai_client(w)
    model = get_llm_endpoint()
    route = get_llm_route()
    if route is LLMRoute.GATEWAY:
        model = gateway_model_name(model)

    call_kwargs: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "timeout": eval_llm_timeout_seconds(),
    }
    if route is LLMRoute.GATEWAY:
        call_kwargs["extra_headers"] = tag_header(
            "gso-optimize", run_id=os.getenv("GSO_RUN_ID") or None
        )
```

> Per R1: do **not** add a `reasoning_effort` retry here — `call_llm` sends no tools. The existing `response_format` strip-retry (`:200`) is unchanged.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `./scripts/test.sh packages/genie-space-optimizer/tests/unit/test_llm_client_gateway.py packages/genie-space-optimizer/tests/unit/test_llm_client_timeout.py -v`
Expected: PASS (new gateway tests + the existing timeout/normalization tests, which run default `classic` and are unaffected).

- [ ] **Step 7: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/llm_client.py packages/genie-space-optimizer/tests/unit/test_llm_client_gateway.py
git commit -m "feat(ai-gateway): Phase 1 site 3 — route GSO call_llm through the seam (+run-scoped tag)"
```

---

## Task 5: Contract grep + full-suite verification

**Files:** none (verification only — no commit unless a gap is found).

**Interfaces:** Consumes Tasks 1–4.

- [ ] **Step 1: Full suite is green and grew**

Run: `./scripts/test.sh`
Expected: PASS, count ≥ 2751 + the new tests (2 site-1 + 6 site-2 + 3 site-3 = 11 → ~2762). Zero failures. `backend/tests/test_llm_route_parity.py` green (seam untouched).

- [ ] **Step 2: Contract grep (a) — no chat site hard-codes `/invocations` outside the seam**

Run:
```bash
rg -n "/serving-endpoints/.*\{.*\}/invocations|/invocations" backend/services/llm_utils.py backend/services/create_agent.py
```
Expected: **no matches** in either call-site file (the only remaining `/invocations` literals live in `llm_route.py`'s classic branch + tests). Paste the (empty) result into the PR.

- [ ] **Step 3: Contract grep (b) — every gateway chat site tags via the seam**

Run:
```bash
rg -n "resolve_chat|tag_header|extra_headers" backend/services/llm_utils.py backend/services/create_agent.py packages/genie-space-optimizer/src/genie_space_optimizer/optimization/llm_client.py
```
Expected: site 1 + site 2 show `resolve_chat(...)` + `extra_headers`; site 3 shows `tag_header(...)` + `extra_headers`. No call site builds the header string by hand.

- [ ] **Step 4: Seam + deps untouched**

Run:
```bash
git diff --name-only <phase1-base>..HEAD -- backend/services/llm_route.py packages/genie-space-optimizer/src/genie_space_optimizer/optimization/llm_route.py pyproject.toml uv.lock requirements.txt
```
Expected: **empty** — the seam twins and dependency files are unchanged (parity + no-new-dep constraints hold).

- [ ] **Step 5: Record results** in the PR / run report (suite count delta, both greps, the empty seam/dep diff). No code change; nothing to commit.

---

## Self-Review (run against the spec)

**1. Spec coverage (§6 Phase 1 + Appendix A.2):**
- Site 1 `llm_utils.py` → Task 1 ✅ (component param, `resolve_chat`, model-in-body, header merge).
- Site 2 `create_agent._stream_llm` → Tasks 2–3 ✅ (route+tag, `space_id` scope, `[DONE]`+`finish_reason` terminators pinned, `reasoning_effort` retry).
- Site 3 GSO `get_openai_client`+`call_llm` → Task 4 ✅ (route base_url, model map, run-scoped tag).
- Regression guards: default-off classic-unchanged tests at every site ✅; streaming terminator test ✅; `reasoning_effort` retry test (rejecting + requiring models) ✅; contract grep ✅ (Task 5). Probe-suite re-run is the deploy-time integration gate (spec §8) — out of scope for these offline unit tasks, noted for the PR.
- Deliberately deferred (documented): embeddings/site 4 = Phase 1b; `model_catalog`/BYOK/site 5 = Phase 2; 404-downgrade = Phase 3.

**2. Placeholder scan:** every code/test step carries real code; every anchor is a fresh file:line verified this session; no "TBD"/"similar to"/"add error handling". ✅

**3. Type consistency:** `resolve_chat(host, endpoint, component, *, space_id=…)` and `ResolvedCall(.url/.model/.extra_headers)` used identically across Tasks 1–2; `gateway_model_name`/`tag_header`/`get_llm_route`/`LLMRoute`/`is_reasoning_effort_400` match the Phase-0 seam signatures (Appendix A.1); `_stream_llm`/`_async_stream_llm` gain the same `space_id: str | None = None` and forward it consistently. ✅

---

## Execution Handoff

Plan complete and saved to `scripts/ai-gateway-validation/ai-gateway-phase1-plan.md`. Two execution options:

1. **Subagent-Driven (recommended)** — fresh subagent per task, task review (spec + quality) between tasks, broad review at the end. Task 3 depends on Task 2 (same function); Tasks 1/4 are independent.
2. **Inline Execution** — execute in this session via executing-plans, batch with checkpoints.

Which approach?
