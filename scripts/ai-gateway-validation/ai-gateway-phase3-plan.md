# AI Gateway Migration — Phase 3 Implementation Plan (404-Downgrade / Governance UX)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** On the gateway route, turn a `404` on a configured model — which the gateway returns for BOTH "not entitled" and "unknown model" (validated: the two are indistinguishable at the HTTP layer, both `error_code: NOT_FOUND`) — into ONE clean, standardized user-facing outcome ("Model unavailable or access not granted."), instead of dumping a raw error body. **Scope decision (user, 2026-09-25): clean-downgrade messaging only** — no automatic classic-route fallback, and no changes to the metric-view advisor's `suggest_only` governance. AI Gateway stays the primary path; when it can't serve the request, the user sees a simple message.

**Architecture:** Add one pure classifier `is_model_unavailable_404(status, body)` + one message constant `MODEL_UNAVAILABLE_MESSAGE` to the seam (`llm_route.py`, both byte-identical twins), symmetric with the existing `is_reasoning_effort_400`. Then, at each site that renders an LLM failure, gate on `get_llm_route() is LLMRoute.GATEWAY` and, when the classifier matches, raise `RuntimeError(MODEL_UNAVAILABLE_MESSAGE)` instead of the raw-body error. That message flows unchanged to the surfaces that already render failures: the Create Agent SSE `error` event (`create_agent.py:438-441`), any `call_serving_endpoint` caller, and the GSO optimize task's persisted failed-phase status row. The **classic route is byte-identical** (a classic 404 — e.g. a mistyped curated endpoint — keeps today's raw-body error), and **only 404 downgrades** (other non-200s are unchanged).

**Tech Stack:** Python 3.12, FastAPI, httpx, Databricks SDK requests.Session, OpenAI SDK (GSO); pytest via `./scripts/test.sh` (`uv run --frozen --extra dev pytest`).

**Spec:** `scripts/ai-gateway-validation/ai-gateway-migration-spec.md` — §3 (the 404-downgrade contract), §6 Phase 3, §7 (entitlement regression row). Evidence: `report-20260913-175958.md` S6 / "De-entitled leg" (404 `NOT_FOUND` body shape). Where this plan and the spec differ, the recorded Rulings below win; the spec is binding otherwise. **The `suggest_only` wording in §6 is the mv-advisor governance term; per the scope decision it is explicitly out of scope — the generic-LLM equivalent here is the "unavailable" message.**

## Global Constraints

- **Default-off / gateway-only.** Every downgrade branch is gated on `get_llm_route() is LLMRoute.GATEWAY`. On the classic route (the shipped default), a 404 keeps today's exact raw-body `RuntimeError` — byte-identical. There is no gateway 404 on the classic path, so Phase 3 is inert until the flag flips.
- **404-only.** ONLY `status == 404` downgrades. Every other non-200 (400 that isn't the reasoning retry, 401, 403, 5xx after retries, etc.) keeps its current handling verbatim. `403` is NOT a downgrade trigger (spec §3: entitlement failures are 404s, never 403s).
- **Streaming safety (Phase 1 lesson).** In `_stream_llm`, the 404 check MUST use `resp.status_code` only and MUST live inside the existing `if not resp.ok:` block — never read `resp.text` on a 200 (that forces a full `iter_content` buffer and defeats SSE). Pass `body=""` to the classifier at the streaming site.
- **Seam twins stay byte-identical.** `backend/services/llm_route.py` and `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/llm_route.py` get the SAME additions; `backend/tests/test_llm_route_parity.py` must stay green.
- **One source of copy.** The user-facing string lives ONLY in `MODEL_UNAVAILABLE_MESSAGE` (seam). No site hard-codes the wording; tests assert against the constant.
- **No new config.** Phase 3 reuses `GENIE_LLM_ROUTE`. No new env var, no `app.yaml` change, no job parameter, no 4-mirror change. Do NOT touch `databricks.yml` / `gso_job.py` / `job_launcher.py` / `trigger.py`.
- **No fallback, no governance coupling.** Do NOT add any classic-route retry. Do NOT modify `backend/services/mv_suggest.py` or any mv-advisor governance surface.
- **Test invocation.** Always `./scripts/test.sh <paths>`. Never bare `pytest`.
- **Baseline.** Post-Phase-2 the full suite is **2796 passed**. Phase 3 adds tests + small branches; the count rises — below 2796 is a regression. Do NOT edit the mv-advisor rule/playbook baseline line.

### Recorded Rulings

- **R8 — classifier keys on `status == 404` alone (body-agnostic).** `is_model_unavailable_404(status, body)` returns `status == 404`; the `body` param is accepted (symmetry with `is_reasoning_effort_400`, future-proofing) but not required, because §3 says the two 404 causes are indistinguishable and BOTH emit `NOT_FOUND`. Safety comes from the CALLER's gateway-route gate: on the fixed, valid gateway chat URL a 404 can only mean model-not-found / not-entitled; a classic 404 (different cause) is never routed here. *If wrong:* an unrelated gateway 404 (none known) would show "unavailable" instead of a raw body — still a safe, truthful message.
- **R9 — downgrade = a clean RuntimeError, not a silent success.** A failed chat call has no answer to return, so "downgrade, not raise" (spec §7) means *raise a standardized, legible message* rather than dump the raw provider body. Tests assert `str(exc) == MODEL_UNAVAILABLE_MESSAGE`. This keeps the existing error-rendering paths (SSE `error` event, GSO failed-phase status row) unchanged in shape — only the message content improves. *If wrong:* none — the surfaces already render an error; this only makes it legible.
- **R10 — GSO is in scope but light-touch.** GSO `call_llm` already degrades a 404 into a failed-phase status row (the optimize task's per-phase try/except). Phase 3 only standardizes the *message* at `call_llm`'s final `raise last_err`, so the persisted row reads the clean copy instead of a raw OpenAI stack. No structural change to the retry loop or phase handling.

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `backend/services/llm_route.py` | Seam (backend twin) | Add `MODEL_UNAVAILABLE_MESSAGE` + `is_model_unavailable_404` |
| `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/llm_route.py` | Seam (GSO twin) | Byte-identical same addition |
| `backend/services/llm_utils.py` | Site 1 (`call_serving_endpoint`) | Gateway 404 → `MODEL_UNAVAILABLE_MESSAGE` before the raw-body raise |
| `backend/services/create_agent.py` | Site 2 (`_stream_llm`) | Gateway 404 → `MODEL_UNAVAILABLE_MESSAGE` inside `if not resp.ok:` |
| `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/llm_client.py` | Site 3 (`call_llm`) | Gateway 404 at final raise → `MODEL_UNAVAILABLE_MESSAGE` |
| `backend/tests/test_llm_route.py` + GSO `tests/unit/test_llm_route.py` | Seam tests | classifier truth table + constant present |
| `backend/tests/test_llm_utils.py`, `test_create_agent_gateway.py`, GSO `tests/unit/test_llm_client_gateway.py` | Adoption tests | gateway-404 downgrade; classic-404 + other-non-200 unchanged |

**Consumed seam surface (new, added in Task 1):** `is_model_unavailable_404(status: int, body_text: str) -> bool`, `MODEL_UNAVAILABLE_MESSAGE: str`. Sites also use the existing `get_llm_route()` / `LLMRoute`.

**Untouched surfaces (verify empty diff):** `leakage.get_embedding` (site 4 already returns `None` on any failure → firewall degrades), `mv_suggest.py`, `model_catalog.py`, `app.yaml`, all four job-config mirrors, dependency manifests.

---

## Task 1: Add the 404 classifier + message to the seam (both twins)

**Files:**
- Modify: `backend/services/llm_route.py` — append after `is_reasoning_effort_400` (`:72-74`)
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/llm_route.py` — identical append
- Test: `backend/tests/test_llm_route.py`, `packages/genie-space-optimizer/tests/unit/test_llm_route.py`

**Interfaces:** Produces `MODEL_UNAVAILABLE_MESSAGE`, `is_model_unavailable_404`. Consumed by Tasks 2–3.

- [ ] **Step 1: Write failing seam tests** (both files — mirror each other)

Add to `backend/tests/test_llm_route.py` and the GSO twin `packages/genie-space-optimizer/tests/unit/test_llm_route.py`:

```python
def test_is_model_unavailable_404_true_only_on_404():
    from <MODULE> import is_model_unavailable_404
    assert is_model_unavailable_404(404, '{"error_code":"NOT_FOUND","message":"x does not exist"}') is True
    assert is_model_unavailable_404(404, "") is True            # body-agnostic (R8)
    assert is_model_unavailable_404(403, "forbidden") is False  # 403 is NOT the trigger (§3)
    assert is_model_unavailable_404(400, "reasoning_effort") is False
    assert is_model_unavailable_404(200, "") is False


def test_model_unavailable_message_is_the_single_copy():
    from <MODULE> import MODEL_UNAVAILABLE_MESSAGE
    assert MODEL_UNAVAILABLE_MESSAGE == "Model unavailable or access not granted."
```

`<MODULE>` = `backend.services.llm_route` (backend) / `genie_space_optimizer.optimization.llm_route` (GSO).

- [ ] **Step 2: Run to confirm RED**

Run: `./scripts/test.sh backend/tests/test_llm_route.py packages/genie-space-optimizer/tests/unit/test_llm_route.py -v`
Expected: FAIL — symbols don't exist yet.

- [ ] **Step 3: Add the symbols to BOTH twins (byte-identical)**

Append after `is_reasoning_effort_400`:

```python
MODEL_UNAVAILABLE_MESSAGE = "Model unavailable or access not granted."


def is_model_unavailable_404(status: int, body_text: str) -> bool:
    """Downgrade trigger (§3): the gateway returns 404 for BOTH not-entitled and
    unknown-model (indistinguishable at the HTTP layer). Body-agnostic; callers
    MUST gate on the gateway route so a classic 404 keeps its raw-body error."""
    return status == 404
```

- [ ] **Step 4: Run to confirm GREEN + parity**

Run: `./scripts/test.sh backend/tests/test_llm_route.py packages/genie-space-optimizer/tests/unit/test_llm_route.py backend/tests/test_llm_route_parity.py -v`
Expected: PASS, including the parity test (twins byte-identical).

- [ ] **Step 5: Commit**

```bash
git add backend/services/llm_route.py packages/genie-space-optimizer/src/genie_space_optimizer/optimization/llm_route.py backend/tests/test_llm_route.py packages/genie-space-optimizer/tests/unit/test_llm_route.py
git commit -m "feat(ai-gateway): Phase 3 seam — is_model_unavailable_404 + MODEL_UNAVAILABLE_MESSAGE (both twins)"
```

---

## Task 2: Downgrade at the two interactive backend sites (llm_utils + create_agent)

**Files:**
- Modify: `backend/services/llm_utils.py` — imports (`:12`); non-200 handler (`:118-121`)
- Modify: `backend/services/create_agent.py` — imports (the existing `from ...llm_route import ...` line); `_stream_llm` `if not resp.ok:` block (`:1062-1068`)
- Test: `backend/tests/test_llm_utils.py`, `backend/tests/test_create_agent_gateway.py`

**Interfaces:** Consumes Task 1. No signature changes.

- [ ] **Step 1: Write failing tests**

`backend/tests/test_llm_utils.py` (follow the existing `_capture_post` / monkeypatch style):

```python
def test_call_serving_endpoint_gateway_404_downgrades(monkeypatch):
    monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")
    # httpx.post -> 404 with the validated NOT_FOUND body
    ...  # stub resp.status_code=404, resp.text='{"error_code":"NOT_FOUND",...}'
    with pytest.raises(RuntimeError) as ei:
        llm_utils.call_serving_endpoint([{"role": "user", "content": "hi"}], model="databricks-claude-sonnet-4-6")
    assert str(ei.value) == llm_utils.MODEL_UNAVAILABLE_MESSAGE  # imported from seam


def test_call_serving_endpoint_classic_404_keeps_raw_body(monkeypatch):
    monkeypatch.delenv("GENIE_LLM_ROUTE", raising=False)  # classic
    ...  # 404 stub
    with pytest.raises(RuntimeError) as ei:
        llm_utils.call_serving_endpoint([{"role": "user", "content": "hi"}])
    assert "Serving endpoint returned 404" in str(ei.value)   # unchanged


def test_call_serving_endpoint_gateway_500_keeps_raw_body(monkeypatch):
    monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")
    ...  # 500 stub (after retries exhausted or non-retryable path)
    with pytest.raises(RuntimeError) as ei:
        llm_utils.call_serving_endpoint([{"role": "user", "content": "hi"}])
    assert "Serving endpoint returned 500" in str(ei.value)   # only 404 downgrades
```

`backend/tests/test_create_agent_gateway.py` (reuse the `_Resp` / `_Session` / `_agent_with_session` helpers already there):

```python
def test_stream_gateway_404_downgrades(monkeypatch):
    monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")
    # _Session.post -> _Resp(status_code=404, ok=False, text='{"error_code":"NOT_FOUND",...}')
    agent = _agent_with_session(...)
    with pytest.raises(RuntimeError) as ei:
        list(agent._stream_llm([{"role": "user", "content": "hi"}], tools=None, space_id="sp"))
    assert str(ei.value) == create_agent.MODEL_UNAVAILABLE_MESSAGE


def test_stream_classic_404_keeps_raw_body(monkeypatch):
    monkeypatch.delenv("GENIE_LLM_ROUTE", raising=False)
    # _Resp(status_code=404, ok=False, text="raw provider body")
    with pytest.raises(RuntimeError) as ei:
        list(agent._stream_llm([{"role": "user", "content": "hi"}]))
    assert "LLM endpoint returned 404" in str(ei.value)


def test_stream_gateway_200_never_reads_text(monkeypatch):
    # Guard the Phase-1 SSE lesson: a 200 stream must not touch resp.text.
    monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")
    # _Resp(status_code=200, ok=True) whose .text raises if accessed
    ...  # assert streaming yields normally, .text never read
```

- [ ] **Step 2: Run to confirm RED** — `./scripts/test.sh backend/tests/test_llm_utils.py backend/tests/test_create_agent_gateway.py -v`

- [ ] **Step 3: Implement `llm_utils`**

Extend the seam import (`:12`):

```python
from backend.services.llm_route import (
    LLMRoute,
    MODEL_UNAVAILABLE_MESSAGE,
    get_llm_route,
    is_model_unavailable_404,
    resolve_chat,
)
```

Change the non-200 handler (`:118-121`):

```python
    if resp.status_code != 200:
        if get_llm_route() is LLMRoute.GATEWAY and is_model_unavailable_404(resp.status_code, ""):
            logger.warning("Gateway model unavailable/entitlement 404 for %s", model)
            raise RuntimeError(MODEL_UNAVAILABLE_MESSAGE)
        raise RuntimeError(
            f"Serving endpoint returned {resp.status_code}: {resp.text[:500]}"
        )
```

- [ ] **Step 4: Implement `create_agent._stream_llm`**

Add `MODEL_UNAVAILABLE_MESSAGE, is_model_unavailable_404` to the existing `from ...llm_route import (...)` block. Then, inside the `try:`/`if not resp.ok:` block (`:1062-1068`), add the 404 downgrade FIRST (status-only; never touches `resp.text` on a 200):

```python
        try:
            if not resp.ok:
                if get_llm_route() is LLMRoute.GATEWAY and is_model_unavailable_404(resp.status_code, ""):
                    logger.warning("Gateway model unavailable/entitlement 404 for %s", effective_model)
                    raise RuntimeError(MODEL_UNAVAILABLE_MESSAGE)
                error_body = resp.text[:1000]
                logger.error("LLM endpoint returned %s: %s", resp.status_code, error_body)
                raise RuntimeError(
                    f"LLM endpoint returned {resp.status_code}: {error_body[:300]}"
                )
            ...  # unchanged streaming loop
```

- [ ] **Step 5: Run to confirm GREEN** — `./scripts/test.sh backend/tests/test_llm_utils.py backend/tests/test_create_agent_gateway.py -v`

- [ ] **Step 6: Commit**

```bash
git add backend/services/llm_utils.py backend/services/create_agent.py backend/tests/test_llm_utils.py backend/tests/test_create_agent_gateway.py
git commit -m "feat(ai-gateway): Phase 3 sites 1-2 — gateway 404 downgrades to a clean message (create-agent + llm_utils)"
```

---

## Task 3: Downgrade the GSO `call_llm` message (light-touch)

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/llm_client.py` — imports (existing `from ...llm_route import (...)`, `:19-23`); final `raise last_err` (`:230`)
- Test: `packages/genie-space-optimizer/tests/unit/test_llm_client_gateway.py`

**Interfaces:** Consumes Task 1. R10 (message-only).

- [ ] **Step 1: Write failing tests** (reuse existing `_reset` / `_fake_wc` / `_fake_openai_client` helpers)

```python
def test_call_llm_gateway_404_downgrades(monkeypatch):
    monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")
    # fake OpenAI client whose chat.completions.create raises an error with status_code=404
    with pytest.raises(RuntimeError) as ei:
        call_llm(_fake_wc(), messages=[{"role": "user", "content": "hi"}], max_retries=0)
    assert str(ei.value) == MODEL_UNAVAILABLE_MESSAGE


def test_call_llm_classic_404_raises_original(monkeypatch):
    monkeypatch.delenv("GENIE_LLM_ROUTE", raising=False)
    # same 404-raising client
    with pytest.raises(Exception) as ei:
        call_llm(_fake_wc(), messages=[{"role": "user", "content": "hi"}], max_retries=0)
    assert not isinstance(ei.value, RuntimeError) or str(ei.value) != MODEL_UNAVAILABLE_MESSAGE  # original error preserved
```

> Use a fake exception class exposing `.status_code = 404` (mimicking `openai.NotFoundError`) so the test needs no network and no real openai types.

- [ ] **Step 2: Run to confirm RED**

- [ ] **Step 3: Implement**

Extend the seam import (`:19-23`) with `MODEL_UNAVAILABLE_MESSAGE, is_model_unavailable_404`. Replace the final `raise last_err` (`:230`):

```python
    if get_llm_route() is LLMRoute.GATEWAY and is_model_unavailable_404(
        getattr(last_err, "status_code", 0) or 0, str(last_err)
    ):
        raise RuntimeError(MODEL_UNAVAILABLE_MESSAGE) from last_err
    raise last_err  # type: ignore[misc]
```

- [ ] **Step 4: Run to confirm GREEN** — `./scripts/test.sh packages/genie-space-optimizer/tests/unit/test_llm_client_gateway.py -v`

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/llm_client.py packages/genie-space-optimizer/tests/unit/test_llm_client_gateway.py
git commit -m "feat(ai-gateway): Phase 3 site 3 — GSO call_llm gateway 404 raises the clean message (message-only)"
```

---

## Task 4: Contract grep + full-suite verification

**Files:** none (verification only).

- [ ] **Step 1: Full suite green + grew** — `./scripts/test.sh` → ≥ 2796 + new tests, zero failures; `test_llm_route_parity.py` green.

- [ ] **Step 2: Contract grep (a) — every downgrade is gateway-gated + 404-only + single-copy**

```bash
rg -n "is_model_unavailable_404|MODEL_UNAVAILABLE_MESSAGE|LLMRoute.GATEWAY" \
  backend/services/llm_utils.py backend/services/create_agent.py \
  packages/genie-space-optimizer/src/genie_space_optimizer/optimization/llm_client.py
```
Expected: at each of the 3 sites, `is_model_unavailable_404(...)` is `and`-chained with `get_llm_route() is LLMRoute.GATEWAY`, and the raised value is the bare `MODEL_UNAVAILABLE_MESSAGE` constant (no inline copy). Paste into the PR.

- [ ] **Step 3: Contract grep (b) — no hard-coded copy, no fallback, no 403 trigger, mv_suggest untouched**

```bash
rg -n "access not granted|unavailable" backend/services/llm_utils.py backend/services/create_agent.py packages/genie-space-optimizer/src/genie_space_optimizer/optimization/llm_client.py
rg -n "classic|fallback|403" backend/services/llm_utils.py backend/services/create_agent.py | rg -i "404|fallback|403" || true
```
Expected: the literal wording appears ONLY as the imported constant reference (no string literal at a site); no classic-fallback retry added; no `403` downgrade branch. Confirm `git diff --name-only` does NOT include `mv_suggest.py`.

- [ ] **Step 4: Untouched surfaces**

```bash
git diff --name-only <phase3-base>..HEAD -- \
  backend/services/leakage.py backend/services/mv_suggest.py \
  packages/genie-space-optimizer/src/genie_space_optimizer/optimization/leakage.py \
  backend/services/model_catalog.py app.yaml databricks.yml \
  scripts/deploy_lib/gso_job.py \
  packages/genie-space-optimizer/src/genie_space_optimizer/backend/job_launcher.py \
  packages/genie-space-optimizer/src/genie_space_optimizer/integration/trigger.py \
  pyproject.toml uv.lock requirements.txt
```
Expected: **empty**.

- [ ] **Step 5: Record results** in the PR (suite delta, both greps, empty untouched diff). No commit.

---

## Self-Review (against the spec)

**1. Spec coverage (§3 + §6 Phase 3 + §7):**
- 404 (not-entitled OR unknown-model) on a configured model → clean "unavailable", not a hard raw error → Tasks 2–3 ✅; classifier unifies both causes (§3 "same handler as unknown-model") → Task 1 ✅.
- Cannot key on 403 → R8/tests assert 403 is NOT a trigger ✅.
- Classic route error handling unchanged; new branch only augments (§7 guard) → classic-404 + other-non-200 tests ✅.
- "A test fires the de-entitled 404 shape and asserts downgrade, not raise (raw)" → gateway-404 tests assert `MODEL_UNAVAILABLE_MESSAGE` ✅ (R9).
- Deliberately excluded (scope decision): auto classic-fallback; `mv_suggest` `suggest_only` governance; `GET /permissions/{id}` probe (never added).
- Site 4 embeddings already degrades to `None` (no change) — verified untouched.

**2. Placeholder scan:** anchors fresh this session — seam tail `llm_route.py:72-74`; `llm_utils.py:118-121`; `_stream_llm` `if not resp.ok:` `create_agent.py:1062-1068` with SSE surfacing at `:438-441`/`:205`; GSO `call_llm` final raise `llm_client.py:230`. 404 body shape from `report-…S6`. No "TBD". ✅

**3. Type consistency:** `is_model_unavailable_404(int, str) -> bool` mirrors `is_reasoning_effort_400`; `MODEL_UNAVAILABLE_MESSAGE: str`; no signature changes at any site. ✅

---

## Execution Handoff

Plan complete and saved to `scripts/ai-gateway-validation/ai-gateway-phase3-plan.md`. Two execution options:

1. **Subagent-Driven (recommended)** — fresh subagent per task, review between tasks, broad review at the end. Order 1 → 2 → 3 → 4; Tasks 2–3 depend on Task 1's seam symbols.
2. **Inline Execution** — execute in this session via executing-plans.

Which approach?
