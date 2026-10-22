# AI Gateway Migration — Phase 1b Implementation Plan (Embeddings / Site 4)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Route the one *embeddings* call site (`leakage.get_embedding`) through the Phase 0 seam so gateway embedding traffic carries the `leakage-embed` cost-attribution tag — default-off, with the classic SDK path kept verbatim.

**Architecture:** `get_embedding` today calls the typed SDK `w.serving_endpoints.query(name=ep, input=[text])`, which exposes no custom-header hook — so it cannot carry the tag. Under `GENIE_LLM_ROUTE=gateway` the function instead issues a raw `POST {host}/ai-gateway/mlflow/v1/embeddings` (via `httpx`, already a GSO dependency) with the model mapped to `system.ai.*` and the tag header attached, then parses `data[0].embedding` — the *same* response shape the SDK returns, so the existing defensive parser is reused unchanged. Under `classic` (the default) the SDK call is byte-identical to today. Every failure still returns `None`, so the firewall's degrade-to-n-gram/fingerprint fallback is untouched.

**Tech Stack:** Python 3.12, Databricks SDK, `httpx==0.28.1` (existing GSO dep); pytest via `./scripts/test.sh` (`uv run --frozen --extra dev pytest`).

**Spec:** `scripts/ai-gateway-validation/ai-gateway-migration-spec.md` — §1.1 (site 4 is the transport nuance), §6 Phase 1b, §1.4 (model mapping), §7 (embeddings-firewall regression row), Appendix A.2 site 4. The seam is already shipped (Phase 0) and consumed unchanged. Where this plan and the spec differ, the recorded Ruling below wins; the spec is the binding authority for everything else.

## Global Constraints

- **Default-off / fail-safe.** With `GENIE_LLM_ROUTE=classic` (the default), `get_embedding` MUST behave exactly as today: the same `w.serving_endpoints.query(name=ep, input=[text])` SDK call, the same parse, the same return values. The classic branch must NOT touch `w.config`, `httpx`, or the seam's URL/header machinery at all.
- **The seam is NOT modified.** `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/llm_route.py` (and its backend twin) are consumed, never edited. `backend/tests/test_llm_route_parity.py` stays green untouched.
- **Failure → `None` (firewall fallback preserved).** Any error on the gateway branch — non-200 status, network exception, malformed body — MUST return `None`, exactly as the classic branch does on failure, so `precompute_benchmark_embeddings` degrades to the n-gram + SQL-fingerprint layer. Never raise out of `get_embedding`.
- **Tag (§1.3, Decision 1).** Header key `Databricks-Ai-Gateway-Request-Tags`; value `{"application":"genie-workbench","component":"leakage-embed"}`. Built only via the seam's `resolve_embeddings(...).extra_headers` — no hand-written tag string. Embeddings are not run/space-scoped here, so no `run_id`/`space_id` extras.
- **Model mapping = seam only.** `resolve_embeddings` maps the endpoint via `gateway_model_name` (`databricks-bge-large-en` → `system.ai.bge-large-en`; `databricks-gte-large-en` → `system.ai.gte-large-en`). Whether a given `system.ai.*` embedding endpoint exists on the gateway is a deploy-time / probe concern (S3b), not this diff's — the mapping is mechanical and the classic default holds until the flag flips.
- **No new dependency.** `httpx` is already pinned in `packages/genie-space-optimizer/pyproject.toml:30`. Do NOT touch `pyproject.toml`/`uv.lock`/`requirements.txt`.
- **Test invocation.** Always `./scripts/test.sh <paths>`. Never bare `pytest`.
- **Baseline.** Post-Phase-1 the full suite is **2765 passed**. Phase 1b only adds tests + edits one function; the count rises — below 2765 is a regression. Do NOT edit the mv-advisor rule/playbook baseline line (its parity test guards the two copies match each other, not the suite count; this AI-Gateway work rides no doc-freeze).

### Recorded Ruling

- **R3 — branch on `get_llm_route()`, not `rc.use_legacy_sdk`.** Appendix A.2 site 4 phrases the switch as `if rc.use_legacy_sdk: <SDK> else: <httpx>`, which would require computing `resolve_embeddings(host, …)` (and thus reading `w.config.host`) on the classic path too. To keep the classic branch from touching `w.config` at all — preserving byte-identical behavior and not disturbing SDK-mock call sites — branch directly on `get_llm_route() is LLMRoute.GATEWAY`: classic runs the untouched SDK call with no `w.config`/seam access; only the gateway branch calls `resolve_embeddings` and reads `host`/auth. Functionally identical to the spec (classic still keeps the SDK `query()`), just a safer branch key. *If wrong:* none — the `use_legacy_sdk` field simply goes unused at this site; the observable behavior matches the spec.

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/leakage.py` | Firewall embeddings (`get_embedding`, and `preflight_embedding_endpoint` which delegates to it) | Add `httpx` + seam imports; route `get_embedding` on gateway to a raw embeddings POST with the tag; classic SDK path verbatim |
| `packages/genie-space-optimizer/tests/unit/test_leakage_embedding_gateway.py` (new) | Site 4 tests | classic-unchanged, gateway tags/maps/parses, failure→None, preflight-under-gateway |

**Consumed seam surface (unchanged, from Phase 0):** `resolve_embeddings(host, endpoint, component, *, route=None, run_id=None, space_id=None) -> ResolvedCall(url, model, extra_headers, use_legacy_sdk)`, `get_llm_route() -> LLMRoute`, `LLMRoute.{CLASSIC,GATEWAY}`. On gateway, `resolve_embeddings` returns `url = f"{host}/ai-gateway/mlflow/v1/embeddings"`, `model = gateway_model_name(endpoint)`, `extra_headers = tag_header("leakage-embed")`.

---

## Task 1: Route `get_embedding` through the seam on gateway (+ leakage-embed tag)

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/leakage.py` — imports (`:22-28`); `get_embedding` body (`:189-217`; SDK call at `:200`, parser at `:206-217`)
- Test: `packages/genie-space-optimizer/tests/unit/test_leakage_embedding_gateway.py` (new)

**Interfaces:**
- Consumes: GSO seam `resolve_embeddings`, `get_llm_route`, `LLMRoute` (already exported; the sibling `llm_client.py` already imports from this module).
- Produces: no signature change — `get_embedding(text, w, endpoint=None)` is unchanged; behavior gains a gateway branch. `preflight_embedding_endpoint` inherits routing transitively (it calls `get_embedding`), so it needs no edit.

- [ ] **Step 1: Write the failing tests** (new file `packages/genie-space-optimizer/tests/unit/test_leakage_embedding_gateway.py`)

```python
"""Site-4 (leakage.get_embedding) AI-Gateway routing tests."""
from __future__ import annotations

import json
from types import SimpleNamespace

from genie_space_optimizer.optimization import leakage


def _wc():
    return SimpleNamespace(config=SimpleNamespace(
        host="https://example.databricks.com/",
        authenticate=lambda: {"Authorization": "Bearer tok"},
    ))


def test_classic_uses_sdk_query_verbatim(monkeypatch):
    monkeypatch.delenv("GENIE_LLM_ROUTE", raising=False)
    called = {}

    def fake_post(*a, **k):
        called["posted"] = True
        return SimpleNamespace(status_code=200, json=lambda: {"data": [{"embedding": [9.9]}]})

    monkeypatch.setattr(leakage.httpx, "post", fake_post)
    w = SimpleNamespace(serving_endpoints=SimpleNamespace(
        query=lambda name, input: SimpleNamespace(data=[SimpleNamespace(embedding=[0.1, 0.2, 0.3])])))
    out = leakage.get_embedding("hello", w, endpoint="databricks-bge-large-en")
    assert out == [0.1, 0.2, 0.3]          # from the SDK, not httpx's 9.9
    assert "posted" not in called          # classic never touches httpx


def test_gateway_posts_maps_and_tags(monkeypatch):
    monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")
    captured = {}

    def fake_post(url, json=None, headers=None, timeout=None):
        captured.update(url=url, json=json, headers=headers)
        return SimpleNamespace(status_code=200, json=lambda: {"data": [{"embedding": [0.5, 0.6]}]})

    monkeypatch.setattr(leakage.httpx, "post", fake_post)
    out = leakage.get_embedding("q", _wc(), endpoint="databricks-bge-large-en")
    assert out == [0.5, 0.6]
    assert captured["url"] == "https://example.databricks.com/ai-gateway/mlflow/v1/embeddings"
    assert captured["json"] == {"model": "system.ai.bge-large-en", "input": ["q"]}
    assert captured["headers"]["Authorization"] == "Bearer tok"
    tags = json.loads(captured["headers"]["Databricks-Ai-Gateway-Request-Tags"])
    assert tags == {"application": "genie-workbench", "component": "leakage-embed"}


def test_gateway_non_200_returns_none(monkeypatch):
    monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")
    monkeypatch.setattr(leakage.httpx, "post",
                        lambda *a, **k: SimpleNamespace(status_code=404, json=lambda: {}))
    assert leakage.get_embedding("q", _wc(), endpoint="databricks-bge-large-en") is None


def test_gateway_exception_returns_none(monkeypatch):
    monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")

    def boom(*a, **k):
        raise RuntimeError("network down")

    monkeypatch.setattr(leakage.httpx, "post", boom)
    assert leakage.get_embedding("q", _wc(), endpoint="databricks-bge-large-en") is None


def test_preflight_under_gateway(monkeypatch):
    monkeypatch.setenv("GENIE_LLM_ROUTE", "gateway")
    monkeypatch.setattr(leakage.httpx, "post",
                        lambda *a, **k: SimpleNamespace(status_code=200,
                                                        json=lambda: {"data": [{"embedding": [1.0]}]}))
    assert leakage.preflight_embedding_endpoint(_wc(), endpoint="databricks-bge-large-en") is True
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./scripts/test.sh packages/genie-space-optimizer/tests/unit/test_leakage_embedding_gateway.py -v`
Expected: FAIL — `leakage` has no `httpx` attribute (not imported yet), and the gateway tests fail because `get_embedding` still calls the SDK `query()` regardless of route.

- [ ] **Step 3: Add the imports**

In `leakage.py`, add `httpx` to the stdlib/third-party imports (`:22-28`):

```python
import httpx
```

And add the seam import (near the top, after the existing imports):

```python
from genie_space_optimizer.optimization.llm_route import (
    LLMRoute,
    get_llm_route,
    resolve_embeddings,
)
```

- [ ] **Step 4: Route `get_embedding` on gateway (classic branch verbatim)**

Replace the fetch block (`:198-203`) so the route decides the transport; keep the existing parser (`:204-217`) unchanged, shared by both branches:

```python
    ep = endpoint or EMBEDDING_ENDPOINT
    route = get_llm_route()
    try:
        if route is LLMRoute.GATEWAY:
            host = w.config.host.rstrip("/")
            rc = resolve_embeddings(host, ep, "leakage-embed", route=route)
            r = httpx.post(
                rc.url,
                json={"model": rc.model, "input": [text]},
                headers={**w.config.authenticate(), **rc.extra_headers},
                timeout=30,
            )
            if r.status_code != 200:
                logger.debug("get_embedding gateway HTTP %s for endpoint=%s", r.status_code, ep)
                return None
            resp = r.json()
        else:
            resp = w.serving_endpoints.query(name=ep, input=[text])
    except Exception as exc:
        logger.debug("get_embedding failed for endpoint=%s: %s", ep, exc)
        return None
    # Databricks serving endpoints AND the gateway embeddings route both return
    # the OpenAI-compatible shape (data[0].embedding) — or a raw list. Parse both.
    try:
        data = getattr(resp, "data", None) or (resp.get("data") if isinstance(resp, dict) else None)
        if data and isinstance(data, list):
            first = data[0]
            emb = getattr(first, "embedding", None) or (
                first.get("embedding") if isinstance(first, dict) else None
            )
            if isinstance(emb, list):
                return [float(x) for x in emb]
    except Exception:
        logger.debug("get_embedding response parse failed", exc_info=True)
    return None
```

> The classic branch is the original `w.serving_endpoints.query(name=ep, input=[text])` call, now inside the shared `try`; on the classic path `w.config`, `httpx`, and the seam are never touched (R3). `preflight_embedding_endpoint` needs no change — it calls `get_embedding`, so it routes automatically.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `./scripts/test.sh packages/genie-space-optimizer/tests/unit/test_leakage_embedding_gateway.py -v`
Expected: PASS (5 tests). Also run the leakage suite to confirm no regression: `./scripts/test.sh -k leakage -v`.

- [ ] **Step 6: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/leakage.py packages/genie-space-optimizer/tests/unit/test_leakage_embedding_gateway.py
git commit -m "feat(ai-gateway): Phase 1b site 4 — route leakage embeddings through the seam (+leakage-embed tag)"
```

---

## Task 2: Contract grep + full-suite verification

**Files:** none (verification only — no commit unless a gap is found).

**Interfaces:** Consumes Task 1.

- [ ] **Step 1: Full suite is green and grew**

Run: `./scripts/test.sh`
Expected: PASS, count ≥ 2765 + 5 new tests (~2770). Zero failures. `backend/tests/test_llm_route_parity.py` green (seam untouched).

- [ ] **Step 2: Contract grep (a) — the embeddings site no longer hard-codes the SDK query as the only path**

Run:
```bash
rg -n "serving_endpoints.query|ai-gateway/mlflow/v1/embeddings|resolve_embeddings" packages/genie-space-optimizer/src/genie_space_optimizer/optimization/leakage.py
```
Expected: `resolve_embeddings(...)` present on the gateway branch; `w.serving_endpoints.query(...)` present only inside the `else` (classic) branch — no gateway path bypasses the seam. Paste the result into the PR.

- [ ] **Step 3: Contract grep (b) — the tag lands at the embeddings site via the seam**

Run:
```bash
rg -n "leakage-embed|extra_headers|Databricks-Ai-Gateway-Request-Tags" packages/genie-space-optimizer/src/genie_space_optimizer/optimization/leakage.py
```
Expected: `resolve_embeddings(host, ep, "leakage-embed", …)` + `rc.extra_headers` merged into the POST headers; no hand-written tag-header string literal in `leakage.py`.

- [ ] **Step 4: Seam + deps untouched**

Run:
```bash
git diff --name-only <phase1b-base>..HEAD -- \
  packages/genie-space-optimizer/src/genie_space_optimizer/optimization/llm_route.py \
  backend/services/llm_route.py pyproject.toml uv.lock requirements.txt \
  packages/genie-space-optimizer/pyproject.toml
```
Expected: **empty** — seam twins and dependency manifests unchanged.

- [ ] **Step 5: Record results** in the PR / run report (suite count delta, both greps, the empty seam/dep diff). No code change; nothing to commit.

---

## Self-Review (run against the spec)

**1. Spec coverage (§6 Phase 1b + Appendix A.2 site 4):**
- Site 4 `leakage.get_embedding` → Task 1 ✅ (gateway raw `POST /ai-gateway/mlflow/v1/embeddings`, model mapped, `leakage-embed` tag, `data[0].embedding` parse reused).
- `preflight_embedding_endpoint` (named as a surface in §6 Phase 1b) → covered transitively (delegates to `get_embedding`); pinned by `test_preflight_under_gateway` ✅.
- Regression guards: classic SDK path kept verbatim (byte-identical, `test_classic_uses_sdk_query_verbatim`) ✅; failure→`None` firewall-degradation preserved (`non_200`/`exception` tests) ✅; contract grep ✅ (Task 2). Probe S3b + `preflight` are the deploy-time integration gate (spec §7/§8) — out of scope for these offline unit tasks, noted for the PR.
- Deliberately deferred: model-catalog/BYOK = Phase 2; 404-downgrade = Phase 3.

**2. Placeholder scan:** every code/test step carries real code; anchors are fresh file:line verified this session (`EMBEDDING_ENDPOINT` default `databricks-bge-large-en` at `leakage.py:51-53`, SDK call at `:200`, parser at `:206-217`); `httpx` confirmed a GSO dep (`pyproject.toml:30`). No "TBD"/"similar to". ✅

**3. Type consistency:** `resolve_embeddings(host, endpoint, component, *, route=…)` and `ResolvedCall(.url/.model/.extra_headers)` match the Phase-0 seam (Appendix A.1) and the way `llm_client.py` already consumes the twin; `get_embedding` return type (`list[float] | None`) is unchanged. ✅

---

## Execution Handoff

Plan complete and saved to `scripts/ai-gateway-validation/ai-gateway-phase1b-plan.md`. Two execution options:

1. **Subagent-Driven (recommended)** — fresh subagent per task, task review between tasks, broad review at the end. Task 2 depends on Task 1; both are small.
2. **Inline Execution** — execute in this session via executing-plans.

Which approach?
