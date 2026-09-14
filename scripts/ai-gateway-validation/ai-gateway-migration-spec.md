# AI Gateway migration — implementation spec

- **Status:** Draft for review. Pre-deploy validation is complete (all gates PASS or a decided design
  choice) — see `report-20260913-175958.md` in this folder for the evidence behind every claim here.
- **Reproduce the evidence:** `probe.sh --profile <P> [--warehouse-id <id>]` (full suite),
  `probe.sh --profile <P> --byok <catalog.schema.name>` (BYOK / S9).
- **Placement note:** kept next to its validation artifacts on purpose. This is an implementation spec
  for a *new* initiative — it is not an mv-advisor doc and rides no doc-freeze.

---

## 0. Decision (TL;DR)

Route Workbench + GSO LLM/embedding traffic through the **Unity AI Gateway** unified path
(`/ai-gateway/mlflow/v1/...`) with a per-request cost-attribution tag, behind a single flag
`GENIE_LLM_ROUTE={classic|gateway}` that **defaults to `classic`**. The classic
`/serving-endpoints/{name}/invocations` path stays as the flag-off fallback.

**Why now:** the only mechanism that records Workbench spend for attribution
(`system.ai_gateway.usage.request_tags`) captures **gateway traffic only** — the classic
`/invocations` path is *not* recorded (validated: 3,110 gateway rows vs 0 `/invocations` rows over 2h).
So "tag the current path, migrate later" is impossible; the route swap and the tag ship together.

**Why it's safe:** default-off flag → zero behavior change until flipped; one seam → one place to reason
about; classic path proven still-green (S7); every request shape the app makes was validated on the
gateway first (streaming+tools on Claude *and* GPT, embeddings, JSON, 429, BYOK).

---

## 1. The seam — one client factory (route + tags + model mapping)

Today the app has **five** independent places that build an LLM/embedding call, each hard-coding the
classic base URL and each authenticating differently. The core of this change is to collapse the
*routing decision* (base URL + model name + tag header) into **one factory** consumed by all five.

### 1.1 The five call sites (validated, fresh file:line)

| # | Surface | Today | Auth mechanism | Tag injection under gateway |
|---|---|---|---|---|
| 1 | `backend/services/llm_utils.py:86` `call_serving_endpoint` | `httpx.post` → `{host}/serving-endpoints/{model}/invocations` | `client.config.authenticate()` headers | add key to the `headers` dict |
| 2 | `backend/services/create_agent.py:1006` `_stream_llm` (streaming + tools) | SDK `session.post` → `.../invocations` | SDK pre-auth `requests.Session` | add `headers=` on `session.post` |
| 3 | `packages/…/optimization/llm_client.py:129` `get_openai_client` (GSO `call_llm` + `optimizer_utils`) | `OpenAI(base_url=f"{host}/serving-endpoints")` | bearer token on the OpenAI client | `default_headers=` on `OpenAI(...)` |
| 4 | `packages/…/optimization/leakage.py:200` `get_embedding` | SDK `w.serving_endpoints.query(name=ep, input=[text])` | SDK | **switch to raw POST** `/ai-gateway/mlflow/v1/embeddings` (SDK `query()` can't carry the tag header) |
| 5 | `backend/services/model_catalog.py:15` curated list + `validate_chat_model:90` | classic endpoint names | n/a (name resolution) | n/a — this is the *mapping* surface, see §1.4 |

> Site 4 is the one behavioral nuance: the embeddings call goes through the SDK's typed
> `serving_endpoints.query()`, which does not expose a custom-header hook. To attribute embeddings the
> seam must issue a raw `POST /ai-gateway/mlflow/v1/embeddings` (validated: 200, dim=1024). This is a
> **separate diff** from the four chat sites and ships in its own phase (§6, Phase 1b).

### 1.2 Route resolution

```
GENIE_LLM_ROUTE = classic (default) | gateway

base(classic) = {host}/serving-endpoints/{model}/invocations      # model in the URL path
base(gateway) = {host}/ai-gateway/mlflow/v1/{chat|embeddings}      # model in the body
```

The factory returns `(url, model_id, headers)` so each call site keeps its own transport (httpx / SDK
session / OpenAI SDK) but stops deciding *where* and *how to tag*.

### 1.3 The tag (cost attribution)

Header (validated to land in `system.ai_gateway.usage.request_tags`, gateway path only):

```
Databricks-Ai-Gateway-Request-Tags: {"application":"genie-workbench","component":"<feature>"}
```

`component` vocabulary (proposed, one per feature seam): `create-agent`, `iq-scan`, `plan-builder`,
`gso-optimize`, `mv-suggest`, `leakage-embed`. Open decision: whether to also carry `run_id` /
`space_id` for per-run rollups (the MAP column supports it; adds cardinality).

### 1.4 Model-name mapping

- **Curated `databricks-*` models** → `system.ai.` + name with the `databricks-` prefix stripped.
  Validated for chat (Claude + GPT) *and* embeddings (`databricks-gte-large-en` → `system.ai.gte-large-en`).
- **The trap:** never echo a UC `entity_name`/`full_name` (e.g. `gte_large_en_v1_5`) — those 404. Map
  the *endpoint* name, don't reuse the registered-model name.
- **BYOK models** → pass the customer's **full 3-level UC id** through unchanged (§2). The strip rule
  does not apply. `validate_chat_model` (`model_catalog.py:90`) must accept an arbitrary
  `catalog.schema.name` when route=gateway and BYOK is enabled, instead of rejecting anything off the
  curated list.

---

## 2. BYOK / `reasoning_effort` handling

A customer's own Unity Gateway **Model Service** (`catalog.schema.name`) is **drop-in on the
Workbench's own path**: it answers `/ai-gateway/mlflow/v1/chat/completions` with the standard OpenAI
shape (validated: 200, backing `gpt-5.6-sol`). **No Responses↔chat adapter is required.**

Two BYOK rules the seam must encode:

1. **Arbitrary model ids.** The picker/`validate_chat_model` accepts full 3-level UC ids for BYOK; no
   strip rule, no curated-list gate. Because auto-enumeration of Model Services is a gap (they appear in
   neither `serving-endpoints list` nor UC `registered-models`, and `ai-gateway/model-services` 404s),
   **BYOK ids are user-configured** (a settings / per-space field), not auto-populated. Auto-discovery is
   a fast-follow once the correct list API is identified — it does not block attribution.

2. **`reasoning_effort:"none"` via retry, NOT always-send.** When a BYOK service is backed by a
   *reasoning* model (e.g. `gpt-5.6-sol`), function tools on `/chat/completions` return a hard **400**
   unless the request sends `reasoning_effort:"none"` (or uses `/responses`). **But you cannot just
   always send it:** validated — `system.ai.claude-sonnet-4-6` **400s** on `reasoning_effort` with
   `"Extra inputs are not permitted"`, while `gpt-5-4` accepts it. So the seam uses the **retry
   pattern, symmetric with the existing `response_format` strip-retry** (`llm_client.py:200`): send the
   tool request plain; if the response is a **400 whose body mentions `reasoning_effort`**, retry once
   with `reasoning_effort:"none"`. This is model-version-specific (`gpt-5-4`/`gpt-5-2` need neither), so
   never assume GPT-family uniformity and never hard-code the flag on.

---

## 3. The 404-downgrade contract (entitlement)

**Entitlement failures surface as `404 NOT_FOUND`, not `403`.** Validated: an un-entitled SP calling
`…pbi_migration.test1` gets `404 "…does not exist"`, while an entitled user gets 200 on the same
service. The gateway deliberately **hides** Model Services the caller can't access.

Contract for the app:

- The downgrade to `suggest_only` (or "model unavailable") **cannot be keyed on 403** — a de-entitled
  identity never produces one. A **404 on a model the app believes is configured** MUST be treated as
  *not-entitled / unavailable → downgrade*, not as a hard error.
- This **collapses onto the same handler as unknown-model** (S6 also returns 404). Fewer new branches.
- UX copy: "model unavailable or access not granted" — the two cases are indistinguishable at the HTTP
  layer, and asserting one over the other would be wrong.
- Do **not** probe `GET /permissions/{id}` to pre-check entitlement — it reports the SP's privileges,
  not the user's. The live call is the entitlement test.

---

## 4. The 4-mirror config lockstep

`GENIE_LLM_ROUTE` reaches the GSO job as a parameter, so it follows the existing **`llm_model`
precedent exactly** — the closest analog (an app/operator-selected value that must reach every task).
A param present in fewer than all its places is a bug.

| # | File | How `llm_model` is treated today (mirror it) |
|---|---|---|
| 1. Root bundle | `databricks.yml:110` (`- name: llm_model`) + each task's `base_parameters` (`:151/170/191/220`) | add `genie_llm_route` job param + pass it through on every task |
| 2. Deploy lib | `scripts/deploy_lib/gso_job.py` — per-task param lists (`:46/55/66/80`), default map (`:107`), default override (`:243`) | add to each task list + default map |
| 3. Run-now trigger | `packages/…/integration/trigger.py` `trigger_optimization` (`:94`); job_parameters stringify (`:275`), `llm_model=config.llm_model` (`:302`, `:396`) | assemble `genie_llm_route` into the run_now `job_parameters` |
| 4. App env (non-job surfaces) | `app.yaml` (like `LLM_MODEL`) | Workbench-side reads route from env |

> **Note (verified):** `llm_model` is intentionally **NOT** in `packages/genie-space-optimizer/databricks.yml`
> — `gso_job.py:32` documents it as "a Workbench-specific extra beyond the package bundle's params."
> So a new app-selected route param follows the `llm_model` set above, **not** the generic four-mirror
> list in the mv-advisor rule (which governs mv-advisor's own params). If the notebook-install path
> deploys the job from the package bundle, confirm whether the package `databricks.yml` also needs the
> param before flipping the flag on that path.

---

## 5. Non-goals (explicit out-of-scope for the first cut)

- No Responses-API adapter (chat/completions is drop-in for BYOK too — §2).
- No auto-enumeration of BYOK Model Services (config field now; discovery later).
- No change to the retry/backoff logic — existing 429 retry is retained as-is (validated headroom).
- No `response_format:json_object` dependency — it 400s on **both** paths in the test workspace; the
  existing strip-and-retry fallback (`llm_client.py:200`) is the real path and works on the gateway.
- No new MLflow experiments/judges (out of scope, unchanged).

---

## 6. Roadmap (reviewable) — what changes, why, where, and how we prevent regression

> Read this section top-to-bottom to understand the whole change. Each phase is independently
> shippable, default-off until the flag flips, and gated by re-running `probe.sh` + the unit suites.

### Phase 0 — Seam scaffold (no behavior change)  → concrete signatures in **Appendix A.1**
- **What:** add `GENIE_LLM_ROUTE` (default `classic`) and the `resolve_chat` / `resolve_embeddings`
  factory returning a `ResolvedCall(url, model, extra_headers)`. Nothing calls it yet.
- **Why:** land the decision point in one place before touching hot paths.
- **Surfaces:** new module (backend) + a GSO twin (or shared util); `app.yaml` env; the 4 config mirrors (§4).
- **Regression guard:** flag defaults classic → the factory returns today's exact classic values.
  Unit tests assert `classic` output byte-for-byte matches the current URL/headers. No call site changed yet.

### Phase 1 — Route the 4 chat sites through the seam (+ tags)  → adoption deltas in **Appendix A.2**
- **What:** sites 1–3 (llm_utils, create_agent `_stream_llm`, GSO `get_openai_client`) call the factory;
  inject the tag header per transport (§1.1). This is the unit that delivers attribution.
- **Why:** attribution needs the gateway path; can't be decoupled from the route swap.
- **Surfaces:** `llm_utils.py:86`, `create_agent.py:1006`, `llm_client.py:129`. Streaming+tools is the
  gating shape.
- **Regression guard:**
  - Default-off: with `classic`, these three are unchanged.
  - Streaming parser already provider-agnostic — the `[DONE]` break (`create_agent.py:1048`) is retained,
    and GPT's "no `[DONE]`, close-on-`finish_reason`" ending is handled by the natural `iter_lines`
    end (validated). **No parser change needed**, but a test pins both terminations.
  - Tool path uses the `reasoning_effort` retry (§2), NOT always-send — a test pins that Claude
    (rejects the flag) and a reasoning model (requires it) both succeed through the one retry.
  - Re-run `probe.sh` S1–S4 + S7 before flipping the flag in any environment.

### Phase 1b — Embeddings site (separate diff)
- **What:** site 4 (`leakage.get_embedding`) switches from SDK `serving_endpoints.query()` to a raw
  `POST /ai-gateway/mlflow/v1/embeddings` under the flag, so the tag header can be attached.
- **Why:** different transport; can't tag the typed SDK call.
- **Surfaces:** `leakage.py:200`. Also the firewall preflight (`preflight_embedding_endpoint`).
- **Regression guard:** classic branch keeps the SDK call verbatim; gateway branch validated (dim=1024).
  Firewall degradation path (embeddings disabled → n-gram/fingerprint) is untouched and still the
  failure fallback.

### Phase 2 — Model catalog + BYOK
- **What:** `model_catalog` maps curated → `system.ai.*` and accepts arbitrary 3-level UC ids for BYOK;
  `validate_chat_model` (`:90`) stops hard-rejecting off-list ids when route=gateway + BYOK on. Tool path
  sends `reasoning_effort:"none"`.
- **Why:** the real "can the customer use their own model?" answer, plus reasoning-model safety.
- **Surfaces:** `model_catalog.py:15/90`; the seam's model-mapping step.
- **Regression guard:** curated-list behavior on the classic route is unchanged; BYOK acceptance is
  strictly additive and gated on route=gateway. Unit tests for strip-rule, entity_name-trap, and
  passthrough.

### Phase 3 — Governance UX (404-downgrade)
- **What:** map gateway **404 on a configured model** → `suggest_only` / "unavailable", reusing the
  unknown-model handler (S6). Distinguish gateway-unavailable (region/preview) → auto classic fallback.
- **Why:** entitlement failures are 404s (§3); today an unexpected 404 could surface as a hard error.
- **Surfaces:** error handling in the seam + the create/optimize surfaces that render failures.
- **Regression guard:** classic route error handling unchanged; the new 404 branch only augments. A test
  fires the de-entitled shape (404 body) and asserts downgrade, not raise.

---

## 7. Regression matrix (surface × guard × probe)

| Surface | Risk if wrong | Guard | Acceptance probe |
|---|---|---|---|
| Create Agent streaming+tools | broken agent loop | default-off flag; provider-agnostic terminator | `probe.sh` S4 (Claude+GPT) + multi-turn |
| GSO `call_llm` JSON output | optimizer stalls | keep `response_format` strip-retry (`:200`) | S3 + S3c |
| Embeddings firewall | leakage check silently off | classic SDK path retained; degrade path intact | S3b + preflight |
| Cost attribution | no spend visibility (the whole point) | tag at every site; grep contract | S5 + `--requery` |
| Entitlement | hard error instead of downgrade | 404→downgrade branch | `--leg403` (404 shape) |
| BYOK reasoning model | 400 on tools | `reasoning_effort:none` on tool path | `--byok` |
| Classic fallback | can't roll back | classic path untouched, flag-selected | S7 |
| Job param drift | run uses wrong route | 4-mirror lockstep (§4) | grep all 4 mirrors present |

## 8. Test plan

- **Probe suite is the integration gate.** Re-run `probe.sh` (full) + `--byok` before flipping the flag
  in any environment; attach the report to the PR.
- **Unit tests (ship in the same commit as each phase):** route factory (classic byte-identical +
  gateway shape), model mapping (strip rule, entity_name-trap, BYOK passthrough), tag header presence at
  all 4 chat sites, streaming terminator (both `[DONE]` and `finish_reason` endings), 404→downgrade.
- **Baseline:** do not regress `./scripts/test.sh` (681 backend + 1512 GSO). New modules add tests; bump
  the baseline line + playbook copy in the same commit (`test_rules_parity.py` enforces the two).
- **Contract grep (per repo rule):** after each phase, a repo-wide grep proving (a) no chat call site
  still hard-codes `/serving-endpoints/{...}/invocations` outside the classic branch of the seam, and
  (b) the tag header string appears at every gateway call site — pasted into the PR.

## 9. Decisions (resolved)

1. **Tag cardinality — RESOLVED: lean base + scoped extras.** Always emit
   `{"application":"genie-workbench","component":<feature>}`. Additionally attach `run_id` on
   GSO job-scoped calls (a run exists) and `space_id` on space-scoped calls (create-agent, iq-scan,
   mv-suggest); omit them where there is no such scope. `request_tags` is a MAP column, so this adds no
   schema cost and enables per-run / per-space cost rollups without polluting unscoped calls. The tag
   builder (§ Appendix A `request_tags`) takes optional `run_id`/`space_id` and drops empties.
2. **BYOK enumeration — RESOLVED: config-only for v1.** The customer enters the full 3-level UC model id
   in settings (or a per-space field); the picker accepts it via `validate_chat_model`. Auto-discovery
   is deferred: no list API surfaces Model Services today (absent from `serving-endpoints list`, UC
   `registered-models`; `ai-gateway/model-services` 404s). Tracked as a follow-up probe, not a blocker.
3. **SP-row confirmation — RESOLVED: DONE.** Requery confirmed the SP row as
   `requester_type=SERVICE_PRINCIPAL` (`leg403-1789344730`, `mlflow/v1/chat/completions`). No residual.
4. **Default flip — RESOLVED: stays `classic` until soak criteria met.** `GENIE_LLM_ROUTE` defaults
   `classic`. Flipping the default to `gateway` is its **own later PR** (changing only the default + the
   baseline note), gated on ALL of: (i) one environment soaked ≥7 days on `gateway` with attribution
   rows verified via `--requery`; (ii) full `probe.sh` + `--byok` green post-deploy in that env;
   (iii) both the OBO-user and SP legs confirmed there; (iv) zero create/optimize regressions observed.

---

## Appendix A — Seam interface (Phase 0 module + Phase 1 adoption)

Concrete signatures for the factory (§1). The Workbench app and the GSO wheel do **not** share a Python
package, so this lands as **two byte-identical small modules** — `backend/services/llm_route.py` and
`packages/genie-space-optimizer/src/genie_space_optimizer/optimization/llm_route.py` — pinned by a
parity test (same mechanism as `test_rules_parity.py`). All logic is pure/stateless and unit-testable
with no network.

### A.1 Phase 0 — the module (no call site touched yet)

```python
from __future__ import annotations
import json, os
from dataclasses import dataclass
from enum import Enum

class LLMRoute(str, Enum):
    CLASSIC = "classic"
    GATEWAY = "gateway"

_GATEWAY_CHAT  = "/ai-gateway/mlflow/v1/chat/completions"
_GATEWAY_EMBED = "/ai-gateway/mlflow/v1/embeddings"
_TAG_HEADER    = "Databricks-Ai-Gateway-Request-Tags"
_APPLICATION   = "genie-workbench"

def get_llm_route() -> LLMRoute:
    """GENIE_LLM_ROUTE env; anything but 'gateway' (incl. unset/typo) -> CLASSIC (fail-safe)."""
    return LLMRoute.GATEWAY if (os.environ.get("GENIE_LLM_ROUTE") or "").strip().lower() == "gateway" \
        else LLMRoute.CLASSIC

def gateway_model_name(endpoint: str) -> str:
    """Classic endpoint name -> gateway model id.
       '<cat>.<sch>.<name>' (BYOK 3-level UC id) -> unchanged;
       already 'system.ai.*' -> unchanged;
       'databricks-<x>' -> 'system.ai.<x>' (validated for chat AND embeddings)."""
    if "." in endpoint or endpoint.startswith("system.ai."):
        return endpoint
    return "system.ai." + endpoint.removeprefix("databricks-")

def request_tags(component: str, *, run_id: str | None = None,
                 space_id: str | None = None) -> dict[str, str]:
    tags = {"application": _APPLICATION, "component": component}
    if run_id:   tags["run_id"] = run_id       # scoped extras (Decision 1); empties dropped
    if space_id: tags["space_id"] = space_id
    return tags

def tag_header(component: str, **scope) -> dict[str, str]:
    return {_TAG_HEADER: json.dumps(request_tags(component, **scope))}

@dataclass(frozen=True)
class ResolvedCall:
    url: str                        # full endpoint URL ("" == "keep the legacy SDK path", embeddings)
    model: str | None               # model id for the BODY (gateway); None when it's in the URL (classic)
    extra_headers: dict[str, str]   # merge into the request (tag header on gateway; {} on classic)
    use_legacy_sdk: bool = False    # embeddings classic branch -> keep w.serving_endpoints.query()

def resolve_chat(host: str, endpoint: str, component: str, *, route: LLMRoute | None = None,
                 run_id: str | None = None, space_id: str | None = None) -> ResolvedCall:
    host = host.rstrip("/"); r = route or get_llm_route()
    if r is LLMRoute.CLASSIC:
        return ResolvedCall(f"{host}/serving-endpoints/{endpoint}/invocations", None, {})
    return ResolvedCall(f"{host}{_GATEWAY_CHAT}", gateway_model_name(endpoint),
                        tag_header(component, run_id=run_id, space_id=space_id))

def resolve_embeddings(host: str, endpoint: str, component: str, *, route: LLMRoute | None = None,
                       run_id: str | None = None, space_id: str | None = None) -> ResolvedCall:
    host = host.rstrip("/"); r = route or get_llm_route()
    if r is LLMRoute.CLASSIC:
        return ResolvedCall("", None, {}, use_legacy_sdk=True)   # keep SDK query()
    return ResolvedCall(f"{host}{_GATEWAY_EMBED}", gateway_model_name(endpoint),
                        tag_header(component, run_id=run_id, space_id=space_id))

def is_reasoning_effort_400(status: int, body_text: str) -> bool:
    """Retry trigger for the tool path (§2): Claude 400s on the flag, reasoning models require it."""
    return status == 400 and "reasoning_effort" in body_text
```

**Phase 0 tests (classic must be byte-identical to today):** `get_llm_route` default/typo → CLASSIC;
`resolve_chat(classic)` URL == the current `{host}/serving-endpoints/{model}/invocations` with `model
is None` and `extra_headers == {}`; `gateway_model_name` for strip-rule, `system.ai.*` idempotence,
and BYOK passthrough; `request_tags` drops empty scope.

### A.2 Phase 1 — adoption at each site (signature deltas + shape)

- **Site 1 `llm_utils.call_serving_endpoint` (`:86`)** — add a `component: str = "workbench"` param; then:
  `rc = resolve_chat(host, model, component)`; `url = rc.url`; put `rc.model` in the body only if set;
  `headers = {**auth_headers, **rc.extra_headers}`. 429 retry loop unchanged.
- **Site 2 `create_agent._stream_llm` (`:1006`)** — `rc = resolve_chat(host, effective_model,
  "create-agent", space_id=session.space_id)`; body gets `rc.model` if set; `session.post(rc.url,
  headers=rc.extra_headers, stream=True, …)`. Wrap the first attempt: on
  `is_reasoning_effort_400(status, text)` re-POST once with `body["reasoning_effort"]="none"`. The
  `[DONE]` break (`:1048`) stays.
- **Site 3 GSO `get_openai_client` (`:129`) + `call_llm` (`:136`)** — base_url becomes
  `f"{host}/ai-gateway/mlflow/v1"` and `default_headers=tag_header("gso-optimize", run_id=run_id)` on
  gateway (else today's `{host}/serving-endpoints`, no headers). `call_llm` maps
  `model=gateway_model_name(model)` on gateway and adds the `reasoning_effort` retry **next to** the
  existing `response_format` retry (`:200`) — same shape, one more fallback.
- **Site 4 `leakage.get_embedding` (`:200`)** — `rc = resolve_embeddings(host, ep, "leakage-embed")`;
  if `rc.use_legacy_sdk` keep `w.serving_endpoints.query(name=ep, input=[text])` verbatim; else raw
  `httpx.post(rc.url, json={"model": rc.model, "input":[text]}, headers={**auth, **rc.extra_headers})`
  and read `data[0].embedding` (validated dim=1024).
- **Site 5 `model_catalog.validate_chat_model` (`:90`)** — when `get_llm_route() is GATEWAY` and BYOK is
  enabled, accept a well-formed 3-level UC id in addition to the curated set (Decision 2).
