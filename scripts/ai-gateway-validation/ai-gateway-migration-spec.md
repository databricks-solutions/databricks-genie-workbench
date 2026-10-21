# AI Gateway migration — implementation spec

- **Status:** Implemented on `feature/ai-gateway-migration` behind default-off flags — Phases 0, 1, 1b,
  2 and 3 have landed (§6); not yet merged. This spec describes the **as-built** design; where a phase
  plan's recorded ruling changed the original proposal, the change is stated in place (rulings R1–R10
  live in the per-phase plans in this folder). What remains before merge and before the default flip
  is in §10 — including an open gap: the GSO job cannot run on the gateway route, because the run-now
  value is always `classic` and the job never reads the parameter anyway (§4).
- **Evidence:** pre-implementation validation (all gates PASS or a decided design choice) is in
  `report-20260913-175958.md` in this folder. It predates the code; a post-deploy re-run is still owed
  (§10).
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

Before this change the app had **five** independent places that built an LLM/embedding call, each
hard-coding the classic base URL and each authenticating differently. The change collapses the
*routing decision* (base URL + model name + tag header) into **one factory** (`llm_route.py`, two
byte-identical twins — Appendix A) consumed by all five.

### 1.1 The five call sites (as built)

| # | Surface | Classic branch (flag-off, unchanged) | Auth mechanism | Gateway branch (as built) |
|---|---|---|---|---|
| 1 | `backend/services/llm_utils.py:59` `call_serving_endpoint` | `httpx.post` → `{host}/serving-endpoints/{model}/invocations` | `client.config.authenticate()` headers | `resolve_chat(host, model, component)` (`:94`); tag merged into the `headers` dict; new `component` param, default `"workbench"` |
| 2 | `backend/services/create_agent.py:1118` `_stream_llm` (streaming + tools; `_async_stream_llm` at `:1222` wraps it) | SDK `session.post` → `.../invocations` | SDK pre-auth `requests.Session` | `resolve_chat(..., "create-agent", space_id=...)` (`:1146`); tag via `headers=` on `session.post` |
| 3 | `packages/…/optimization/llm_client.py:118` `get_openai_client` + `call_llm` (`:147`) | `OpenAI(base_url=f"{host}/serving-endpoints")` | bearer token on the OpenAI client | `base_url={host}/ai-gateway/mlflow/v1` (`:135-141`, client cached per `host\|route`); model mapped (`:184`); tag sent **per request** as `extra_headers=` on `chat.completions.create` (`:192-193`), not `default_headers` on the cached client (ruling R2) |
| 4 | `packages/…/optimization/leakage.py:207` `get_embedding` | SDK `w.serving_endpoints.query(name=ep, input=[text])` (`:233`) | SDK | raw `httpx.post` to `/ai-gateway/mlflow/v1/embeddings` (`:219-231`); branches on `get_llm_route()` so the classic path never reads `w.config` (ruling R3); new `component` kwarg, default `"leakage-embed"` |
| 5 | `backend/services/model_catalog.py:17` curated list + `validate_chat_model` (`:108`) | curated endpoint names only | n/a (name resolution) | additionally accepts a 3-level UC id when route=gateway **and** `GENIE_BYOK_ENABLED` (`:124-129`) — see §1.4 |

> Site 4 is the one behavioral nuance: the embeddings call goes through the SDK's typed
> `serving_endpoints.query()`, which does not expose a custom-header hook. To attribute embeddings the
> gateway branch issues a raw `POST /ai-gateway/mlflow/v1/embeddings` (validated: 200, dim=1024). Any
> non-200 or exception returns `None`, so the firewall's existing degradation path still applies.
> It shipped as its own phase (§6, Phase 1b).

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

`component` values emitted today (as built):

| `component` | Emitted by | Scoped extra |
|---|---|---|
| `create-agent` | `_stream_llm` (`create_agent.py:1146`) and its repair call to site 1 (`create_agent.py:730`) | `space_id` on the stream |
| `plan-builder` | `plan_builder.py:182` (site 1) | — |
| `gso-optimize` | GSO `call_llm` (`llm_client.py:193`) | `run_id` from `GSO_RUN_ID`, which the job notebooks export |
| `leakage-embed` | `get_embedding` default (`leakage.py:207`) | — |
| `mv-suggest` | metric-view scoring embeddings (`mv_scoring.py:366`) | — |
| `workbench` | `call_serving_endpoint` default, for any caller that doesn't pass one (`llm_utils.py:64`) | — |

The originally proposed `iq-scan` is not emitted: no call site passes it. Tag
cardinality (`run_id` / `space_id`) is settled in §9 Decision 1.

### 1.4 Model-name mapping

- **Curated `databricks-*` models** → `system.ai.` + name with the `databricks-` prefix stripped.
  Validated for chat (Claude + GPT) *and* embeddings (`databricks-gte-large-en` → `system.ai.gte-large-en`).
- **The trap:** never echo a UC `entity_name`/`full_name` (e.g. `gte_large_en_v1_5`) — those 404. Map
  the *endpoint* name, don't reuse the registered-model name.
- **BYOK models** → pass the customer's **full 3-level UC id** through unchanged (§2). The strip rule
  does not apply. As built, `validate_chat_model` (`model_catalog.py:108`) still accepts curated names
  first, and additionally accepts a well-formed `catalog.schema.name` only when **both**
  `GENIE_LLM_ROUTE=gateway` and `GENIE_BYOK_ENABLED` is truthy (`:124-129`). "Well-formed" is
  `^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+){2}$` (`:42`, ruling R5), which rejects the entity_name trap,
  2-level and 4-level ids. `GENIE_BYOK_ENABLED` (`app.yaml:107`, default `"false"`; truthy =
  `1/true/yes/on`, `model_catalog.py:46`) is an app-env gate only, not a job parameter (ruling R7).
  `list_chat_models` is unchanged: the dropdown stays curated-only (ruling R6).

---

## 2. BYOK / `reasoning_effort` handling

A customer's own Unity Gateway **Model Service** (`catalog.schema.name`) is **drop-in on the
Workbench's own path**: it answers `/ai-gateway/mlflow/v1/chat/completions` with the standard OpenAI
shape (validated: 200, backing `gpt-5.6-sol`). **No Responses↔chat adapter is required.**

Two BYOK rules the seam must encode:

1. **Arbitrary model ids.** The picker/`validate_chat_model` accepts full 3-level UC ids for BYOK; no
   strip rule, no curated-list gate. Because auto-enumeration of Model Services is a gap (they appear in
   neither `serving-endpoints list` nor UC `registered-models`, and `ai-gateway/model-services` 404s),
   **BYOK ids are user-configured**, not auto-populated. As built, the id arrives through the existing
   `llm_model` override that the create / auto-optimize APIs already accept; there is **no frontend
   entry field yet** (deferred, §10). Auto-discovery is a fast-follow once the correct list API is
   identified — it does not block attribution.

2. **`reasoning_effort:"none"` via retry, NOT always-send.** When a BYOK service is backed by a
   *reasoning* model (e.g. `gpt-5.6-sol`), function tools on `/chat/completions` return a hard **400**
   unless the request sends `reasoning_effort:"none"` (or uses `/responses`). **But you cannot just
   always send it:** validated — `system.ai.claude-sonnet-4-6` **400s** on `reasoning_effort` with
   `"Extra inputs are not permitted"`, while `gpt-5-4` accepts it. So the tool path uses a **retry
   pattern**: send the tool request plain; if the response is a **400 whose body mentions
   `reasoning_effort`**, retry once with `reasoning_effort:"none"`. This is model-version-specific
   (`gpt-5-4`/`gpt-5-2` need neither), so never assume GPT-family uniformity and never hard-code the
   flag on.

   **As built (ruling R1):** the retry exists **only at site 2** (`create_agent.py:1189-1195`), the one
   path that sends function tools. It is gated on route=gateway **and** `status_code == 400` *before*
   `resp.text` is read, so a 200 SSE stream is never buffered, and the retry POST sits outside the
   429/502/503 backoff loop. GSO `call_llm` does **not** carry it: it sends no `tools` (it uses
   `response_format` JSON mode), so the 400 is unreachable there. If `call_llm` ever gains a `tools`
   parameter, add the retry next to its `response_format` strip-retry at the same time.

---

## 3. The 404-downgrade contract (entitlement)

**Entitlement failures surface as `404 NOT_FOUND`, not `403`.** Validated: an un-entitled SP calling
`…pbi_migration.test1` gets `404 "…does not exist"`, while an entitled user gets 200 on the same
service. The gateway deliberately **hides** Model Services the caller can't access.

Contract for the app:

- The downgrade **cannot be keyed on 403** — a de-entitled identity never produces one. A **404 on a
  model the app believes is configured** MUST be treated as *not-entitled / unavailable*, not dumped
  as a raw provider error.
- This **collapses onto the same handler as unknown-model** (S6 also returns 404). Fewer new branches.
- UX copy: "model unavailable or access not granted" — the two cases are indistinguishable at the HTTP
  layer, and asserting one over the other would be wrong.
- Do **not** probe `GET /permissions/{id}` to pre-check entitlement — it reports the SP's privileges,
  not the user's. The live call is the entitlement test.

**As built (Phase 3; scope decision 2026-09-25 — clean-downgrade messaging only):**

- The seam adds `MODEL_UNAVAILABLE_MESSAGE = "Model unavailable or access not granted."`
  (`llm_route.py:77`) and `is_model_unavailable_404(status, body_text)` (`:80`). The check is
  `status == 404` alone, and the body is ignored (ruling R8). This is safe because every caller
  first checks route=gateway, and on the fixed gateway chat URL a 404 can only mean
  model-not-found or not-entitled.
- "Downgrade" means **raise a clean `RuntimeError(MODEL_UNAVAILABLE_MESSAGE)`** in place of the
  raw-body error (ruling R9). A failed chat call has no answer to return. The existing failure
  surfaces render it unchanged in shape: the Create Agent SSE `error` event, any
  `call_serving_endpoint` caller, and the GSO optimize task's failed-phase status row.
- Sites: `llm_utils.py:124-127`; `create_agent.py:1198-1201`, inside `if not resp.ok:` so a 200 is
  never read; GSO `call_llm`'s final raise at `llm_client.py:232-236`, which changes the message
  only, with no change to the retry loop (ruling R10). Embeddings (site 4) already return `None` on
  any failure and are unchanged.
- Gateway-only and 404-only. A classic-route 404 (e.g. a mistyped curated endpoint) and every other
  non-200 keep today's raw-body error, byte for byte.
- **Not built, by decision:** no automatic fallback to the classic route on a gateway failure, and no
  coupling to the metric-view advisor's `suggest_only` governance (`mv_suggest.py` untouched). The
  original Phase 3 wording proposed both; the 2026-09-25 decision dropped them.

---

## 4. The 4-mirror config lockstep

`GENIE_LLM_ROUTE` reaches the GSO job as a parameter, so it follows the existing **`llm_model`
precedent exactly** — the closest analog (an app/operator-selected value that must reach every task).
A param present in fewer than all its places is a bug.

| # | File | As built (`genie_llm_route`, mirroring `llm_model`) |
|---|---|---|
| 1. Root bundle | `databricks.yml` | job param declared (`:115`, beside `llm_model` at `:110`) and passed through on every task's `base_parameters` (`:157/177/199/229`) |
| 2. Deploy lib | `scripts/deploy_lib/gso_job.py` | in all four per-task param lists (`:47/57/69/84`); default `"classic"` in the default map (`:112`). Unlike `llm_model`, it has no `cfg` override (`:272`), so a deployed job defaults to classic |
| 3. Run-now map | `packages/…/backend/job_launcher.py` `submit_optimization` | `genie_llm_route` kwarg, default `"classic"` (`:78`); `job_parameters` value is `genie_llm_route or os.getenv("GENIE_LLM_ROUTE", "classic")` (`:132`). The original spec named `trigger.py` here; the launcher is where `run_now` parameters are actually assembled. **Defect:** the only caller, `integration/trigger.py:396`, passes no route, and the kwarg default `"classic"` is truthy, so the app-env fallback is unreachable and every run is sent `classic` (see open gap below). `llm_model` avoids this because its default is `""` (`:77`) |
| 4. App env (non-job surfaces) | `app.yaml` | `GENIE_LLM_ROUTE`, value `"classic"` (`:98`). `GENIE_BYOK_ENABLED` (`:107`) sits beside it but is app-only, not a job param (§1.4) |

> **Note (verified):** `llm_model` is intentionally **NOT** in `packages/genie-space-optimizer/databricks.yml`
> — `gso_job.py:32` documents it as "a Workbench-specific extra beyond the package bundle's params."
> So a new app-selected route param follows the `llm_model` set above, **not** the generic four-mirror
> list in the mv-advisor rule (which governs mv-advisor's own params). **Still open:** if the
> notebook-install path deploys the job from the package bundle, confirm whether the package
> `databricks.yml` also needs the param before turning the flag on for that path. Today it has
> neither `llm_model` nor `genie_llm_route`.

> **OPEN GAP — the GSO job can never run on the gateway route.** Two independent breaks, both
> needed fixes:
>
> **(a) The run-now value is always `classic`.** As row 3 shows, `submit_optimization` sends
> `"classic"` unless a caller passes the kwarg, and none does. `backend/tests/test_deploy_lib.py:1331`
> pins `launcher_params.get("genie_llm_route") == "classic"`, so the test enshrines the default
> rather than catching it. Fix: default the kwarg to `""` (the `llm_model` shape) so the app's
> `GENIE_LLM_ROUTE` reaches the run, and add a test that sets the app env to `gateway` and asserts
> the run parameter follows.
>
> **(b) The job never consumes the parameter.** Even when delivered, no job notebook reads
> `genie_llm_route`. `llm_model` works because each
> notebook does `dbutils.widgets.text("llm_model", "")` and then sets `os.environ["LLM_MODEL"]`
> (`jobs/run_intake_and_snapshot.py:94,111,121`, `jobs/run_benchmark_qc_and_repair.py:142,154,156`,
> `jobs/run_optimize.py:96,114,138`). The seam's `get_llm_route()` reads only
> `os.environ["GENIE_LLM_ROUTE"]` (`llm_route.py:18`), and no notebook sets it. Fix: the `llm_model`
> pattern, meaning a `genie_llm_route` widget in every notebook that calls an LLM, exported to
> `GENIE_LLM_ROUTE`, plus a test pinning it.
>
> **Result today:** inside the GSO job, sites 3 and 4 always take the classic route, so
> `gso-optimize`, `leakage-embed` and `mv-suggest` traffic from the job is never tagged. The
> Workbench-side sites (1, 2, 5) are unaffected because they read the app env directly. No phase plan
> scheduled either fix; both must land before the flag is turned on for the job (§10).

---

## 5. Non-goals (explicit out-of-scope for the first cut)

- No Responses-API adapter (chat/completions is drop-in for BYOK too — §2).
- No auto-enumeration of BYOK Model Services (config field now; discovery later).
- No change to the retry/backoff logic — existing 429 retry is retained as-is (validated headroom).
- No `response_format:json_object` dependency — it 400s on **both** paths in the test workspace; the
  existing strip-and-retry fallback (`llm_client.py:218-226`) is the real path and works on the gateway.
- No new MLflow experiments/judges (out of scope, unchanged).
- No automatic fallback to the classic route when the gateway fails, and no change to the metric-view
  advisor's `suggest_only` governance (Phase 3 scope decision, §3).
- No frontend BYOK entry field in v1 (§2).

---

## 6. Roadmap (reviewable) — what changes, why, where, and how we prevent regression

> Read this section top-to-bottom to understand the whole change. Each phase is independently
> shippable, default-off until the flag flips, and gated by re-running `probe.sh` + the unit suites.
> Every phase below has **landed** on `feature/ai-gateway-migration`; each carries a **Status** line
> with its commits and per-phase plan. The plans' checkboxes were not ticked as work landed, so the
> commits are the record.

### Phase 0 — Seam scaffold (no behavior change)  → concrete signatures in **Appendix A.1**
- **Status:** landed 2026-09-14 — `b19e4b58`, `db2bbd27`, `add2f557`, `573127b0`
  (`ai-gateway-phase0-plan.md`). Parameter declared in every mirror, but the run-now value is stuck
  at `classic` and the job never reads it (§4 open gap).
- **What:** add `GENIE_LLM_ROUTE` (default `classic`) and the `resolve_chat` / `resolve_embeddings`
  factory returning a `ResolvedCall(url, model, extra_headers)`. Nothing calls it yet.
- **Why:** land the decision point in one place before touching hot paths.
- **Surfaces:** `backend/services/llm_route.py` + byte-identical GSO twin, pinned by
  `backend/tests/test_llm_route_parity.py`; `app.yaml` env; the config mirrors (§4).
- **Regression guard:** flag defaults classic → the factory returns today's exact classic values.
  Unit tests assert `classic` output byte-for-byte matches the current URL/headers. No call site changed yet.

### Phase 1 — Route the 3 chat sites through the seam (+ tags)  → adoption deltas in **Appendix A.2**
- **Status:** landed 2026-09-14 — `86877183`, `d2dfcbae`, `6b597b77`, `b12c4ba5`, `0d2aba0f`
  (`ai-gateway-phase1-plan.md`, rulings R1–R2).
- **What:** sites 1–3 (llm_utils, create_agent `_stream_llm`, GSO `get_openai_client` / `call_llm`) call
  the factory and inject the tag header per transport (§1.1). This is the unit that delivers attribution.
- **Why:** attribution needs the gateway path; can't be decoupled from the route swap.
- **Surfaces:** `llm_utils.py:59`, `create_agent.py:1118`, `llm_client.py:118/147`; component tags at the
  two site-1 callers (`create_agent.py:730`, `plan_builder.py:182`). Streaming+tools is the gating shape.
- **Regression guard:**
  - Default-off: with `classic`, these three are unchanged.
  - Streaming parser already provider-agnostic — the `[DONE]` break (`create_agent.py:1213`) is retained,
    and GPT's "no `[DONE]`, close-on-`finish_reason`" ending is handled by the natural `iter_lines`
    end (validated). **No parser change needed**, but a test pins both terminations.
  - Tool path uses the `reasoning_effort` retry (§2), NOT always-send, at site 2 only (R1). It is
    gated on gateway + 400 so a 200 SSE body is never read (`0d2aba0f`). A test pins that Claude
    (rejects the flag) and a reasoning model (requires it) both succeed through the one retry.
  - Re-run `probe.sh` S1–S4 + S7 before flipping the flag in any environment.

### Phase 1b — Embeddings site (separate diff)
- **Status:** landed 2026-09-14 — `447923c9`, `b1755f2d` (`ai-gateway-phase1b-plan.md`, ruling R3).
- **What:** site 4 (`leakage.get_embedding`) switches from SDK `serving_endpoints.query()` to a raw
  `POST /ai-gateway/mlflow/v1/embeddings` under the flag, so the tag header can be attached. Review
  added a `component` kwarg so metric-view scoring tags as `mv-suggest` (`mv_scoring.py:366`).
- **Why:** different transport; can't tag the typed SDK call.
- **Surfaces:** `leakage.py:207`. The firewall preflight (`preflight_embedding_endpoint`, `:281`)
  calls `get_embedding`, so it follows the route without its own change.
- **Regression guard:** classic branch keeps the SDK call verbatim; gateway branch validated (dim=1024).
  Firewall degradation path (embeddings disabled → n-gram/fingerprint) is untouched and still the
  failure fallback.

### Phase 2 — Model catalog + BYOK
- **Status:** landed 2026-09-14 — `00ca9a4a`, `2d35a7a0` (`ai-gateway-phase2-plan.md`, rulings R5–R7).
- **What:** `validate_chat_model` (`model_catalog.py:108`) stops hard-rejecting a well-formed 3-level UC
  id when route=gateway **and** the new `GENIE_BYOK_ENABLED` flag is on (§1.4). The curated →
  `system.ai.*` mapping lives in the seam (`gateway_model_name`, `llm_route.py:24`), not in
  `model_catalog`. No `reasoning_effort` change here: that is the Phase 1 retry (§2), never
  always-send.
- **Why:** the real "can the customer use their own model?" answer.
- **Surfaces:** `model_catalog.py:42/46/108`; `app.yaml:107`. Backend only: no frontend entry field,
  and `list_chat_models` is unchanged (R6).
- **Regression guard:** curated-list behavior on the classic route is unchanged; BYOK acceptance is
  strictly additive and gated on route=gateway **and** the BYOK flag. Unit tests for strip-rule,
  entity_name-trap, passthrough, and the flag's truthy/falsy variants.

### Phase 3 — Clean 404 downgrade (governance UX)
- **Status:** landed 2026-09-25 — `40b41c69`, `eaf75349`, `02c0ed33` (`ai-gateway-phase3-plan.md`,
  rulings R8–R10). The plan's Task 4 verification (full suite, contract greps, untouched-surface diff)
  has no recorded result. Run it for the PR (§10).
- **What:** on the gateway route, a **404 on a configured model** raises the single standardized
  `MODEL_UNAVAILABLE_MESSAGE` in place of the raw provider body, at sites 1, 2 and 3 (§3). Scope
  decision 2026-09-25: **messaging only**. There is no automatic classic fallback and no
  `suggest_only` coupling, though the original wording here proposed both.
- **Why:** entitlement failures are 404s (§3); without this a de-entitled user sees a raw error body.
- **Surfaces:** seam (`llm_route.py:77/80`, both twins) + `llm_utils.py:124-127`,
  `create_agent.py:1198-1201`, `llm_client.py:232-236`.
- **Regression guard:** classic route error handling unchanged; only gateway 404 is affected. Tests
  fire the de-entitled 404 shape and assert `str(exc) == MODEL_UNAVAILABLE_MESSAGE`. They also pin that
  a classic 404 and a gateway 500 keep the raw error, and that a 200 stream never reads `resp.text`.

---

## 7. Regression matrix (surface × guard × probe)

| Surface | Risk if wrong | Guard | Acceptance probe |
|---|---|---|---|
| Create Agent streaming+tools | broken agent loop | default-off flag; provider-agnostic terminator | `probe.sh` S4 (Claude+GPT) + multi-turn |
| GSO `call_llm` JSON output | optimizer stalls | keep `response_format` strip-retry (`llm_client.py:218-226`) | S3 + S3c |
| Embeddings firewall | leakage check silently off | classic SDK path retained; degrade path intact | S3b + preflight |
| Cost attribution | no spend visibility (the whole point) | tag at every site; grep contract | S5 + `--requery` |
| Entitlement | raw provider error instead of a clean message | gateway-gated 404 → `MODEL_UNAVAILABLE_MESSAGE` (§3) | `--leg403` (404 shape) |
| BYOK reasoning model | 400 on tools | one-shot `reasoning_effort:"none"` retry on the create-agent tool path (§2) | `--byok` |
| Classic fallback | can't roll back | classic path untouched, flag-selected | S7 |
| Job param drift | run uses wrong route | mirror lockstep (§4) **plus** a job-side widget → `GENIE_LLM_ROUTE` export (not yet built, §4 open gap) | grep all mirrors present **and** every LLM-calling job notebook reads the widget |

## 8. Test plan

- **Probe suite is the integration gate.** Re-run `probe.sh` (full) + `--byok` before flipping the flag
  in any environment; attach the report to the PR.
- **Unit tests (shipped in the same commit as each phase):** route factory (classic byte-identical +
  gateway shape) and twin parity, model mapping (strip rule, entity_name-trap, BYOK passthrough, BYOK
  flag variants), tag header presence at the three chat sites and the embeddings site, streaming
  terminator (both `[DONE]` and `finish_reason` endings), the `reasoning_effort` retry, and
  gateway-404 → `MODEL_UNAVAILABLE_MESSAGE` (plus classic-404 / gateway-500 unchanged).
- **Baseline:** `./scripts/test.sh` measured **4012 passed** on 2026-10-20 at the merge commit
  `941aed5b`, which builds this branch on PR #332 (`feature/metric-view-advisor`, itself carrying
  main). One combined run of both suites; before the merge the branch tip `12d574a8` measured 2818.
  Below this is a regression. The mv-advisor rule's per-suite baseline line is not maintained by this
  initiative (Phase 3 plan constraint).
- **Contract grep (per repo rule):** after each phase, a repo-wide grep proving (a) no chat call site
  still hard-codes `/serving-endpoints/{...}/invocations` outside the classic branch of the seam, and
  (b) the tag header string appears at every gateway call site — pasted into the PR.

## 9. Decisions (resolved)

1. **Tag cardinality — RESOLVED: lean base + scoped extras.** Always emit
   `{"application":"genie-workbench","component":<feature>}`. Additionally attach `run_id` on
   GSO job-scoped calls (a run exists) and `space_id` on space-scoped calls; omit them where there is
   no such scope. As built, only the create-agent stream carries `space_id` and only GSO `call_llm`
   carries `run_id` (§1.3). The `mv-suggest` embedding call carries neither. `request_tags` is a MAP column, so this adds no
   schema cost and enables per-run / per-space cost rollups without polluting unscoped calls. The tag
   builder (§ Appendix A `request_tags`) takes optional `run_id`/`space_id` and drops empties.
2. **BYOK enumeration — RESOLVED: config-only for v1.** The customer supplies the full 3-level UC model
   id through the existing `llm_model` override, and `validate_chat_model` accepts it behind
   `GENIE_BYOK_ENABLED` (§1.4). A settings / per-space entry field is not built yet (§10). Auto-discovery
   is deferred: no list API surfaces Model Services today (absent from `serving-endpoints list`, UC
   `registered-models`; `ai-gateway/model-services` 404s). Tracked as a follow-up probe, not a blocker.
3. **SP-row confirmation — RESOLVED: DONE.** Requery confirmed the SP row as
   `requester_type=SERVICE_PRINCIPAL` (`leg403-1789344730`, `mlflow/v1/chat/completions`). No residual.
4. **Default flip — RESOLVED: stays `classic` until soak criteria met.** `GENIE_LLM_ROUTE` defaults
   `classic`. Flipping the default to `gateway` is its **own later PR** (changing only the default + the
   baseline note), gated on ALL of: (i) one environment soaked ≥7 days on `gateway` with attribution
   rows verified via `--requery`; (ii) full `probe.sh` + `--byok` green post-deploy in that env;
   (iii) both the OBO-user and SP legs confirmed there; (iv) zero create/optimize regressions observed.
5. **Phase 3 scope — RESOLVED 2026-09-25: clean-downgrade messaging only.** AI Gateway stays the
   primary path. A gateway 404 shows `MODEL_UNAVAILABLE_MESSAGE`. There is no automatic classic
   fallback and no coupling to the metric-view advisor's `suggest_only` governance (§3).

---

## 10. Remaining work

**Before merge**

1. **Make the GSO job able to use the gateway (§4 open gap, both parts).** (a) Default
   `submit_optimization`'s `genie_llm_route` to `""` so the app's `GENIE_LLM_ROUTE` reaches the run,
   and update the `test_deploy_lib.py` assertion that pins `"classic"`. (b) Add a `genie_llm_route`
   widget to every job notebook that calls an LLM and export it to `GENIE_LLM_ROUTE`, with a test.
   Until both land, no job traffic is routed or tagged.
2. **Package bundle question (§4 note).** Decide whether `packages/genie-space-optimizer/databricks.yml`
   needs `genie_llm_route` for the notebook-install path.
3. **Phase 3 Task 4 verification.** Full `./scripts/test.sh`, the two contract greps (§8), and an
   empty diff on the untouched surfaces. Paste the results into the PR.
4. **Bring in `main` and open the PR.** No PR exists yet. `main` has moved since the branch point,
   and the files changed on both sides include `create_agent.py` (site 2) and the job-config files
   that carry `genie_llm_route` (`databricks.yml`, the package `databricks.yml`, `gso_job.py`).
5. **Post-deploy probe run.** Run `probe.sh` (full) + `--byok` against a deployment of this branch and
   attach the report (§8). The only report on file predates the code.

**After merge**

6. **Default flip** to `gateway`, as its own PR, once Decision 4's soak criteria are met.

**Deferred (not needed for v1)**

7. Frontend BYOK entry field (settings / per-space).
8. Auto-enumeration of BYOK Model Services, once a list API exists (Decision 2).
9. `reasoning_effort` retry in GSO `call_llm`, only if it gains a `tools` parameter (§2, R1).

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

# --- added in Phase 3 (§3) ---
MODEL_UNAVAILABLE_MESSAGE = "Model unavailable or access not granted."

def is_model_unavailable_404(status: int, body_text: str) -> bool:
    """Downgrade trigger (§3): the gateway returns 404 for BOTH not-entitled and
    unknown-model (indistinguishable at the HTTP layer). Body-agnostic; callers
    MUST gate on the gateway route so a classic 404 keeps its raw-body error."""
    return status == 404
```

The block above matches `backend/services/llm_route.py` as built, apart from blank-line spacing.
`backend/tests/test_llm_route_parity.py` keeps the GSO twin byte-identical to it.

**Phase 0 tests (classic must be byte-identical to today):** `get_llm_route` default/typo → CLASSIC;
`resolve_chat(classic)` URL == the current `{host}/serving-endpoints/{model}/invocations` with `model
is None` and `extra_headers == {}`; `gateway_model_name` for strip-rule, `system.ai.*` idempotence,
and BYOK passthrough; `request_tags` drops empty scope.

### A.2 Adoption at each site (as built, Phases 1–3)

- **Site 1 `llm_utils.call_serving_endpoint` (`:59`)** — new `component: str = "workbench"` param
  (`:64`); `rc = resolve_chat(host, model, component)` (`:94`); `rc.model` goes in the body only if
  set; `headers = {**auth_headers, **rc.extra_headers}`. 429 retry loop unchanged. Phase 3: on
  gateway, a 404 raises `MODEL_UNAVAILABLE_MESSAGE` before the raw-body raise (`:124-127`).
- **Site 2 `create_agent._stream_llm` (`:1118`)** — `rc = resolve_chat(host, effective_model,
  "create-agent", space_id=space_id)` (`:1146`); body gets `rc.model` if set; `session.post(url,
  json=body, stream=True, timeout=120, headers=rc.extra_headers)`. After the 429/502/503 backoff
  loop, a single `reasoning_effort` retry gated on gateway **and** `status_code == 400` (checked
  before `resp.text` is read) re-POSTs with `reasoning_effort:"none"` (`:1189-1195`). Phase 3: inside
  `if not resp.ok:`, a gateway 404 raises `MODEL_UNAVAILABLE_MESSAGE` (`:1198-1201`). The `[DONE]`
  break (`:1213`) stays. `_async_stream_llm` (`:1222`) delegates here, so it inherits all of this.
- **Site 3 GSO `get_openai_client` (`:118`) + `call_llm` (`:147`)** — `base_url` is
  `{host}/ai-gateway/mlflow/v1` on gateway, else `{host}/serving-endpoints` (`:135-138`); the client
  cache key is `host|route`. `call_llm` maps `model = gateway_model_name(model)` on gateway (`:184`)
  and sends the tag **per request** as `extra_headers=tag_header("gso-optimize",
  run_id=os.getenv("GSO_RUN_ID") or None)` (`:192-193`, R2), not as `default_headers` on the cached
  client. **No `reasoning_effort` retry** here (R1: no tools are sent). The `response_format`
  strip-retry (`:218-226`) is unchanged. Phase 3: on gateway, a final error with `status_code == 404`
  re-raises as `RuntimeError(MODEL_UNAVAILABLE_MESSAGE) from last_err` (`:232-236`).
- **Site 4 `leakage.get_embedding` (`:207`)** — new keyword `component="leakage-embed"`. It branches
  on `get_llm_route()` rather than `rc.use_legacy_sdk`, so the classic path never touches
  `w.config` (R3). On gateway it calls `resolve_embeddings(host, ep, component, route=route)` and then
  `httpx.post(rc.url, json={"model": rc.model, "input":[text]}, headers={**w.config.authenticate(),
  **rc.extra_headers})` (`:219-231`). Classic keeps `w.serving_endpoints.query(name=ep, input=[text])`
  verbatim (`:233`). Any non-200 or exception returns `None`. Validated dim=1024.
- **Site 5 `model_catalog.validate_chat_model` (`:108`)** — curated names first; then, only when
  `get_llm_route() is GATEWAY` **and** `GENIE_BYOK_ENABLED` is truthy, a well-formed 3-level UC id
  (`_UC_MODEL_ID_RE`, `:42`) is returned verbatim (`:124-129`). Otherwise `ModelValidationError`.
