# AI Gateway migration — implementation spec

- **Status:** Implemented on `feature/ai-gateway-migration`, built on PR #332
  (`feature/metric-view-advisor`) and reviewed as the stacked PR #400; not yet merged. Phases 0, 1, 1b,
  2 and 3 landed default-off (§6); the gateway is now the **default from the start** (Decision 4), the
  GSO job follows the app's route (§4), and both installers carry the route and BYOK settings plus an
  install-time gateway check (§4.1). This spec describes the **as-built** design; where a phase
  plan's recorded ruling changed the original proposal, the change is stated in place (rulings R1–R10
  live in the per-phase plans in this folder). What remains is the fevm-serverless smoke test (§10).
- **Evidence:** pre-implementation validation is in `report-20260913-175958.md`; the re-run on
  fevm-serverless on 2026-10-20, which decided the default (GO: 18 pass, 0 fail, the two known
  `response_format` warnings), is in `report-20261020-191745.md`. Both predate a deployment of the
  branch; the deployed-app run is the smoke test (§10).
- **Reproduce the evidence:** `probe.sh --profile <P> [--warehouse-id <id>]` (full suite),
  `probe.sh --profile <P> --byok <catalog.schema.name>` (BYOK / S9).
- **Placement note:** kept next to its validation artifacts on purpose. This is an implementation spec
  for a *new* initiative — it is not an mv-advisor doc and rides no doc-freeze.

---

## 0. Decision (TL;DR)

Route Workbench + GSO LLM/embedding traffic through the **Unity AI Gateway** unified path
(`/ai-gateway/mlflow/v1/...`) with a per-request cost-attribution tag, selected by a single setting
`GENIE_LLM_ROUTE={gateway|classic}` that **defaults to `gateway`**. The classic
`/serving-endpoints/{name}/invocations` path stays available, but only as an explicit operator
setting (`llm_route=classic` at install); nothing falls back to it automatically.

**Why now:** the only mechanism that records Workbench spend for attribution
(`system.ai_gateway.usage.request_tags`) captures **gateway traffic only** — the classic
`/invocations` path is *not* recorded (validated: 3,110 gateway rows vs 0 `/invocations` rows over 2h).
So "tag the current path, migrate later" is impossible; the route swap and the tag ship together.

**Why it's safe:** Unity AI Gateway is GA; every request shape the app makes was validated on the
gateway (streaming+tools on Claude *and* GPT, embeddings, JSON, 429, BYOK), most recently on
2026-10-20; one seam → one place to reason about; the install fails up front, with a pointer to
`llm_route=classic`, when the workspace cannot reach the gateway (§4.1); and the classic path is
proven still-green (S7) for operators who choose it.

---

## 1. The seam — one client factory (route + tags + model mapping)

Before this change the app had **five** independent places that built an LLM/embedding call, each
hard-coding the classic base URL and each authenticating differently. The change collapses the
*routing decision* (base URL + model name + tag header) into **one factory** (`llm_route.py`, two
byte-identical twins — Appendix A) consumed by all five.

### 1.1 The five call sites (as built)

| # | Surface | Classic branch (`llm_route=classic`, unchanged) | Auth mechanism | Gateway branch (as built) |
|---|---|---|---|---|
| 1 | `backend/services/llm_utils.py:87` `call_serving_endpoint` | `httpx.post` → `{host}/serving-endpoints/{model}/invocations` | `client.config.authenticate()` headers | `resolve_chat(host, model, component)` (`:122`); tag merged into the `headers` dict; new `component` param, default `"workbench"` |
| 2 | `backend/services/create_agent.py:1118` `_stream_llm` (streaming + tools; `_async_stream_llm` at `:1244` wraps it) | SDK `session.post` → `.../invocations` | SDK pre-auth `requests.Session` | `resolve_chat(..., "create-agent", space_id=...)` (`:1146`); tag via `headers=` on `session.post` |
| 3 | `packages/…/optimization/llm_client.py:118` `get_openai_client` + `call_llm` (`:170`) | `OpenAI(base_url=f"{host}/serving-endpoints")` | bearer token on the OpenAI client | `base_url={host}/ai-gateway/mlflow/v1` (`:135-141`, client cached per `host\|route`); model mapped and tag built by `resolve_gso_chat` (`:147-159`); tag sent **per request** as `extra_headers=` on `chat.completions.create` (spread at `:210`), not `default_headers` on the cached client (ruling R2) |
| 4 | `packages/…/optimization/leakage.py:207` `get_embedding` | SDK `w.serving_endpoints.query(name=ep, input=[text])` (`:233`) | SDK | raw `httpx.post` to `/ai-gateway/mlflow/v1/embeddings` (`:219-231`); branches on `get_llm_route()` so the classic path never reads `w.config` (ruling R3); new `component` kwarg, default `"leakage-embed"` |
| 5 | `backend/services/model_catalog.py:17` curated list + `validate_chat_model` (`:108`) | curated endpoint names only | n/a (name resolution) | additionally accepts a 3-level UC id when route=gateway **and** `GENIE_BYOK_ENABLED` (`:124-129`) — see §1.4 |

> Site 4 is the one behavioral nuance: the embeddings call goes through the SDK's typed
> `serving_endpoints.query()`, which does not expose a custom-header hook. To attribute embeddings the
> gateway branch issues a raw `POST /ai-gateway/mlflow/v1/embeddings` (validated: 200, dim=1024). Any
> non-200 or exception returns `None`, so the firewall's existing degradation path still applies.
> It shipped as its own phase (§6, Phase 1b).

### 1.2 Route resolution

```
GENIE_LLM_ROUTE = gateway (default; unset, empty or unknown) | classic (explicit only)

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
| `gso-optimize` | GSO `call_llm` and `optimizer_utils._traced_llm_call`, both through `resolve_gso_chat` (`llm_client.py:158`) | `run_id` from `GSO_RUN_ID`, which the job notebooks export |
| `leakage-embed` | `get_embedding` default (`leakage.py:207`) | — |
| `mv-suggest` | metric-view scoring embeddings (`mv_scoring.py:377`) | — |
| `workbench` | `call_serving_endpoint` default, for any caller that doesn't pass one (`llm_utils.py:92`) | — |

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
  2-level and 4-level ids. `GENIE_BYOK_ENABLED` (`app.yaml:111-112`, installer default `"false"`; truthy =
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

   **As built (ruling R1):** the retry exists **only at site 2** (`create_agent.py:1210-1217`), the one
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
  (`llm_route.py:86`) and `is_model_unavailable_404(status, body_text)` (`:89`). The check is
  `status == 404` alone, and the body is ignored (ruling R8). This is safe because every caller
  first checks route=gateway, and on the fixed gateway chat URL a 404 can only mean
  model-not-found or not-entitled.
- "Downgrade" means **raise a clean `RuntimeError(MODEL_UNAVAILABLE_MESSAGE)`** in place of the
  raw-body error (ruling R9). A failed chat call has no answer to return. The existing failure
  surfaces render it unchanged in shape: the Create Agent SSE `error` event, any
  `call_serving_endpoint` caller, and the GSO optimize task's failed-phase status row.
- Sites: `llm_utils.py:151-153`; `create_agent.py:1220-1223`, inside `if not resp.ok:` so a 200 is
  never read; GSO `call_llm`'s final raise through `raise_if_model_unavailable` (`llm_client.py:162-167`, called at `:249`; `_traced_llm_call` calls it too), which changes the message
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
| 1. Root bundle | `databricks.yml` | bundle variable `genie_llm_route`, default `"gateway"` (`:22-24`), set by `deploy.sh --var` (`scripts/deploy.sh`); job param `default: ${var.genie_llm_route}` (`:119-120`, beside `llm_model` at `:113-114`) and passed through on every task's `base_parameters` (`:161/181/203/233`) |
| 2. Deploy lib | `scripts/deploy_lib/gso_job.py` | in all four per-task param lists (`:49/59/71/86`); default `"gateway"` in the default map (`:114`), overridden with `cfg.llm_route` by `build_job_settings` (`:255`), so the notebook-installed job carries the operator's choice |
| 3. Run-now map | `packages/…/backend/job_launcher.py` `submit_optimization` | `genie_llm_route` kwarg, default `""` (`:78`, the `llm_model` shape at `:77`); `job_parameters` value is `genie_llm_route or os.getenv("GENIE_LLM_ROUTE") or "gateway"` (`:132`). The only caller, `integration/trigger.py:384`, passes no route, so every run is sent the app's own `GENIE_LLM_ROUTE` |
| 4. App env (non-job surfaces) | `app.yaml` | `GENIE_LLM_ROUTE`, value `"__GENIE_LLM_ROUTE__"` (`:101-102`), filled by both installers (§4.1). `GENIE_BYOK_ENABLED` (`:111-112`) sits beside it, filled the same way, but is app-only, not a job param (§1.4) |

**Job-side consumption.** Each task notebook declares a `genie_llm_route` widget and, when it is
non-empty, exports it to `GENIE_LLM_ROUTE` before any LLM call, the same way `llm_model` becomes
`LLM_MODEL`: `jobs/run_intake_and_snapshot.py:95,123`, `jobs/run_benchmark_qc_and_repair.py:143,158`,
`jobs/run_optimize.py:143-146` and `jobs/run_publish_and_audit.py:81,98` (publish also gained the
`llm_model` widget it lacked). An empty widget leaves the env unset, which the seam reads as
`gateway`. `test_phase7_job_dag.py` pins the export in every notebook and the launcher following the
app env.

> **Note (verified):** `llm_model` and `genie_llm_route` are intentionally **NOT** in
> `packages/genie-space-optimizer/databricks.yml` — `gso_job.py:32-37` documents them as
> Workbench-specific extras beyond the package bundle's params, and
> `test_gso_job_settings_mirror_package_bundle_4task` pins exactly these two as the difference. So the
> route param follows the `llm_model` set above, **not** the generic four-mirror list in the
> mv-advisor rule (which governs mv-advisor's own params). **Closed:** the notebook installer does
> not deploy the package bundle; it creates the job through the Jobs API from
> `gso_job.build_job_settings` (`scripts/deploy_lib/install.py` → `gso_job.py`), so the package
> bundle needs neither param.

### 4.1 Install-time settings and the gateway check

- **Settings.** `InstallConfig.llm_route` (`gateway` default, validated against `gateway|classic`) and
  `InstallConfig.byok_enabled` (default `false`) (`scripts/deploy_lib/config.py:47-48`, `:99-100`).
  The install notebook exposes both as dropdowns (`notebooks/install.py:66-67`, passed at
  `:140-141`); `install.py`'s `llm_route_replacements` (`:30`) fills the two `app.yaml` placeholders.
  The `deploy.sh` path reads `GENIE_LLM_ROUTE` / `GENIE_BYOK_ENABLED` from `.env.deploy`
  (`scripts/deploy-config.sh`, validated, documented in `.env.deploy.template`), seds the same
  placeholders and passes `--var genie_llm_route`. Its leftover-placeholder check matches digits too,
  so an unfilled placeholder stops the deploy.
- **Gateway check.** On the gateway route both installers send one 8-token chat request to
  `/ai-gateway/mlflow/v1/chat/completions` with the mapped model and fail the install on any error,
  pointing at `llm_route=classic`: `verify_ai_gateway` (`scripts/deploy_lib/verify.py:26`, called at
  `install.py:60`) and `_preflight_check_ai_gateway` (`scripts/preflight.sh:347`, called at
  `scripts/deploy.sh:194`). On classic the check is skipped. **Deviation from the plan:** the check
  runs as the installing user, not the app SP — the installer holds no credentials for the app SP. It
  proves the workspace and model are reachable on the gateway; the SP leg is proven in the smoke test
  (§10).
- **User scope.** `serving.serving-endpoints` maps to `model-serving` and does not cover the gateway,
  so the app requests the documented `ai-gateway` user scope beside it in `app.yaml`,
  `scripts/deploy_lib/apps.py` `APP_SCOPES` and `scripts/deploy.sh`, pinned identical by
  `test_user_api_scopes_identical_in_all_three_lists`. Adding a scope makes each existing user
  consent once more.
- **SP retry for user-identity calls (as built).** On the fevm-serverless smoke test the deployed
  app's forwarded user token was refused with `403 Invalid scope, required scopes: ai-gateway`
  despite the scope (the request carried a CLI token, which never passes the consent screen; the same
  token also lacked `genie`). So, per the decided fallback, site 1 (`call_serving_endpoint`: plan
  builder, the Create Agent repair call and the other backend callers) and site 2 (`_stream_llm`)
  re-send a gateway 403 whose body names a scope once as the app SP, with the same tags, and log a
  WARNING — the same try-as-user-then-SP shape as the Genie reads (`genie_client.call_with_sp_fallback`).
  It is gateway-only and 403-only; a token that carries the scope keeps the user as the requester in
  `system.ai_gateway.usage`, and nothing falls back to the classic route.
- **Non-`databricks-*` models.** `gateway_model_name` maps `databricks-<x>` to `system.ai.<x>` and
  passes dotted UC names through. Any other `LLM_MODEL` (a custom serving endpoint name) maps to a
  `system.ai.*` name that does not exist, the install check fails, and at run time the gateway 404
  surfaces as `MODEL_UNAVAILABLE_MESSAGE`. Operators on such an endpoint set `llm_route=classic`.

---

## 5. Non-goals (explicit out-of-scope for the first cut)

- No Responses-API adapter (chat/completions is drop-in for BYOK too — §2).
- No auto-enumeration of BYOK Model Services (config field now; discovery later).
- No change to the retry/backoff logic — existing 429 retry is retained as-is (validated headroom).
- No `response_format:json_object` dependency — it 400s on **both** paths in the test workspace; the
  existing strip-and-retry fallback (`llm_client.py:235-243`) is the real path and works on the gateway.
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
> commits are the record. The phases shipped default-off; the "default `classic`" wording inside
> them is the history of that cut. After them, the job path, installer settings, user scope and
> default commits made `gateway` the default (§4, §4.1, Decision 4).

### Phase 0 — Seam scaffold (no behavior change)  → concrete signatures in **Appendix A.1**
- **Status:** landed 2026-09-14 — `b19e4b58`, `db2bbd27`, `add2f557`, `573127b0`
  (`ai-gateway-phase0-plan.md`). Parameter declared in every mirror; the job did not read it until
  the job-path commit (§4).
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
- **Surfaces:** `llm_utils.py:87`, `create_agent.py:1118`, `llm_client.py:118/170`; component tags at the
  two site-1 callers (`create_agent.py:730`, `plan_builder.py:182`). Streaming+tools is the gating shape.
- **Regression guard:**
  - Default-off: with `classic`, these three are unchanged.
  - Streaming parser already provider-agnostic — the `[DONE]` break (`create_agent.py:1235`) is retained,
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
  added a `component` kwarg so metric-view scoring tags as `mv-suggest` (`mv_scoring.py:377`).
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
  `system.ai.*` mapping lives in the seam (`gateway_model_name`, `llm_route.py:33`), not in
  `model_catalog`. No `reasoning_effort` change here: that is the Phase 1 retry (§2), never
  always-send.
- **Why:** the real "can the customer use their own model?" answer.
- **Surfaces:** `model_catalog.py:42/46/108`; `app.yaml:111-112`. Backend only: no frontend entry field,
  and `list_chat_models` is unchanged (R6).
- **Regression guard:** curated-list behavior on the classic route is unchanged; BYOK acceptance is
  strictly additive and gated on route=gateway **and** the BYOK flag. Unit tests for strip-rule,
  entity_name-trap, passthrough, and the flag's truthy/falsy variants.

### Phase 3 — Clean 404 downgrade (governance UX)
- **Status:** landed 2026-09-25 — `40b41c69`, `eaf75349`, `02c0ed33` (`ai-gateway-phase3-plan.md`,
  rulings R8–R10). The plan's Task 4 verification (full suite, contract greps, untouched-surface diff)
  has no recorded result of its own; the full suite now runs at every commit on the stacked branch,
  and `test_llm_route_contract.py` turns the contract greps into a test (§7).
- **What:** on the gateway route, a **404 on a configured model** raises the single standardized
  `MODEL_UNAVAILABLE_MESSAGE` in place of the raw provider body, at sites 1, 2 and 3 (§3). Scope
  decision 2026-09-25: **messaging only**. There is no automatic classic fallback and no
  `suggest_only` coupling, though the original wording here proposed both.
- **Why:** entitlement failures are 404s (§3); without this a de-entitled user sees a raw error body.
- **Surfaces:** seam (`llm_route.py:86/89`, both twins) + `llm_utils.py:151-153`,
  `create_agent.py:1220-1223`, `llm_client.py:162-167`.
- **Regression guard:** classic route error handling unchanged; only gateway 404 is affected. Tests
  fire the de-entitled 404 shape and assert `str(exc) == MODEL_UNAVAILABLE_MESSAGE`. They also pin that
  a classic 404 and a gateway 500 keep the raw error, and that a 200 stream never reads `resp.text`.

---

## 7. Regression matrix (surface × guard × probe)

| Surface | Risk if wrong | Guard | Acceptance probe |
|---|---|---|---|
| Create Agent streaming+tools | broken agent loop | provider-agnostic terminator; validated on Claude and GPT before the default | `probe.sh` S4 (Claude+GPT) + multi-turn |
| GSO `call_llm` JSON output | optimizer stalls | keep `response_format` strip-retry (`llm_client.py:235-243`) | S3 + S3c |
| Embeddings firewall | leakage check silently off | classic SDK path retained; degrade path intact | S3b + preflight |
| Cost attribution | no spend visibility (the whole point) | tag at every site; grep contract | S5 + `--requery` |
| Entitlement | raw provider error instead of a clean message | gateway-gated 404 → `MODEL_UNAVAILABLE_MESSAGE` (§3) | `--leg403` (404 shape) |
| BYOK reasoning model | 400 on tools | one-shot `reasoning_effort:"none"` retry on the create-agent tool path (§2) | `--byok` |
| Classic route | can't roll back | classic path untouched, selected by `llm_route=classic` | S7 |
| Job param drift | run uses wrong route | mirror lockstep (§4) **plus** the job-side widget → `GENIE_LLM_ROUTE` export in every task notebook | `test_phase7_job_dag.py` export and launcher pins |
| Gateway unreachable | every LLM call fails after install | install-time gateway check fails the install with the classic remedy (§4.1) | `verify_ai_gateway` / `_preflight_check_ai_gateway` tests |
| Raw LLM call outside the seam | untagged or wrong-route traffic | `test_llm_route_contract.py` scans `backend/` and the GSO source for raw call shapes | the contract test |

## 8. Test plan

- **Probe suite is the integration gate.** Re-run `probe.sh` (full) + `--byok` against each deployment
  of the branch; attach the report to the PR.
- **Unit tests (shipped in the same commit as each phase):** route factory (classic byte-identical +
  gateway shape) and twin parity, model mapping (strip rule, entity_name-trap, BYOK passthrough, BYOK
  flag variants), tag header presence at the three chat sites and the embeddings site, streaming
  terminator (both `[DONE]` and `finish_reason` endings), the `reasoning_effort` retry, and
  gateway-404 → `MODEL_UNAVAILABLE_MESSAGE` (plus classic-404 / gateway-500 unchanged).
- **Baseline:** `./scripts/test.sh` measured **4043 passed** (1793 backend + 2250 GSO) on
  2026-10-20 at the SP-retry commit. The merge commit `941aed5b`, which builds this branch on PR #332
  (`feature/metric-view-advisor`, itself carrying main), measured 4012; before the merge the branch
  tip `12d574a8` measured 2818. Below this is a regression. Because the branch now carries PR #332,
  the mv-advisor rule's per-suite floor applies here too and is kept in step at each commit.
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
4. **Default — RESOLVED 2026-10-20: `gateway` from the start.** Unity AI Gateway is GA, so the
   earlier plan (default `classic`, flip in a later PR after a 7-day soak) is withdrawn.
   `get_llm_route()` returns `CLASSIC` only for an explicit `classic`; unset or empty means `gateway`,
   and an unknown value means `gateway` with a one-time warning. Every default agrees: the root
   bundle variable, `gso_job.JOB_PARAMETERS`, the launcher fallback, `InstallConfig`, the install
   notebook and `deploy-config.sh`. `classic` is an explicit operator setting with no automatic
   fallback. The fevm-serverless smoke test (§10) replaces the soak as acceptance: both installers,
   Create Agent, the plan builder and a full optimization on the gateway, with attribution rows for
   both the user and SP requesters. BYOK stays default-off.
5. **Phase 3 scope — RESOLVED 2026-09-25: clean-downgrade messaging only.** AI Gateway stays the
   primary path. A gateway 404 shows `MODEL_UNAVAILABLE_MESSAGE`. There is no automatic classic
   fallback and no coupling to the metric-view advisor's `suggest_only` governance (§3).

---

## 10. Remaining work

**Done on the stacked branch (PR #400)**

- PR #332 brought up to `main` and this branch built on it (`941aed5b`); stacked PR #400 opened.
- The GSO job follows the app's route, and the optimizer's traced call goes through the seam (§4).
- Installer route and BYOK settings, and the install-time gateway check (§4.1).
- The `ai-gateway` user scope in all three scope lists (§4.1).
- `gateway` as the default everywhere (Decision 4).
- The fevm-serverless probe re-run: GO (`report-20261020-191745.md`).

**Before merge — the fevm-serverless smoke test (acceptance)**

1. Re-run `probe.sh` (full) and `--byok`.
2. Notebook install from a Git folder on the branch with default settings (gateway), then one
   `deploy.sh` install from `.env.deploy.fevm-serverless`.
3. A Create Agent session (streaming + tools), the plan builder, and a full four-task optimization
   with metric-view suggestions on. This is where the deployed app's scoped token is proven on the
   gateway; if it is refused, apply the SP fallback (§4.1).
4. `system.ai_gateway.usage` rows for `create-agent` (with `space_id`), `plan-builder`,
   `gso-optimize` (with `run_id`, from every task), `leakage-embed` and `mv-suggest`, from both the
   user and SP requesters (allow about 22 minutes of ingestion lag).
5. Negative checks: a made-up model shows `MODEL_UNAVAILABLE_MESSAGE`; a reinstall with
   `llm_route=classic` still works.
6. Attach the probe report and the query output to PR #400.

**Smoke results (fevm-serverless, 2026-10-20, as run)**

- Probe: GO (18 PASS / 0 FAIL / 2 WARN / 2 SKIP). The `--byok` leg is not runnable: the workspace
  has no customer model service.
- Notebook install (Git folder, defaults) and `deploy.sh --update` both pass the install-time
  gateway check and render `GENIE_LLM_ROUTE=gateway`, `GENIE_BYOK_ENABLED=false`, the `ai-gateway`
  scope, and the job's `genie_llm_route=gateway`. A reinstall with `llm_route=classic` renders
  classic in both places and Create Agent still streams.
- Create Agent streams on the gateway. The requests carried a CLI token, which never passes the
  app's consent screen, so the forwarded user token was refused (`required scopes: ai-gateway`) and
  the §4.1 SP retry carried every `create-agent` call. The same token lacks the UC, SQL and `genie`
  scopes, so the session cannot pass table inspection and the plan builder was not reached; that
  leg, and `USER` requester rows, need a browser session.
- Three optimization runs through `trigger_optimization` with metric-view suggestions on, all as
  the app SP: every task's `gso-optimize` calls carry the run's `run_id`; the advisor scored 10
  candidates and persisted 5, with 264 `mv-suggest` embedding rows. A run whose loop executed (one
  attempt, target 0.99) published at 91.18%.
- `leakage-embed` rows cannot appear: `precompute_benchmark_embeddings` and
  `preflight_embedding_endpoint` have no caller outside `leakage.py` and its tests, so a job never
  enables the firewall's embedding layer. The gateway path for it is covered by unit tests only.
- Negative check: a made-up model returns `MODEL_UNAVAILABLE_MESSAGE` from both
  `call_serving_endpoint` and GSO `call_llm` against the live gateway.
- Found outside gateway scope: the notebook installer needs the installing user to hold
  `servicePrincipal.user` on the app SP before it can bind the job's `run_as`; and a new app's SP
  gets no grants on the space's data schemas, so benchmark validation rejects every benchmark until
  someone grants them.

**After PR #332 squash-merges**

- Rebase only the gateway commits onto `main` and point PR #400 at `main`.

**Deferred (not needed for v1)**

- Frontend BYOK entry field (settings / per-space).
- Auto-enumeration of BYOK Model Services, once a list API exists (Decision 2).
- `reasoning_effort` retry in GSO `call_llm`, only if it gains a `tools` parameter (§2, R1).

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
import json, logging, os
from dataclasses import dataclass
from enum import Enum

class LLMRoute(str, Enum):
    CLASSIC = "classic"
    GATEWAY = "gateway"

_GATEWAY_CHAT  = "/ai-gateway/mlflow/v1/chat/completions"
_GATEWAY_EMBED = "/ai-gateway/mlflow/v1/embeddings"
_TAG_HEADER    = "Databricks-Ai-Gateway-Request-Tags"
_APPLICATION   = "genie-workbench"

_warned_routes: set[str] = set()

def get_llm_route() -> LLMRoute:
    """GENIE_LLM_ROUTE env; only an explicit 'classic' -> CLASSIC. Unset/empty -> GATEWAY (the default);
       an unknown value -> GATEWAY with a one-time warning, never a silent fall back to classic."""
    raw = (os.environ.get("GENIE_LLM_ROUTE") or "").strip().lower()
    if raw == "classic":
        return LLMRoute.CLASSIC
    if raw not in ("", "gateway") and raw not in _warned_routes:
        _warned_routes.add(raw)
        logging.getLogger(__name__).warning("Unknown GENIE_LLM_ROUTE %r; using the gateway route", raw)
    return LLMRoute.GATEWAY

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

**Phase 0 tests (classic must be byte-identical to today):** `get_llm_route` default/typo → CLASSIC
(since Decision 4: unset/empty → GATEWAY, explicit `classic` → CLASSIC, typo → GATEWAY with one
warning, known values silent);
`resolve_chat(classic)` URL == the current `{host}/serving-endpoints/{model}/invocations` with `model
is None` and `extra_headers == {}`; `gateway_model_name` for strip-rule, `system.ai.*` idempotence,
and BYOK passthrough; `request_tags` drops empty scope.

### A.2 Adoption at each site (as built, Phases 1–3)

- **Site 1 `llm_utils.call_serving_endpoint` (`:87`)** — new `component: str = "workbench"` param
  (`:92`); `rc = resolve_chat(host, model, component)` (`:122`); `rc.model` goes in the body only if
  set; `headers = {**auth_headers, **rc.extra_headers}`. The 429 retry loop moved unchanged into
  `_post_with_retries`. On gateway, a 403 whose body names a scope (`is_gateway_scope_403`) is
  re-sent once as the app SP with the same tags, logged as a WARNING (§4.1). Phase 3: on gateway, a
  404 raises `MODEL_UNAVAILABLE_MESSAGE` before the raw-body raise (`:151-153`).
- **Site 2 `create_agent._stream_llm` (`:1118`)** — `rc = resolve_chat(host, effective_model,
  "create-agent", space_id=space_id)` (`:1146`); body gets `rc.model` if set; `session.post(url,
  json=body, stream=True, timeout=120, headers=rc.extra_headers)`. After the 429/502/503 backoff
  loop, the same gateway scope-403 retry as site 1 re-sends once on the SP's session (status checked
  before `resp.text` is read). Then a single `reasoning_effort` retry gated on gateway **and**
  `status_code == 400` (checked before `resp.text` is read) re-POSTs with `reasoning_effort:"none"`
  (`:1210-1217`). Phase 3: inside `if not resp.ok:`, a gateway 404 raises `MODEL_UNAVAILABLE_MESSAGE`
  (`:1220-1223`). The `[DONE]` break (`:1235`) stays. `_async_stream_llm` (`:1244`) delegates here,
  so it inherits all of this.
- **Site 3 GSO `get_openai_client` (`:118`) + `call_llm` (`:170`)** — `base_url` is
  `{host}/ai-gateway/mlflow/v1` on gateway, else `{host}/serving-endpoints` (`:135-138`); the client
  cache key is `host|route`. `resolve_gso_chat` (`:147-159`) returns the model and route kwargs: on
  gateway, `gateway_model_name(model)` and `extra_headers=tag_header("gso-optimize",
  run_id=os.getenv("GSO_RUN_ID") or None)`, sent **per request** (spread at `:210`, R2), not as
  `default_headers` on the cached client. `optimizer_utils._traced_llm_call` uses the same helper.
  **No `reasoning_effort` retry** here (R1: no tools are sent). The `response_format` strip-retry
  (`:235-243`) is unchanged. Phase 3: on gateway, a final error with `status_code == 404` re-raises
  as `RuntimeError(MODEL_UNAVAILABLE_MESSAGE) from last_err` through `raise_if_model_unavailable`
  (`:162-167`, called at `:249`).
- **Site 4 `leakage.get_embedding` (`:207`)** — new keyword `component="leakage-embed"`. It branches
  on `get_llm_route()` rather than `rc.use_legacy_sdk`, so the classic path never touches
  `w.config` (R3). On gateway it calls `resolve_embeddings(host, ep, component, route=route)` and then
  `httpx.post(rc.url, json={"model": rc.model, "input":[text]}, headers={**w.config.authenticate(),
  **rc.extra_headers})` (`:219-231`). Classic keeps `w.serving_endpoints.query(name=ep, input=[text])`
  verbatim (`:233`). Any non-200 or exception returns `None`. Validated dim=1024.
- **Site 5 `model_catalog.validate_chat_model` (`:108`)** — curated names first; then, only when
  `get_llm_route() is GATEWAY` **and** `GENIE_BYOK_ENABLED` is truthy, a well-formed 3-level UC id
  (`_UC_MODEL_ID_RE`, `:42`) is returned verbatim (`:124-129`). Otherwise `ModelValidationError`.
