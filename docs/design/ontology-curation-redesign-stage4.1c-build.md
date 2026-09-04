# Ontology — Curation Redesign · Stage 4.1c build spec (single wheel-native LLM client, injected identity)

Follow-up to **Stage 4.1b** (deploy-verified, build §9). Stage-4.1b proved the batch Page
engine produces **corroboration-complete, attached, decoded** Pages — but **every Page is
`certify=false`** because the LLM body drafter degrades to the deterministic stub on the
job cluster. Root cause (build §9): the ontology enrichers import the LLM client from
`backend`, which is **not on the job's Python path**, so the import raises → stub →
`llm_ok=False` → `certify=false`. Stage-4.1c removes that dependency inversion by pointing
the ontology enrichers at the wheel's **own** best-practice LLM client, and then
consolidates the repo onto a single client.

- **Spec umbrella:** `docs/design/ontology-curation-redesign-build.md` (§8 Pages; honor
  §11–§14). This file is the 4.1c addendum.
- **Decisions:** `docs/design/mv-advisor-playbook.md` — new **MV-D65** (single wheel-native
  LLM client, injected identity). Honor MV-D35 / D43 / D45 / D49 / D50 / D55 / D57 / D63 /
  D64.
- **Grain / auth:** Metastore (MV-D49). Batch reads + LLM calls run as the job `run_as`
  identity (MV-D50); interactive calls run OBO in the app.

## 1. Problem — the dependency inversion behind `certify=0`

The repo has **two** LLM clients:

| | `backend/services/llm_utils.call_serving_endpoint` | `genie_space_optimizer.optimization.llm_client.call_llm` |
|---|---|---|
| Transport | raw **httpx** POST to `/serving-endpoints/{model}/invocations` | **openai SDK** at `{host}/serving-endpoints` |
| Retries | manual (429/502/503) | retry + backoff + `response_format`-reject fallback |
| Observability | none | **`mlflow.openai.autolog()`** token/cost/latency spans |
| Auth | `get_workspace_client()` (OBO ContextVar) | **injected** `WorkspaceClient`, token refreshed per call |
| Config | `LLM_MODEL` | `get_llm_endpoint()` = `GSO_LLM_ENDPOINT`→`LLM_MODEL`→default |
| Lives in | `backend/` (app-only) | **the wheel** (job-safe) |

The allowed dependency direction is **`backend → wheel`** (the app imports
`genie_space_optimizer` throughout; the wheel is a dependency of the app). But the three
ontology enrichers invert it — a **`wheel → backend`** import:

- `ontology/pages.py::default_page_drafter` → `from backend.services.llm_utils import call_serving_endpoint`
- `ontology/cluster.py::default_namer` → same
- `ontology/er.py::default_adjudicator` → same

On the job cluster `backend` is absent, so all three degrade: pages → stub
(`certify=false`), Domain names → the deterministic `default_namer` fallback, ER near-ties
→ unmerged. The wheel already ships a **superior, job-safe** client (`call_llm`: openai SDK
+ mlflow autolog + retries + token refresh) that these enrichers ignore. Fixing the
inversion simultaneously (a) lets batch **certify**, (b) restores LLM Domain-naming and ER
adjudication in batch, and (c) removes a layering violation.

## 2. Goals

- **4.1c-fix (Step 1):** the ontology enrichers call the wheel's own LLM client, not
  `backend`. Batch `certify=true` becomes achievable (measure⊕column, `llm_ok=True`);
  namer + adjudicator are LLM-backed in batch. **This is the certify unblock.**
- **4.1c-consolidate (Step 2):** one canonical wheel-native client
  (`genie_space_optimizer.common.llm`); `backend`'s callers migrate to it; the httpx
  `call_serving_endpoint` is retired (backend keeps only the JSON helpers). One client,
  one config resolver, identity **injected** (OBO in app, `run_as` in job).

Non-goals: no new archetype/trigger/gate logic (that's 4.1b, done); no prompt changes
beyond what the port requires; no new UI. Genie-history trigger stays dormant.

## 3. Design

### 3.1 Step 1 — repoint the ontology enrichers (the certify unblock)

**New module `genie_space_optimizer/common/llm.py`** — the single low-level client, moved
(not forked) from `optimization/llm_client.py`:

- `get_openai_client(w: WorkspaceClient | None) -> OpenAI` — host-cached, bearer token
  **refreshed every call** (moved verbatim; it already takes an injected `w`).
- `call_llm_core(w, *, messages, model=None, max_tokens=None, response_format=None,
  max_retries=LLM_MAX_RETRIES) -> tuple[str, Any]` — retry + backoff + `response_format`
  fallback + content-block normalization (moved from `call_llm`, **minus** the
  optimization-only `fit_messages` packing). `model=None ⇒ get_llm_endpoint()`.

**`optimization/llm_client.py` stays behavior-identical:** `get_openai_client` re-exports
from `common.llm`; `call_llm` becomes a thin wrapper = `fit_messages(messages)` packing →
`common.llm.call_llm_core(...)`, still returning `(text, response)` and still attaching
`_gso_prompt_pack_stats`. No optimization caller changes.

**Ontology enrichers** (`pages.default_page_drafter`, `cluster.default_namer`,
`er.default_adjudicator`): replace the `from backend.services.llm_utils import
call_serving_endpoint` (+ `validate_chat_model`) block with
`from genie_space_optimizer.common.llm import call_llm_core`. Each factory gains an
**injected** `w: WorkspaceClient | None = None`; the inner call becomes
`text, _ = call_llm_core(w, messages=[...], model=model, max_tokens=...)`. Keep the
existing `try/except → return "" / (None, None)` degrade wrapper **unchanged** (MV-D43):
any import/call failure still falls back to stub / default name / unmerged. Drop the
`backend.services.model_catalog.validate_chat_model` call — in batch the model is already a
plain endpoint name (`get_llm_endpoint()`); per-run overrides are validated by the caller
(the app) before the name reaches the wheel.

**Job wiring (`jobs/run_ontology_materialize.py`):** build the run_as `WorkspaceClient`
once and pass it into `default_page_drafter(w=...)`, `default_namer(w=...)`,
`default_adjudicator(w=...)`. Ensure `LLM_MODEL` (or `GSO_LLM_ENDPOINT`) resolves in the
job env — same wiring class as `warehouse_id` in 4.1a; if absent, `get_llm_endpoint()`
falls back to the default endpoint. Call `mlflow.openai.autolog()` once at job start,
guarded (best-effort; failure ⇒ no tracing, run continues). Endpoint access: the `run_as`
identity (MV-D50) needs `CAN QUERY` on the serving endpoint.

Net effect: on the job cluster the drafter now reaches a **live** LLM → `body_source=llm`
→ `llm_ok=True` → for a corroborated, shape-authoritative, synonym-covered, non-conflicting
Page, `certify=true` (pages.py:988). Degrade path is preserved for outages.

### 3.2 Step 2 — consolidate `backend` onto the same client

- `backend/services/plan_builder.py` and `backend/services/create_agent.py`: replace
  `call_serving_endpoint([...], model=..., max_tokens=...)` with
  `call_llm_core(get_workspace_client(), messages=[...], model=..., max_tokens=...)[0]`
  (backend already depends on the wheel; it passes its **OBO** client).
- `backend/services/llm_utils.py`: **retire** `call_serving_endpoint` (or leave a one-line
  deprecated shim delegating to `call_llm_core`). Keep `parse_json_from_llm_response`,
  `_repair_json`, and a `get_llm_model` that re-exports `get_llm_endpoint()` — these are
  legitimate backend-side helpers with no transport concern.
- `backend/services/model_catalog.validate_chat_model` stays in `backend` (it guards the
  curated UI endpoint list); backend callers validate the user's override **before**
  calling `call_llm_core`.
- Config: one resolver — `get_llm_endpoint()` (honors `GSO_LLM_ENDPOINT` then `LLM_MODEL`).

## 4. Data-model impact
None. No new tables/columns/DDL. `body_source`/`certify` already ride existing `evidence`
JSON (MV-D49). No new dependency (`openai`, `databricks-sdk` are already wheel deps;
`mlflow[databricks]` already present).

## 5. Guardrails / invariants
- **Behavior-preserving for optimization.** `optimization.llm_client.call_llm` keeps its
  signature, packing, tuple return, and `_gso_prompt_pack_stats` — GSO callers are
  untouched; their outputs must be byte-identical.
- **Degrade-not-hang (MV-D43).** The enrichers' `try/except` fallbacks are unchanged; an
  LLM/endpoint failure still yields stub / default name / unmerged and the run still
  `succeeded`. autolog is best-effort.
- **Layering.** After this, **no `wheel → backend` import remains** anywhere in
  `genie_space_optimizer`. (Add a guard test asserting no `from backend` in the wheel.)
- **Identity injected, never resolved inside the wheel.** The app passes its OBO client;
  the job passes its `run_as` client. `common.llm` never calls `backend.services.auth`.
- **Exact pins.** No new dependency; no `^`/`~` ranges.

## 6. Acceptance (offline — the agent's job; stops before deploy)
- `test_common_llm` (new): token refresh per call; retry/backoff on transient error;
  `response_format`-reject → retry-without fallback; content-block normalization;
  `model=None ⇒ get_llm_endpoint()`; `model="x"` honored; injected `w` used (no implicit
  `WorkspaceClient()` when `w` given).
- `test_ontology_pages` / `_cluster` / `_er`: the `default_*` factories call
  `common.llm.call_llm_core` (monkeypatched), **not** `backend`; a raising client ⇒ the
  existing degrade (stub / default name / `(None,None)`); a governed coded col + measure on
  one concept with a **stubbed-OK** client ⇒ `certify=true` (llm_ok path exercised).
- `test_ontology_er.py` monkeypatch repointed from `llm_utils.call_serving_endpoint` to the
  new client.
- `optimization` suite: `call_llm` output/packing/`pack_stats` unchanged (regression).
- Step 2: `plan_builder` / `create_agent` unit tests call `call_llm_core` (monkeypatched)
  with the OBO client; `test_llm_utils` still green for the retained JSON helpers.
- New **layering guard**: a test asserting the `genie_space_optimizer` tree contains no
  `from backend` / `import backend`.
- `./scripts/test.sh` green; wheel unit suite green.

## 7. Deploy-verify (human-gated, after offline green)
Deploy (`SKIP_FRONTEND_BUILD=1 ./scripts/deploy.sh --update`, fevm-serverless); confirm the
`run_as` identity has `CAN QUERY` on the `LLM_MODEL` endpoint and `LLM_MODEL` is in the job
env. Trigger scoped to `["serverless_stable_6t92c3_catalog"]`, then on `genie_ont_pages`:
- `body_source=llm` count `> 0` (was 0 — the drafter now reaches the LLM).
- `certify=true` count `> 0` (measure⊕column corroboration + `llm_ok`).
- Domain names show LLM-authored labels (namer live), and MLflow traces (if autolog on)
  show ontology LLM spans.
- No regression: 4.1b invariants hold — Taxonomy > 0, **0** surfaced-but-unattached.

## 8. Risks / mitigations
- **Per-run LLM cost** (~hundreds of Page drafts + names) → keep the existing per-concept
  gating (only corroborated/shape-authoritative Pages are worth drafting); a follow-up may
  add a concurrency cap / draft-only-`certify_shape` filter. Declined here to keep the port
  minimal.
- **Endpoint not granted to `run_as`** → drafter degrades to stub (no failure); deploy-verify
  step checks the grant explicitly.
- **Moving `call_llm` internals** could perturb GSO output → the regression test pins
  `call_llm` behavior; the move is a lift-and-shift with re-export.
