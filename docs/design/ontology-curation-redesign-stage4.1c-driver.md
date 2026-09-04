# Ontology — Stage 4.1c Goal-Mode driver (single wheel-native LLM client, injected identity)

Copy-paste launcher for **Stage 4.1c** with a long-running agent (Claude Code / Cursor
Goal Mode). Run on the **`ontology`** branch **after Stage 4.1b landed + deploy-verified**
(commit `d85ed3b5`, build §9). Additive/behavior-preserving: it repoints the ontology LLM
enrichers off `backend` onto the wheel's own client, then consolidates. No new route,
frame, DDL, or dependency. The agent produces offline code + green tests and **stops before
deploy**.

- **Spec (source of truth):** `docs/design/ontology-curation-redesign-stage4.1c-build.md`
  (honor the umbrella `ontology-curation-redesign-build.md` §8, §11–§14)
- **Decisions:** `docs/design/mv-advisor-playbook.md` — **MV-D65** (single wheel-native LLM
  client, injected identity); honor MV-D35 / D43 / D49 / D50 / D57 / D63 / D64.
- **Live evidence:** build §1 + §9 (run `934403918953760`: 641 Pages, all
  `body_source=stub`, all `certify=false`; cause = `wheel → backend` LLM import degrades on
  the job cluster).

---

## Driver prompt (paste verbatim)

GOAL: Stage 4.1c — point the 3 ontology LLM enrichers at the wheel's OWN client (not backend)
so batch can certify, then consolidate the repo onto ONE wheel-native client.
Behavior-preserving. Offline code + green tests; STOP before deploy.

SPEC: docs/design/ontology-curation-redesign-stage4.1c-build.md (umbrella …-build.md §8,
§11–§14). DECISIONS: mv-advisor-playbook.md MV-D65; honor MV-D43/D49/D50/D63/D64.

CONTEXT: Two LLM clients exist — backend/services/llm_utils.call_serving_endpoint (httpx,
app-only) and genie_space_optimizer.optimization.llm_client.call_llm (openai SDK + mlflow
autolog + retries + token refresh, wheel-native). The 3 ontology enrichers do a wheel→backend
import, so on the job cluster they degrade → all 641 Pages body_source=stub, certify=false.

STEP 1 (certify unblock):
1. NEW genie_space_optimizer/common/llm.py — the single low-level client, MOVED (not forked)
   from optimization/llm_client.py: get_openai_client(w) (host-cached, token refreshed per
   call) + call_llm_core(w, *, messages, model=None, max_tokens=None, response_format=None,
   max_retries=LLM_MAX_RETRIES) -> (text, response), keeping retry/backoff + response_format
   fallback + content-block normalization. EXCLUDE fit_messages packing. model=None ⇒
   get_llm_endpoint().
2. optimization/llm_client.py behavior-identical: get_openai_client re-exports from common;
   call_llm = thin wrapper (fit_messages then common.call_llm_core), same (text,response) +
   _gso_prompt_pack_stats. GSO callers/outputs unchanged.
3. ontology pages.default_page_drafter / cluster.default_namer / er.default_adjudicator:
   replace the `from backend.services.llm_utils import call_serving_endpoint` (+
   validate_chat_model) block with `from genie_space_optimizer.common.llm import
   call_llm_core`. Each factory gains injected w:WorkspaceClient|None=None; inner call =
   `text,_ = call_llm_core(w, messages=[...], model=model, max_tokens=...)`. KEEP each
   try/except degrade wrapper UNCHANGED (MV-D43). Drop validate_chat_model.
4. jobs/run_ontology_materialize.py: build the run_as WorkspaceClient once, pass w=… into the
   three factories. Call mlflow.openai.autolog() once at start, guarded. get_llm_endpoint()
   already resolves the endpoint.

STEP 2 (consolidate):
5. backend/services/plan_builder.py + create_agent.py: replace call_serving_endpoint([...],
   model=…, max_tokens=…) with call_llm_core(get_workspace_client(), messages=[...], model=…,
   max_tokens=…)[0] (app OBO client).
6. backend/services/llm_utils.py: retire call_serving_endpoint (or 1-line shim → call_llm_core);
   KEEP parse_json_from_llm_response, _repair_json, get_llm_model (re-export get_llm_endpoint()).
   validate_chat_model stays in backend; callers validate overrides first.

HARD GUARDRAILS: behavior-preserving — NO new API model/route/frame, DDL/column, governed-tag
write, or dependency (openai/databricks-sdk/mlflow already deps). Identity INJECTED, never
resolved inside the wheel — common.llm must NOT import backend.services.auth. Degrade-not-hang
(MV-D43): enricher fallbacks unchanged; autolog best-effort. AFTER this, NO `from backend`
under genie_space_optimizer/ — add a layering guard test.

ACCEPTANCE (offline): test_common_llm — token refresh; retry/backoff; response_format fallback;
content-block normalize; model=None⇒get_llm_endpoint(); injected w honored. ontology
pages/cluster/er — default_* call common.call_llm_core (monkeypatched) NOT backend; raising
client ⇒ existing degrade; governed coded col + measure on one concept, OK client ⇒
certify=true; repoint test_ontology_er.py monkeypatch. optimization — call_llm output/packing
unchanged. plan_builder/create_agent — call_llm_core (monkeypatched), OBO client. NEW
layering-guard test (no `from backend` in the wheel). ./scripts/test.sh + wheel suite green.

WORKFLOW: branch `ontology`. Do NOT deploy, run the job, or touch UC governance. When
offline-green, STOP and report the diff + test summary; a human runs deploy-verify.

---

## After the run (human-gated)
Confirm the `run_as` identity has `CAN QUERY` on the `LLM_MODEL` endpoint and `LLM_MODEL` is
in the job env. Deploy `SKIP_FRONTEND_BUILD=1 ./scripts/deploy.sh --update` (fevm-serverless),
trigger with `catalog_allowlist=["serverless_stable_6t92c3_catalog"]`, then verify per build
§7: `body_source=llm` > 0, `certify=true` > 0, Domain names LLM-authored, and no 4.1b
regression (Taxonomy > 0, **0** surfaced-but-unattached).
