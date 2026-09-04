# Ontology — Stage 4.1d Goal-Mode driver (Step 3: on-demand single-Page "Draft with AI" in the app, OBO)

Copy-paste launcher for **Stage 4.1d Step 3** with a long-running agent. Run on the
**`ontology`** branch **after Steps 1–2 landed** (deterministic certify + capped auto-draft +
`body_source` preservation). Step 3 lets a curator pull LLM prose for **one** Page on demand
— the long tail that the bounded batch intentionally leaves as a stub. Runs **OBO** in the
app (MV-D50). Additive; offline code + green tests; **stops before deploy**.

- **Spec:** `docs/design/ontology-curation-redesign-stage4.1d-build.md` §3.3, §4, §5, §6.
  **Decisions:** `mv-advisor-playbook.md` — MV-D66; honor MV-D43/D49/D50/D65.
- **Seam:** backend `backend/ontology/routers/drafts.py` (the existing OBO POST pattern:
  `_obo_email(request)`, `asyncio.to_thread`, app-state Delta write via the SQL warehouse),
  `services/mirror.py` (page read), wheel `genie_space_optimizer.common.llm` (the MV-D65
  client — app passes its OBO `WorkspaceClient`), `pages.py` gates (reuse). Frontend
  `frontend/src/ontology/api.ts`, `types.ts`, `components/PageDraftCard.tsx`.

---

## Driver prompt (paste verbatim)

GOAL: Stage 4.1d Step 3 — a curator "Draft with AI" action for ONE Page. Backend OBO route
drafts the body with the shared wheel-native LLM client, runs the SAME gates as batch, writes
body + body_source="llm_ondemand" + facts_hash for (metastore_id, page_id), returns the new
body. Frontend button on the Page card. Additive. Offline code + green tests; STOP before
deploy.

SPEC: docs/design/ontology-curation-redesign-stage4.1d-build.md §3.3/§4/§5/§6. DECISIONS:
mv-advisor-playbook.md MV-D66; honor MV-D43/D49/D50/D65. Read first. Steps 1–2 are DONE;
Step 4 (bulk) is NOT in scope.

CONTEXT: bounded batch leaves the long tail as deterministic stubs. drafts.py already has the
OBO write pattern (POST /decision → _obo_email + asyncio.to_thread + Delta write via SQL
warehouse). The wheel client is genie_space_optimizer.common.llm (MV-D65) with an INJECTED
identity — the app passes get_workspace_client() (OBO). Step 2's MERGE preserves
llm_ondemand bodies across refreshes.

BUILD A — backend service (backend/ontology/services/draft_body.py, new): draft_one(page_id,
*, metastore_id, w) → loads the Page facts from the mirror (title/archetype/synonyms/
source_fqns/evidence), calls common.llm.call_llm_core(w, messages=…, max_tokens=…) with the
SAME prompt the batch drafter uses (factor the prompt-builder out of pages.default_page_
drafter so both share it — DRY, no fork), validates with the SAME identifier/chunk-safe/
specificity/leakage gates (extract them from pages.py as pure helpers if not already), then
UPDATEs genie_ont_pages SET body=…, evidence=<merged: body_source='llm_ondemand',
facts_hash=…, body_stale=false> WHERE metastore_id=… AND page_id=… via the SQL warehouse
(same execution path as decisions). On empty/failed draft or failed gate ⇒ return the current
body unchanged with ok=false reason (degrade, MV-D43). NEVER change structure/certify/score.

BUILD B — backend route (drafts.py): POST /api/ontology/pages/{page_id}/draft-body → resolve
ms, build the app OBO WorkspaceClient, asyncio.to_thread(draft_body.draft_one, …), return
{ok, page_id, body, body_source, as_of}. Add DraftBodyResponse to backend/ontology/models.py.
This is a NEW route in the EXISTING drafts router — no new router, no UC/tag write.

BUILD C — frontend: add draftPageBody(pageId) to ontology/api.ts (POST, returns the new
body); mirror DraftBodyResponse + body_source into types.ts; add a "Draft with AI" button to
PageDraftCard.tsx that calls it, shows a spinner, and swaps the body in place on success
(error ⇒ toast; card otherwise unchanged). Keep the deterministic stub visible until success.

HARD GUARDRAILS: additive — one NEW route in the existing router, one NEW service, one NEW
frontend button; NO new router, NO new DDL/column (body_source/facts_hash ride evidence),
NO governed-tag write, NO new dependency. Identity OBO in the app, never SP, never resolved in
the wheel (MV-D65). Same gates + same prompt as batch (no fork). Degrade-not-hang: any failure
⇒ unchanged body + ok=false. certify/score/structure never mutated here.

ACCEPTANCE (offline): test_ontology_draft_body — draft_one with a stub client that returns
prose ⇒ writes body_source="llm_ondemand" + facts_hash and returns ok=true; a client that
returns "" or raises ⇒ body unchanged, ok=false, run does not 500; a draft that fails a gate
⇒ rejected, body unchanged. Route test (FastAPI TestClient): POST returns the typed response;
OBO email resolved from headers. Frontend typecheck + lint green. ./scripts/test.sh + wheel
suite green.

WORKFLOW: branch `ontology`. Do NOT deploy or run the job. When offline-green, STOP and report
the diff + test summary; a human runs deploy-verify.

---

## After the run (human-gated)
Deploy (full `./scripts/deploy.sh --update` — frontend changed, so no SKIP_FRONTEND_BUILD),
open a Page draft, click "Draft with AI", confirm the body updates and `genie_ont_pages` shows
`evidence.body_source="llm_ondemand"`; trigger a batch refresh and confirm the on-demand body
**survives** (Step 2 preservation) while stubs refresh.
