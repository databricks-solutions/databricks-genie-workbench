# Ontology — Stage 4.1d Steps 3+4 **backend** Goal-Mode driver (curator "Draft with AI")

## ⚙️ Parallel-build lane header — Lane 2 of Wave 2 (READ FIRST)

You run in an **isolated git worktree** off the `ontology` HEAD (`isolation: worktree`,
`worktree.baseRef: "head"`). Two sibling lanes edit the repo concurrently. This lane owns a
whole disjoint subtree (`backend/ontology/**` + a backend test), so merges are conflict-free —
keep it that way. **This is the backend half of Stage-4.1d Steps 3 & 4; the frontend half is
Lane 3.** Build to the pinned contract below so Lane 3 integrates cleanly at merge.

- **OWNS (create/edit freely):**
  - `backend/ontology/services/draft_body.py` — **new** service (single + bulk drafting).
  - `backend/tests/test_ontology_draft_body.py` — **new** unit + route tests.
  - `backend/ontology/routers/drafts.py` — **append** the 3 new routes at the END; do not
    reflow the existing `GET /drafts` / `POST /decision`.
  - `backend/ontology/models.py` — **append** `DraftBodyResponse`, `BulkDraftStart`,
    `BulkDraftStatus` at the END.
  - `backend/ontology/services/mirror.py` — **append** a read helper only if one does not
    already exist (`read_page_drafts` almost certainly does — reuse it; do not reflow).
- **REUSE by import only (do NOT edit):** the wheel `genie_space_optimizer.common.llm`
  (`call_llm_core`, MV-D65) and `genie_space_optimizer.ontology.pages` (the `default_page_drafter`
  prompt-builder + the `identifier_gate` / `chunk_safe_gate` / `specificity_gate` helpers). They
  are already pure/importable post-4.1i. **If** a helper is not importable as-is, that is a
  **finding to report — never edit the wheel from this lane** (the wheel is off-limits this wave).
- **OFF-LIMITS (do NOT touch):** all of `frontend/`, all of `packages/…/genie_space_optimizer/`
  (wheel — import-only), `backend/ontology/routers/{apply,graph,preflight,taxonomy,tags,settings,
  refresh,inventory}.py`, and `docs/design/mv-advisor-playbook.md`.
- **MERGE-ORDER:** after Lane 1; **before Lane 3** (Lane 3's UI targets these endpoints).
- **Launch:** via the `ontology-lane-builder` subagent — see `ontology-wave2-launcher.md`.

### 🔒 Frozen API contract (Lane 2 implements; Lane 3 mirrors — do not drift)

- `POST /api/ontology/pages/{page_id}/draft-body`
  → `DraftBodyResponse { ok: bool, page_id: str, body: str, body_source: str, as_of: str }`
  — `body_source="llm_ondemand"` on success; on any failure `ok=false` + the **unchanged** body.
- `POST /api/ontology/subdomains/{domain_id}/draft-bodies`
  → `BulkDraftStart { task_id: str, total: int }`
- `GET  /api/ontology/subdomains/{domain_id}/draft-bodies/status?task_id=…`
  → `BulkDraftStatus { done: int, total: int, running: bool,
       results: [ { page_id: str, ok: bool, reason: str | null } ] }`
  — bulk-drafted bodies stamp `body_source="llm_bulk"`.

---

## Spec & decisions

- **Spec (source of truth):** `docs/design/ontology-curation-redesign-stage4.1d-build.md`
  §3.3 (Step 3), §3.4 (Step 4), §4, §5, §6. **Decisions:** `mv-advisor-playbook.md` — **MV-D66**;
  honor **MV-D43** (degrade-not-hang), **MV-D49** (metastore grain — no DDL/column), **MV-D50**
  (OBO identity in the app), **MV-D65** (single wheel-native LLM client, identity injected).
- **Project rules:** `AGENTS.md`. Read the spec sections first.
- Supersedes the backend portions of the older `…-stage4.1d-step3-driver.md` /
  `…-stage4.1d-step4-driver.md` (which bundled frontend); the frontend moved to Lane 3.

---

## Driver prompt (paste verbatim into the subagent)

```text
GOAL: Stage-4.1d Steps 3+4 BACKEND ONLY — a curator "Draft with AI" for ONE Page (Step 3) and
for a WHOLE sub-domain (Step 4). Backend OBO routes draft the body with the SHARED wheel-native
LLM client (MV-D65), run the SAME gates as the batch drafter (import them — no fork), write
body + body_source + facts_hash for (metastore_id, page_id) via the SQL warehouse, and return
the typed responses in the frozen contract. Additive; offline code + green tests; STOP before
deploy. Branch: ontology. NO frontend, NO wheel edits, NO new router, NO new DDL/column.

SPEC: docs/design/ontology-curation-redesign-stage4.1d-build.md §3.3/§3.4/§4/§5/§6.
DECISIONS: mv-advisor-playbook.md MV-D66; honor MV-D43/D49/D50/D65. RULES: AGENTS.md. Read first.

CONTEXT: The bounded batch (Steps 1–2, LANDED) leaves the long tail as deterministic stubs;
Step 2's MERGE preserves llm_ondemand/llm_bulk bodies across re-materialize. drafts.py already
has the OBO write pattern (POST /decision → _obo_email(request) + asyncio.to_thread + Delta write
via the SQL warehouse). Pages carry domain_id = the sub-domain they elevate a concept for
(ddl.py ~L105), so "bulk by sub-domain" is a group-by over mirror.read_page_drafts. The wheel
client is genie_space_optimizer.common.llm.call_llm_core(w, messages=…, max_tokens=…) with an
INJECTED identity — the app passes get_workspace_client() (OBO). The batch drafter's prompt-builder
and the identifier/chunk-safe/specificity/leakage gates live in ontology.pages — IMPORT and reuse
them; if one is not importable as-is, REPORT it as a finding, do NOT edit the wheel.

BUILD A — service backend/ontology/services/draft_body.py (new):
  draft_one(page_id, *, metastore_id, w) -> loads the Page facts from the mirror
  (title/archetype/synonyms/source_fqns/evidence), builds the SAME prompt the batch drafter uses
  (reuse pages.default_page_drafter's builder), calls common.llm.call_llm_core, validates with the
  SAME gates, then UPDATEs genie_ont_pages SET body=…, evidence=<merged: body_source='llm_ondemand',
  facts_hash=…, body_stale=false> WHERE metastore_id=… AND page_id=… via the SQL warehouse (the
  decisions.py execution path). On empty/failed draft or a failed gate ⇒ return the CURRENT body
  unchanged with ok=false + reason (MV-D43). NEVER change structure/certify/score.
  draft_subdomain(domain_id, *, metastore_id, w, max_workers=4) -> reads the sub-domain's Pages
  (read_page_drafts grouped by domain_id), runs draft_one for each under a BOUNDED pool (stdlib:
  ThreadPoolExecutor or a semaphore + asyncio.to_thread — NO new dependency), stamping
  body_source='llm_bulk'; returns a per-Page summary [{page_id, ok, reason}]. Per-Page errors are
  captured, never abort the batch. Keep an in-process task registry (module-level dict) keyed by
  task_id so a long group can be polled; results best-effort, bounded to ONE sub-domain.

BUILD B — routes (append to backend/ontology/routers/drafts.py) + models (append to models.py):
  POST /api/ontology/pages/{page_id}/draft-body -> resolve ms, build the app OBO WorkspaceClient,
    asyncio.to_thread(draft_body.draft_one, …) -> DraftBodyResponse{ok,page_id,body,body_source,as_of}.
  POST /api/ontology/subdomains/{domain_id}/draft-bodies -> start the task under OBO ->
    BulkDraftStart{task_id,total}.
  GET  /api/ontology/subdomains/{domain_id}/draft-bodies/status?task_id=… ->
    BulkDraftStatus{done,total,running,results:[{page_id,ok,reason}]}.
  NEW routes in the EXISTING drafts router (prefix /api/ontology) — no new router, no UC/tag write.
  Human-initiated only.

HARD GUARDRAILS: additive — NEW routes in the EXISTING router, one NEW service, append-only models;
NO new router, NO new DDL/column (body_source/facts_hash/body_stale RIDE the evidence JSON, MV-D49),
NO governed-tag write (no SET/UNSET/CREATE TAG, no manage_uc_tags), NO new dependency (stdlib
concurrency only, MV-D45). Identity OBO in the app, never SP, never resolved in the wheel (MV-D65).
Same gates + same prompt as batch (import, no fork). BOUNDED: small worker cap, scoped to ONE
sub-domain, human-initiated — cannot runaway-cost the estate. Degrade-not-hang: any failure ⇒
unchanged body + ok=false; per-Page failure captured, task still completes; never a 500.
certify/score/structure NEVER mutated. NEVER touch frontend/, the wheel, or the playbook.

ACCEPTANCE (offline, backend/tests/test_ontology_draft_body.py):
  - draft_one with a stub client returning valid prose ⇒ writes body_source="llm_ondemand" + facts_hash,
    returns ok=true; a client returning "" or raising ⇒ body unchanged, ok=false, no 500; a draft that
    fails a gate ⇒ rejected, body unchanged, ok=false.
  - draft_subdomain over a 3-Page sub-domain with a stub client ⇒ all 3 body_source="llm_bulk",
    per-Page ok summary; a client that raises on one Page ⇒ that Page ok=false, the other two succeed,
    task completes; the worker cap is respected (never more than max_workers in flight — assert via an
    instrumented stub).
  - Route tests (FastAPI TestClient): draft-body returns the typed DraftBodyResponse; bulk start returns
    {task_id,total}; status returns progress then final results; OBO email resolved from headers.
  - ./scripts/test.sh green; the wheel suite green (unchanged); uv.lock / package-lock.json untouched.

WORKFLOW: branch ontology. Do NOT deploy or run the job. When offline-green, STOP and report the
worktree branch, `git diff --stat`, and the test summary; a human runs the deploy-verify gate.
```

---

## After the run (human-gated — runs with Lane 3 merged)

Full `./scripts/deploy.sh --update` (frontend changed via Lane 3), then exercise the buttons the
Lane-3 UI adds: "Draft with AI" on a Page and "Draft this sub-domain with AI" on a sub-domain
header; confirm `genie_ont_pages` shows `evidence.body_source ∈ {llm_ondemand, llm_bulk}`, then
trigger a batch refresh and confirm the on-demand/bulk bodies **survive** (Step-2 preservation)
while stubs refresh. Record the pass in the playbook (human edit — the agent never touches it).
