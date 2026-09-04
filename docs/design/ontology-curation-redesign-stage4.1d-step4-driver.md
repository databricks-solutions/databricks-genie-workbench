# Ontology — Stage 4.1d Goal-Mode driver (Step 4: bulk "Draft this sub-domain with AI", OBO)

Copy-paste launcher for **Stage 4.1d Step 4** with a long-running agent. Run on the
**`ontology`** branch **after Step 3 landed** (on-demand single-Page drafting). Step 4 lets a
curator draft **every Page in one sub-domain** in a single, human-initiated action — bounded
concurrency, OBO (MV-D50). It reuses Step 3's service end-to-end; the only new mechanics are
the group-by, the worker cap, and a progress poll for long groups. Additive; offline code +
green tests; **stops before deploy**.

- **Spec:** `docs/design/ontology-curation-redesign-stage4.1d-build.md` §3.4, §4, §5, §6.
  **Decisions:** `mv-advisor-playbook.md` — MV-D66; honor MV-D43/D49/D50/D65.
- **Seam:** Step-3 `backend/ontology/services/draft_body.py` (reuse `draft_one`),
  `routers/drafts.py`, `services/mirror.py` (`read_page_drafts` → group by `domain_id`, the
  sub-domain id per `ddl.py:105`). Frontend `ontology/api.ts`, `types.ts`, the sub-domain
  header component + `PageDraftCard.tsx`.

---

## Driver prompt (paste verbatim)

GOAL: Stage 4.1d Step 4 — a curator "Draft this sub-domain with AI" action. Backend OBO route
groups the Pages whose domain_id == the chosen sub-domain and drafts them with BOUNDED
concurrency by reusing Step 3's draft_one; long groups run as a backgrounded task with a
progress poll. Frontend action on the sub-domain header. Additive. Offline code + green
tests; STOP before deploy.

SPEC: docs/design/ontology-curation-redesign-stage4.1d-build.md §3.4/§4/§5/§6. DECISIONS:
mv-advisor-playbook.md MV-D66; honor MV-D43/D49/D50/D65. Read first. Steps 1–3 are DONE —
REUSE draft_one; do NOT re-implement drafting or gates.

CONTEXT: Pages carry domain_id = the sub-domain they elevate a concept for (ddl.py:105), so
"bulk by sub-domain" is a group-by over mirror.read_page_drafts. Step 3's draft_one already
does load→draft→gate→write for one Page under OBO, writing body_source (here "llm_bulk").
Step 2's MERGE preserves llm_bulk bodies across refreshes.

BUILD A — backend service (extend draft_body.py): draft_subdomain(domain_id, *, metastore_id,
w, max_workers=SMALL) → reads the sub-domain's Pages, runs draft_one for each under a bounded
worker pool (a small cap, e.g. 4; asyncio.to_thread + a semaphore or a ThreadPoolExecutor —
no new dependency), stamping body_source="llm_bulk". Returns a per-Page summary
[{page_id, ok, reason}]. Per-Page errors are captured, never abort the batch (MV-D43). Keep an
in-process job registry keyed by a task_id (dict) so a long group can be polled; results are
best-effort and bounded (one sub-domain).

BUILD B — backend routes (drafts.py): POST /api/ontology/subdomains/{domain_id}/draft-bodies
→ start the task under OBO, return {task_id, total}. GET
/api/ontology/subdomains/{domain_id}/draft-bodies/status?task_id=… → {done, total,
results, running}. Add BulkDraftStart/BulkDraftStatus models to models.py. NEW routes in the
EXISTING drafts router; no new router; no UC/tag write. Human-initiated only.

BUILD C — frontend: add startBulkDraft(domainId) + pollBulkDraft(domainId, taskId) to
ontology/api.ts; mirror the two models in types.ts; add a "Draft this sub-domain with AI"
action to the sub-domain header with a confirm (it costs LLM calls), a progress indicator
(done/total), and per-Page refresh as results land. Reuse PageDraftCard body-swap from Step 3.

HARD GUARDRAILS: additive — NEW routes in the existing router, service reuse (draft_one, no
fork), NEW frontend action; NO new router, NO new DDL/column (body_source rides evidence),
NO governed-tag write, NO new dependency (stdlib concurrency only). BOUNDED: small worker cap,
scoped to ONE sub-domain, human-initiated — it can't runaway-cost the estate. Identity OBO,
never SP, never resolved in the wheel (MV-D65). Degrade-not-hang: per-Page failure captured;
the task still completes. certify/score/structure never mutated.

ACCEPTANCE (offline): test_ontology_draft_body — draft_subdomain over a 3-Page sub-domain
with a stub client drafts all 3, body_source="llm_bulk", returns a per-Page ok summary;
a client that raises on one Page ⇒ that Page ok=false, the other two succeed, task completes;
concurrency respects the worker cap (never more than N in flight — assert via an instrumented
stub). Route tests (TestClient): start returns {task_id,total}; status returns progress and
final results. Frontend typecheck + lint green. ./scripts/test.sh + wheel suite green.

WORKFLOW: branch `ontology`. Do NOT deploy or run the job. When offline-green, STOP and report
the diff + test summary; a human runs deploy-verify.

---

## After the run (human-gated)
Deploy (full `./scripts/deploy.sh --update` — frontend changed), pick a sub-domain, run
"Draft this sub-domain with AI", watch progress complete, confirm each Page shows
`evidence.body_source="llm_bulk"`; trigger a batch refresh and confirm the bulk bodies
**survive** (Step 2) while stubs refresh.
