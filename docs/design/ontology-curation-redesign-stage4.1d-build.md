# Ontology — Curation Redesign · Stage 4.1d build spec (bounded drafting: deterministic certify + capped super-sure auto-draft + curator single/bulk)

Follow-up to **Stage 4.1c**. The 4.1c deploy-verify (run `775414490043851`) proved the
wheel-native LLM client works in batch — but the job **drafted all 641 Pages
sequentially**, ran **~68 min**, and hit the **60-min task timeout → FAILED** (no snapshot
written). Mass-drafting the whole estate every refresh is the wrong lever. This stage
**bounds** it: the batch drafts only a small, hard-capped "super sure" set; everything else
carries the deterministic stub; and the LLM prose for the long tail is pulled **on demand**
by a curator (single Page, or bulk by sub-domain). The certify *recommendation* is
decoupled from prose and computed deterministically.

- **Spec umbrella:** `docs/design/ontology-curation-redesign-build.md` (§8 Pages; honor
  §11–§14). This file is the 4.1d addendum; it **supersedes the "draft all" posture of
  Stage 4.1c** while keeping 4.1c's client consolidation + injected identity (MV-D65).
- **Decisions:** `docs/design/mv-advisor-playbook.md` — new **MV-D66** (bounded drafting).
  Honor MV-D35 / D43 / D49 / D50 / D57 / D63 / D64 / D65.
- **Grain / auth:** Metastore (MV-D49). Batch runs as job `run_as` (MV-D50); on-demand /
  bulk drafting runs OBO in the app.

## 1. Problem — mass batch drafting times out; certify is over-coupled to prose

- `pages._finalize` **drafts every candidate inline** (`mine_pages` loops all concepts →
  `_finalize` → `_draft_body`). On the airline estate that is 641 sequential
  `max_tokens=400` serving calls ≈ 50 min on top of the ~18-min baseline → **68 min → task
  timeout (3600s) → FAILED**, snapshot never MERGEd.
- `certify` (pages.py:991) requires `llm_ok`:
  `certify = certify_shape AND corroborated AND syn_ok AND not conflict AND llm_ok`.
  So the *only* way to make a Page "Ready to certify" was to LLM-draft it — which is what
  forced drafting all 641. But `certify` is surfaced to the curator as a **recommendation**
  ("Ready to certify" — `mirror.py:442`, `PageDraftCard.tsx:85`), and nothing is trusted
  until a **human approves** (consent ledger → Phase 5). The stub body already passes every
  safety gate (identifier / chunk-safe / specificity); `llm_ok` adds prose polish, not
  correctness. Coupling the recommendation to prose is both the cost driver and weaker
  governance than a deterministic, explainable recommendation.

## 2. Goals

- **4.1d-certify (decouple):** `certify` is a **deterministic** recommendation —
  `certify_shape AND corroborated AND syn_ok AND not conflict` (drop `llm_ok`). Computed
  for **every** strong Page regardless of body source, so the curator sees the full
  priority list.
- **4.1d-auto (cap the batch):** the batch LLM-drafts **only the "super sure" set** — 
  certify-eligible + `corroboration ≥ page_autodraft_min_corroboration` (default **3**),
  **top N by `score`**, hard-capped at `page_autodraft_max_pages` (default **50**). Everyone
  else keeps the deterministic stub. Bounded → the job can never time out on drafting again.
- **4.1d-body (survive re-materialize):** a `body_source` marker so on-demand / bulk /
  human bodies are **preserved** across refreshes; only the `auto` tier is re-drafted by the
  batch.
- **4.1d-single (app):** a curator "Draft with AI" action for one Page (on-demand, OBO).
- **4.1d-bulk (app):** a curator "Draft this sub-domain with AI" action — Pages already
  carry `domain_id` = the **sub-domain** they elevate a concept for (`ddl.py:105`), so this
  is a group-by; bounded concurrency; human-initiated.

Non-goals: no auto-certification (a human still approves → consent → Phase 5); no new
archetype/trigger logic; Genie-history stays dormant.

## 3. Design

### 3.1 Step 1 — deterministic certify + capped super-sure auto-draft (batch)

**Decouple certify (`pages.py`).** Drop `llm_ok` from the `certify` product (line 991):
`certify = bool(spec.certify_shape and corroborated and syn_ok and not conflict)`. The
`llm_ok` value still rides `evidence.body_source` (`"llm"` vs `"stub"`) and still scales
`confidence` (the `not llm_ok ⇒ ×0.8` line stays) — it just no longer gates the
recommendation. No other gate changes (identifier / chunk-safe / specificity / leakage /
synonyms / conflict all unchanged).

**Two-pass drafting in `mine_pages`.** Today `_finalize` drafts inline. Restructure so the
LLM is called for **≤ N** Pages:

1. **Pass A — deterministic finalize-all.** Call `_finalize(..., drafter=None)` for every
   candidate. With `drafter=None`, `_draft_body` returns the stub (`llm_ok=False`) — and
   because certify no longer needs `llm_ok`, every strong Page already gets the correct
   `certify`, `confidence`, `score`, stub `body`, and `evidence` (`body_source="stub"`).
2. **Pass B — select the super-sure set.** From the Pass-A candidates keep those with
   `certify is True` **and** `evidence.corroboration ≥ page_autodraft_min_corroboration`;
   sort by `score` desc (tie-break `page_id`); take the first `page_autodraft_max_pages`.
3. **Pass C — draft only those.** For each selected Page, call the injected `drafter` on its
   `facts()`; if it returns non-empty and passes the **same** identifier / chunk-safe /
   specificity / leakage gates, replace `body` and set `evidence.body_source="llm_auto"`
   (else keep the stub — degrade, MV-D43). Re-run `flag_duplicates` after.

`mine_pages` keeps the injected `drafter` param (MV-D65); it simply calls it ≤ N times
instead of per-candidate. A `drafter=None` run is fully deterministic (all stubs) and
certify still lights up — so offline tests need no LLM.

**Config (MV-D57 surface).** Add to `OntologySettings` + job params + widgets, with in-code
defaults: `page_autodraft_min_corroboration=3`, `page_autodraft_max_pages=50`. Round-trip
old rows to defaults (Stage-3/3.2 pattern). `page_autodraft_max_pages=0` ⇒ pure-stub batch.

**Job (`run_ontology_materialize.py`).** Keep the `page_drafter=pages.default_page_drafter(
w=_ont_llm_w)` wiring and `mlflow.openai.autolog()` — now bounded to ≤ N calls. Thread the
two new config values into `mine_pages`. Namer / ER adjudicator unchanged (cheap; ~140 tiny
name calls + near-tie ER — they never threatened the timeout).

### 3.2 Step 2 — `body_source` preservation across re-materialize (batch)

The batch overwrites `body` every run (snapshot MERGE). `page_id` is derived/stable, so add
`body_source` to the persisted Page row (rides `evidence` JSON — **no DDL change**) with
values `stub | llm_auto | llm_ondemand | llm_bulk | human`. The snapshot MERGE **preserves**
the existing `body` + `body_source` when the stored source is in
`{llm_ondemand, llm_bulk, human}` (curator work is durable); it **refreshes** `stub` and
`llm_auto` rows normally. A light **staleness** rule: store a `facts_hash` in `evidence`;
if the new run's facts hash differs from the preserved row's, keep the body but set
`evidence.body_stale=true` so the UI can offer a re-draft. Reuse the existing
snapshot-preservation pattern (the run-ledger `delete_unmatched=False` +
`_project_to_schema` precedent in `materialize.py`).

### 3.3 Step 3 — on-demand single draft (app)

`POST /api/ontology/pages/{page_id}/draft-body` (backend, OBO). Loads the Page facts from
the mirror, calls the shared wheel-native client (`genie_space_optimizer.common.llm`, the
app passes its OBO `WorkspaceClient`), runs the **same** gates as batch, writes
`body`/`body_source="llm_ondemand"`/`facts_hash` for `(metastore_id, page_id)`, returns the
new body. Frontend: a "Draft with AI" button on `PageDraftCard` + a `draftPageBody` call in
`ontology/api.ts`; on success swap the body in place. Degrades to a clear error toast; never
mutates structure/`certify`.

### 3.4 Step 4 — bulk draft by sub-domain (app)

`POST /api/ontology/subdomains/{domain_id}/draft-bodies` (backend, OBO). Groups the Pages
whose `domain_id == {domain_id}` (the sub-domain), drafts them with **bounded concurrency**
(a small worker cap), writing `body_source="llm_bulk"`. Returns a per-Page result summary;
long groups run as a backgrounded async task with a progress poll (`GET …/draft-bodies/
status`). Human-initiated only. Frontend: a "Draft this sub-domain with AI" action on the
sub-domain header + progress UI. Bounded so a chosen sub-domain can't runaway-cost.

## 4. Data-model impact
None. No new tables/columns/DDL. `body` already exists; `body_source`/`facts_hash`/
`body_stale` ride `evidence` JSON (MV-D49). Config values are settings/job-params/widgets
(MV-D57 pattern).

## 5. Guardrails / invariants
- **Bounded batch LLM.** Hard cap `page_autodraft_max_pages`; the batch can never draft more
  than N Pages. `max=0` ⇒ zero page LLM calls (pure stub).
- **Deterministic-first.** `drafter=None` ⇒ a fully deterministic, all-stub run whose
  `certify` recommendations are identical — offline tests need no LLM.
- **No auto-certification.** `certify` is a *recommendation*; a human approves → consent →
  Phase 5. Nothing here writes governed tags.
- **Reuse, don't fork.** Same drafter + gates; MV-D65 client + injected identity unchanged;
  snapshot-preservation reuses the run-ledger precedent.
- **Degrade-not-hang (MV-D43).** A missing/raising drafter ⇒ stub for that Page; the run
  still `succeeded`.
- **Grain + identity.** Metastore keys; batch `run_as`, app OBO (never resolved in the
  wheel). Exact pins; no new dependency.

## 6. Acceptance (offline — the agent's job; stops before deploy) — **Step 1**
- `test_ontology_pages`: certify is deterministic — a corroborated, shape-authoritative,
  synonym-covered, non-conflicting concept ⇒ `certify=true` **with `drafter=None`** (no LLM);
  a single-artifact / synonym-short / conflicting concept ⇒ `certify=false`. `body_source`
  is `"stub"` when undrafted.
- `test_ontology_pages` (cap): with a stub drafter that marks bodies, **≤
  `page_autodraft_max_pages`** Pages get `body_source="llm_auto"`, selected as the top-`score`
  certify+`corroboration≥min` set (deterministic order); `max_pages=0` ⇒ zero drafted;
  a **raising** drafter ⇒ those Pages keep the stub and the run still succeeds.
- `test_ontology_materialize`: the drafter is invoked **at most N** times regardless of Page
  count; config round-trips old rows to defaults.
- `./scripts/test.sh` + wheel unit suite green.

## 7. Deploy-verify (human-gated, after offline green) — **Step 1**
Deploy (`SKIP_FRONTEND_BUILD=1 ./scripts/deploy.sh --update`, fevm-serverless), trigger
scoped to `["serverless_stable_6t92c3_catalog"]`, then confirm on `genie_ont_pages`:
- Job **completes well under the 3600s task timeout** (bounded drafts).
- `certify=true` count `> 0` (deterministic — the ~591 corroborated Pages), independent of
  body source.
- `body_source="llm_auto"` count `> 0` **and `≤ page_autodraft_max_pages`**.
- No 4.1b/4.1c regression: Taxonomy `> 0`, **0** surfaced-but-unattached.

## 8. Risks / mitigations
- **"Super sure" bar too strict/loose** → both knobs are config; start corroboration ≥3 /
  N=50 and tune from the live yield.
- **Auto-drafted bodies wiped on next run** → handled in Step 2 (`llm_auto` is intentionally
  refreshable; only curator bodies are preserved).
- **Bulk sub-domain runaway cost** → bounded concurrency + it's human-initiated + scoped to
  one sub-domain (Step 4).
