# Ontology GA hardening + "How it works" · Goal-Mode driver

> **On the `ontology` branch.** PHASED (P1→P8), ONE phase per run, each its own commit + a STOP at a
> deploy-verify gate. Closes the six "What to close before general availability" gaps (the explainer
> mock's GA list, sourced from the PRD's "Appendix D — Open discrepancies"), then builds the
> "How it works" tab (MV-D108 P2) on top of a scan that actually honours Settings. Proposed register
> line: **MV-D109** (Ontology GA hardening). Authority: `docs/design/ontology-prd.md` (the master PRD;
> code beats docs, the PRD follows code). Where this driver and the MV-D108 driver
> (`ontology-scan-explainer-driver.md`) disagree, this driver + PRD Part C win.
>
> **Doc freeze.** This driver is uncommitted until P1: it rides inside P1's code commit (MV-D9 /
> doc-freeze). Each later phase refreshes its status row here in its own code commit.

## Status at a glance
| Phase | GA gap (mock #) | Scope | Status |
|---|---|---|---|
| **P1** | #5 admin gate | Server-side admin gate on every `/api/ontology/*` router | ✅ offline-green (deploy-verify pending) |
| **P2** | #6 messages | Scan/Refresh failures surfaced; access-panel + grant copy corrected; Map Approve → "Open in Review" (deep link) | ⬜ |
| **P3** | #4 cards reappear | Drafts read consults the consent + suppression ledgers; approved cards render "Approved, ready to apply" | ⬜ |
| **P4** | #2 context switch | Forward `external_context_*` to on-demand runs; HIPAA control in Settings | ⬜ |
| **P5** | #1 nightly no-op | Settings mirrored to a Delta row the nightly run reads when its allowlist is empty | ⬜ |
| **P6** | #3 Page leak | Design record (accepted): Pages cannot leak benchmarks by construction + a guard test | ⬜ |
| **P7** | — | Append-only `page_count` on `OntologyScanStats` | ⬜ |
| **P8** | — | "How it works" tab (MV-D108 P2) per PRD Part C | ⬜ |

## Decisions recorded (2026-09-30, human)
- **Stages:** the tab renders PRD **Part C** exactly (8 inputs · 9 processing · 5 outputs). The MV-D108
  driver's 8-stage list is superseded (PRD "C.4 Reconciliation with the driver's 8 stages").
- **Pages count:** add `page_count` via a **read-time count** over `genie_ont_pages` (P7). Resolves the
  PRD Part C.4 **TBD — confirm**.
- **Tab content:** product sections only: the three-lane flow chart with linked highlight, the
  allowlist-scope toggle + scope matrix, the "what dismissed means" strip, and live last-scan numbers.
  **Excluded:** the CUJ unit chart, the GA-gap list, harness F1 and test counts, and any hard-coded
  6t92c3 figure.
- **Order:** fix the gaps FIRST, then build the tab, so the explainer describes working behaviour.
- **Map Approve:** replace the stub with **"Open in Review"** that deep-links to the proposal (also
  closes PRD Appendix D #45). No second accept flow.
- **Approved cards:** stay in Review, rendered **"Approved, ready to apply"**; dismissed cards are hidden.
- **Nightly:** the app mirrors Settings to a **Delta** row; the nightly job reads it.
- **Page leak:** **accept the current design** (P6). Not a scanner; a documented invariant + guard test.
- **Decision reversal** (un-approve / un-dismiss, PRD Appendix D #38): **out of scope**, later follow-on.
- **P1 admin signal (decided at P1 build, human):** the shared gate is header → DEV fallbacks → the
  caller's **OBO** `current_user.me()` groups, cached per token (300 s), fail-closed, never the SP.
  Reason: Databricks Apps does not forward `X-Forwarded-Groups` (`backend/routers/auth.py:28-31`,
  commit e11655ad), so the header-only `is_admin_request` would 403 every real admin on deploy. The
  shared gate is re-exported for GenieWatch, so `watch/routers/admin.py:18` now admits real admins as
  well (it refused them on Apps before); `/api/auth/me` reuses the same `obo_groups` helper.
- **P1 hardening (pre-deploy review, 2026-09-30):** on Databricks Apps the gate IGNORES
  `X-Forwarded-Groups`. The Apps proxy never sets it, so any value is caller-supplied; trusting it
  let anyone pass the gate by sending `X-Forwarded-Groups: admins`. Off Apps (tests, local) the
  header still counts, matched as the exact group `admins` (no substring: `data-admins` is not
  admin). `/api/auth/me` uses the same `is_admin_request` predicate.

## Why now
The Ontology tab reads as finished but six gaps change what a customer experiences: the nightly scan
records `skipped` every night, the company-context switch is inert, decided cards come back on reload,
the admin gate is only in the browser, and several messages are wrong or missing. An explainer built on
top of those gaps would either document defects or describe behaviour the code does not have. Closing
them first lets P8 state plainly what the scan does.

---

## Grounded facts (code, read 2026-09-30)

### P1 — admin gate (PRD Appendix D #26)
- The only admin check is the frontend: `isAdmin` in `frontend/src/App.tsx:73`, gating the nav at
  `App.tsx:218` and the page at `App.tsx:290`.
- A reusable server-side guard already exists: `require_admin` `backend/watch/_auth.py:39` (403 unless
  `is_admin_request` `backend/watch/_auth.py:17` sees `admins` in `X-Forwarded-Groups`, with the same
  DEV fallbacks as `/api/auth/me` `backend/routers/auth.py:26-38`). It is applied as a route dependency
  at `backend/watch/routers/admin.py:18`.
- Ontology routers are registered at `backend/main.py:241-252`; router modules in
  `backend/ontology/routers/`: `apply`, `drafts`, `graph`, `inventory`, `preflight`, `refresh`,
  `settings`, `tags`, `taxonomy` (e.g. `APIRouter(prefix="/api/ontology")` `drafts.py:38`).
- Positive-controlled absence (run from repo root after `databricks.yml` resolved): no
  `/api/ontology` caller exists outside `frontend/src/ontology/`, so gating every ontology route does not
  break another surface.
- Route tests build their own `TestClient` apps with no admin header (e.g.
  `backend/tests/test_ontology_drafts.py:28`, `test_ontology_apply.py:198`, `test_ontology_preflight.py:24`).
  Gating will 403 them. Fix the tests with a shared fixture (admin header or
  `app.dependency_overrides`), never by weakening the gate.

### P2 — messages (PRD Appendix D #27, #28, #33, #34, #42, #45)
- **Scan failure hidden.** `runScan` `frontend/src/ontology/OntologyPage.tsx:188` awaits
  `triggerRefresh()` (`:194`) and discards the result. `refresh.trigger()` never raises on failure: it
  returns a status with a plain `message` for "job not deployed"
  (`backend/ontology/services/refresh.py:271-275`) and "launch failed" (`refresh.py:298-302`), and
  `state="queued"` only on success (`refresh.py:304-308`). The frontend therefore shows "Scanning…" and
  then a cold Review.
- **Refresh failure silent.** `onRefresh` `frontend/src/ontology/components/FreshnessControls.tsx:109-118`
  swallows errors (`catch { setBusy(false) }`) and never reads `status.message`.
- **Access panel says the tab writes nothing.** Section title "Not used this release" with subtitle
  "Ontology is read-only and writes nothing to Unity Catalog"
  (`frontend/src/ontology/components/PermissionBanner.tsx:255-263`); pinned by
  `PermissionBanner.test.tsx:143`. Apply is live (`execute_apply_plan`
  `backend/ontology/services/apply.py:419`); the `membership_write` tier is a real OBO probe that reads
  `not_exercised` only when no catalogs are in scope (`membership_write_status`
  `backend/ontology/services/grants.py:210-228`).
- **Grant copy disagrees.** The blocked-state notice asks for a service-principal grant
  (`GrantGateNotice` `OntologyPage.tsx:64-81`, text at `:72-74`); the access intro says no SP grant is
  needed to view (`PermissionBanner.tsx:223-225`). The truth depends on `read_identity`
  (`OntologySettings` `backend/ontology/models.py:265`) and each preflight tier carries its `identity`.
- **Map Approve is a stub.** `onApprove` sets a hint only
  (`frontend/src/ontology/components/EstateGraph.tsx:1917`); `GraphInspector` renders it when present
  (`GraphInspector.tsx:199-203`).
- **"Better proposal" loses its target.** `TaxonomyView` passes `proposalId`
  (`TaxonomyView.tsx:58,73`) but `OntologyPage` drops it: `onReview={() => setTab("review")}`
  (`OntologyPage.tsx:406`). `DraftsView` has no focus prop today (`DraftsView.tsx:22-33`).

### P3 — decided cards reappear (PRD Appendix D #37, #40)
- `read_domain_drafts` `backend/ontology/services/mirror.py:442` filters only on `evidence.surfaced`
  (`:465`), the tier (`:467`) and the MV-D101 reuse no-op (`:473`). `read_page_drafts` (`mirror.py:582`)
  likewise (`:591`). Neither reads the ledgers.
- Ledgers: `genie_ont_consents` / `genie_ont_suppressions` (DDL `ontology/ddl.py:162`, `:174`), written
  ONLY by `record_decision` `backend/ontology/services/decisions.py:80`, keyed
  `(metastore_id, proposal_kind, proposal_id)` (`decisions.py:53-55`). Consents record
  `state='approved'` (`decisions.py:117`); apply later flips it. A consent reader already exists:
  `read_approved_consents` `mirror.py:612` (via `_delta_query`, SP).
- The frontend removes the card optimistically and claims it "never resurfaces"
  (`frontend/src/ontology/components/DraftsView.tsx:119`, removal callbacks at `:190-192`, `:214-216`).
- `DomainDraft` (`models.py:365`) and `PageDraft` (`models.py:396`) have been extended additively before
  (`confidence`, `sources`, `asset_why`, `links`), so an additive defaulted field follows precedent.
- Page consents are copy-ready-only (`copy-ready-only` `backend/ontology/services/apply.py:222-225`):
  approving a Page publishes nothing, and the UI does not say so (#40).

### P4 — company context (PRD Appendix D #3, #29)
- The job already reads `external_context_enabled`, `external_context_sources`,
  `external_context_hipaa_baa` (widgets `jobs/run_ontology_materialize.py:132-135`, parsed `:230-238`,
  consumed `:1127`).
- `_launch` `backend/ontology/services/refresh.py:201-259` forwards allowlist, curation policy, alignment
  and `company_name`, and **no** `external_context_*` key. `trigger()` passes Settings at `refresh.py:277-296`.
- `ExternalContext` `backend/ontology/models.py:53` has `enabled` + `sources` only; no HIPAA field. It is
  persisted as JSON via `save_settings` `backend/ontology/services/ont_settings.py:180-209` (so an
  additive key needs no Lakebase column). The form: `SettingsForm.tsx:64`, `:107`, `:364-376`.
- The job declares only 8 job parameters (`databricks.yml:268-288`); on-demand runs already forward
  undeclared keys (curation policy, alignment) and deploy-verified runs honour them. Re-confirm live that
  the new keys arrive (the run's parameters in the Jobs UI).

### P5 — nightly scan (PRD Appendix D #1, #2)
- Schedule `0 0 7 * * ?` (`databricks.yml:260-263`); `catalog_allowlist` defaults `"[]"`
  (`databricks.yml:277-278`); the job parses it at `run_ontology_materialize.py:155-158`; an empty scope
  records `skipped` (`_has_scope` `ontology/materialize.py:661-677`).
- Settings live in **Lakebase**, keyed by workspace (`lakebase.ont_get_settings(_workspace_id())`
  `ont_settings.py:124`; `_workspace_id` `:44`), which the job cannot read.
- Ontology Delta tables register in `_ONT_ALL_DDL` `ontology/ddl.py:338` and are created by
  `ensure_ontology_tables` `ontology/ddl.py:361`. The job is DABs-only
  (`scripts/deploy_lib/install.py:87-89`), so the GSO four-mirror parameter rule does not apply; P5 adds
  **no** job parameter.
- The app already writes app-state Delta via the SQL warehouse (`decisions.py:122-124`); SP-attributed
  bookkeeping writes follow `apply.py:622-625`.

### P6 — Page leak (PRD Appendix D #10) — accepted design
- The batch has an oracle seam: `run_materialize(page_oracle=…)` `ontology/materialize.py:596`, handed to
  the miner at `:813`, checked at `ontology/pages.py:1039`. The notebook passes none. The oracle's own
  docstring records the design: no-op without a corpus, "which is the normal ontology run (Page mining
  has no benchmark corpus in scope)" (`optimization/leakage.py:917-918`).
- What the drafter sees: `_DraftSpec.facts()` `ontology/pages.py:534-546`: archetype, title, concept,
  description, definition, rules, synonyms, sources, related. No benchmark field. On-demand drafting
  rebuilds the same spec (`_spec_from_row` `backend/ontology/services/draft_body.py:221`).

### P7 / P8 — explainer
- `OntologyScanStats` `backend/ontology/models.py:317-327` (append-only, MV-D108 P1);
  `compute_scan_stats` `backend/ontology/services/refresh.py:153`, `get_scan_stats` `:184`, route
  `GET /api/ontology/scan-stats` `backend/ontology/routers/refresh.py:31`, frontend `getScanStats`
  `frontend/src/ontology/api.ts:99`.
- `genie_ont_pages` DDL `ontology/ddl.py:140-154` (keyed `(metastore_id, page_id)`, `evidence` JSON
  carries `surfaced`).
- Tabs: `ONTOLOGY_TABS` `frontend/src/ontology/tabs.ts:7-13` (Overview · Review · Map · Estate · Settings).

## Testability seam
Every new decision is a pure function: the admin predicate is `is_admin_request` (already pure over
headers); the ledger overlay is `apply_decisions(drafts, consents, suppressions)`; the settings-mirror
resolver is `resolve_nightly_settings(widgets, mirror_row)`; the page count is a pure fold over rows;
the tab's stage data is a side-effect-free `scanNarrative.ts`. No I/O in the unit tests.

---

## GOAL PROMPT (paste the block verbatim into Goal Mode; under the 4000-character limit)

```text
Execute ONE phase of docs/design/ontology-ga-hardening-driver.md (Ontology GA hardening + "How it works", proposed MV-D109) on the `ontology` branch, then STOP.

Which phase: the first ⬜ row in the driver's "Status at a glance" table. Its full spec is under "Phase specs" (P1-P8), its code anchors under "Grounded facts", its human-decided constraints under "Decisions recorded". Those sections are binding: do not re-decide them.

Every phase:
1. PLAN first (workspace READ-BEFORE-WRITE contract): files to touch, fresh quotes of the current code, every symbol with file:line. If an anchor has drifted or a symbol is missing, STOP and report; never scaffold a stand-in.
2. Build only that phase. Additive only: frozen models unchanged, new fields append-only and defaulted; apply.py stays the single UC write path; the job never writes the ledgers or the settings mirror; no new npm/py dependency; no endpoint beyond what the phase names.
3. Tests ship in the same commit, on the pure seams listed under "Testability seam".
4. VERIFY: ./scripts/test.sh (report backend + GSO counts; if tests were added, bump the floor in BOTH parity-mirrored rule copies); frontend npx tsc -b, npm run lint, npx vitest run; genie_space_optimizer import path resolves inside this checkout and the sqlglot version is reported, both via uv run --frozen --extra dev; any contract or copy change is proven by a repo-wide grep with the output pasted; git status shows only the intended changes (uv.lock and frontend/package-lock.json untouched). Phases that change a visible surface also run the FIDELITY GATE (mockup emitter, both themes).
5. In the same commit: flip the phase's status row in this driver, close its PRD Appendix D items, and update the explainer mock's GA list if affected. docs/design/ is gitignored, so stage docs with git add -f. P1's commit also carries this driver and registers MV-D109 (proposed) in mv-advisor-playbook.md.
6. Commit, do not push. STOP and hand over that phase's checklist from "Deploy-verify gate".
```

## Phase specs (binding detail for the Goal prompt)

P1 — Server-side admin gate.
1. Move `require_admin` / `is_admin_request` from `backend/watch/_auth.py` to a shared module
   (e.g. `backend/services/admin_gate.py`); re-export from `backend/watch/_auth.py` so
   `watch/routers/admin.py:18` is unchanged in behaviour.
2. Add `dependencies=[Depends(require_admin)]` to EVERY ontology `APIRouter` (all nine modules). One
   gate, router-level, no per-route opt-outs.
3. Tests: a non-admin (no `admins` group) gets 403 on one route per router; an admin passes. Existing
   route tests get a shared admin fixture; no gate weakening.
4. Register MV-D109 (proposed) in `mv-advisor-playbook.md`; commit this driver with P1.

P2 — Correct and complete the messages.
1. `runScan` + `FreshnessControls.onRefresh` read the returned `OntologyRefreshStatus`: poll only when
   `state` is `queued`/`running`; otherwise render `status.message` in a `role="alert"` region and reset
   the busy state. A thrown error renders the same alert.
2. `PermissionBanner`: replace the "Not used this release" / "writes nothing to Unity Catalog" copy with
   a status-true section: apply writes governed tags **as you**, only after a preview, and can be undone;
   it is inactive until catalogs are chosen. Update `PermissionBanner.test.tsx`.
3. Grant copy: `GrantGateNotice` names the identity from the blocked read tier (`tier.identity`), so it
   agrees with the access intro for both `obo` and `sp`.
4. Map: replace the stub `onApprove` with **"Open in Review"** →
   `onOpenInReview(proposalId)`; `OntologyPage` routes both it and `TaxonomyView`'s `onReview(proposalId)`
   to Review with a `focusProposalId`; `DraftsView` scrolls to and highlights that card (no-op if absent).
5. VERIFY greps (paste output): zero `Not used this release`, zero `writes nothing to Unity Catalog`,
   zero `Phase-5 apply gate`, and zero `onReview={() => setTab("review")}` left in `frontend/src`.

P3 — Ledger-aware Review.
1. Pure `apply_decisions(drafts, consents, suppressions)`: drop any draft whose
   `(kind, proposal_id)` is suppressed; drop any whose consent is `applied`; tag `approved` ones.
2. Additive defaulted `decision: Literal["approved"] | None = None` on `DomainDraft` + `PageDraft`
   (mirror 1:1 into `frontend/src/ontology/types.ts`). Readers: reuse `read_approved_consents`' query
   shape; add a suppression read via `_delta_query`. A ledger read failure degrades to "no overlay"
   (MV-D43) and logs; it never 500s.
3. `DraftsView`: dismiss removes the card; approve renders the card in an **"Approved, ready to apply"**
   state (initialised ONLY from `decision` or the POST's persisted `recorded="consent"`: the
   terminal-state rule). Page cards in that state say publishing is manual (copy to Discover) (#40).
   Fix the `DraftsView.tsx:119` comment.
4. Tests: suppressed → absent; approved → present + tagged; applied → absent; ledger failure → drafts
   unchanged; the card's approved state never derives from a null/ambient value.

P4 — Company context reaches the scan.
1. Additive `hipaa_baa: bool = False` on `ExternalContext` (JSON-persisted; no Lakebase column). Settings
   form: a HIPAA/BAA control shown with the external-context toggle; when on, the preflight enrichment
   tier reports blocked.
2. `_launch` forwards `external_context_enabled`, `external_context_sources` (JSON),
   `external_context_hipaa_baa` from `settings.external_context`.
3. Tests: `_launch` job_parameters carry the three keys for on/off/HIPAA; default settings forward
   `false` / `{}` / `false` (byte-identical estate-only run).

P5 — Nightly scan honours Settings.
1. New Delta table `genie_ont_settings` (DDL in `ontology/ddl.py`, registered in `_ONT_ALL_DDL`),
   one row per `(metastore_id, workspace_id)`: the full `OntologySettings` as JSON + `updated_at` +
   `updated_by` (OBO email). Written by the app as SP after every successful Lakebase save
   (`CREATE TABLE IF NOT EXISTS` from the wheel DDL first); a mirror failure logs and never fails the save.
2. Job: when `trigger == "nightly"` and the allowlist widget is empty, read this workspace's mirror row
   (workspace id resolved in-job) and use its values for every Settings-derived knob. Pure resolver
   `resolve_nightly_settings(widget_values, mirror_row)`: explicit widgets win; missing/invalid row →
   today's behaviour (`skipped`). The job never writes the table.
3. Tests: resolver precedence; empty row → skip preserved; the firewall test's write allowlist still holds.

P6 — Page-leak design record (no scanner).
1. PRD Appendix D #10 → resolved-by-design, citing `_DraftSpec.facts()` and `leakage.py:917-918`.
2. Guard test (GSO): `_DraftSpec.facts()` keys equal the known set and none names a benchmark /
   question / expected-SQL field; `run_ontology_materialize.py` and `backend/ontology/services/draft_body.py`
   never import `BenchmarkCorpus` or read a benchmark source. If a future change feeds benchmarks to
   drafting, this test fails and the oracle must be wired at that point.

P7 — `page_count` on scan stats.
1. Append-only `page_count: int | None = None` on `OntologyScanStats` (+ `types.ts`).
2. Count `genie_ont_pages` rows for the metastore where `evidence.surfaced` is true (**default:
   surfaced, the set Review shows; confirm at P7**). None (never 0) when the read fails or the ledger
   is cold.
3. Tests: counts from fixture rows; failure → None.

P8 — "How it works" tab (MV-D108 P2, as amended here).
1. `{ id: "scan", label: "How it works" }` in `ONTOLOGY_TABS` between Estate and Settings.
2. Pure `scanNarrative.ts` holding PRD Part C verbatim (ids, plain titles, narratives, optional flags,
   live-stat bindings). Every number comes from `OntologyScanStats`; cold ⇒ "—".
3. `ScanExplainerPanel`: React port of the approved mock's flow chart (linked highlight, keyboard
   focusable, `role="tooltip"` detail), allowlist-scope toggle + matrix, dismissed strip, last-scan
   summary incl. `page_count`, ONE "Scan the estate" CTA reusing `runScan`. Wrapped in the graph error
   boundary. Optional stages (industry, company context) read their real on/off state from Settings.
4. Amend `ontology-scan-explainer-driver.md` in this commit (PRD Appendix D #17), and register MV-D108.

GUARDRAILS (every phase): additive; frozen models unchanged (new fields append-only + defaulted); the
single UC write path (`apply.py`) and its firewall untouched; the job never writes the ledgers or the
settings mirror; MV-D23 plain language; MV-D43 degrade-never-block; dual-theme + a11y; no new endpoint
except where a phase names one (none do).

ACCEPTANCE (per phase, offline): `./scripts/test.sh` green (report counts; bump the floor in BOTH
parity-mirrored rule copies if tests were added); frontend `npx tsc -b` · `npm run lint` · `npx vitest
run` green; `git status -- uv.lock frontend/package-lock.json` clean. Then STOP.

---

## Deploy-verify gate (human, after each phase's offline-green)
`./scripts/deploy.sh --update` (profile from `.env.deploy`, today 6t92c3).
- **P1:** as admin, the tab works; `curl` an ontology route with a non-admin token → 403; the same
  non-admin call with `X-Forwarded-Groups: admins` added → still 403.
- **P2:** break the job id (or use a workspace without it) → Overview shows the message, not
  "Scanning…"; the access panel copy is truthful; Map "Open in Review" lands on the card; Estate
  "Better proposal" lands on its card.
- **P3:** dismiss a card, reload → gone; approve a card, reload → "Approved, ready to apply"; the
  Review-level Apply bar still includes it.
- **P4:** enable company context + save + Scan → the run's parameters show the three keys; the run log
  shows "Context Pack resolved … enabled=True"; HIPAA on → resolver never built.
- **P5:** save Settings; trigger the job with `trigger=nightly` and no allowlist → it scans the saved
  catalogs (not `skipped`).
- **P6:** offline only.
- **P7:** `GET /api/ontology/scan-stats` returns `page_count`.
- **P8:** FIDELITY GATE against the updated mock (9 processing steps), both themes; five-second test:
  can a new admin say what a scan reads, does and produces?

## Explainer mock (reference frame)
`genie-ontology-explainer.html` (workspace root, user-named) is the approved visual reference for P8.
Before P8 it must be updated to PRD Part C (processing 10 → 9; "Build the map" moves to Outputs), and
its GA list shrinks as P1–P6 land.

## Non-goals
Decision reversal (un-approve / un-dismiss, #38); Settings validation (#32), up-front source picking
(#30), auto-draft controls (#31); Map "Both" (#43) and Map drafts wiring (#44); live per-run progress;
catalog sharding. Each remains in PRD Appendix D.
