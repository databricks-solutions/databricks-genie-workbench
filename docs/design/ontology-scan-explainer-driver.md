# Ontology "How scanning works" explainer tab + last-run stats · Goal-Mode driver

> **On the `ontology` branch.** PHASED (P1→P2). **P1 BUILT + deploy-verified (6t92c3, 2026-09-22);
> P2 remaining.** Each phase its own commit + STOP at a deploy-verify
> gate. P1 is a THIN backend read (one new append-only model + one route over data the materialize job
> already writes); P2 is a frontend-only explainer tab that recomposes surfaces + counts already
> fetched. NO wheel/job/DDL change, NO new dep. Proposed register line: **MV-D108** (the Ontology tab
> explains what "Scan the estate" does — a clean, infographic-style walk of how the ONE materialize job
> collects signals and turns them into domains / sub-domains / Pages, grounded in the user's own
> last-run numbers). Determinism/dual-theme preserved; MV-D23 (no jargon/SQL in the primary read),
> MV-D43 (degrade, never block), MV-D80 mockup-fidelity + a11y gates apply.

## Status at a glance
| Phase | Scope | Status |
|---|---|---|
| **P1** | `genie_ont_runs` last-run stats read — new append-only `OntologyScanStats` model + `GET /api/ontology/scan-stats` + pure resolver + tests | ✅ **BUILT + deploy-verified (6t92c3, 2026-09-22)** — `scan-stats` live (domain_count 338 · tag_count 2923 · ungrouped 72 · dur 1967s), 10-field shape, counts `int\|None`; `./scripts/test.sh` 3164 |
| **P2** | "How it works" tab — `ScanExplainerPanel` + pure `scanNarrative.ts`, 8-stage infographic (incl. **Align & enrich** — industry data models + web/external context) hydrated from `inventory`+`scan-stats`, signal legend, CTA reusing `runScan` | ⬜ REMAINING |

## Why now
"Scan the estate" is the ONE job behind the whole ontology (nightly + on-demand), triggered from
several buttons that all call the same `runScan` → `triggerRefresh` → one materialize run. A user has
no way to understand what that job does, what signals it reads, or what it produces — it is a black box
that takes ~minutes and then the estate changes. The charter's zero-user-burden + evidence-led
principles (MV-D23/D38) ask for a plain-language account of the pipeline, and the honest way to show it
is against the user's OWN last run (how many domains/tags/Pages it produced, how long it took), not a
static diagram. Every datum for that already exists — the job writes it to `genie_ont_runs`; the app
just never surfaces the counts.

## Grounded facts (code, 2026-09-22)
- **One job, one task.** `databricks.yml:290` `task_key: ontology_materialize` →
  `packages/genie-space-optimizer/src/genie_space_optimizer/jobs/run_ontology_materialize.py`
  (`databricks.yml:292`); nightly `0 0 7 * * ?` + on-demand; `max_concurrent_runs: 1`
  (`databricks.yml:257`). On-demand launch: `refresh.trigger()` → `jobs.run_now` on `GSO_ONT_JOB_ID`
  (`backend/ontology/services/refresh.py:198-244`, id at `:208`), in-flight-guarded (`:204`).
- **The pipeline (the infographic).** `run_materialize` (`ontology/materialize.py:579`): empty-scope
  guard (`:661-677`) → read governed_tags/assignments/asset_types/metric_views/agents (`:680-690`) →
  `_gather_structural_signals` + `graph.build_signal_graph` (`:697`,`:710`) → `pagerank_centrality`
  (`:732`) → cluster into domains/sub-domains (`build_domain_rows` `:181`) + LLM naming + ER identity →
  **align & enrich (optional, default off)** → `mine_pages` (Pages + Related) → MERGE the `genie_ont_*`
  snapshots + a `genie_ont_runs` ledger row (`:766-768`).
- **Signals feeding the graph** (for the legend, reuse the Map's vocabulary): join-key/FK overlap,
  MV membership, shared schema, table lineage (MV-D105 P1), co-query co-occurrence (MV-D105 P2),
  agent-scope + dashboard-scope read edges, optional semantic-sim (default off).
- **Align & enrich (the "industry context / data models / web search" question) — opt-in, provenanced.**
  Two independent overlays, BOTH default-off and byte-identical when off (MV-D44), that refine domain
  NAMES/labels only (never membership): (a) **industry-reference alignment / data models** — `industry_reference`
  → `alignment.align()` + `rank.apply_alignment` (`materialize.py:916-929`; `ontology/alignment.py`,
  reference models in `ontology/reference_models.py`, MV-D58/§9), emitting typed correspondences onto
  `evidence.rank.alignment`; (b) **external / web-search context pack** — `context_pack` →
  `rank.apply_context_prior` (`materialize.py:889-903`; `ontology/context_pack.py` +
  `ontology/web_search.py` + `ontology/context_registry.py`, MV-D57/Stage C). Web-sourced content is
  best-effort, LeakageOracle-scanned (`ontology/context_firewall.py`), and labeled "informational —
  not certified"; a failed search never blocks a run. The explainer MUST show these as OPTIONAL and
  state whether they ran (off unless enabled in Settings), and carry the not-certified label.
- **The stats already exist, unsurfaced.** The run row written at `materialize.py:637-651` carries
  `trigger`, `state`, `scope_allowlist`, `started_at`, `finished_at`, `as_of`, `tag_count`,
  `domain_count`, `ungrouped_count`. It is read whole by `mirror.latest_run` (`mirror.py:94`) and
  `mirror.latest_succeeded_run` (`mirror.py:118`). `refresh.compute_status` (`refresh.py:66`) has BOTH
  dicts in hand but surfaces none of the counts — `OntologyRefreshStatus` (`models.py:302`) has only
  state/source/mirror_as_of/last_run_id/last_run_state/message.
- **`OntologyRefreshStatus` is FROZEN.** It sits above the "Phase-1/2/3a-c models above are FROZEN /
  APPEND-ONLY" marker (`models.py:311-315`) — so the stats read is a NEW append-only model + route, not
  a field add to the frozen model.
- **What the tab hydrates from.** `OntologyInventory{catalogs_scanned, metric_view_count,
  genie_agent_count, governed_tag_count, as_of}` (`models.py:82-87`) via `GET /api/ontology/inventory`
  (`routers/inventory.py:35`, frontend `api.ts:77`); refresh state via `GET /api/ontology/refresh`
  (`routers/refresh.py:21`, frontend `api.ts:92`).
- **Where the tab plugs in.** `ONTOLOGY_TABS` (`frontend/src/ontology/tabs.ts:7-13`) +
  `OntologyPage` tab switch/render; the scan routine to reuse is `runScan` (`OntologyPage.tsx:182`).

## Testability seam
All new logic is PURE + unit-testable: P1's resolver is `compute_scan_stats(head, succeeded)` — a pure
function of the two run-ledger dicts (mirrors `compute_status`, `refresh.py:66`), so no I/O in the test.
P2's `scanNarrative.ts` derives the 7 stages + the honest count line from `{inventory, scanStats}` as a
side-effect-free module (react-refresh/only-export-components rule); `ScanExplainerPanel` is prop-driven.
No wheel, no job, no new dep.

---

## GOAL PROMPT (paste verbatim into Goal Mode — run ONE phase, then STOP)

Build the **Ontology "How scanning works" explainer**, `ontology` branch, ONE PHASE per run. Additive,
dual-theme, determinism/a11y-preserving, MV-D23 (no SQL/jargon in the primary read) + MV-D43 (degrade,
never block). NO wheel/job/DDL change, NO new dep (`frontend/package-lock.json` + `uv.lock` untouched).
Proposed **MV-D108**. Each phase: backend `./scripts/test.sh` and/or frontend `tsc -b`/`lint`/`test` all
green (report counts), then STOP for deploy-verify.

P1 — genie_ont_runs last-run stats read (backend, thin).
1. New APPEND-ONLY model `OntologyScanStats` (`backend/ontology/models.py`, below the FROZEN marker at
   `:311`): `{ last_run_state, trigger, domain_count:int|None, tag_count:int|None,
   ungrouped_count:int|None, started_at:str|None, finished_at:str|None, duration_seconds:float|None,
   as_of:str|None, scope_allowlist:list[str] }`. Mirror 1:1 into `frontend/src/ontology/types.ts`.
2. Pure resolver `refresh.compute_scan_stats(head, succeeded)` (mirrors `compute_status`, `refresh.py:66`):
   prefer the succeeded run for counts, derive `duration_seconds` from started/finished (None if either
   missing); all counts default None (never 0) when absent (MV-D43). Add
   `refresh.get_scan_stats()` reusing `mirror.latest_run`/`latest_succeeded_run` (`mirror.py:94,118`) —
   no new query.
3. Route `GET /api/ontology/scan-stats` in `routers/refresh.py` returning `model_dump(mode="json")`;
   frontend `getScanStats()` in `api.ts`. NO POST, no write path touched.

P2 — "How it works" explainer tab (frontend-only).
1. Add `{ id:"scan", label:"How it works" }` to `ONTOLOGY_TABS` (`tabs.ts`) between Map and Settings;
   wire the panel + icon in `OntologyPage` (component-only file).
2. Pure `scanNarrative.ts`: `buildStages()` → the 8 ordered stages (Scope · Read · Signals · Rank ·
   Cluster · **Align & enrich (optional)** · Pages · Publish), each `{icon, title, purpose(plain), io,
   optional?}`; the Align-&-enrich stage names both overlays (industry data models / reference models,
   MV-D58; external + web-search context, MV-D57/Stage C), is flagged `optional`, and carries an
   "informational — not certified" note for web content. `buildScanSummary(inventory, scanStats)` → the
   honest one-liner ("Your last scan read N catalogs and produced D domains · P Pages in Ms", degrading
   to a neutral "not scanned yet" when cold). Side-effect-free (react-refresh).
3. `ScanExplainerPanel` (prop-driven): the stage infographic (horizontal cards, dual-theme), a signal
   legend reusing the Map edge-kind vocabulary, the last-run summary from `scanStats`, and ONE CTA
   ("Scan the estate", reusing `runScan`). Optional stages render visibly OPTIONAL (off unless enabled).
   Cold/degraded ⇒ stages still render, numbers read "—".

GUARDRAILS (every phase): additive; no wheel/job/DDL/dep change; `OntologyRefreshStatus` + all frozen
models UNCHANGED (new model is append-only); pure resolver/narrative in side-effect-free modules; MV-D23
primary read; MV-D43 degrade; dual-theme; the scan write path + gates UNCHANGED (only referenced).

TESTS: P1 (pytest) — `compute_scan_stats` prefers succeeded, derives duration, all-None when the ledger
is empty/partial, and never emits 0 for a missing count; route returns the shape. P2 (vitest) —
`buildStages` order/count (8) incl. the `optional` Align-&-enrich stage carrying the not-certified note;
`buildScanSummary` hydrates from a fixture and degrades when cold; `ScanExplainerPanel` renders all
stages + one CTA + calls `runScan` and marks optional stages OPTIONAL; cold variant shows "—".

ACCEPTANCE (per phase, offline): P1 `./scripts/test.sh` green (report count; floor bump if backend
tests added — update BOTH parity-mirrored copies); P2 `npx tsc -b`·`npm run lint`·`npm run test` green;
`git status -- uv.lock frontend/package-lock.json` clean. Then STOP for deploy-verify.

---

## Deploy-verify gate (human, after each phase's offline-green — the STOP checkpoint)
`./scripts/deploy.sh --update` (reads `GENIE_DEPLOY_PROFILE` from `.env.deploy`, today `fevm-serverless`
→ 6t92c3). **No materialize run needed** — this reads the existing `genie_ont_runs`; it changes no data.
- **P1:** the app's `GET /api/ontology/scan-stats` returns the last succeeded run's `domain_count` /
  `tag_count` / `ungrouped_count` + a derived `duration_seconds` (confirm against the run ledger / app
  logs; on a cold metastore it returns all-None, not zeros). Backend-only — deploy with
  `SKIP_FRONTEND_BUILD=1` is fine.
- **P2:** frontend build ON. Open Ontology → **How it works**: the 8-stage pipeline reads in plain
  language; the **Align & enrich** stage names industry data models (MV-D58) + external/web context
  (MV-D57/Stage C), is marked OPTIONAL (off unless enabled), and carries the "informational — not
  certified" label for web content; the signal legend matches the Map; the last-run summary shows YOUR
  numbers (domains/Pages/duration) and degrades to "—" when cold; the "Scan the estate" CTA launches the
  same job. Five-second test: can a new admin state what scanning does and what it produces?
Then mark the phase BUILT and (after P2) register **MV-D108** in `mv-advisor-playbook.md` + flip the
backlog, in the same commit as the code (MV-D9 / doc-freeze).

## Non-goals / out of scope
No change to the materialize job, its phases, the signal producers, or the `genie_ont_*` schema (P1 only
READS the ledger the job already writes). No new API beyond the single `scan-stats` GET. No engine/
proposal changes. No new npm/py dep. Live per-run progress streaming (a running job's stage-by-stage
telemetry) is a future consideration, not this driver — this explains the pipeline + the last completed
run, not a live run feed.
